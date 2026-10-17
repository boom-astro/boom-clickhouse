use boom_clickhouse_example::{PreparedBatch, Row, ZtfHistory};
use serde_json::{json, Value};

fn point() -> Value {
    json!({"jd":2461000.5,"band":"g","programid":1,"psfFlux":-5,"psfFluxErr":2,
           "forcediffimflux":-50,"magzpsci":25,"procstatus":"0","extra":{"b":2,"a":1}})
}

#[test]
fn preserves_full_records_and_keeps_kinds_separate() {
    let record = point();
    let upper = json!({"jd":2461000.5,"band":"r","programid":1,"diffmaglim":21,"psfFluxErr":2});
    let batch = PreparedBatch::ztf(
        "ZTF_demo",
        1,
        std::slice::from_ref(&record),
        &[upper],
        std::slice::from_ref(&record),
    )
    .unwrap();
    let rows: Vec<Row> = batch
        .body
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].flux, Some(-5.0));
    let stored = rows
        .iter()
        .map(|row| json!({"kind":row.kind,"payload":row.payload}).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let history = ZtfHistory::from_json_each_row(&stored).unwrap();
    assert_eq!(history.prv_candidates, vec![record.clone()]);
    assert_eq!(history.fp_hists, vec![record]);
    assert_eq!(history.prv_nondetections.len(), 1);
}

#[test]
fn stable_batch_for_reordered_inputs_and_duplicate_points() {
    let a = point();
    let mut b = point();
    b["jd"] = json!(2461000.6);
    let batch = PreparedBatch::ztf(
        "ZTF_demo",
        1,
        &[a.clone(), b.clone(), a.clone()],
        &[] as &[Value],
        &[] as &[Value],
    )
    .unwrap();
    let reordered =
        PreparedBatch::ztf("ZTF_demo", 1, &[b, a], &[] as &[Value], &[] as &[Value]).unwrap();
    assert_eq!(batch.rows, 2);
    assert_eq!(batch.body, reordered.body);
    assert_eq!(batch.token, reordered.token);
}

#[test]
fn rejects_conflicts_and_invalid_measurements() {
    let valid = point();
    let mut conflict = point();
    conflict["psfFlux"] = json!(50);
    assert!(PreparedBatch::ztf(
        "ZTF_demo",
        1,
        &[valid, conflict],
        &[] as &[Value],
        &[] as &[Value]
    )
    .is_err());
    for (field, bad) in [
        ("jd", json!(0)),
        ("psfFluxErr", json!(0)),
        ("psfFluxErr", json!(-1)),
        ("psfFluxErr", Value::Null),
        ("psfFlux", json!("bad")),
        ("band", json!("j")),
    ] {
        let mut record = point();
        record[field] = bad;
        assert!(
            PreparedBatch::ztf("ZTF_demo", 1, &[record], &[] as &[Value], &[] as &[Value]).is_err(),
            "{field}"
        );
    }
}
