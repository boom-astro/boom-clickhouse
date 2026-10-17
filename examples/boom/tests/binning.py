#!/usr/bin/env python3
"""Verify fixed-width bins against an independent weighted-flux reference."""
import argparse
import collections
import json
import math
import os
from pathlib import Path
import urllib.parse
import urllib.request

import integration
import proposal
from storage_profile import query


def reference(history, cadence):
    if not 1 <= cadence <= 31536000:
        raise ValueError("cadence must be 1..31536000 seconds")
    bins = collections.defaultdict(lambda: {"n_records": 0, "weights": [], "weighted_flux": []})
    for field, kind in [("prv_candidates", "detection"), ("prv_nondetections", "upper_limit"), ("fp_hists", "forced")]:
        for point in history.get(field, []):
            seconds = int((point["jd"] - 2440587.5) * 86400)
            key = (seconds // cadence * cadence, point["band"], kind, point["programid"])
            item = bins[key]
            item["n_records"] += 1
            flux, error = point.get("psfFlux"), point.get("psfFluxErr")
            if kind != "upper_limit" and flux is not None and error is not None:
                if not math.isfinite(flux) or not math.isfinite(error) or error <= 0:
                    raise ValueError("invalid source measurement")
                weight = 1 / error**2
                item["weights"].append(weight)
                item["weighted_flux"].append(flux * weight)
    result = []
    for (bucket, band, kind, program), item in sorted(bins.items()):
        weight = math.fsum(item["weights"])
        result.append({"bucket_start_unix": bucket, "band": band, "kind": kind, "programid": program,
                       "n_records": item["n_records"], "n_measured": len(item["weights"]),
                       "flux": math.fsum(item["weighted_flux"]) / weight if weight else None,
                       "flux_err": 1 / math.sqrt(weight) if weight else None,
                       "revision": history["version"]})
    return result


def compare(expected, actual):
    def key(row):
        return row["bucket_start_unix"], row["band"], row["kind"], row["programid"]
    expected = {key(row): row for row in expected}
    actual = {key(row): row for row in actual}
    assert expected.keys() == actual.keys(), "bin identity mismatch"
    for identity, row in expected.items():
        for field, value in row.items():
            observed = actual[identity][field]
            if field in ["flux", "flux_err"] and value is not None:
                assert observed is not None and math.isfinite(observed) and math.isclose(value, observed, rel_tol=1e-10, abs_tol=1e-12), (identity, field, value, observed)
            else:
                assert value == observed, (identity, field, value, observed)


def bins(url, database, object_id, cadence):
    sql = (proposal.BOOM / "src/photometry/bins.sql").read_text()
    return query(url, database, sql, param_object_id=object_id, param_cadence_seconds=cadence)


def verify_histories(url, database, histories):
    result = []
    for history in histories:
        for cadence in [900, 3600, 86400]:
            actual = bins(url, database, history["_id"], cadence)
            expected = reference(history, cadence)
            compare(expected, actual)
            result.append({"object_id": history["_id"], "cadence_seconds": cadence, "bins": len(actual), "result": "PASS"})
    return result


def fixture(directory, port, artifacts):
    database = "boom_shadow_replay_bins_" + directory.name.replace("-", "_")
    url = f"http://127.0.0.1:{port}"
    query(url, "default", f"CREATE DATABASE `{database}`")
    query(url, database, (proposal.BOOM / "src/photometry/schema.sql").read_text())
    def point(jd, flux, error, band="g", program=1):
        return {"jd": jd, "band": band, "programid": program, "psfFlux": flux, "psfFluxErr": error}
    history = {"_id": "ZTF_bin_fixture", "version": 1,
               "prv_candidates": [point(2461000.51, 10, 2), point(2461000.52, 20, 1),
                                  point(2461000.51, 100, 1, program=3), point(2461000.51, 40, 4, band="r"),
                                  point(2461001.51, 50, 5)],
               "fp_hists": [point(2461000.51, -5, 2), point(2461000.52, None, 2)],
               "prv_nondetections": [point(2461000.53, None, 2)]}
    def insert(history, token):
        rows = []
        for field, kind in [("prv_candidates", "detection"), ("fp_hists", "forced"), ("prv_nondetections", "upper_limit")]:
            for value in history[field]:
                rows.append({"object_id": history["_id"], "kind": kind, "band": value["band"], "jd": value["jd"],
                             "programid": value["programid"], "revision": history["version"],
                             "flux": value["psfFlux"], "flux_err": value["psfFluxErr"],
                             "payload": json.dumps(value, sort_keys=True, separators=(",", ":"))})
        target = url + "/?" + urllib.parse.urlencode({"database": database, "query": "INSERT INTO boom_photometry FORMAT JSONEachRow",
                "insert_deduplication_token": token, "wait_end_of_query": 1})
        body = ("\n".join(json.dumps(row) for row in rows) + "\n").encode()
        with urllib.request.urlopen(urllib.request.Request(target, data=body), timeout=30) as response:
            assert response.status == 200
    insert(history, "first")
    insert(history, "first")
    cases = verify_histories(url, database, [history])
    original = json.loads(json.dumps(history))
    history["version"] = 2
    history["prv_candidates"][0]["psfFlux"] = 30
    insert(history, "correction")
    insert(original, "older-replay-bypass-token-dedup")
    cases += verify_histories(url, database, [history])
    daily = bins(url, database, history["_id"], 86400)
    measured = next(row for row in daily if row["kind"] == "detection" and row["band"] == "g" and row["programid"] == 1)
    assert measured["n_measured"] == 2 and math.isclose(measured["flux"], 22)
    forced = next(row for row in daily if row["kind"] == "forced")
    assert forced["flux"] == -5 and forced["n_records"] == 2 and forced["n_measured"] == 1
    upper = next(row for row in daily if row["kind"] == "upper_limit")
    assert upper["flux"] is None and upper["flux_err"] is None and upper["n_measured"] == 0
    assert bins(url, database, "absent", 86400) == []
    result = {"status": "PASS", "cases": cases, "checks": ["weighted flux and uncertainty", "duplicate and overlapping snapshots",
        "new correction survives old replay", "negative forced flux", "missing flux excluded from weights",
        "upper limits retained as counts", "program and band isolation", "UTC boundaries and multiple cadences", "empty reads"],
        "tolerance": "bin sums only: relative 1e-10, absolute 1e-12 for accumulation order; history/model parity stays exact"}
    (artifacts / "bins.json").write_text(json.dumps(result, indent=2) + "\n")
    (artifacts / "bins.md").write_text("## Corrected, deduplicated flux bins: PASS\n\n" + "\n".join(f"- {name}" for name in result["checks"]) + "\n")
    print("PASS: corrected, deduplicated flux bins across cadences, measurement kinds, programs and bands")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url")
    parser.add_argument("--database")
    parser.add_argument("--histories", type=Path)
    parser.add_argument("--artifacts", type=Path, default=Path(os.environ.get("DEMO_ARTIFACTS", "/tmp/boom-bin-report")))
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=True)
    if args.histories:
        if not args.url or not args.database:
            parser.error("--histories requires --url and --database")
        cases = verify_histories(args.url, args.database, json.loads(args.histories.read_text()))
        (args.artifacts / "real-corpus-bins.json").write_text(json.dumps({"status": "PASS", "cases": cases}, indent=2) + "\n")
        print(f"PASS: {len(cases)} real-corpus object/cadence comparisons")
    else:
        integration.ARTIFACTS = args.artifacts
        integration.main(lambda directory, port: fixture(directory, port, args.artifacts), require_binary=False)
