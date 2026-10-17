#!/usr/bin/env python3
"""Compare BOOM ingestion with/without its shadow, then measure outage recovery.

Captured input is an explicit timestamped, SHA-256-verified manifest. The default
is a larger fixture-derived CI workload and cannot establish production capacity.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import urllib.request
import xml.etree.ElementTree as ET

import integration
import proposal
from replay_proxy import fault_gate


def percentile(values, percentile):
    values = sorted(values)
    if not values:
        return None
    return values[max(0, math.ceil(len(values) * percentile / 100) - 1)]


def summary(report):
    """Evidence gates describe missing evidence separately from test success."""
    phases = report["phases"]
    healthy = phases["shadow"]
    samples = healthy["samples"]
    peak_age = max((r["lag"]["oldest_age_seconds"] or 0 for r in samples), default=0)
    target = report["targets"]["packets_per_second"]
    lag_target = report["targets"]["max_lag_seconds"]
    evaluated = [r["parity"]["source"] for r in healthy["enrichment"]
                 if r["parity"]["source"]["status"] == "evaluated"]
    gates = {
        "captured_workload": report["workload"]["kind"] == "captured",
        "recorded_arrivals": report["workload"].get("provenance", {}).get("timing_basis") == "supplied_arrival_schedule",
        "repeated_object_coverage": report["workload"].get("unique_alerts", 0) > report["workload"]["objects"],
        "deployed_filter_inventory": bool(report["filters"] and report["filters_are_deployed_export"]),
        "history_parity": all(len(p["histories"]) == report["workload"]["objects"] for k, p in phases.items() if k != "baseline"),
        "enrichment_parity": all(any(r["parity"]["source"]["status"] == "evaluated" for r in p["enrichment"]) and
                                 all(r["parity"]["equal"] for r in p["enrichment"])
                                 for k, p in phases.items() if k != "baseline"),
        "throughput_target": None if target is None else healthy["settled_packets_per_second"] >= target,
        "sampled_lag_target": None if lag_target is None else peak_age <= lag_target,
        "recovery_headroom": None if target is None else phases["recovery"]["recovery"]["references_per_second"] > target,
    }
    # Even all measured gates passing does not establish production failover,
    # retention, authorization, or maximum capacity.
    return {"gates": gates, "verdict": "more_evidence_required",
            "recommended_next_step": "review captured replay evidence and operational gates" if all(v is True for v in gates.values()) else "collect the missing workload/filter/target evidence before a retention decision",
            "model_coverage": {"acai_and_btsbot_alerts": len(evaluated),
                               "applecider_alerts": sum(r["applecider_eligible"] for r in evaluated)},
            "sampled_peak_age_seconds": peak_age,
            "ingestion_elapsed_overhead_percent": 100 * (healthy["ingest_seconds"] / phases["baseline"]["ingest_seconds"] - 1)}


def write_report(report, artifacts):
    decision = summary(report)
    report["decision"] = decision
    (artifacts / "benchmark.json").write_text(json.dumps(report, indent=2) + "\n")
    lines = ["# BOOM photometry architecture experiment", "",
             f"Workload: **{report['workload']['kind']}**, {report['workload']['packets']} packets, "
             f"{report['workload']['unique_alerts']} distinct alerts, {report['workload']['objects']} objects.", "",
             f"Delivery settings: {report.get('delivery', {}).get('concurrent_deliveries', 'unrecorded')} concurrent deliveries, "
             f"{report.get('delivery', {}).get('drain_rows', 'unrecorded')} references per poll.", "",
             "## Measured behavior", "",
             "| Phase | Ingestion packets/s | Converged packets/s | Ingest elapsed | Converged elapsed |",
             "| --- | ---: | ---: | ---: | ---: |"]
    for name, phase in report["phases"].items():
        lines.append(f"| {name} | {phase['ingest_packets_per_second']:.2f} | {phase['settled_packets_per_second']:.2f} | "
                     f"{phase['ingest_seconds']:.2f} s | {phase['settled_seconds']:.2f} s |")
    shadow = report["phases"]["shadow"]
    recovery = report["phases"]["recovery"]["recovery"]
    lines += ["", f"Ingestion elapsed overhead: {decision['ingestion_elapsed_overhead_percent']:.1f}% for this offered workload.",
              f"Recovery: {recovery['pending_at_restore']} durable references in {recovery['seconds']:.2f} s "
              f"({recovery['references_per_second']:.2f} references/s) after restoring delivery, with no new input.",
              f"Peak sampled pending age: {decision['sampled_peak_age_seconds']:.2f} s.", "",
              "| Measured latency | p50 | p95 | p99 |", "| --- | ---: | ---: | ---: |"]
    for name, values in [("Mongo populated-history reads", report["phases"]["baseline"]["concurrent_read_ms"]),
                         ("CH populated-history reads", shadow["concurrent_read_ms"]),
                         ("Producer schedule lateness", shadow["producer"]["dispatch_lateness_ms"])]:
        fields = [percentile(values, p) for p in [50, 95, 99]]
        lines.append(f"| {name} | " + " | ".join("unobserved" if x is None else f"{x:.2f} ms" for x in fields) + " |")
    lines += ["", "Ingestion includes Redis queueing and acknowledgement; converged time includes outbox drain. "
              "All history, model, and final storage comparisons run after the timed phases. "
              "The baseline runs first; cache/order effects and the single worker limit interpretation. "
              "A paced input measures the offered workload, not maximum capacity. Sampled peaks are lower bounds. "
              f"Empty history reads excluded from percentiles: Mongo {report['phases']['baseline'].get('empty_history_reads', 'unrecorded')}, "
              f"CH {shadow.get('empty_history_reads', 'unrecorded')}.", "",
              "## Storage accounting", "",
              "| Phase | Mongo database allocated data | Mongo indexes | Aux logical bytes | CH compressed columns | CH parts on disk |",
              "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for name, phase in report["phases"].items():
        store = phase["storage"]
        mongo, aux, ch = store["mongo_database"], store["mongo_aux"], store["clickhouse"]
        lines.append(f"| {name} | {mongo['storageSize']} | {mongo['indexSize']} | {aux['size']} | "
                     f"{ch['compressed_column_bytes'] if ch else 'n/a'} | {ch['bytes_on_disk'] if ch else 'n/a'} |")
    coverage = decision["model_coverage"]
    lines += ["", "Mongo totals include alerts, cutouts, aux, indexes, outbox and parity status where present. "
              "ClickHouse totals include repeated physical versions and complete payloads. Mongo retains all histories: "
              "these figures measure the experiment's additional footprint, not projected retention savings.", "",
              "## Compatibility", "",
              f"Full-history parity covers all {report['workload']['objects']} objects in each shadow phase. "
              f"The healthy phase evaluated {coverage['acai_and_btsbot_alerts']} sampled alerts through ACAI/BTSbot "
              f"and {coverage['applecider_alerts']} eligible alerts through AppleCiDER. "
              "Exact comparisons include typed inputs, tensor hashes, properties, scores and embeddings. "
              "Coverage exclusions and skipped empty light curves remain explicit in benchmark.json.", "",
              "| Filter | Classification | Full-history dependencies |", "| --- | --- | --- |"]
    for row in report["filters"]:
        fields = ", ".join(field for field, _ in row["audit"]["history_fields"]) or "none detected"
        name = row.get("name") or row["file"]
        lines.append(f"| {name} | {row['audit']['classification']} | {fields} |")
    lines += ["", "This is a dependency inventory; it does not execute or approve deployed filters. "
              "Cross-survey catalogs, host associations and SSO context are whatever the isolated replay contains. "
              "Empty catalogs do not establish production context coverage.", "", "## Decision gates", "",
              "| Evidence | Result |", "| --- | --- |"]
    for key, value in decision["gates"].items():
        lines.append(f"| {key} | {'not specified' if value is None else 'observed' if value else 'missing or unmet'} |")
    lines += ["", "**Decision: retain Mongo authority and complete histories.** "
              "ClickHouse is an experimental full-history mirror. Choose a Mongo window only after reviewing "
              "representative captures, active filter semantics, rate/lag targets and production failover. "
              "Retention, tombstones, schema evolution, durable backfill tasks, program authorization and ownership "
              "remain rollout requirements.", "", "Source revisions, packet/template SHA-256 values, schedules, raw samples "
              "and model hashes are in benchmark.json."]
    (artifacts / "decision.md").write_text("\n".join(lines) + "\n")
    suite = ET.Element("testsuite", name="BOOM replay benchmark", tests="3", failures="0")
    for name in report["phases"]:
        ET.SubElement(suite, "testcase", name=name)
    ET.ElementTree(suite).write(artifacts / "benchmark-junit.xml", encoding="utf-8", xml_declaration=True)
    return decision


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, help="Captured corpus JSON: kind=captured, packets=[{path,at_ms,sha256}]")
    parser.add_argument("--objects", type=int, default=32)
    parser.add_argument("--epochs", type=int, default=4)
    parser.add_argument("--interval-ms", type=int, default=10)
    parser.add_argument("--enrichment-samples", type=int, default=16)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--concurrent-deliveries", type=int, default=4)
    parser.add_argument("--filters", type=Path, default=proposal.BOOM / "data/filters")
    parser.add_argument("--deployed-filters", action="store_true", help="Attest the supplied filter directory is a deployed active export")
    parser.add_argument("--target-rate", type=float)
    parser.add_argument("--max-lag-seconds", type=float)
    parser.add_argument("--artifacts", type=Path, default=Path(os.environ.get("DEMO_ARTIFACTS", "/tmp/boom-replay-benchmark")))
    args = parser.parse_args()
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    binary = Path(os.environ.get("BENCHMARK_ADAPTER", str(proposal.BOOM / "target/debug/examples/photometry_benchmark"))).resolve()
    integration.BINARY = binary
    integration.ARTIFACTS = artifacts
    filters = args.filters.resolve()
    if args.manifest:
        manifest = args.manifest.resolve()
        corpus = json.loads(manifest.read_text())
        if corpus.get("kind") != "captured":
            parser.error("--manifest must identify kind=captured")
        for packet in corpus["packets"]:
            packet["path"] = str((manifest.parent / packet["path"]).resolve())
    else:
        corpus = {"kind": "fixture_derived", "templates": [str(p) for p in sorted((proposal.BOOM / "tests/data/alerts/ztf").glob("*.avro"))],
                  "objects": args.objects, "epochs": args.epochs, "interval_ms": args.interval_ms}
    plan = {"corpus": corpus, "timeout_seconds": args.timeout_seconds, "enrichment_samples": args.enrichment_samples,
            "filters_are_deployed_export": args.deployed_filters,
            "target_packets_per_second": args.target_rate, "max_lag_seconds": args.max_lag_seconds}

    def replay(directory, port):
        with fault_gate(port) as proxy_port:
            run_replay(directory, port, proxy_port)

    def run_replay(directory, port, proxy_port):
        database = "boom_shadow_replay_" + directory.name.replace("-", "_")
        for phase in ["baseline", "shadow", "recovery"]:
            request = urllib.request.Request(f"http://127.0.0.1:{port}", data=f"CREATE DATABASE `{database}_{phase}`".encode())
            with urllib.request.urlopen(request, timeout=10) as response:
                assert response.status == 200
        config = directory / "config.json"
        raw_config = proposal.disposable_config(port, database)
        raw_config["benchmark_fault_proxy_url"] = f"http://127.0.0.1:{proxy_port}"
        raw_config["photometry_shadow"]["concurrent_deliveries"] = args.concurrent_deliveries
        config.write_text(json.dumps(raw_config))
        plan_path = directory / "plan.json"
        plan_path.write_text(json.dumps(plan))
        # SharedModels uses relative model paths. Symlink only the immutable
        # model directory; keep cwd outside the checkout to avoid loading .env.
        worker_directory = directory / "worker"
        (worker_directory / "data").mkdir(parents=True)
        (worker_directory / "data/models").symlink_to(proposal.BOOM / "data/models", target_is_directory=True)
        env = {k: v for k, v in os.environ.items() if not k.startswith("BOOM_")}
        env["OTEL_SDK_DISABLED"] = "true"
        source_revision = subprocess.check_output(["git", "-C", str(proposal.BOOM), "rev-parse", "HEAD"], text=True).strip()
        source_dirty = bool(subprocess.check_output(["git", "-C", str(proposal.BOOM), "status", "--porcelain"], text=True).strip())
        binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
        try:
            result = subprocess.run([str(binary), str(config), str(plan_path), str(filters), str(artifacts / "benchmark.json")],
                                    cwd=worker_directory, env=env, stdin=subprocess.DEVNULL, text=True, capture_output=True,
                                    timeout=args.timeout_seconds * 6 + 180)
        except subprocess.TimeoutExpired as error:
            for name, value in [("benchmark.stdout.log", error.stdout), ("benchmark.stderr.log", error.stderr)]:
                (artifacts / name).write_text(value.decode(errors="replace") if isinstance(value, bytes) else value or "")
            raise
        (artifacts / "benchmark.stdout.log").write_text(result.stdout)
        (artifacts / "benchmark.stderr.log").write_text(result.stderr)
        if result.returncode:
            raise RuntimeError(result.stderr[-6000:] or result.stdout[-6000:])
        report = json.loads((artifacts / "benchmark.json").read_text())
        assert report["status"] == "PASS"
        models = []
        for path in sorted((proposal.BOOM / "data/models").glob("*.onnx")):
            models.append({"name": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        report["provenance"] = {"boom_revision": source_revision, "boom_dirty": source_dirty, "binary_sha256": binary_sha256,
                                "demo_revision": subprocess.check_output(["git", "-C", str(integration.ROOT.parents[1]), "rev-parse", "HEAD"], text=True).strip(),
                                "platform": platform.platform(), "cpu_count": os.cpu_count(), "models": models,
                                "clickhouse_version": subprocess.check_output([integration.CH, "local", "--version"], stdin=subprocess.DEVNULL, text=True).strip()}
        decision = write_report(report, artifacts)
        assert decision["gates"]["history_parity"] and decision["gates"]["enrichment_parity"], "missing parity coverage"
        print(result.stdout.strip())
        print(f"PASS: replay benchmark. Decision report: {artifacts / 'decision.md'}")

    try:
        integration.main(replay)
    except Exception as error:
        (artifacts / "decision.md").write_text(f"# BOOM replay benchmark: FAIL\n\n```text\n{error}\n```\n")
        suite = ET.Element("testsuite", name="BOOM replay benchmark", tests="1", failures="1")
        test = ET.SubElement(suite, "testcase", name="benchmark failure")
        ET.SubElement(test, "failure", message=str(error)).text = str(error)
        ET.ElementTree(suite).write(artifacts / "benchmark-junit.xml", encoding="utf-8", xml_declaration=True)
        raise


if __name__ == "__main__":
    main()
