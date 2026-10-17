#!/usr/bin/env python3
"""Replay BOOM's real Avro fixtures through its Redis worker and Mongo outbox.

Requires dedicated, empty development Mongo/Redis services. Owns a temporary
ClickHouse server. Never uses BOOM's .env or a production connection string.
"""
import json
import os
from pathlib import Path
import subprocess
import statistics
import urllib.request
import xml.etree.ElementTree as ET

import integration

BOOM = Path(os.environ.get("BOOM_SOURCE", str(integration.ROOT.parents[2] / "boom"))).resolve()
BINARY = Path(os.environ.get("REPLAY_ADAPTER", str(BOOM / "target/debug/examples/photometry_shadow_replay"))).resolve()
ARTIFACTS = Path(os.environ.get("DEMO_ARTIFACTS", "/tmp/boom-shadow-proposal-report")).resolve()
integration.BINARY = BINARY
integration.ARTIFACTS = ARTIFACTS


def replay(directory, port):
    database = "boom_shadow_replay_" + directory.name.replace("-", "_")
    assert database.isidentifier()
    request = urllib.request.Request(f"http://127.0.0.1:{port}", data=f"CREATE DATABASE `{database}`".encode())
    with urllib.request.urlopen(request, timeout=10) as response:
        assert response.status == 200
    mongo = {"host": "127.0.0.1", "port": int(os.environ.get("REPLAY_MONGO_PORT", "37117")),
             "name": database, "username": "shadow", "password": "shadow-test-only",
             "max_pool_size": 20, "replica_set": None, "srv": False}
    config = {
        "database": mongo,
        "cutouts_storage": {"type": "mongo", **mongo},
        "api": {"domain": "localhost", "auth": {"secret_key": "shadow-test-only",
                "token_expiration": 3600, "admin_username": "shadow", "admin_password": "shadow-test-only",
                "admin_email": "shadow@example.invalid"}},
        "kafka": {"consumer": {}, "producer": {"server": "unused.invalid:9092"}},
        "redis": {"host": "127.0.0.1", "port": int(os.environ.get("REPLAY_REDIS_PORT", "37179"))},
        "crossmatch": {},
        "workers": {"ztf": {"command_interval": 100, "alert": {"n_workers": 1},
            "enrichment": {"n_workers": 1}, "filter": {"n_workers": 1, "refresh_interval_minutes": 15, "max_match_rate": 5}}},
        "photometry_shadow": {"url": f"http://127.0.0.1:{port}", "database": database,
            "username": "default", "password": "", "poll_seconds": 1, "drain_rows": 64},
    }
    path = directory / "config.json"
    path.write_text(json.dumps(config))
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    # Running from temporary storage also prevents AppConfig from loading a
    # checkout's .env. Strip ambient BOOM overrides before starting the worker.
    env = {k: v for k, v in os.environ.items() if not k.startswith("BOOM_")}
    env["OTEL_SDK_DISABLED"] = "true"
    try:
        result = subprocess.run([str(BINARY), str(path), str(BOOM / "tests/data/alerts/ztf"),
            os.environ.get("REPLAY_FILTER_DIR", str(BOOM / "data/filters")), str(ARTIFACTS / "proposal.json")],
            cwd=directory, env=env, stdin=subprocess.DEVNULL, text=True, capture_output=True, timeout=240)
    except subprocess.TimeoutExpired as error:
        for name, value in (("worker.stdout.log", error.stdout), ("worker.stderr.log", error.stderr)):
            if isinstance(value, bytes):
                value = value.decode(errors="replace")
            (ARTIFACTS / name).write_text(value or "")
        raise
    (ARTIFACTS / "worker.stdout.log").write_text(result.stdout)
    (ARTIFACTS / "worker.stderr.log").write_text(result.stderr)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    report = json.loads((ARTIFACTS / "proposal.json").read_text())
    assert report["status"] == "PASS"
    assert all(case["result"] == "PASS" for case in report["cases"])
    assert all(c["parity"]["histories_equal"] and c["parity"]["features_equal"] for c in report["comparisons"])
    report["provenance"] = {
        "boom_revision": subprocess.check_output(["git", "-C", str(BOOM), "rev-parse", "HEAD"], text=True).strip(),
        "boom_dirty": bool(subprocess.check_output(["git", "-C", str(BOOM), "status", "--porcelain"], text=True).strip()),
        "demo_revision": os.environ.get("GITHUB_SHA"),
        "clickhouse_version": subprocess.check_output([integration.CH, "local", "--version"], stdin=subprocess.DEVNULL, text=True).strip(),
    }
    (ARTIFACTS / "proposal.json").write_text(json.dumps(report, indent=2) + "\n")
    lines = ["## Real-alert BOOM shadow proposal: PASS", "",
        "Real Avro packets → Redis alert worker → authoritative MongoDB → durable outbox → ClickHouse → enrichment input.",
        "", "| Recovery/parity scenario | Result |", "| --- | --- |"]
    suite = ET.Element("testsuite", name="Real BOOM worker shadow replay", tests=str(len(report["cases"])), failures="0")
    for case in report["cases"]:
        lines.append(f"| {case['scenario']} | PASS |")
        ET.SubElement(suite, "testcase", name=case["scenario"])
        print(f"PASS: {case['scenario']}", flush=True)
    source = report["storage"]["mongo_aux"]
    outbox = report["storage"]["mongo_outbox"]
    parity = report["storage"]["mongo_parity"]
    sink = report["storage"]["clickhouse"]
    read_ms = statistics.median(row["parity"]["read_ms"] for row in report["comparisons"])
    write_ms = statistics.median(row["parity"]["write_ms"] for row in report["comparisons"])
    mongo_ms = statistics.median(row["parity"]["mongo_snapshot_ms"] for row in report["comparisons"])
    concurrent = report["replay"]["concurrent"]
    lines.extend(["", "### Fixture measurements", "", "| Measurement | Observed |", "| --- | --- |",
        f"| Real / derived packets | {report['replay']['real_packets']} / {report['replay']['derived_late_packets']} |",
        f"| Matching objects / unique photometry rows | {report['matching_objects']} / {sink['unique_rows']} |",
        f"| Median Mongo snapshot / CH write / CH readback | {mongo_ms:.2f} / {write_ms:.2f} / {read_ms:.2f} ms |",
        f"| Concurrent read samples / median / peak pending | {len(concurrent['read_ms'])} / {statistics.median(concurrent['read_ms']):.2f} ms / {concurrent['max_pending']} |",
        f"| Mongo aux allocated data / indexes | {source['storageSize']} / {source['totalIndexSize']} bytes |",
        f"| Mongo outbox allocated data / indexes | {outbox['storageSize']} / {outbox['totalIndexSize']} bytes |",
        f"| Mongo parity status allocated data / indexes | {parity['storageSize']} / {parity['totalIndexSize']} bytes |",
        f"| ClickHouse compressed columns / parts on disk | {sink['compressed_column_bytes']} / {sink['bytes_on_disk']} bytes |",
        f"| Pending outbox references | {report['lag']['pending']} (intentional uncommitted source) |",
        "", "Full timings, source revisions, storage accounting, and provenance are in `proposal.json`."])
    lines.extend(["", "### Filter dependency inventory", "",
        "| Filter | Classification | History fields |", "| --- | --- | --- |"])
    for row in report["filters"]:
        fields = ", ".join(field for field, _ in row["audit"]["history_fields"]) or "none detected"
        lines.append(f"| {row['filter']} | {row['audit']['classification']} | {fields} |")
    lines.extend(["", "Measurements are from a tiny development fixture replay, not a production capacity benchmark.",
        "", "The pending uncommitted-source reference is intentional: the drainer must not publish a phantom alert."])
    (ARTIFACTS / "demo.md").write_text("\n".join(lines) + "\n")
    ET.ElementTree(suite).write(ARTIFACTS / "junit.xml", encoding="utf-8", xml_declaration=True)
    print(result.stdout.strip())


if __name__ == "__main__":
    try:
        integration.main(replay)
    except Exception as error:
        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        (ARTIFACTS / "demo.md").write_text(f"## Real-alert BOOM shadow proposal: FAIL\n\n```text\n{error}\n```\n")
        suite = ET.Element("testsuite", name="Real BOOM worker shadow replay", tests="1", failures="1")
        test = ET.SubElement(suite, "testcase", name="replay failure")
        ET.SubElement(test, "failure", message=str(error)).text = str(error)
        ET.ElementTree(suite).write(ARTIFACTS / "junit.xml", encoding="utf-8", xml_declaration=True)
        raise
