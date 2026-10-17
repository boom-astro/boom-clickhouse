#!/usr/bin/env python3
"""Exercise the Rust HTTP client against a disposable loopback ClickHouse server."""
import json
import math
import os
from pathlib import Path
import socket
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
CH = os.environ.get("CH", str(Path.home() / "clickhouse"))
BINARY = Path(os.environ.get("ADAPTER", str(ROOT / "target/debug/boom-clickhouse-example")))
BOOM_BINARY = os.environ.get("BOOM_ADAPTER")
ARTIFACTS = Path(os.environ["DEMO_ARTIFACTS"]).resolve() if os.environ.get("DEMO_ARTIFACTS") else None
CASES = []


def passed(name, detail):
    CASES.append({"name": name, "detail": detail})
    print(f"PASS: {name}: {detail}", flush=True)


def snapshot(name, value):
    if ARTIFACTS:
        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        (ARTIFACTS / name).write_text(json.dumps(value, indent=2) + "\n")


def report(status, error=None):
    if not ARTIFACTS:
        return
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    adapter = "BOOM ZTF types" if BOOM_BINARY else "Standalone Rust adapter"
    lines = [f"## {adapter}: {status}", "",
             "Real HTTP requests to a disposable ClickHouse server; no MongoDB, Redis, or Kafka required.",
             "", "| Scenario | Result |", "| --- | --- |"]
    for case in CASES:
        lines.append(f"| {case['name']} | {case['detail']} |")
    if error:
        lines.extend(["", "### Failure", "", "```text", error, "```"])
    (ARTIFACTS / "demo.md").write_text("\n".join(lines) + "\n")
    snapshot("results.json", {"status": status, "adapter": adapter, "cases": CASES, "error": error})
    suite = ET.Element("testsuite", name=adapter, tests=str(len(CASES) + bool(error)), failures=str(int(bool(error))))
    for case in CASES:
        test = ET.SubElement(suite, "testcase", name=case["name"])
        ET.SubElement(test, "system-out").text = case["detail"]
    if error:
        test = ET.SubElement(suite, "testcase", name="integration failure")
        ET.SubElement(test, "failure", message=error).text = error
    ET.ElementTree(suite).write(ARTIFACTS / "junit.xml", encoding="utf-8", xml_declaration=True)


def check(directory, port):
    env = {**os.environ, "BOOM_CLICKHOUSE_URL": f"http://127.0.0.1:{port}",
           "BOOM_CLICKHOUSE_DATABASE": "default", "BOOM_CLICKHOUSE_USER": "default",
           "BOOM_CLICKHOUSE_PASSWORD": ""}

    def run(*args, succeeds=True, overrides=None):
        binary = BOOM_BINARY if BOOM_BINARY and args[0] in ("prepare", "write", "read") else str(BINARY)
        result = subprocess.run([binary, *args], env={**env, **(overrides or {})},
                                text=True, capture_output=True, timeout=45, stdin=subprocess.DEVNULL)
        if succeeds:
            assert result.returncode == 0, result.stderr
        else:
            assert result.returncode != 0, result.stdout
        return result.stdout

    fixture = json.loads((ROOT / "fixture.json").read_text())
    path = directory / "object.json"

    def write(snapshot, revision):
        path.write_text(json.dumps(snapshot))
        run("write", str(path), str(revision))

    def nightly():
        return [json.loads(line) for line in run("nightly", "ZTF_demo").splitlines()]

    def sql(query):
        request = urllib.request.Request(env["BOOM_CLICKHOUSE_URL"], data=query.encode())
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.read().decode().strip()

    run("init")
    sql("SYSTEM STOP MERGES boom_photometry")
    sql("ALTER TABLE boom_photometry MODIFY SETTING non_replicated_deduplication_window=1")
    path.write_text(json.dumps(fixture))
    expected = {"prv_candidates": [], "prv_nondetections": [], "fp_hists": []}
    kind_to_array = {"detection": "prv_candidates", "upper_limit": "prv_nondetections", "forced": "fp_hists"}
    for line in run("prepare", str(path), "1").splitlines():
        row = json.loads(line)
        expected[kind_to_array[row["kind"]]].append(json.loads(row["payload"]))
    write(fixture, 1)
    write(fixture, 1)
    history = json.loads(run("read", "ZTF_demo"))
    for name in ("prv_candidates", "prv_nondetections", "fp_hists"):
        assert history[name] == expected[name], (name, history)
    snapshot("history.json", history)
    passed("Exact retry and full record readback", "2 detections, 1 upper limit, 1 forced measurement")
    bins = nightly()
    assert len(bins) == 2, bins
    detection = next(b for b in bins if b["kind"] == "detection")
    assert int(detection["n_points"]) == 2 and math.isclose(detection["flux"], 18), bins
    assert math.isclose(detection["flux_err"], 1 / math.sqrt(1.25)), bins
    assert next(b for b in bins if b["kind"] == "forced")["flux"] == -5, bins
    snapshot("nightly-initial.json", bins)
    passed("Weighted detection bin", "n=2, flux=18, error=0.89442719")
    passed("Measurement kinds", "upper limit retained; negative forced flux=-5 in a separate bin")

    # Another alert includes all the earlier points plus a late point.
    initial = json.loads(json.dumps(fixture))
    late = {**fixture["prv_candidates"][0], "jd": 2461000.7, "psfFlux": 30}
    fixture["prv_candidates"].append(late)
    write(fixture, 1)
    detection = next(b for b in nightly() if b["kind"] == "detection")
    assert int(detection["n_points"]) == 3 and math.isclose(detection["flux"], 20), detection
    passed("Overlapping history and late arrival", "n=3, flux=20; old points counted once")
    write(initial, 1)  # Its token has been evicted; raw duplicates now exist.
    assert int(sql("SELECT count() FROM boom_photometry")) > int(sql("SELECT count() FROM boom_photometry FINAL"))
    detection = next(b for b in nightly() if b["kind"] == "detection")
    assert int(detection["n_points"]) == 3 and math.isclose(detection["flux"], 20), detection
    passed("Expired token with merges disabled", "physical duplicates exist; FINAL keeps n=3, flux=20")

    # Explicit source revisions protect a correction from an older replay.
    old = json.loads(json.dumps(fixture))
    fixture["prv_candidates"][0]["psfFlux"] = 50
    write(fixture, 2)
    write(old, 1)
    detection = next(b for b in nightly() if b["kind"] == "detection")
    assert int(detection["n_points"]) == 3 and math.isclose(detection["flux"], 40 / 1.5), detection
    passed("Correction then older replay", "revision 2 wins; n=3, flux=26.66666667")

    # Private-program measurements remain distinct and retain their metadata.
    private = {**late, "programid": 3, "psfFlux": 100}
    fixture["prv_candidates"].append(private)
    write(fixture, 2)
    bins = nightly()
    assert len(bins) == 3 and next(b for b in bins if b["programid"] == 3)["flux"] == 100, bins
    passed("Program isolation", "program 3 flux=100 stays in a separate bin")

    # Missing forced flux remains missing and never becomes a zero-valued bin.
    fixture["fp_hists"].append({**fixture["fp_hists"][0], "jd": 2461000.8, "psfFlux": None})
    write(fixture, 2)
    assert len(nightly()) == 3
    assert len(json.loads(run("read", "ZTF_demo"))["fp_hists"]) == 2
    snapshot("nightly-final.json", nightly())
    passed("Missing forced flux", "record preserved; no extra weighted contribution")

    # Object IDs are HTTP query parameters, never interpolated SQL.
    fixture["_id"] = "ZTF' OR 1=1 --"
    write(fixture, 2)
    assert len(json.loads(run("read", fixture["_id"]))["prv_candidates"]) == 4
    assert json.loads(run("read", "absent")) == {
        "prv_candidates": [], "prv_nondetections": [], "fp_hists": []}
    run("read", "ZTF_demo", succeeds=False, overrides={"BOOM_CLICKHOUSE_DATABASE": "absent"})
    passed("Parameterized object IDs and empty reads", "quoted ID isolated; absent object returns empty arrays")
    passed("HTTP failure propagation", "missing database returns a failing process status")


def main(check_fn=check):
    if not BINARY.is_file():
        raise SystemExit("Build first: cargo build --manifest-path examples/boom/Cargo.toml")
    with tempfile.TemporaryDirectory(prefix="boom-clickhouse-http-") as tmp:
        directory = Path(tmp)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        config = directory / "server.xml"
        config.write_text(f"""<clickhouse>
          <path>{escape(tmp)}/data/</path>
          <logger><level>warning</level><log>{escape(tmp)}/server.log</log><errorlog>{escape(tmp)}/error.log</errorlog></logger>
          <http_port>{port}</http_port><listen_host>127.0.0.1</listen_host>
          <users_config>{escape(str(config))}</users_config>
          <profiles><default><max_threads>4</max_threads></default></profiles>
          <users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota></default></users>
          <quotas><default/></quotas>
          <background_schedule_pool_size>8</background_schedule_pool_size>
          <background_buffer_flush_schedule_pool_size>4</background_buffer_flush_schedule_pool_size>
          <background_message_broker_schedule_pool_size>4</background_message_broker_schedule_pool_size>
          <background_distributed_schedule_pool_size>4</background_distributed_schedule_pool_size>
        </clickhouse>""")
        with (directory / "console.log").open("w") as log:
            process = subprocess.Popen([CH, "server", "--config-file", str(config)],
                                       cwd=tmp, stdout=log, stderr=log, stdin=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        raise RuntimeError((directory / "console.log").read_text())
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{port}/ping", timeout=1) as response:
                            if response.status == 200:
                                break
                    except (urllib.error.URLError, TimeoutError):
                        pass
                    if time.monotonic() > deadline:
                        raise RuntimeError((directory / "console.log").read_text())
                    time.sleep(0.1)
                check_fn(directory, port)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
                if ARTIFACTS:
                    ARTIFACTS.mkdir(parents=True, exist_ok=True)
                    for name in ("console.log", "server.log", "error.log"):
                        if (directory / name).is_file():
                            shutil.copy2(directory / name, ARTIFACTS / name)
    if check_fn is check:
        print("PASS: real HTTP writes/reads, replay, overlap, late points, revisions, upper limits, programs, null/negative flux, SQL parameters, server errors")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        report("FAIL", str(error))
        raise
    else:
        report("PASS")
