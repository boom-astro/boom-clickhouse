# BOOM architecture replay benchmark

This experiment measures the native BOOM ingestion/shadow path and compares
Mongo-backed and ClickHouse-hydrated enrichment. Mongo retains complete histories
and remains authoritative. The result is `decision.md`, raw `benchmark.json`,
JUnit results, worker output, and ClickHouse logs.

## Run against disposable services

From `boom-clickhouse`, point at the BOOM `clickhouse` checkout:

```sh
export BOOM_SOURCE=/path/to/boom-branches/clickhouse/boom
cargo build --manifest-path "$BOOM_SOURCE/Cargo.toml" --example photometry_benchmark
docker compose -p boom-benchmark -f examples/boom/compose.proposal.yaml up -d --wait
CH="$HOME/clickhouse" python3 examples/boom/tests/benchmark.py \
    --objects 32 --epochs 4 --enrichment-samples 16 \
    --artifacts "$PWD/results/benchmark"
docker compose -p boom-benchmark -f examples/boom/compose.proposal.yaml down -v
```

The native delivery setting defaults to one concurrent delivery. This benchmark
uses four (`--concurrent-deliveries 4`), bounded by the configured per-poll row
limit, and records both settings. Compare explicit settings when evaluating
throughput; changing concurrency changes the workload on both databases.

The default is 128 labeled derivatives of BOOM's two real fixtures, spread across
32 objects, with overlapping histories and distinct late epochs. It establishes
repeatable behavior in CI; it cannot establish production capacity.

CPU ONNX models must be actual LFS files: run `git -C "$BOOM_SOURCE" lfs pull` if
needed. On Linux, install the CPU `onnxruntime==1.24.4` wheel and set
`ORT_DYLIB_PATH` to its `capi/libonnxruntime.so.*`, as in the integration workflow.
The controller links models into a temporary working directory and strips ambient
BOOM environment overrides. It does not load the checkout's `.env`.

## Use a real corpus and deployed filters

Create a manifest of existing packets. No Avro bytes, object IDs, candid values,
observation times, or histories are rewritten:

```sh
python3 examples/boom/tests/prepare_capture.py /path/to/ztf-corpus \
    /tmp/ztf-manifest.json --spacing-ms 10
CH="$HOME/clickhouse" python3 examples/boom/tests/benchmark.py \
    --manifest /tmp/ztf-manifest.json \
    --filters /path/to/ztf_active_filters.jsonl --deployed-filters \
    --enrichment-samples 32 --timeout-seconds 3600 \
    --artifacts "$PWD/results/ztf-corpus"
```

Use fresh dedicated Redis for each run. The replay refuses nonempty Redis,
non-loopback services, databases outside its isolated namespace, GPU mode, and
external publishing sinks. A failure preserves partial phase reports and logs;
restart with fresh services after investigating it.

An archive has no broker arrival timestamps. `--spacing-ms` supplies a synthetic
schedule in filename order, labeled explicitly in provenance. When real arrival
timestamps are available, pass `prepare_capture.py --schedule arrivals.json`,
where the file maps packet basenames to nonnegative arrival milliseconds. The
manifest records each packet's SHA-256 and arrival offset; the benchmark checks
every checksum before and during replay. It preserves duplicates and overlaps,
counts unique candids separately, and rejects out-of-order manifests.

Supply `--target-rate PACKETS_PER_SECOND` and `--max-lag-seconds SECONDS` to
compare measurements with an explicit operational target. These are reported
acceptance gates, not inferred production requirements. Change input pacing and
run multiple times to explore load; one successful paced run is not a saturation
test or a production latency guarantee.

`--filters` accepts a pipeline JSON file, a BOOM filter export, JSONL, or a
directory of JSON/JSONL files. It selects each active version and skips inactive
filters. `--deployed-filters` attests to the supplied source; it does not change
classification or approve a Mongo history window.

## What the phases measure

| Phase | Ingestion | Concurrent reads | Delivery |
| --- | --- | --- | --- |
| baseline | Actual BOOM Redis alert worker | Mongo aux history snapshots | Disabled |
| shadow | Same packets and worker | ClickHouse complete histories | Durable outbox |
| recovery | Same packets and worker | No history-read load during outage | HTTP 503, then healthy endpoint with the same worker |

Each phase has fresh Mongo and ClickHouse databases, production survey indexes,
and the same input schedule. The single Redis service must be dedicated to this
experiment. Baseline runs first, so cache/order effects remain a limitation.
Queue/backlog samples, schedule lateness, ingestion and convergence rates, and
read latency percentiles are recorded. Empty history reads are counted separately
and excluded from populated-history latency percentiles.

Healthy-phase ClickHouse traffic goes directly to ClickHouse; the fault proxy is
used only for the recovery phase, where its overhead is included in timings.
The recovery gate returns HTTP 503 without forwarding writes, then resumes
forwarding to the same ClickHouse database. The worker continues throughout;
no new packets are submitted during recovery. Recovery includes retry/backoff
and existing claims. This is a sink availability experiment. Worker-crash,
expired-lease, and duplicate-alert behavior are separate scenarios in the
existing `proposal.py` replay.

After timing ends, the benchmark checks **every object's existing sink history**
against its accepted Mongo snapshot. Validation is read-only and cannot repair a
missing or stale sink record. It also checks native delivery-status coverage.

An evenly spaced deterministic sample across sorted alert IDs is hydrated into
BOOM's actual enrichment input types and compared at each alert's observation
epoch. The comparison calls production property generation and CPU classification
with the same cutouts and external context for each pair. It compares exact
properties, SHA-256 hashes and shapes of model input tensors, ACAI/BTSbot scores,
eligible AppleCiDER outputs, and embeddings. Empty light curves and unavailable
model inputs are explicit coverage exclusions; a run without evaluated model
comparisons fails its coverage check. Eligible AppleCiDER failures fail the run.

Model comparisons happen outside timed ingestion. These rates therefore measure
**ingestion and shadow delivery**, not end-to-end BOOM classification throughput.
SSO, host and cross-survey context is limited to what the isolated replay contains;
an empty catalog is not evidence for production context compatibility. No Kafka
broker/rebalance, replica failover, GPU parity, or deployed filter execution is
claimed.

## Decision evidence

The report includes Mongo database storage and indexes, aux logical bytes,
ClickHouse compressed columns and physical parts, and complete record payloads.
It retains duplicate physical revisions and does not force compaction. Mongo
histories remain intact: this measures additional footprint rather than projected
retention savings. Allocation and compression figures are point-in-time samples.

The [active-filter inventory](evidence/ztf-active-filter-inventory.json) summarizes
an actual 96-filter ZTF export, identified by SHA-256. It finds 63 filters requiring
history review and 33 without a detected history reference. Dependencies overlap:
62 reference detections, 21 forced photometry, and 10 upper limits. Pipeline bodies,
filter names and identifiers stay out of this aggregate evidence. The detector
is an inventory, not a semantic proof or an inferred lookback window.

Before changing authority or Mongo retention, review representative captures
(including repeated objects, long histories, late/corrected points and program
mix), active filter semantics, measured rate/lag/recovery headroom, and production
failover. Resolve authorization, tombstones, schema evolution, operational ownership
and durable backfill through BOOM's task system. The report never automatically
approves a retention change.
