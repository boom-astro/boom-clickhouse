# boom-clickhouse

[![ClickHouse integration demo](https://github.com/boom-astro/boom-clickhouse/actions/workflows/integration.yml/badge.svg)](https://github.com/boom-astro/boom-clickhouse/actions/workflows/integration.yml)

A prototype of the photometry tier for BOOM: raw time series in ClickHouse,
everything else left in MongoDB. Built to answer three questions with measured
numbers rather than estimates, with correctness checks for late arrivals, retries, and weighted bins.
This remains a storage experiment, not a production ingestion design.

A runnable [BOOM ZTF integration example](examples/boom/README.md) now includes
a Rust HTTP adapter, real BOOM type integration, full record readback, and
disposable-server tests. It uses a separate table and computes bins from
deduplicated rows so overlapping alert histories stay correct.

## Why photometry has to leave MongoDB for Argus

Measured on the Caltech production BOOM, from a 300 document sample of
`ZTF_alerts_aux`:

| | |
| --- | --- |
| Documents | 236,793,611 |
| Storage | 1,693 GB, over 9.2 TB logical, so 5.4x compression |
| Photometry share of a document | **91.8%** |
| Bytes per photometry point | **241** |
| Points per object | 147.7 |

MongoDB's BSON document limit is 16 MB, which at 241 bytes per point is 69,500
points. Argus runs 1200 telescopes on a 60 second base cadence with a 1 second
fast mode, so an eight hour night is 480 points per object:

- 60 second cadence reaches the document limit in **145 nights**
- 1 second fast mode reaches it in **2.4 nights**

The document model does not get slow, it stops working. Everything below
assumes the raw series has to live somewhere else.

## What this prototype contains

```
schema/01_photometry.sql   raw points, one row per measurement
schema/02_binned.sql       nightly bins, maintained incrementally
bin/generate.sql           synthetic Argus-shaped photometry
bin/bench.sh               six reads, three runs with engine timing
bin/bench.sql              shared queries, with deduplicated raw reads
bin/check.py               isolated integration checks
```

Run it with the standalone binary, no server needed:

```sh
./clickhouse local --path ./chdata --queries-file schema/01_photometry.sql
./clickhouse local --path ./chdata --queries-file schema/02_binned.sql
./clickhouse local --path ./chdata --param_N_OBJECTS=1000 --param_N_POINTS=100000 \
    --queries-file bin/generate.sql
CH=./clickhouse bash bin/bench.sh
CH=./clickhouse python3 bin/check.py
```

Use a fresh data directory for these schema files. `CREATE IF NOT EXISTS` does
not migrate existing tables or views; rerunning it against `chdata` will leave
old definitions in place. The checks use a temporary directory and remove it
when finished. Tested with ClickHouse local 26.11.1.120; retry settings and
materialized-view behavior must be checked on the deployment version.

Raw reads use `FINAL` because background replacement is asynchronous. Bins are
UTC calendar days, not observatory-local observing nights. Invalid fluxes,
nonpositive/nonfinite errors, and epochs outside the UInt32 DateTime range are
rejected at insertion; negative finite flux is valid. Flags are stored but do
not currently exclude points from bins. Errors are treated as independent.

The generator writes physically shaped series rather than random numbers: each
object gets a stable baseline, a slow variation and photon noise. Uniform noise
would not compress and would understate the result; constant values would
compress to nothing and overstate it.

## Storage, original measurements at 100 million points

These are the initial agent's recorded measurements, retained as historical
observations. No raw benchmark logs or production sampling script accompany
them. The generator and query corrections below require a fresh measurement
before these figures can be used for capacity or latency planning.

1,000 objects of 100,000 epochs each, which is 208 nights per object and past
the point where a single object no longer fits in a BSON document.

| | bytes per point | versus Mongo |
| --- | --- | --- |
| MongoDB, reported per-point document bytes | 241 | 1x |
| ClickHouse, this schema | **3.43** | **70x smaller** |

Per column, after merges:

| column | bytes per point | compression |
| --- | --- | --- |
| flux | 3.376 | 1.2x |
| jd | 0.022 | 365x |
| ts | 0.013 | 315x |
| object_id | 0.009 | 862x |
| flux_err | 0.005 | 758x |
| band | 0.004 | 224x |
| flags | 0.000 | 23x |

The Mongo per-point estimate and ClickHouse compressed column bytes are not a
validated comparison of physical storage: the document sample, compression,
indexes, marks, and nightly aggregate storage must all be accounted for on both
sides. The reported 70x ratio should not be used as a disk-sizing ratio.

In this synthetic dataset, `jd` nearly vanishes because `ORDER BY (object_id, band, jd)`
makes it monotonic within a run and `DoubleDelta` reduces a regular cadence to
almost no bits, so a regular cadence compresses well; irregular sampling needs a separate measurement. And `flux` is
98.4% of what remains at a compression ratio of 1.2, for this generated Float32 distribution. This does not establish a lower
bound for real survey data or other encodings.

The first version of this schema cost 6.45 bytes per point because `ts`, a
column derived from `jd` purely so that time functions and partitioning work,
carried no codec and cost 3.03 bytes per point on its own, nearly as much as the
flux beside it. Giving it the same codec as `jd` cut it 233x.

## Read performance, original engine timings

The original window cutoff selected only the last fraction of a day, not seven
days, and raw reads omitted `FINAL`. Those numbers do not validate the corrected
queries. `bench.sh` now anchors windows to the dataset maximum JD, uses the
shared SQL with `FINAL`, and prints all three engine timings for each query.

Original timings exclude the roughly 120 ms that `clickhouse local` spends starting up,
which dominated a first attempt at measuring this.

| read | time |
| --- | --- |
| Full light curve, one object, 100,000 points, as enrichment needs | 38 ms |
| Last 7 days, one object, as a filter window needs | 11 ms |
| Nightly binned series, one object, 208 bins | 56 ms |
| Full curves for 1,000 objects, 100M points, batch enrichment | 301 ms |
| Nightly bins, every object | 6 ms |
| Brightest point in 7 days per object, survey wide | 27 ms |

These `FORMAT Null` queries discard results inside the engine. They measure
execution, not transport or enrichment latency for returning 100 million rows.
A service benchmark must also measure serialization, network, concurrency,
cold-cache behavior, and ingestion competing with reads.

## Binning

`photometry_nightly` stores partial aggregation states rather than finished
numbers, and `nightly` resolves them on read. The combination is inverse
variance weighted, so a bin is the maximum likelihood combination of its points:

```
flux = sum(f / e^2) / sum(1 / e^2)      err = 1 / sqrt(sum(1 / e^2))
```

Checked against the same quantity computed directly from the raw rows across
209,666 bins: worst disagreement 1.2e-11, which is floating point noise.

Storing states rather than numbers is what lets a late point land in a night
that has already been written, which forced photometry does routinely. Verified:
inserting one late point into a written night moved the bin from 480 points to
481 and its flux from 965.95 to 1168.24, with no recomputation.

## The correctness problem, and the fix

**A materialized view fires once per inserted block, before ReplacingMergeTree
deduplicates.** So a replayed Kafka range collapses correctly in the raw table
and is counted repeatedly in every bin derived from it.

Measured: inserting the same point four times left `photometry` holding one row,
correctly, while the bin went from 481 points to 484 and its flux from 1168.24
to 1669.29. Nothing errors. The raw series stays right and every binned value
derived from it reads high, which is the worst shape a bug like this can take.

A bounded retry mitigation is an explicit deduplication token on the insert,
carrying whatever identifies the ingest unit, with block deduplication turned on
for the unreplicated tables:

```sql
INSERT INTO photometry (object_id, band, jd, flux, flux_err, flags)
SETTINGS async_insert = 0,
         insert_deduplication_token = 'argus:part7:offset-99123',
         deduplicate_blocks_in_dependent_materialized_views = 1
VALUES (...);
```

With that, three identical inserts give one raw row and one contribution to the
bin. Both schema files now set `non_replicated_deduplication_window = 1000`,
which is off by default on unreplicated tables.

The writer has to supply a token that is stable across replays and distinct
between genuinely different batches. A Kafka topic, partition and offset range
can identify such a batch. Retries must preserve payload, row order, block
boundaries, and relevant insertion settings. Reusing a token for changed data
can silently discard it. The window is finite (1,000 block IDs here), not a
durable exactly-once ledger; replay after eviction or overlapping offset
ranges can still double-count bins. See the [ClickHouse retry documentation](https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries).

The raw key `(object_id, band, jd)` assumes one immutable measurement per epoch
and band. A different token with the same key, including a corrected measurement,
adds another bin contribution while `FINAL` keeps one raw row. Raw replacement
never retracts an old aggregate contribution. Supporting corrections, arbitrary
historical replay, or independent measurements at identical epochs requires a
measurement identity and a durable deduplication/rebuild strategy before this
can serve as a production write path. Failed writes involving materialized views
also require reconciliation; the two tables should not be assumed atomic.

## What this does not yet cover

- **Filters.** `create_ztf_alert_pipeline` joins `alerts` to `alerts_aux` and
  projects `prv_candidates` and `fp_hists` into the document a filter sees, and
  user filters are MongoDB aggregation pipelines written against those fields.
  MongoDB cannot join ClickHouse, so filters cannot see the raw series. The
  intended answer is that Mongo keeps the derived `properties` BOOM already
  computes plus a bounded recent window, and that `uses_field_in_filter`, which
  already drives the watchlist join, decides which filters need more.
- **The write path.** Nothing here reads Kafka; the generator stands in for it.
- **Multi band.** Everything generated is one band. The schema carries a band
  column and orders by it, but the compression figures are single band.
- **Deletes and retention.** Partitioning is monthly on `ts`, so dropping old
  data is a partition drop, but no policy is implemented.
- **Operating two stores.** Backups, monitoring and consistency between Mongo
  and ClickHouse are the real cost of this design and none of it is prototyped.
