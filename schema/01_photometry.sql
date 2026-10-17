-- Raw photometry, one row per measurement.
--
-- ORDER BY puts an object's whole light curve in one contiguous run, which is
-- the dominant read: enrichment fetches every point for one object. The same
-- ordering makes jd monotonic within a run, so DoubleDelta reduces a regular
-- cadence to near zero bits, and Gorilla exploits the slow variation of flux
-- between neighbouring epochs.
--
-- ReplacingMergeTree dedups on the sort key, so replaying a Kafka partition
-- re-inserts rows that collapse on merge. Use FINAL on raw reads for correctness
-- before merges. This does NOT repair incremental bins after an expired replay.
CREATE TABLE IF NOT EXISTS photometry
(
    object_id UInt64   CODEC(DoubleDelta, ZSTD(1)),
    band      Enum8('u' = 1, 'g' = 2, 'r' = 3, 'i' = 4, 'z' = 5, 'y' = 6, 'w' = 7),
    jd        Float64  CODEC(DoubleDelta, ZSTD(1)),
    flux      Float32  CODEC(Gorilla, ZSTD(1)),
    flux_err  Float32  CODEC(Gorilla, ZSTD(1)),
    flags     UInt8    CODEC(ZSTD(1)),
    -- Derived from jd purely so time functions and partitioning work. It is as
    -- monotonic as jd within a part, so it gets the same codec; left uncoded it
    -- costs about as much as the flux it accompanies.
    ts        DateTime('UTC') MATERIALIZED toDateTime((jd - 2440587.5) * 86400.0, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    CONSTRAINT valid_flux CHECK isFinite(flux),
    CONSTRAINT valid_error CHECK isFinite(flux_err) AND flux_err > 0,
    CONSTRAINT valid_jd CHECK isFinite(jd) AND jd >= 2440587.5 AND jd < 2440587.5 + 4294967296.0 / 86400.0
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (object_id, band, jd)
-- Block deduplication is off by default on unreplicated tables. It is what
-- makes a replayed Kafka range a no-op in the binned views as well as here;
-- see the note on insert_deduplication_token in README.md.
SETTINGS non_replicated_deduplication_window = 1000;
