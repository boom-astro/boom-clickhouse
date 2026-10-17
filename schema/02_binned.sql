-- Nightly bins, maintained incrementally as rows arrive.
--
-- Stored as partial aggregation states rather than finished numbers so that
-- bins stay correct when late points land in a night already written, which
-- forced photometry routinely does.
--
-- The weights are inverse variance: the combined flux is sum(f/e^2)/sum(1/e^2)
-- and its error is 1/sqrt(sum(1/e^2)), so a bin is the maximum-likelihood
-- combination of its points rather than a plain mean.
CREATE TABLE IF NOT EXISTS photometry_nightly
(
    object_id UInt64,
    band      Enum8('u' = 1, 'g' = 2, 'r' = 3, 'i' = 4, 'z' = 5, 'y' = 6, 'w' = 7),
    night     Date,
    n         AggregateFunction(count, UInt8),
    sum_w     AggregateFunction(sum, Float64),
    sum_wf    AggregateFunction(sum, Float64),
    jd_min    AggregateFunction(min, Float64),
    jd_max    AggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(night)
ORDER BY (object_id, band, night)
-- Without this the view keeps every replayed copy even though the raw table
-- collapses them, and a bin silently reads high. Measured: four inserts of one
-- point moved a 481-point bin to 484 and its flux from 1168 to 1669.
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS photometry_nightly_mv TO photometry_nightly AS
SELECT
    object_id,
    band,
    toDate(ts, 'UTC') AS night,
    countState(flags)                              AS n,
    sumState(toFloat64(1.0 / (flux_err * flux_err)))        AS sum_w,
    sumState(toFloat64(flux / (flux_err * flux_err)))       AS sum_wf,
    minState(jd)                                   AS jd_min,
    maxState(jd)                                   AS jd_max
FROM photometry
GROUP BY object_id, band, night;

-- Reading a binned light curve: states resolved back to physical quantities.
CREATE VIEW IF NOT EXISTS nightly AS
SELECT
    object_id,
    band,
    night,
    countMerge(n)                               AS n_points,
    sumMerge(sum_wf) / sumMerge(sum_w)          AS flux,
    1.0 / sqrt(sumMerge(sum_w))                 AS flux_err,
    minMerge(jd_min)                            AS jd_first,
    maxMerge(jd_max)                            AS jd_last
FROM photometry_nightly
GROUP BY object_id, band, night;
