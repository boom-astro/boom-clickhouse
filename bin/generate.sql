-- Synthetic Argus-like photometry: {N_OBJECTS} objects, {N_POINTS} epochs each,
-- 60 s cadence in 8 h nights (480 epochs per night).
--
-- The series has to be physically shaped or the compression number is a lie:
-- a real field star is nearly constant, so Gorilla sees small residuals and
-- does well, while uniform noise would not compress and would understate the
-- gain. Each object gets a stable baseline, a slow variation, and photon noise.
INSERT INTO photometry (object_id, band, jd, flux, flux_err, flags)
SELECT
    intDiv(number, {N_POINTS:UInt64}) + 1 AS object_id,
    'w' AS band,
    2461000.5
        + intDiv(number % {N_POINTS:UInt64}, 480)
        + ((number % {N_POINTS:UInt64}) % 480) * (60.0 / 86400.0) AS jd,
    toFloat32(baseline * (1.0 + 0.02 * sin((number % {N_POINTS:UInt64}) / 50.0))
              + randNormal(0, 1) * noise)   AS flux,
    toFloat32(noise)                        AS flux_err,
    0 AS flags
FROM
(
    SELECT
        number,
        50.0 + (cityHash64(intDiv(number, {N_POINTS:UInt64})) % 10000) / 10.0 AS baseline,
        0.02 * baseline + sqrt(baseline) AS noise
    FROM numbers_mt({N_OBJECTS:UInt64} * {N_POINTS:UInt64})
);
