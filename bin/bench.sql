-- END_JD is the latest epoch in the dataset, supplied by bench.sh.
SELECT jd, flux, flux_err FROM photometry FINAL WHERE object_id = 424 ORDER BY jd FORMAT Null;
SELECT jd, flux, flux_err FROM photometry FINAL WHERE object_id = 424 AND jd > {END_JD:Float64} - 7 ORDER BY jd FORMAT Null;
SELECT night, n_points, flux, flux_err FROM nightly WHERE object_id = 424 ORDER BY night FORMAT Null;
SELECT object_id, jd, flux FROM photometry FINAL WHERE object_id BETWEEN 1 AND 1000 FORMAT Null;
SELECT object_id, night, flux, flux_err FROM nightly FORMAT Null;
SELECT object_id, max(flux) FROM photometry FINAL WHERE jd > {END_JD:Float64} - 7 GROUP BY object_id FORMAT Null;
