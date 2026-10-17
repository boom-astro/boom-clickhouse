#!/usr/bin/env python3
"""Small integration checks in disposable storage; never opens ./chdata."""
import os
import math
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CH = os.environ.get("CH", str(Path.home() / "clickhouse"))


def check(db):
    def query(sql, *, succeeds=True):
        result = subprocess.run(
            [CH, "local", "--path", db, "--session_timezone=America/Los_Angeles",
             "--multiquery", "--query", sql],
            text=True, capture_output=True, stdin=subprocess.DEVNULL,
        )
        if succeeds and result.returncode:
            raise AssertionError(result.stderr)
        if not succeeds:
            assert result.returncode and "CONSTRAINT" in result.stderr, result.stderr
        return result.stdout.strip()

    query("\n".join((ROOT / "schema" / name).read_text() for name in
                    ("01_photometry.sql", "02_binned.sql")))
    prefix = "INSERT INTO photometry (object_id, band, jd, flux, flux_err, flags) "
    def insert(token, rows):
        query(prefix + "SETTINGS async_insert=0, "
              "deduplicate_blocks_in_dependent_materialized_views=1, "
              f"insert_deduplication_token='{token}' VALUES {rows}")

    rows = "(1,'w',2461000.5,10,2,0),(1,'w',2461000.6,20,1,0)"
    insert("batch-1", rows)
    insert("batch-1", rows)
    assert query("SELECT count() FROM photometry FINAL") == "2"
    n, flux, error = map(float, query("SELECT n_points, flux, flux_err FROM nightly").split())
    assert n == 2 and math.isclose(flux, 18) and math.isclose(error, 1 / math.sqrt(1.25))
    assert query("SELECT toString(night) FROM nightly") == query(
        "SELECT toString(toDate(toDateTime((2461000.5-2440587.5)*86400, 'UTC'), 'UTC'))")
    insert("late", "(1,'w',2461000.7,30,2,0)")
    insert("band", "(1,'g',2461000.6,90,1,0)")
    insert("month", "(1,'w',2461040.5,50,1,0)")
    # Compare incremental states to direct raw aggregation, including late data,
    # band isolation and separate monthly partitions.
    assert query("""
        SELECT count() FROM nightly AS b INNER JOIN
        (SELECT object_id, band, toDate(ts, 'UTC') AS night, count() AS n,
                sum(flux / (flux_err * flux_err)) / sum(1.0 / (flux_err * flux_err)) AS f,
                1 / sqrt(sum(1.0 / (flux_err * flux_err))) AS e
         FROM photometry FINAL GROUP BY object_id, band, night) AS r
        USING (object_id, band, night)
        WHERE b.n_points != r.n OR abs(b.flux-r.f) > 1e-10 OR abs(b.flux_err-r.e) > 1e-10
    """) == "0"
    assert query("SELECT count() FROM nightly") == "3"
    for error in ("0", "-1", "nan", "inf"):
        query(prefix + f"VALUES (9,'w',2461000.5,10,{error},0)", succeeds=False)
    assert query("SELECT count() FROM photometry FINAL WHERE object_id=9") == "0"
    assert query("SELECT count() FROM nightly WHERE object_id=9") == "0"
    for row in ("(9,'w',2461000.5,nan,1,0)", "(9,'w',0,10,1,0)"):
        query(prefix + "VALUES " + row, succeeds=False)

    # Replacing rows cannot retract an incremental aggregate. Keep this known
    # limitation visible even though FINAL produces a deduplicated raw series.
    insert("overlap", rows)
    assert query("SELECT count() FROM photometry FINAL WHERE object_id=1 AND band='w'") == "4"
    assert query("SELECT sum(n_points) FROM nightly WHERE object_id=1 AND band='w'") == "6"

    # A non-multiple of 480 catches epoch offsets leaking between objects.
    generator = (ROOT / "bin/generate.sql").read_text()
    generator = generator.replace("{N_OBJECTS:UInt64}", "3").replace("{N_POINTS:UInt64}", "3841")
    query("TRUNCATE TABLE photometry; TRUNCATE TABLE photometry_nightly;" + generator)
    assert query("SELECT count() FROM photometry FINAL") == "11523"
    assert query("SELECT uniqExact(start), uniqExact(finish) FROM "
                 "(SELECT min(jd) AS start, max(jd) AS finish FROM photometry FINAL GROUP BY object_id)") == "1\t1"
    end_jd = query("SELECT max(jd) FROM photometry FINAL")
    assert query(f"SELECT count() FROM photometry FINAL WHERE object_id=1 AND jd > {end_jd}-7") == "3360"
    benchmark = (ROOT / "bin/bench.sql").read_text().replace("{END_JD:Float64}", end_jd)
    query(benchmark)
    subprocess.run(["bash", str(ROOT / "bin/bench.sh")], check=True,
                   env={**os.environ, "CH": CH, "DB": db}, capture_output=True, text=True)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="boom-clickhouse-check-") as db:
        check(db)
    print("PASS: replay, late data, weighted bins, bands, partitions, invalid errors, generator and benchmarks")
