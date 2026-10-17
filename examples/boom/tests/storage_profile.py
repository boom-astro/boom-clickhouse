#!/usr/bin/env python3
"""Read-only column and server-counter snapshot for an isolated replay database."""
import argparse
import json
from pathlib import Path
import urllib.parse
import urllib.request

EVENTS = ("InsertedRows", "InsertedBytes", "DuplicatedInsertedBlocks", "MergedRows", "MergedUncompressedBytes",
          "MergeTreeDataWriterRows", "MergeTreeDataWriterUncompressedBytes", "MergeTreeDataWriterCompressedBytes",
          "WriteBufferFromFileDescriptorWriteBytes")


def query(url, database, sql, **parameters):
    target = url.rstrip("/") + "/?" + urllib.parse.urlencode({"database": database, "query": sql,
        "wait_end_of_query": "1", "output_format_json_quote_64bit_integers": "0", **parameters})
    with urllib.request.urlopen(urllib.request.Request(target, data=b""), timeout=60) as response:
        return [json.loads(line) for line in response.read().decode().splitlines() if line.strip()]


def profile(url, database, logical=True):
    parsed = urllib.parse.urlsplit(url)
    if parsed.hostname != "127.0.0.1" or not database.startswith("boom_shadow_replay_"):
        raise ValueError("only isolated loopback replay databases are supported")
    columns = query(url, database, "SELECT name,type,data_compressed_bytes,data_uncompressed_bytes,marks_bytes "
                    "FROM system.columns WHERE database=currentDatabase() AND table='boom_photometry' ORDER BY position FORMAT JSONEachRow")
    events = query(url, database, "SELECT event,value,description FROM system.events WHERE event IN "
                   + str(EVENTS) + " FORMAT JSONEachRow")
    result = {"columns": columns, "server_profile_events": events,
              "counter_scope": "cumulative dedicated-server counters across all benchmark databases; delta phase snapshots before attribution"}
    if logical:
        result.update(query(url, database, "SELECT count() AS unique_rows,sum(length(payload)) AS logical_payload_bytes "
                            "FROM boom_photometry FINAL FORMAT JSONEachRow")[0])
        result.update(query(url, database, "SELECT sum(rows) AS physical_rows,sum(bytes_on_disk) AS bytes_on_disk "
                            "FROM system.parts WHERE active AND database=currentDatabase() AND table='boom_photometry' FORMAT JSONEachRow")[0])
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("database")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.write_text(json.dumps(profile(args.url, args.database), indent=2) + "\n")
