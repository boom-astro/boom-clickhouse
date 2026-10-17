#!/usr/bin/env bash
# Engine timings, with result serialization and process startup excluded.
set -euo pipefail
CH="${CH:-$HOME/clickhouse}"
DB="${DB:-./chdata}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
END_JD=$("$CH" local --path "$DB" --query 'SELECT max(jd) FROM photometry FINAL')

labels=(
  'full light curve, one object (enrichment)'
  'last 7 days, one object (filter window)'
  'nightly binned series, one object (binning)'
  'full curves, 1000 objects (batch enrichment)'
  'nightly bins, all objects (survey-wide scan)'
  'brightest point in 7 days, per object'
)
echo "Engine seconds for each read (three runs; warm-cache comparison):"
printf '%s\n' "${labels[@]}"
for run in 1 2 3; do
  echo "Run $run (same order as above):"
  "$CH" local --path "$DB" --param_END_JD="$END_JD" \
    --queries-file "$ROOT/bin/bench.sql" --time
done
