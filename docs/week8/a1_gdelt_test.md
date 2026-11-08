# Week 8 test A1: full GDELT archive scan on schedule, with precise park matching

Test window: one week of GDELT 2.0 GKG files, 2025-01-06 to 2025-01-12 (UTC), run on 2026-11-08
with `src/ingest/gdelt_gkg.py` (6 workers, one laptop).

| Pass condition (from the proposal) | Result |
| --- | --- |
| Every checksum verifies | 672 of 672 files downloaded, all 672 MD5 checksums match the master file list |
| A rerun reproduces the counts | A second scan into a separate database gave identical totals and identical per-file rows and matches: 914,630 records, 388 matches, 0 differences |
| Projected full scan takes 7 days or less | 3.73 GB in about 121 s = 30.9 MB/s, so the full 2.889 TB archive takes about 26 hours |
| At least 90% of checked matches are about the park | Pending: hand-label `is_about_park` in `gdelt_precision_sample.csv` (100 random matches, seed 7) |

## Volume read in the test week

- Downloaded 3.73 GB zipped; decompressed and scanned 11.55 GB.
- 914,630 well-formed records, 0 malformed lines.
- 388 park matches: YELL 272, YOSE 71, ZION 20, GRSM 13, GRCA 12.

## Matching

A record matches a park when its full name ("Yellowstone National Park", etc.) appears in
V1Locations, V2EnhancedLocations, V1Organizations, or V2.1AllNames. Short names are not used: in a
24-file sample across 2015–2026, the bare word "Zion" appeared in 25 records and only 5 were about the
park. The faster lowercase search used in production gave the same matches as a case-insensitive
regular expression on 30 files spread over 2015–2026.

## Reproduce

```bat
uv run python src/ingest/gdelt_gkg.py scan --start 2025-01-06 --end 2025-01-12 --workers 6
uv run python src/ingest/gdelt_gkg.py --db data/work/gdelt/rerun_check.sqlite scan --start 2025-01-06 --end 2025-01-12 --workers 6
uv run python src/ingest/gdelt_gkg.py sample --n 100
```
