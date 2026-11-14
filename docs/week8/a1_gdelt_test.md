# Week 8 test A1: full GDELT archive scan on schedule, with precise park matching

Test window: one week of GDELT 2.0 GKG files, 2025-01-06 to 2025-01-12 (UTC), run on 2026-11-08
with `src/ingest/gdelt_gkg.py` (6 workers, one laptop).

| Pass condition (from the proposal) | Result |
| --- | --- |
| Every checksum verifies | 672 of 672 files downloaded, all 672 MD5 checksums match the master file list |
| A rerun reproduces the counts | A second scan into a separate database gave identical totals and identical per-file rows and matches: 914,630 records, 388 matches, 0 differences |
| Projected full scan takes 7 days or less | 3.73 GB in about 121 s = 30.9 MB/s, so the full 2.889 TB archive takes about 26 hours. The full scan then ran in 24.5 hours (see below) |
| At least 90% of checked matches are about the park | **Not met: 34 of 100 (34%).** With the refined rule below (park name in the article URL), 27 of the 28 sampled matches it keeps are about the park (96%) |

## Full scan (2026-11-12 to 2026-11-13)

`scan --start 2015-02-18 --end 2026-11-08 --workers 6` on one laptop, from 2026-11-12 10:31 to 2026-11-13 11:01
(24.5 hours, 2,885 GB downloaded in that run, 32.7 MB/s on average), then one rerun of the same command to retry
the 129 files that had not passed.

| | Result |
| --- | --- |
| Files in the master list | 403,694 (2015-02-18 23:00 to 2026-11-08 23:45) |
| Downloaded, MD5 verified and scanned | 403,565 |
| Listed but not on the server (HTTP 404) | 128, also after the retry |
| MD5 mismatch | 1: `20160508140000.gkg.csv.zip`. The master list gives 18,095 bytes and MD5 `09c4cc4fa6bd09367d1828eee3f21a2b`; the server serves 10,276,183 bytes (ETag `430824a461ebe6e411916009a1b3b24b`). The file is excluded, so 2016-05-08 14:00-14:15 has no records |
| Data read | 2,888.87 GB zipped, 9,057.21 GB decompressed |
| Records | 735,225,688 well-formed, 7,396 malformed lines |
| Park matches | 636,731: YELL 280,730, YOSE 184,569, GRSM 60,776, GRCA 59,948, ZION 50,708 |

Exports (`export` per shard, `pull_date=2026-11-13`). Shard s1 is exported with `--allow-incomplete` because of
the one mismatched file; its daily file records it in `files_md5_mismatch`.

| Shard | Dates | Files listed | Zipped GB | Records | Matches | matches_*.csv.gz |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| s1 | 2015-02-18 to 2017-02-28 | 70,896 | 744 | 198,722,433 | 174,316 | 16.1 MB |
| s2 | 2017-03-01 to 2019-06-30 | 79,805 | 727 | 188,161,909 | 187,697 | 17.6 MB |
| s3 | 2019-07-01 to 2023-03-31 | 128,166 | 732 | 181,431,236 | 151,322 | 15.6 MB |
| s4 | 2023-04-01 to 2026-11-08 | 124,827 | 686 | 166,910,110 | 123,396 | 12.4 MB |

The four daily files cover 4,282 calendar days. 17 days list no files at all: the GDELT outage from 2025-06-15
to 2025-07-01. The export row counts equal the scan database: 636,731 matches, 403,694 audit rows, 735,225,688 records.

## Volume read in the test week

- Downloaded 3.73 GB zipped; decompressed and scanned 11.55 GB.
- 914,630 well-formed records, 0 malformed lines.
- 388 park matches: YELL 272, YOSE 71, ZION 20, GRSM 13, GRCA 12.

## Precision check (100 random matches from the test week)

`gdelt_precision_sample.csv` holds 100 random matches (seed 7) with `is_about_park` (1 = the park itself is the
main subject or a central part of the article). Claude drafted a label for each row from the title in the article
URL (`claude_draft`; `?` where the URL has no title); the team member then confirmed every row. Of the 89 rows
with a 1 or 0 draft, all 89 were kept; of the 11 rows drafted `?`, 1 became 1 and 10 became 0.

| Park | About the park |
| --- | ---: |
| YELL | 27 of 67 |
| YOSE | 1 of 12 |
| GRCA | 1 of 4 |
| ZION | 3 of 10 |
| GRSM | 2 of 7 |
| All | **34 of 100** |

Why the full name is not enough: the name appears in articles about something else, often reprinted many times.
- One syndicated column (*Wayne's World*) mentions Yellowstone in passing: 10 of the 100 matches.
- One AP story on grizzly bear protection across the Rocky Mountain states: 7 matches.
- One explainer on aerial firefighting during the California fires: 4 matches. One syndicated radio article: 4 matches.
- Travel rankings and lists ("top destinations of 2025", "most popular natural landmarks").
- Same-name entities: the TV series *Yellowstone*.

Refined rule tested on the same 100 labels: keep a match only when the park's own name appears in the article URL
path (`yellowstone`, `yosemite`, `grand canyon`, `zion`, `smoky`/`smokies`). It keeps 28 matches, 27 of them about the
park (96%), and finds 27 of the 34 relevant matches (79%). The one miss is a *Yellowstone* TV-series article; the 7
relevant matches it drops are multi-park articles (reservations, lodging prices, a podcast) and a Gardiner
weekend-getaway piece. Two other rules do not help: requiring the name in a location field (12 of 47) or counting each
syndicated story once (30 of 77). The URL rule was chosen after seeing these labels, so the next step is to label a
fresh random sample of matches that pass it before the news signal is used in the evaluation.

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
