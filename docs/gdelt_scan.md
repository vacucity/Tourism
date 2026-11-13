# GDELT GKG scan: runbook

`src/ingest/gdelt_gkg.py` reads every 15-minute GDELT 2.0 GKG file in a date range, checks its MD5
against the master file list, keeps the records that name one of the five parks, and writes an
audit row per file to a local SQLite database (`data/work/gdelt/`, not committed). Raw files are
never stored. A stopped scan resumes where it left off.

## Shards

The archive (2.89 TB zipped) can be scanned on one machine: the Week 8 test ran at 30.9 MB/s with
6 workers, which puts the full scan at about 26 hours. It can run in several sittings, because a
stopped scan resumes. These four date ranges of roughly equal size are convenient chunks, and they
also let several machines share the work:

| Shard | `--start` | `--end` | Zipped size |
| --- | --- | --- | --- |
| s1 | 2015-02-18 | 2017-02-28 | 744 GB |
| s2 | 2017-03-01 | 2019-06-30 | 727 GB |
| s3 | 2019-07-01 | 2023-03-31 | 733 GB |
| s4 | 2023-04-01 | 2026-11-08 | 686 GB |

The full scan ran on one laptop in 24.5 hours (2026-11-12 to 11-13); results are in `docs/week8/a1_gdelt_test.md`.
One file, `20160508140000.gkg.csv.zip`, does not match its master-list entry, so s1 is exported with
`--allow-incomplete`.

Before a long run: plug the laptop in and keep the lid open (the script stops Windows from going to
sleep when idle, but closing the lid still suspends it). The full archive downloads about 2.9 TB,
so check that your internet plan has no monthly data cap that this would exceed.

## Steps (Windows cmd, inside your clone of the repository)

1. Get the code and dependencies:

   ```bat
   git pull
   uv sync
   ```

2. Scan your shard (example for s1; use your own row from the table):

   ```bat
   uv run python src/ingest/gdelt_gkg.py scan --start 2015-02-18 --end 2017-02-28 --workers 6
   ```

   Progress prints every 30 seconds with an ETA. To stop, press Ctrl+C. To resume, run the same
   command again: finished files are skipped, and failed files are retried.

3. Check progress from a second window at any time:

   ```bat
   uv run python src/ingest/gdelt_gkg.py status
   ```

4. When the scan finishes, export with the same dates and your shard label:

   ```bat
   uv run python src/ingest/gdelt_gkg.py export --start 2015-02-18 --end 2017-02-28 --shard s1
   ```

   Export refuses to run while any file in the range is unscanned or failed; run the scan command
   again first. It writes three files under `data/landing/gdelt_matches/`, `gdelt_daily/` and
   `gdelt_audit/`.

5. Commit only those files and push:

   ```bat
   git add data/landing/gdelt_matches data/landing/gdelt_daily data/landing/gdelt_audit
   git commit -m "GDELT shard s1"
   git pull --rebase
   git push
   ```

Always use the same `--shard` label. If you export again on a later day, delete your older
`pull_date=` folders for that shard before committing, so the repository holds one export per shard.

## What the exports contain

- `gdelt_matches`: one row per (record, URL, park) with the fields where the full park name was
  found, the source, URL, tone and themes. `file_ts` (UTC) is the time key.
- `gdelt_daily`: one row per calendar day with files listed in the master list, files ok / missing
  on the server / MD5 mismatch / error / not scanned, and the total records read (the denominator).
  Days with no listed files, such as the 2025-06-15 to 2025-07-01 outage, appear with 0 listed.
- `gdelt_audit`: one row per file with bytes downloaded, bytes decompressed and scanned, rows read,
  malformed lines, matches, status and processing time. Its totals show that the full archive was read.

Spark must read these files with `header`, `escape='"'` and `multiLine` options (Python's csv module
doubles quotes; Spark's default escape character is a backslash).
