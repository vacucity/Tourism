# Databricks notebook source
# MAGIC %md
# MAGIC # 07 GDELT news: scan exports -> Bronze, Silver, Gold and two aggregate CSVs
# MAGIC
# MAGIC Builds the news tables for the news forecast test (`08_news_test`, `src/news_forecast.py`).
# MAGIC The input is the three exports of the GDELT scan (`src/ingest/gdelt_gkg.py export`, runbook `docs/gdelt_scan.md`),
# MAGIC which `00_setup` copies from `data/landing/` into the landing Volume:
# MAGIC - `gdelt_matches`: one row per (record, URL, park) where a GKG record names the park in full;
# MAGIC - `gdelt_daily`: one row per UTC day: GKG files listed and scanned, and the records read (the denominator);
# MAGIC - `gdelt_audit`: one row per 15-minute GKG file.
# MAGIC
# MAGIC Each export is a `pull_date=YYYY-MM-DD` folder with one file per shard (`s1`..`s4`, one date range each).
# MAGIC
# MAGIC | Table | Grain |
# MAGIC | --- | --- |
# MAGIC | `bronze_gdelt_matches` | one row per CSV row per pull |
# MAGIC | `bronze_gdelt_daily` | one row per CSV row per pull |
# MAGIC | `bronze_gdelt_audit` | one row per CSV row per pull |
# MAGIC | `silver_gdelt_matches` | one row per (record_id, url, park_code), from the newest pull of its shard |
# MAGIC | `silver_gdelt_files` | one row per 15-minute GKG file (`file_ts`), from the newest pull of its shard |
# MAGIC | `silver_gdelt_days` | one row per UTC day, from the newest pull of its shard |
# MAGIC | `gold_month_gkg` | one row per month of `gold_park_month`: 132 months, 2015-01..2025-12 |
# MAGIC | `gold_park_month_news` | one row per park x month: the 660 keys of `gold_park_month` |
# MAGIC
# MAGIC Every month in these tables is the UTC month of the GDELT file (`file_ts`), not a local month at the park.
# MAGIC `url_rule` is rule R0 of `docs/week8/a1_part2_spec.md`, taken from `src/gdelt_url_rule.py`.
# MAGIC
# MAGIC Run `00_setup` and `03_gold` first. Always use **Run all**: every cell relies on the first one. Safe to rerun:
# MAGIC every table and file is overwritten. The notebook prints gates and totals only, never the park x month values.

# COMMAND ----------

import csv
import gzip
import hashlib
import importlib
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
from pyspark import cloudpickle
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# The working directory of a notebook is the folder that contains it: <repo>/databricks/notebooks
ROOT = Path.cwd().parents[1]
MODULE = ROOT / "src" / "gdelt_url_rule.py"
assert MODULE.is_file(), f"Not found: {MODULE}. Open this notebook from the Git folder."
sys.path.insert(0, str(ROOT / "src"))
import gdelt_url_rule  # noqa: E402

importlib.reload(gdelt_url_rule)  # a session opened before the last Pull would otherwise keep the old code
# A gate runs the Python rule gdelt_url_rule.passes() in a UDF. The Spark workers cannot import src/,
# so the module's code is sent with the UDF.
cloudpickle.register_pickle_by_value(gdelt_url_rule)

# The Databricks paths. tests/run_notebook_local.py defines LANDING and EXPORTS before it runs this cell,
# so the same notebook runs on a laptop against the repository's data/landing.
LANDING = globals().get("LANDING", "/Volumes/workspace/nps_tourism/landing")
EXPORTS = Path(globals().get("EXPORTS", "/Volumes/workspace/nps_tourism/exports"))
spark.sql("USE workspace.nps_tourism")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.nps_tourism.exports")
# file_ts is a UTC time. No column depends on the session time zone (days and months are cut from the file_ts
# text), but with UTC the times Spark prints read the same way.
spark.conf.set("spark.sql.session.timeZone", "UTC")

DATASETS = {"matches": "gdelt_matches", "daily": "gdelt_daily", "audit": "gdelt_audit"}  # file prefix -> folder
SHARDS = ["s1", "s2", "s3", "s4"]
RERUN = "Rerun 00_setup to copy data/landing/ into the Volume"

print("gdelt_url_rule.py sha256", hashlib.sha256(MODULE.read_bytes()).hexdigest())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight: the export files in the Volume
# MAGIC
# MAGIC Nothing is written until every check passes.
# MAGIC - `gdelt_matches/`, `gdelt_daily/` and `gdelt_audit/` hold only `pull_date=YYYY-MM-DD` folders, as in `01_bronze`.
# MAGIC - Every file is named `<dataset>_<shard>.csv` or `<dataset>_<shard>.csv.gz`, with a shard of `SHARDS`.
# MAGIC - (a) Each of s1..s4 is in at least one pull of each dataset.
# MAGIC - (b) Within a pull, the three datasets hold the same shards: an export writes the three files of a shard together.
# MAGIC - (c) One file per (dataset, pull, shard).
# MAGIC - (d) All files of a dataset have the same header: the same column names in the same order, read with Python's
# MAGIC   csv module. Bronze reads all pulls of a dataset at once, and Spark takes the column names from one file only.
# MAGIC
# MAGIC The runbook re-exports a single shard into a new `pull_date=` folder, and `00_setup` never deletes a file, so the
# MAGIC Volume can hold older pulls of a shard. Silver keeps the newest pull of each shard; this cell prints which one.

# COMMAND ----------

FILE_NAME = re.compile(r"(matches|daily|audit)_(s[0-9]+)[.]csv([.]gz)?")

found = {}  # (dataset, pull folder) -> {shard: [file names]}
for dataset, folder in DATASETS.items():
    entries = dbutils.fs.ls(f"{LANDING}/{folder}/")
    pulls = sorted(e.name.rstrip("/") for e in entries if e.name.startswith("pull_date="))  # oldest first
    stray = [e.name for e in entries if not e.name.startswith("pull_date=")]
    assert pulls, f"{folder}: no pull_date=YYYY-MM-DD folder found. {RERUN}"
    assert not stray, f"{folder}: only pull_date= folders may be here, move these: {stray}"
    for pull in pulls:
        by_shard = defaultdict(list)
        for e in dbutils.fs.ls(f"{LANDING}/{folder}/{pull}/"):
            m = FILE_NAME.fullmatch(e.name)
            assert m and m.group(1) == dataset, (
                f"{folder}/{pull}: unexpected file {e.name}, expected {dataset}_<shard>.csv or .csv.gz")
            assert m.group(2) in SHARDS, f"{folder}/{pull}/{e.name}: shard {m.group(2)} is not one of {SHARDS}"
            by_shard[m.group(2)].append(e.name)
        found[(dataset, pull)] = dict(by_shard)

# (a) each shard is in at least one pull of each dataset
for dataset, folder in DATASETS.items():
    have = {shard for (d, _), shards in found.items() if d == dataset for shard in shards}
    missing = [shard for shard in SHARDS if shard not in have]
    assert not missing, f"{folder}: no pull holds shard {missing}. {RERUN}"
# (b) within a pull, the same shards in the three datasets
for pull in sorted({p for _, p in found}):
    shard_sets = {dataset: sorted(found.get((dataset, pull), {})) for dataset in DATASETS}
    assert len({tuple(s) for s in shard_sets.values()}) == 1, (
        f"{pull}: the datasets hold different shards {shard_sets}. {RERUN}")
# (c) one file per (dataset, pull, shard)
doubles = {(d, p, s): names for (d, p), shards in found.items() for s, names in shards.items() if len(names) > 1}
assert not doubles, f"more than one file per (dataset, pull, shard), keep one of each: {doubles}"


def csv_header(path: str) -> tuple[str, ...]:
    """The header row of one export file, read with Python's csv module (the module that wrote it)."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", newline="", encoding="utf-8") as f:
        return tuple(next(csv.reader(f), []))  # an empty file has no header: () differs from any real header


# (d) the same header in every file of a dataset
for dataset, folder in DATASETS.items():
    headers = defaultdict(list)  # header -> ["pull/file", ...]
    for (d, pull), shards in found.items():
        if d == dataset:
            for names in shards.values():
                for name in names:
                    headers[csv_header(f"{LANDING}/{folder}/{pull}/{name}")].append(f"{pull}/{name}")
    assert len(headers) == 1, (
        f"{folder}: the files have different headers, so Spark would read some columns under the wrong names. "
        f"Re-export them with one column order. Header -> files: {dict(headers)}")
    [(columns, files)] = headers.items()
    print(f"{folder}: the {len(files)} files have the same header of {len(columns)} columns")

# The pull Silver keeps for each shard: the newest pull that holds it, the same in the three datasets by (b)
CHOSEN = {shard: max(p for (d, p), shards in found.items() if d == "matches" and shard in shards) for shard in SHARDS}
for shard, pull in CHOSEN.items():
    older = sorted(p for (d, p), shards in found.items() if d == "matches" and shard in shards and p < pull)
    files = ", ".join(found[(dataset, pull)][shard][0] for dataset in DATASETS)
    print(f"{shard}: keeps {pull} ({files})" + (f"; older pulls not used: {older}" if older else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: three all-string tables
# MAGIC
# MAGIC Reads every pull of each dataset at once. Spark turns each `pull_date=...` folder name into a `pull_date` DATE column.
# MAGIC Every CSV value stays text; `_source_file` and `_ingested_at` record the file and the load time, as in `01_bronze`.
# MAGIC The read options: `header`, `escape` and `multiLine` follow `docs/gdelt_scan.md`; FAILFAST and `enforceSchema`
# MAGIC are checks.
# MAGIC - `escape='"'`: the files come from Python's csv module, which doubles a quote inside a field (Spark's default
# MAGIC   escape character is a backslash);
# MAGIC - `multiLine`: a quoted field may run across a line end;
# MAGIC - FAILFAST: a broken row stops the run instead of loading as NULLs;
# MAGIC - `enforceSchema` false: Spark checks the header of every file, column by column, against the column names it
# MAGIC   uses and stops at a file that differs, instead of reading that file's columns by position. Check (d) of the
# MAGIC   pre-flight tests the same with Python before anything is written.
# MAGIC
# MAGIC Gate: each file loads as many rows as Python's csv module reads from it, and no value ends in a carriage return
# MAGIC (the exports end their lines with CRLF). A carriage return would hide inside a number: TRY_CAST ignores it.

# COMMAND ----------

def read_export(folder: str) -> DataFrame:
    """All pulls of one export dataset, every value as text."""
    return (spark.read.option("header", True).option("escape", '"').option("multiLine", True)
            .option("mode", "FAILFAST").option("enforceSchema", False).csv(f"{LANDING}/{folder}/"))


def write_bronze(raw: DataFrame, table: str) -> None:
    """Every CSV column as string + pull_date and the lineage columns, then overwrite the Delta table."""
    bronze = raw.select(
        *[F.col(f"`{c}`").cast("string").alias(c) for c in raw.columns if c != "pull_date"],
        F.col("pull_date").cast("date").alias("pull_date"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    (bronze.write.mode("overwrite")
        .option("overwriteSchema", "true")  # columns may change between runs
        .saveAsTable(table))


for dataset, folder in DATASETS.items():
    write_bronze(read_export(folder), f"bronze_gdelt_{dataset}")

# COMMAND ----------

csv.field_size_limit(2**31 - 1)  # a themes field may be longer than the default limit of 131,072 characters
LINEAGE = ["pull_date", "_source_file", "_ingested_at"]


def csv_rows(path: str) -> int:
    """Data rows of one export file, read with Python's csv module (the module that wrote it)."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # minus the header row


# Gate: rows per file = Python's csv module, and no text value ends in a carriage return
for dataset, folder in DATASETS.items():
    table = f"bronze_gdelt_{dataset}"
    bronze = spark.table(table)
    text_columns = [c for c in bronze.columns if c not in LINEAGE]
    per_file = bronze.groupBy("_source_file").agg(
        F.count(F.lit(1)).alias("_rows"),
        *[F.count_if(F.col(f"`{c}`").endswith("\r")).alias(c) for c in text_columns]).collect()
    # _source_file is a full path; (pull folder, file name) identifies the file
    loaded = {tuple(r["_source_file"].rsplit("/", 2)[1:]): r["_rows"] for r in per_file}
    in_files = {(pull, name): csv_rows(f"{LANDING}/{folder}/{pull}/{name}")
                for (d, pull), shards in found.items() if d == dataset
                for names in shards.values() for name in names}
    differ = {k: (loaded.get(k, 0), in_files.get(k)) for k in loaded.keys() | in_files.keys()
              if loaded.get(k, 0) != in_files.get(k)}
    assert not differ, f"{table}: rows per file (Spark, csv module) differ: {differ}"
    cr = {c: sum(r[c] for r in per_file) for c in text_columns}
    assert not any(cr.values()), f"{table}: values that end in a carriage return: { {c: n for c, n in cr.items() if n} }"
    print(f"{table}: {sum(loaded.values()):,} rows from {len(in_files)} files = the rows Python's csv module reads "
          f"in each file; 0 values end in a carriage return")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: typed tables from the newest pull of each shard
# MAGIC
# MAGIC - The shard is in the file name (`matches_s1.csv.gz` -> `s1`). For each shard the rows of its newest pull are kept:
# MAGIC   DENSE_RANK over the (shard, pull) pairs, as in `02_silver`.
# MAGIC - Values are typed with TRY_CAST, and an empty field becomes NULL. A gate then requires 0 NULLs in every typed
# MAGIC   column, so a value that does not parse stops the run instead of becoming a silent NULL.
# MAGIC - `file_ts` (`YYYYMMDDHHMMSS`, UTC) stays text, the key of the exports. `file_day` and `month_key` are cut from that
# MAGIC   text, so they are its UTC day and month whatever the session time zone.
# MAGIC - `silver_gdelt_matches` adds `url_path` and `url_rule` (1 = pass, 0 = fail): Spark SQL that `src/gdelt_url_rule.py`
# MAGIC   builds from the same strings as its Python `passes()`. `record_id` alone is not unique (some 2015-2019 files
# MAGIC   reuse a record id for two URLs), so the key is (record_id, url, park_code), as in the scan database.
# MAGIC - `silver_gdelt_files` is the audit. `processed_at` is the local wall-clock time of the scan, with no zone, so it
# MAGIC   stays text.

# COMMAND ----------

SHARD_REGEX = "_(s[0-9]+)[.]csv([.]gz)?$"  # matches_s1.csv.gz -> s1 (the same text as in the two %sql cells below)

spark.sql(f"""
CREATE OR REPLACE TABLE silver_gdelt_matches
COMMENT 'Grain: one row per (record_id, url, park_code), from the newest pull of its shard. file_ts is the UTC time of the GDELT file; file_day and month_key are its UTC day and month. url_rule = 1 when the URL path holds the name of the park of the row (rule R0, src/gdelt_url_rule.py)'
AS
WITH pulls AS (   -- every (shard, pull) pair; pull_rank 1 = the newest pull of the shard
  SELECT shard, pull_date, DENSE_RANK() OVER (PARTITION BY shard ORDER BY pull_date DESC) AS pull_rank
  FROM (SELECT DISTINCT regexp_extract(_source_file, '{SHARD_REGEX}', 1) AS shard, pull_date
        FROM bronze_gdelt_matches) AS pairs
),
newest AS (       -- the rows of the newest pull of each shard; an empty field becomes NULL
  SELECT NULLIF(b.record_id, '') AS record_id, NULLIF(b.file_ts, '') AS file_ts, NULLIF(b.park_code, '') AS park_code,
         NULLIF(b.matched_fields, '') AS matched_fields, NULLIF(b.source, '') AS source, NULLIF(b.url, '') AS url,
         NULLIF(b.tone, '') AS tone, NULLIF(b.themes, '') AS themes, p.shard, b.pull_date
  FROM bronze_gdelt_matches AS b
  JOIN pulls AS p
    ON p.shard = regexp_extract(b._source_file, '{SHARD_REGEX}', 1) AND p.pull_date = b.pull_date
  WHERE p.pull_rank = 1
)
SELECT record_id, file_ts,
       TRY_CAST(CONCAT(SUBSTR(file_ts, 1, 4), '-', SUBSTR(file_ts, 5, 2), '-', SUBSTR(file_ts, 7, 2)) AS DATE) AS file_day,
       TRY_CAST(SUBSTR(file_ts, 1, 6) AS INT) AS month_key,
       park_code, matched_fields, source, url,
       {gdelt_url_rule.spark_sql_path("url")} AS url_path,
       {gdelt_url_rule.spark_sql_case("url", "park_code")} AS url_rule,
       TRY_CAST(tone AS DOUBLE) AS tone,
       themes, shard, pull_date
FROM newest
""")
print("silver_gdelt_matches: url_rule =", gdelt_url_rule.spark_sql_case("url", "park_code"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_gdelt_files
# MAGIC COMMENT 'Grain: one row per 15-minute GKG file (file_ts), from the newest pull of its shard: the audit of the scan. file_ts is the UTC time of the file; file_day and month_key are its UTC day and month'
# MAGIC AS
# MAGIC WITH pulls AS (   -- every (shard, pull) pair; pull_rank 1 = the newest pull of the shard
# MAGIC   SELECT shard, pull_date, DENSE_RANK() OVER (PARTITION BY shard ORDER BY pull_date DESC) AS pull_rank
# MAGIC   FROM (SELECT DISTINCT regexp_extract(_source_file, '_(s[0-9]+)[.]csv([.]gz)?$', 1) AS shard, pull_date
# MAGIC         FROM bronze_gdelt_audit) AS pairs
# MAGIC ),
# MAGIC newest AS (
# MAGIC   SELECT b.*, p.shard
# MAGIC   FROM bronze_gdelt_audit AS b
# MAGIC   JOIN pulls AS p
# MAGIC     ON p.shard = regexp_extract(b._source_file, '_(s[0-9]+)[.]csv([.]gz)?$', 1) AND p.pull_date = b.pull_date
# MAGIC   WHERE p.pull_rank = 1
# MAGIC )
# MAGIC SELECT NULLIF(file_ts, '') AS file_ts,
# MAGIC        TRY_CAST(CONCAT(SUBSTR(file_ts, 1, 4), '-', SUBSTR(file_ts, 5, 2), '-', SUBSTR(file_ts, 7, 2)) AS DATE) AS file_day,
# MAGIC        TRY_CAST(SUBSTR(file_ts, 1, 6) AS INT)  AS month_key,
# MAGIC        NULLIF(url, '')                         AS url,
# MAGIC        TRY_CAST(expected_bytes AS BIGINT)      AS expected_bytes,
# MAGIC        TRY_CAST(downloaded_bytes AS BIGINT)    AS downloaded_bytes,
# MAGIC        TRY_CAST(decompressed_bytes AS BIGINT)  AS decompressed_bytes,
# MAGIC        TRY_CAST(md5_ok AS INT)                 AS md5_ok,
# MAGIC        TRY_CAST(rows_read AS BIGINT)           AS rows_read,       -- well-formed records (27 fields)
# MAGIC        TRY_CAST(malformed_rows AS BIGINT)      AS malformed_rows,
# MAGIC        TRY_CAST(matched_rows AS BIGINT)        AS matched_rows,
# MAGIC        NULLIF(status, '')                      AS status,          -- ok | missing_on_server | md5_mismatch | error
# MAGIC        NULLIF(error, '')                       AS error,
# MAGIC        TRY_CAST(seconds AS DOUBLE)             AS seconds,
# MAGIC        NULLIF(processed_at, '')                AS processed_at,    -- local wall-clock time of the scan, no zone
# MAGIC        shard, pull_date
# MAGIC FROM newest;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_gdelt_days
# MAGIC COMMENT 'Grain: one row per UTC day, from the newest pull of its shard: GKG files listed in the master list and scanned, and the records read in the ok files (the denominator of the news counts). month_key is the UTC month'
# MAGIC AS
# MAGIC WITH pulls AS (   -- every (shard, pull) pair; pull_rank 1 = the newest pull of the shard
# MAGIC   SELECT shard, pull_date, DENSE_RANK() OVER (PARTITION BY shard ORDER BY pull_date DESC) AS pull_rank
# MAGIC   FROM (SELECT DISTINCT regexp_extract(_source_file, '_(s[0-9]+)[.]csv([.]gz)?$', 1) AS shard, pull_date
# MAGIC         FROM bronze_gdelt_daily) AS pairs
# MAGIC ),
# MAGIC newest AS (
# MAGIC   SELECT b.*, p.shard
# MAGIC   FROM bronze_gdelt_daily AS b
# MAGIC   JOIN pulls AS p
# MAGIC     ON p.shard = regexp_extract(b._source_file, '_(s[0-9]+)[.]csv([.]gz)?$', 1) AND p.pull_date = b.pull_date
# MAGIC   WHERE p.pull_rank = 1
# MAGIC ),
# MAGIC typed AS (
# MAGIC   SELECT TRY_CAST(day AS DATE)                    AS day,
# MAGIC          TRY_CAST(files_listed AS INT)            AS files_listed,
# MAGIC          TRY_CAST(files_ok AS INT)                AS files_ok,
# MAGIC          TRY_CAST(files_missing_on_server AS INT) AS files_missing_on_server,
# MAGIC          TRY_CAST(files_md5_mismatch AS INT)      AS files_md5_mismatch,
# MAGIC          TRY_CAST(files_error AS INT)             AS files_error,
# MAGIC          TRY_CAST(files_not_scanned AS INT)       AS files_not_scanned,
# MAGIC          TRY_CAST(records AS BIGINT)              AS records,   -- well-formed records in the ok files of the day
# MAGIC          shard, pull_date
# MAGIC   FROM newest
# MAGIC )
# MAGIC SELECT day, YEAR(day) * 100 + MONTH(day) AS month_key,
# MAGIC        files_listed, files_ok, files_missing_on_server, files_md5_mismatch, files_error, files_not_scanned,
# MAGIC        records, shard, pull_date
# MAGIC FROM typed;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver checkpoints
# MAGIC
# MAGIC Every gate compares the tables with each other or with the pre-flight of this run. The totals of the 2026-11-13
# MAGIC exports are printed at the end for comparison, not asserted: a later export changes them.

# COMMAND ----------

QUARTER_HOUR = "^[0-9]{10}(00|15|30|45)00$"  # YYYYMMDDHH, then minute 00, 15, 30 or 45, then second 00
FILE_TS_CHECK = (f"COUNT_IF(NOT (file_ts RLIKE '{QUARTER_HOUR}') "
                 f"OR TRY_TO_TIMESTAMP(file_ts, 'yyyyMMddHHmmss') IS NULL) AS file_ts_not_on_a_quarter_hour")
# The columns that may not be NULL: every typed column and every key. Only source, themes and error may be empty.
NOT_NULL = {
    "silver_gdelt_matches": ["record_id", "file_ts", "file_day", "month_key", "park_code", "matched_fields", "url",
                             "url_path", "url_rule", "tone", "shard", "pull_date"],
    "silver_gdelt_files": ["file_ts", "file_day", "month_key", "url", "expected_bytes", "downloaded_bytes",
                           "decompressed_bytes", "md5_ok", "rows_read", "malformed_rows", "matched_rows", "status",
                           "seconds", "processed_at", "shard", "pull_date"],
    "silver_gdelt_days": ["day", "month_key", "files_listed", "files_ok", "files_missing_on_server",
                          "files_md5_mismatch", "files_error", "files_not_scanned", "records", "shard", "pull_date"],
}
FORMAT_CHECKS = {
    "silver_gdelt_matches": [FILE_TS_CHECK, "COUNT_IF(url_rule NOT IN (0, 1)) AS url_rule_not_0_or_1"],
    "silver_gdelt_files": [FILE_TS_CHECK, "COUNT_IF(md5_ok NOT IN (0, 1)) AS md5_ok_not_0_or_1",
                           "COUNT_IF(status NOT IN ('ok', 'missing_on_server', 'md5_mismatch', 'error')) "
                           "AS unknown_status"],
    "silver_gdelt_days": [],
}
KEYS = {"silver_gdelt_matches": "record_id, url, park_code", "silver_gdelt_files": "file_ts", "silver_gdelt_days": "day"}

# Gate: 0 NULLs after TRY_CAST and in the keys; file_ts has 14 digits on a quarter hour
for table, columns in NOT_NULL.items():
    checks = [f"COUNT_IF({c} IS NULL) AS {c}" for c in columns] + FORMAT_CHECKS[table]
    bad = {k: v for k, v in spark.sql(f"SELECT {', '.join(checks)} FROM {table}").first().asDict().items() if v}
    assert not bad, f"{table}: NULL or malformed values {bad}"
    print(f"{table}: 0 NULLs in {len(columns)} typed and key columns"
          + ("; file_ts has 14 digits on a quarter hour" if "file_ts" in columns else ""))

# Gate: unique keys, and each table kept the pull that the pre-flight chose for each shard
for table, key in KEYS.items():
    rows, keys, pairs = spark.sql(f"""
        SELECT COUNT(*), COUNT(DISTINCT {key}), ARRAY_SORT(COLLECT_SET(CONCAT(shard, ' ', CAST(pull_date AS STRING))))
        FROM {table}""").first()
    kept = dict(pair.split(" ") for pair in pairs)
    assert rows == keys, f"{table}: {rows:,} rows but {keys:,} distinct ({key})"
    # The dict holds one pull per shard, so two kept pulls of a shard would collapse into one: count the pairs first
    assert len(kept) == len(pairs), f"{table}: a shard kept more than one pull: {pairs}"
    assert kept == {shard: pull.removeprefix("pull_date=") for shard, pull in CHOSEN.items()}, (
        f"{table}: kept pulls {kept}, the pre-flight chose {CHOSEN}")
    print(f"{table}: {rows:,} rows = {keys:,} distinct ({key}), all from the pulls the pre-flight chose")

# COMMAND ----------

# Gate: every file is in one shard only; its match rows = the audit's matched_rows; matches come only from ok files
match_files, not_in_audit, not_ok, count_differs, two_shards, other_shard = spark.sql("""
    WITH per_file AS (
      SELECT file_ts, COUNT(*) AS n, COUNT(DISTINCT shard) AS shards, MIN(shard) AS shard
      FROM silver_gdelt_matches
      GROUP BY file_ts
    )
    SELECT COUNT(m.file_ts),
           COUNT_IF(m.file_ts IS NOT NULL AND f.file_ts IS NULL),
           COUNT_IF(m.file_ts IS NOT NULL AND f.status <> 'ok'),
           COUNT_IF(f.file_ts IS NOT NULL AND COALESCE(m.n, 0) <> f.matched_rows),
           COUNT_IF(m.shards > 1),
           COUNT_IF(m.shard <> f.shard)
    FROM silver_gdelt_files AS f
    FULL OUTER JOIN per_file AS m ON m.file_ts = f.file_ts""").first()
assert not_in_audit == 0, f"{not_in_audit} files with matches have no audit row"
assert not_ok == 0, f"{not_ok} files with matches are not ok in the audit"
assert count_differs == 0, f"{count_differs} files: match rows differ from the audit's matched_rows"
assert two_shards == other_shard == 0, (
    f"files in two shards: {two_shards} among the matches, {other_shard} in another shard than their audit row")
print(f"silver_gdelt_matches: rows per file = the audit's matched_rows in every file; the {match_files:,} files "
      f"with matches are ok in the audit and in the same shard")

# Gate: every day of the daily file agrees with the audit rows of that day (files by status, and records =
# rows_read of the ok files), each day is in one shard, and the days run without a gap
(days, audit_only, records_differ, files_differ, two_shards, first_day, last_day, calendar_days) = spark.sql("""
    WITH audit AS (
      SELECT file_day,
             COUNT_IF(status = 'ok') AS ok, COUNT_IF(status = 'missing_on_server') AS missing,
             COUNT_IF(status = 'md5_mismatch') AS md5_mismatch, COUNT_IF(status = 'error') AS error,
             SUM(CASE WHEN status = 'ok' THEN rows_read ELSE 0 END) AS records,
             COUNT(DISTINCT shard) AS shards, MIN(shard) AS shard
      FROM silver_gdelt_files
      GROUP BY file_day
    )
    SELECT COUNT(d.day),
           COUNT_IF(d.day IS NULL),
           COUNT_IF(d.day IS NOT NULL AND d.records <> COALESCE(a.records, 0)),
           COUNT_IF(d.day IS NOT NULL AND (d.files_ok <> COALESCE(a.ok, 0)
                    OR d.files_missing_on_server <> COALESCE(a.missing, 0)
                    OR d.files_md5_mismatch <> COALESCE(a.md5_mismatch, 0)
                    OR d.files_error <> COALESCE(a.error, 0))),
           COUNT_IF(a.shards > 1 OR a.shard <> d.shard),
           MIN(d.day), MAX(d.day), DATEDIFF(MAX(d.day), MIN(d.day)) + 1
    FROM silver_gdelt_days AS d
    FULL OUTER JOIN audit AS a ON a.file_day = d.day""").first()
assert audit_only == 0, f"{audit_only} days have audit rows but no row in the daily file"
assert records_differ == 0, f"{records_differ} days: records differ from the audit's rows_read of ok files"
assert files_differ == 0, f"{files_differ} days: files by status differ from the audit"
assert two_shards == 0, f"{two_shards} days have files in two shards"
assert days == calendar_days, f"{days:,} days from {first_day} to {last_day}, but the calendar has {calendar_days:,}"
print(f"silver_gdelt_days: {days:,} days without a gap ({first_day} to {last_day}); per day, records = the audit's "
      f"rows_read of ok files and the files by status = the audit")

# Gate: the Python rule gdelt_url_rule.passes() gives url_rule on every row; the comparison runs inside Spark
spark.udf.register("r0_python", gdelt_url_rule.passes, T.BooleanType())
rows, differ, python_passes = spark.sql("""
    SELECT COUNT(*),
           COUNT_IF(NOT (r0_python(park_code, url) <=> (url_rule = 1))),
           COUNT_IF(r0_python(park_code, url))
    FROM silver_gdelt_matches""").first()
assert differ == 0, f"{differ:,} rows where Python passes() and Spark url_rule differ"
print(f"silver_gdelt_matches: Python passes() = Spark url_rule on all {rows:,} rows ({python_passes:,} pass)")

# COMMAND ----------

# For information: the totals of the exports of 2026-11-13 (README, docs/gdelt_scan.md, docs/week8/a1_part2_spec.md).
# A later export changes them, so they are printed, not asserted.
EXPORTS_2026_11_13 = {
    "match rows": 636_731, "GKG files (audit rows)": 403_694, "  ok": 403_565, "  missing_on_server": 128,
    "  md5_mismatch": 1, "  error": 0, "days": 4_282, "records read in ok files": 735_225_688,
    "files listed but not scanned": 0,
    "match rows of records that match 2+ parks": 56_920, "extra rows of a (url, park) seen again": 1_010,
    "source empty": 8, "themes empty": 31_464,
    "url_rule = 1": 184_220,
}
BY_PARK_2026_11_13 = {  # (match rows, url_rule = 1)
    "GRCA": (59_948, 24_668), "GRSM": (60_776, 10_062), "YELL": (280_730, 94_952),
    "YOSE": (184_569, 44_899), "ZION": (50_708, 9_639),
}

match_totals = spark.sql("""
    SELECT COUNT(*) AS rows, COUNT_IF(url_rule = 1) AS passes,
           COUNT_IF(source IS NULL) AS source_null, COUNT_IF(themes IS NULL) AS themes_null,
           COUNT(*) - COUNT(DISTINCT url, park_code) AS repeated_url_park
    FROM silver_gdelt_matches""").first()
multi_park = spark.sql("""
    SELECT COALESCE(SUM(n), 0) FROM (
      SELECT record_id, url, COUNT(*) AS n FROM silver_gdelt_matches GROUP BY record_id, url HAVING COUNT(*) > 1)
""").first()[0]
file_totals = spark.sql("""
    SELECT COUNT(*) AS files, COUNT_IF(status = 'ok') AS ok, COUNT_IF(status = 'missing_on_server') AS missing,
           COUNT_IF(status = 'md5_mismatch') AS md5_mismatch, COUNT_IF(status = 'error') AS error
    FROM silver_gdelt_files""").first()
day_totals = spark.sql("SELECT COUNT(*) AS days, SUM(records) AS records, SUM(files_not_scanned) AS not_scanned "
                       "FROM silver_gdelt_days").first()
this_run = {
    "match rows": match_totals.rows,
    "GKG files (audit rows)": file_totals.files,
    "  ok": file_totals.ok,
    "  missing_on_server": file_totals.missing,
    "  md5_mismatch": file_totals.md5_mismatch,
    "  error": file_totals.error,
    "days": day_totals.days,
    "records read in ok files": day_totals.records,
    "files listed but not scanned": day_totals.not_scanned,
    "match rows of records that match 2+ parks": multi_park,
    "extra rows of a (url, park) seen again": match_totals.repeated_url_park,
    "source empty": match_totals.source_null,
    "themes empty": match_totals.themes_null,
    "url_rule = 1": match_totals.passes,
}
print(f"{'Silver':<44} {'this run':>13} {'2026-11-13':>13}")
for name, value in this_run.items():
    print(f"{name:<44} {value:>13,} {EXPORTS_2026_11_13[name]:>13,}")

by_park = {r.park_code: (r.rows, r.passes) for r in spark.sql("""
    SELECT park_code, COUNT(*) AS rows, COUNT_IF(url_rule = 1) AS passes
    FROM silver_gdelt_matches GROUP BY park_code""").collect()}
print(f"\n{'park_code':<10} {'rows':>9} {'2026-11-13':>11} {'url_rule=1':>11} {'2026-11-13':>11}")
for park in sorted(by_park.keys() | BY_PARK_2026_11_13.keys(), key=str):
    rows, passes = by_park.get(park, (0, 0))
    rows_then, passes_then = BY_PARK_2026_11_13.get(park, (0, 0))
    print(f"{str(park):<10} {rows:>9,} {rows_then:>11,} {passes:>11,} {passes_then:>11,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1: gold_month_gkg, one row per month of gold_park_month
# MAGIC
# MAGIC The denominator of the news counts. Month level: the values are never summed over parks.
# MAGIC - `files_listed`, `files_ok`, `records`: GKG files in the master list, files scanned ok, and well-formed records in
# MAGIC   the ok files, summed over the days of the UTC month (`silver_gdelt_days`).
# MAGIC - `slots` = 96 x the days of the calendar month (one file every 15 minutes), from `month_start` of `gold_park_month`;
# MAGIC   `coverage` = `files_ok / slots`.
# MAGIC - A month with no day in the daily file has 0 in every count, never NULL. That is only 2015-01: the first GKG
# MAGIC   file of the scan is from 2015-02-18 23:00 UTC.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_month_gkg
# MAGIC COMMENT 'Grain: one row per month of gold_park_month (132 months, 2015-01..2025-12). A month is the UTC month of the GDELT file. GKG files listed and ok, slots = 96 x days of the month, coverage = files_ok / slots, and records = well-formed records in the ok files. Month level: never sum over parks'
# MAGIC AS
# MAGIC WITH months AS (
# MAGIC   SELECT DISTINCT month_key, month_start FROM gold_park_month
# MAGIC ),
# MAGIC by_month AS (
# MAGIC   SELECT month_key, SUM(files_listed) AS files_listed, SUM(files_ok) AS files_ok, SUM(records) AS records
# MAGIC   FROM silver_gdelt_days
# MAGIC   GROUP BY month_key
# MAGIC )
# MAGIC SELECT m.month_key, m.month_start,
# MAGIC        COALESCE(b.files_listed, 0)                             AS files_listed,
# MAGIC        COALESCE(b.files_ok, 0)                                 AS files_ok,
# MAGIC        96 * DAY(LAST_DAY(m.month_start))                       AS slots,
# MAGIC        COALESCE(b.files_ok, 0) / (96 * DAY(LAST_DAY(m.month_start))) AS coverage,
# MAGIC        COALESCE(b.records, 0)                                  AS records
# MAGIC FROM months AS m
# MAGIC LEFT JOIN by_month AS b ON b.month_key = m.month_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2: gold_park_month_news, one row per key of gold_park_month
# MAGIC
# MAGIC The rows come from `gold_park_month` itself, so `08_news_test` can join the tables 1:1 on `(park_code, month_key)`.
# MAGIC - `news_all`: match rows (record, URL, park) in the UTC month of their GDELT file. A record that names two parks
# MAGIC   counts once in each, and a URL seen again in a later record counts again, the same basis as `records`.
# MAGIC - `news_url`: the rows with `url_rule = 1`, rule R0.
# MAGIC - `news_all_per_million`, `news_url_per_million` = 1e6 x the count / `records` of `gold_month_gkg` in that month.
# MAGIC   The share is the only treatment of missing files (the 2025-06-15..2025-07-01 outage); there is no coverage threshold.
# MAGIC - The four news columns are NULL where the month has no ok GKG file (only 2015-01); elsewhere a park-month with no
# MAGIC   match has 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_park_month_news
# MAGIC COMMENT 'Grain: one row per park x month (keys of gold_park_month). A month is the UTC month of the GDELT file. news_all = match rows (record, URL, park), news_url = those with url_rule = 1 (rule R0); per million = 1e6 x count / records of gold_month_gkg in the month. NULL where the month has no ok GDELT file (2015-01)'
# MAGIC AS
# MAGIC WITH counts AS (
# MAGIC   SELECT park_code, month_key, COUNT(*) AS news_all, COUNT_IF(url_rule = 1) AS news_url
# MAGIC   FROM silver_gdelt_matches
# MAGIC   GROUP BY park_code, month_key
# MAGIC )
# MAGIC SELECT g.park_code, g.month_key, g.month_start,
# MAGIC        -- 1e6 is a DOUBLE literal; TRY_DIVIDE gives NULL instead of an error if records were 0
# MAGIC        CASE WHEN k.files_ok > 0 THEN COALESCE(c.news_all, 0) END                            AS news_all,
# MAGIC        CASE WHEN k.files_ok > 0 THEN COALESCE(c.news_url, 0) END                            AS news_url,
# MAGIC        CASE WHEN k.files_ok > 0 THEN TRY_DIVIDE(1e6 * COALESCE(c.news_all, 0), k.records) END AS news_all_per_million,
# MAGIC        CASE WHEN k.files_ok > 0 THEN TRY_DIVIDE(1e6 * COALESCE(c.news_url, 0), k.records) END AS news_url_per_million
# MAGIC FROM gold_park_month AS g
# MAGIC LEFT JOIN gold_month_gkg AS k ON k.month_key = g.month_key
# MAGIC LEFT JOIN counts AS c ON c.park_code = g.park_code AND c.month_key = g.month_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold checkpoints
# MAGIC
# MAGIC Gates and totals only: no park x month value is shown.

# COMMAND ----------

# Gate: gold_month_gkg has one row per month of gold_park_month and no month on one side only
rows, keys = spark.sql("SELECT COUNT(*), COUNT(DISTINCT month_key) FROM gold_month_gkg").first()
base = spark.sql("SELECT COUNT(DISTINCT month_key) FROM gold_park_month").first()[0]
only_in_base, only_in_gkg = spark.sql("""
    SELECT COUNT_IF(k.month_key IS NULL), COUNT_IF(g.month_key IS NULL)
    FROM (SELECT DISTINCT month_key FROM gold_park_month) AS g
    FULL OUTER JOIN gold_month_gkg AS k ON k.month_key = g.month_key""").first()
assert rows == keys == base, f"gold_month_gkg: {rows} rows, {keys} distinct months; gold_park_month has {base}"
assert only_in_base == only_in_gkg == 0, f"months on one side only: {only_in_base} in gold_park_month, {only_in_gkg} here"

# Gate: no NULL, files_ok <= files_listed <= slots, and coverage between 0 and 1
gkg = spark.table("gold_month_gkg")
nulls = {c: n for c, n in gkg.select([F.count_if(F.col(c).isNull()).alias(c) for c in gkg.columns]).first()
         .asDict().items() if n}
over, outside = spark.sql("""
    SELECT COUNT_IF(files_ok > files_listed OR files_listed > slots), COUNT_IF(coverage < 0 OR coverage > 1)
    FROM gold_month_gkg""").first()
assert not nulls, f"gold_month_gkg: NULLs {nulls}"
assert over == 0, f"gold_month_gkg: {over} months with files_ok > files_listed or files_listed > slots"
assert outside == 0, f"gold_month_gkg: {outside} months with coverage outside [0, 1]"

# Gate: the totals equal Silver, counted in this run over the months of gold_park_month
gold_listed, gold_ok, gold_records = spark.sql(
    "SELECT SUM(files_listed), SUM(files_ok), SUM(records) FROM gold_month_gkg").first()
days_listed, days_ok, days_records = spark.sql("""
    SELECT SUM(files_listed), SUM(files_ok), SUM(records) FROM silver_gdelt_days
    WHERE month_key IN (SELECT month_key FROM gold_park_month)""").first()
audit_ok, audit_records = spark.sql("""
    SELECT COUNT_IF(status = 'ok'), SUM(CASE WHEN status = 'ok' THEN rows_read ELSE 0 END) FROM silver_gdelt_files
    WHERE month_key IN (SELECT month_key FROM gold_park_month)""").first()
assert (gold_listed, gold_ok, gold_records) == (days_listed, days_ok, days_records), (
    f"gold_month_gkg (listed, ok, records) {(gold_listed, gold_ok, gold_records)}, "
    f"silver_gdelt_days {(days_listed, days_ok, days_records)}")
assert (gold_ok, gold_records) == (audit_ok, audit_records), (
    f"gold_month_gkg (ok, records) {(gold_ok, gold_records)}, silver_gdelt_files {(audit_ok, audit_records)}")
print(f"gold_month_gkg: {rows} rows = {base} months of gold_park_month, 0 on one side only, 0 NULLs, "
      f"files_ok <= files_listed <= slots, coverage in [0, 1]")
print(f"gold_month_gkg totals = Silver in the same months: {gold_listed:,} files listed, {gold_ok:,} ok, "
      f"{gold_records:,} records")

# COMMAND ----------

# Gate: one row per key of gold_park_month and no key on one side only, so the join in 08 is 1:1
rows = spark.sql("SELECT COUNT(*) FROM gold_park_month_news").first()[0]
keys = spark.sql("SELECT COUNT(*) FROM (SELECT DISTINCT park_code, month_key FROM gold_park_month_news) AS k").first()[0]
base = spark.table("gold_park_month").count()
joined, only_in_gold_park_month, only_in_news = spark.sql("""
    SELECT COUNT(*), COUNT_IF(n.park_code IS NULL), COUNT_IF(g.park_code IS NULL)
    FROM gold_park_month AS g
    FULL OUTER JOIN gold_park_month_news AS n
      ON n.park_code = g.park_code AND n.month_key = g.month_key""").first()
assert rows == keys == base == joined, f"rows {rows}, distinct keys {keys}, gold_park_month {base}, joined {joined}"
assert only_in_gold_park_month == only_in_news == 0, (
    f"keys on one side only: {only_in_gold_park_month} in gold_park_month, {only_in_news} here")
print(f"gold_park_month_news: {rows} rows = {keys} distinct keys = the {base} rows of gold_park_month; "
      f"0 keys on one side only")

# Gate: NULL only in the four news columns, and exactly in the months without an ok GKG file
NEWS_COLUMNS = ["news_all", "news_url", "news_all_per_million", "news_url_per_million"]
news = spark.table("gold_park_month_news")
null_cells = {c: n for c, n in news.select([F.count_if(F.col(c).isNull()).alias(c) for c in news.columns]).first()
              .asDict().items() if n}
no_file_months = sorted(r.month_key for r in spark.sql("SELECT month_key FROM gold_month_gkg WHERE files_ok = 0").collect())
null_months = sorted(r.month_key for r in spark.sql(f"""
    SELECT DISTINCT month_key FROM gold_park_month_news
    WHERE {' OR '.join(f'{c} IS NULL' for c in NEWS_COLUMNS)}""").collect())
not_null_there = spark.sql(f"""
    SELECT COUNT(*) FROM gold_park_month_news AS n JOIN gold_month_gkg AS k USING (month_key)
    WHERE k.files_ok = 0 AND ({' OR '.join(f'n.{c} IS NOT NULL' for c in NEWS_COLUMNS)})""").first()[0]
assert set(null_cells) <= set(NEWS_COLUMNS), f"NULLs outside the news columns: {null_cells}"
assert null_months == no_file_months and not_null_there == 0, (
    f"news NULL in months {null_months}; months without an ok file {no_file_months}; "
    f"{not_null_there} rows of those months with a value")
# The scan starts on 2015-02-18, so 2015-01 is the only month without a file: 4 columns x 5 parks = 20 cells
assert no_file_months == [201501] and sum(null_cells.values()) == 20, (
    f"expected 20 NULL cells, all in 201501; found {null_cells} in the months {null_months}")
print(f"gold_park_month_news: {sum(null_cells.values())} NULL cells, all in the 4 news columns of the months without "
      f"an ok GKG file {no_file_months}")

# Gate: news_url <= news_all, and the sums equal the Silver rows of this run in the months of gold_park_month
over = spark.sql("SELECT COUNT_IF(news_url > news_all) FROM gold_park_month_news").first()[0]
gold_all, gold_url = spark.sql("SELECT SUM(news_all), SUM(news_url) FROM gold_park_month_news").first()
silver_all, silver_url = spark.sql("""
    SELECT COUNT(*), COUNT_IF(url_rule = 1) FROM silver_gdelt_matches
    WHERE month_key IN (SELECT month_key FROM gold_park_month)""").first()
assert over == 0, f"{over} rows with news_url > news_all"
assert (gold_all, gold_url) == (silver_all, silver_url), (
    f"Gold sums (news_all, news_url) {(gold_all, gold_url)}, Silver rows in the same months {(silver_all, silver_url)}")
print(f"gold_park_month_news: news_url <= news_all in every row; sums = Silver rows in the months of gold_park_month: "
      f"news_all {gold_all:,}, news_url {gold_url:,}")

# COMMAND ----------

# For information: totals over 2015-01..2025-12, and the months with less than 95% of the 15-minute files.
# The values of the 2026-11-13 exports, measured on the export files, are printed for comparison.
WINDOW_2026_11_13 = {"news_all": 612_919, "news_url": 174_604, "records": 703_098_249}
WINDOW_BY_PARK_2026_11_13 = {  # (news_all, news_url)
    "GRCA": (56_664, 23_085), "GRSM": (58_840, 9_647), "YELL": (270_675, 90_045),
    "YOSE": (179_127, 43_088), "ZION": (47_613, 8_739),
}
print(f"{'2015-01..2025-12':<18} {'this run':>13} {'2026-11-13':>13}")
for name, value in {"news_all": gold_all, "news_url": gold_url, "records": gold_records}.items():
    print(f"{name:<18} {value:>13,} {WINDOW_2026_11_13[name]:>13,}")

totals = {r.park_code: (r.news_all, r.news_url) for r in spark.sql("""
    SELECT park_code, SUM(news_all) AS news_all, SUM(news_url) AS news_url
    FROM gold_park_month_news GROUP BY park_code""").collect()}
print(f"\n{'park_code':<10} {'news_all':>9} {'2026-11-13':>11} {'news_url':>9} {'2026-11-13':>11}")
for park in sorted(totals.keys() | WINDOW_BY_PARK_2026_11_13.keys(), key=str):
    got_all, got_url = totals.get(park, (0, 0))
    then_all, then_url = WINDOW_BY_PARK_2026_11_13.get(park, (0, 0))
    print(f"{str(park):<10} {got_all:>9,} {then_all:>11,} {got_url:>9,} {then_url:>11,}")

low = spark.sql("SELECT month_key, files_ok, slots, coverage FROM gold_month_gkg WHERE coverage < 0.95 "
                "ORDER BY month_key").collect()
print(f"\nMonths with coverage < 0.95: {len(low)}")
for r in low:
    print(f"  {r.month_key}: {r.files_ok:,} of {r.slots:,} slots ({100 * r.coverage:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export: two aggregate CSVs for the news test
# MAGIC
# MAGIC Same rules as `04_export_gold` and `05_reservations`: one header row, NULL as an empty field, dates as
# MAGIC `YYYY-MM-DD`, whole numbers without `.0`, `\n` line ends, and a read-back check against the table.
# MAGIC - `gold_month_gkg.csv`: the 132 months, ordered by `month_key`.
# MAGIC - `gold_park_month_news.csv`: the 660 rows, ordered by `park_code`, `month_key`.
# MAGIC
# MAGIC `tests/check_gdelt_gold.py` recomputes both files from `data/landing` without Spark and compares them.
# MAGIC Safe to rerun: the files are overwritten.

# COMMAND ----------

INTEGER_TYPES = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)


def export(query: str, order_by: list[str], csv_name: str) -> None:
    """Write an aggregate query result to one CSV in the exports Volume, then read the file back."""
    df = spark.sql(query)
    pdf = df.toPandas().sort_values(order_by, ignore_index=True)
    # toPandas() turns an integer column with NULLs into floats; Int64 keeps whole numbers and writes NULL as ""
    for field in df.schema.fields:
        if isinstance(field.dataType, INTEGER_TYPES):
            pdf[field.name] = pdf[field.name].astype("Int64")
    path = EXPORTS / csv_name
    pdf.to_csv(path, index=False, lineterminator="\n")
    print(f"{len(pdf)} rows, {path.stat().st_size:,} bytes: {path}")

    # Read the file back as text: same columns in the same order, same row count,
    # and an empty field exactly where the table has NULL
    check = pd.read_csv(path, dtype=str, keep_default_na=False)
    null_counts = ", ".join(f"COUNT(*) - COUNT({c}) AS {c}" for c in df.columns)
    table_nulls = spark.sql(f"SELECT {null_counts} FROM ({query}) AS q").first().asDict()
    file_nulls = {c: int((check[c] == "").sum()) for c in check.columns}
    assert list(check.columns) == df.columns, "column names or order differ from the table"
    assert len(check) == df.count(), "row count differs from the table"
    assert file_nulls == table_nulls, f"NULL counts differ: file {file_nulls}, table {table_nulls}"
    print("Columns with NULLs (file = table):", {c: n for c, n in file_nulls.items() if n})


export("SELECT * FROM gold_month_gkg", ["month_key"], "gold_month_gkg.csv")
export("SELECT * FROM gold_park_month_news", ["park_code", "month_key"], "gold_park_month_news.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the files
# MAGIC
# MAGIC 1. In the left sidebar, click **Catalog**, then open **workspace > nps_tourism > Volumes > exports**.
# MAGIC 2. For `gold_month_gkg.csv` and `gold_park_month_news.csv`, click the kebab menu (three dots), then
# MAGIC    **Download file**. If the browser adds ` (1)` to a name, rename the file back.
# MAGIC 3. Keep the two files in a folder outside the repository until the news result is committed: `gdelt_sample.py`
# MAGIC    draw and score refuse a working tree with an untracked file. From the repository root, compare them with a
# MAGIC    recomputation from `data/landing` (the command of `tests/check_gdelt_gold.py`; it ends with
# MAGIC    `RESULT: both files match the recomputation`), with `<folder>` the folder that holds them:
# MAGIC
# MAGIC    ```
# MAGIC    uv run --no-project --python 3.12 --with pandas==2.2.3 --with numpy==2.1.3 --with duckdb==2.0.0 python -I tests/check_gdelt_gold.py --month-gkg <folder>/gold_month_gkg.csv --park-month-news <folder>/gold_park_month_news.csv
# MAGIC    ```
