# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Reservations: private RIDB pulls -> Bronze, Silver, Gold and two aggregate CSVs
# MAGIC
# MAGIC Builds the reservation tables for the A2 part 2 forecast test (`docs/week8/a2_part2_spec.md`).
# MAGIC The input is the pulls that `src/extract_reservations.py` writes from MySQL, uploaded by hand to
# MAGIC `/Volumes/workspace/nps_tourism/landing/ridb_reservations/pull_date=YYYY-MM-DD/` (Parquet files and `_manifest.json`).
# MAGIC These files are row level and private under the RIDB terms ("publish aggregates only"). They are not in git,
# MAGIC this notebook shows counts only, and only the two aggregate CSVs at the end leave Databricks.
# MAGIC
# MAGIC | Table | Grain |
# MAGIC | --- | --- |
# MAGIC | `bronze_ridb_reservations` | one row per extracted row per pull |
# MAGIC | `silver_reservations` | one row per `reservation_key`, from the newest pull (row level, never exported) |
# MAGIC | `gold_facility_month_reservations` | one row per facility x family x stay-start month |
# MAGIC | `gold_park_month_reservations` | one row per park x month: the 660 keys of `gold_park_month` |
# MAGIC
# MAGIC Run `03_gold` first. Always use **Run all**: every cell relies on the first one.
# MAGIC Notebooks 00-04 never read these files, so they still run for anyone without them.

# COMMAND ----------

import json
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.sql("USE workspace.nps_tourism")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.nps_tourism.exports")
LANDING = "/Volumes/workspace/nps_tourism/landing/ridb_reservations"
EXPORTS = Path("/Volumes/workspace/nps_tourism/exports")
PRIVATE = "The files come from src/extract_reservations.py and are private under the RIDB terms"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight: the uploaded files match their manifests
# MAGIC
# MAGIC Nothing is written until every check passes.
# MAGIC - `ridb_reservations/` exists and holds only `pull_date=YYYY-MM-DD` folders, as in `01_bronze`.
# MAGIC - Each pull folder holds its `_manifest.json` and exactly the Parquet files the manifest lists, with the same byte sizes.
# MAGIC - Each manifest names the pull before it (`previous.pull_date`; null for a full pull), and that must be the folder
# MAGIC   just before it. A pull that was never uploaded breaks this chain.

# COMMAND ----------

assert Path(LANDING).is_dir(), (
    f"{LANDING} does not exist: create it in the landing Volume and upload each pull_date= folder into it. {PRIVATE}")
entries = dbutils.fs.ls(f"{LANDING}/")
pulls = sorted(e.name.rstrip("/") for e in entries if e.name.startswith("pull_date="))  # oldest first
stray = [e.name for e in entries if not e.name.startswith("pull_date=")]
assert pulls, f"{LANDING}: no pull_date=YYYY-MM-DD folder found. {PRIVATE}"
assert not stray, f"{LANDING}: only pull_date= folders may be here, move these: {stray}"

manifests = {}  # pull folder name -> its manifest, oldest first
for i, pull in enumerate(pulls):
    folder = Path(LANDING) / pull
    manifest_file = folder / "_manifest.json"
    assert manifest_file.is_file(), f"{pull}: _manifest.json is missing, upload it with the Parquet files. {PRIVATE}"
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    assert f"pull_date={manifest['pull_date']}" == pull, f"{pull}: this manifest belongs to pull_date={manifest['pull_date']}"

    listed = {f["name"]: f["bytes"] for f in manifest["files"]}
    found = {p.name: p.stat().st_size for p in folder.iterdir() if p.name != "_manifest.json"}
    missing = sorted(listed.keys() - found.keys())
    unlisted = sorted(found.keys() - listed.keys())
    wrong_size = sorted(name for name in listed.keys() & found.keys() if found[name] != listed[name])
    assert not missing, f"{pull}: listed in the manifest but not uploaded: {missing}. {PRIVATE}"
    assert not unlisted, f"{pull}: not in the manifest, delete these: {unlisted}"
    assert not wrong_size, f"{pull}: size differs from the manifest, upload these again: {wrong_size}"

    previous = manifest["previous"]["pull_date"] if manifest["previous"] else None
    before = pulls[i - 1].removeprefix("pull_date=") if i else None
    assert previous == before, (
        f"{pull}: its manifest follows pull {previous}, but the folder before it is {before}. "
        f"Upload every pull in order; after a --full pull, delete the older folders. {PRIVATE}")
    manifests[pull] = manifest
    print(f"{pull}: {manifest['mode']} pull, {manifest['rows_new']:,} new + {manifest['rows_changed']:,} changed rows "
          f"in {len(listed)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: bronze_ridb_reservations
# MAGIC
# MAGIC Reads every pull at once. Spark turns each `pull_date=...` folder name into a `pull_date` DATE column and
# MAGIC skips `_manifest.json` (file names that start with `_` are not data).
# MAGIC The Parquet types are kept: the all-string rule in `01_bronze` exists because CSV and JSON types are guessed,
# MAGIC and the extract writes an explicit schema.

# COMMAND ----------

raw = spark.read.parquet(LANDING)
(raw.select("*", F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingested_at"))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_ridb_reservations"))

# COMMAND ----------

# Gate: every file loaded the rows its manifest lists, so each pull holds rows_new + rows_changed
loaded = {(str(r.pull_date), r["_source_file"].rsplit("/", 1)[1]): r.n for r in spark.sql("""
    SELECT pull_date, _source_file, COUNT(*) AS n
    FROM bronze_ridb_reservations
    GROUP BY pull_date, _source_file""").collect()}
manifest_rows = {(m["pull_date"], f["name"]): f["rows"] for m in manifests.values() for f in m["files"]}
assert loaded == manifest_rows, (
    f"rows per file differ from the manifests: {sorted(set(loaded.items()) ^ set(manifest_rows.items()))}")
for m in manifests.values():
    rows = sum(n for (pull_date, _), n in loaded.items() if pull_date == m["pull_date"])
    assert rows == m["rows_new"] + m["rows_changed"], f"pull_date={m['pull_date']}: {rows:,} rows in Bronze"
    print(f"bronze_ridb_reservations, pull_date={m['pull_date']}: {rows:,} rows = manifest rows_new + rows_changed")
print("For information: the first pull of FY2015-FY2025 holds 7,586,234 rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: silver_reservations, one row per reservation_key
# MAGIC
# MAGIC - The same key can arrive in several pulls: the newest pull wins, as in `02_silver`.
# MAGIC   Within one pull a key is in one file only: new keys lie above the previous pull's largest key, changed keys at or below it.
# MAGIC - `family` sorts every inventory type into one program family. `Site` (FY2015-FY2018 files) and `CAMPING` (FY2019+)
# MAGIC   are the same campsites, so both are `camping`, unless the product's use type is `Day`.
# MAGIC   Five facilities sell tickets or parking under other types and get their own family first.
# MAGIC - `facility_name` loses backslashes and line breaks, so a name stays on one line of a CSV.
# MAGIC   The FY2018 file gives two names these characters (Mather and Big Creek campgrounds).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_reservations
# MAGIC COMMENT 'Grain: one row per reservation_key (newest pull). Row level, private: never exported'
# MAGIC AS
# MAGIC SELECT reservation_key, fiscal_year, park_code, facility_id,
# MAGIC        trim(translate(facility_name, chr(92) || chr(10) || chr(13), '')) AS facility_name,
# MAGIC        product_id, use_type, inventory_type,
# MAGIC        start_date, end_date, number_of_people, total_paid, ordered_at, ordered_date_utc,
# MAGIC        CASE
# MAGIC          -- Mariposa Grove bus parking, then GRSM Firefly and Eclipse events (their lotteries stay lotteries)
# MAGIC          WHEN facility_id IN ('274348', '10112471') THEN 'bus_parking'
# MAGIC          WHEN facility_id IN ('233374', '258488', '255304') AND inventory_type NOT LIKE '%LOTTERY%' THEN 'event'
# MAGIC          WHEN inventory_type IN ('Site', 'CAMPING') AND COALESCE(use_type, '') <> 'Day' THEN 'camping'
# MAGIC          WHEN inventory_type IN ('Site', 'CAMPING') THEN 'dayuse_site'
# MAGIC          WHEN inventory_type IN ('PERMIT', 'Entrance', 'ERR') THEN 'permit'
# MAGIC          WHEN inventory_type IN ('LOTTERY', 'LOTTERY_PERMIT', 'LOTTERY_TICKET', 'LOTTERY_CAMPING', 'QUEUE_LOTTERY') THEN 'lottery'
# MAGIC          WHEN inventory_type IN ('TICKET', 'TIMED_ENTRY') THEN 'entry_ticket'
# MAGIC          WHEN inventory_type = 'ACTIVITYPASS' THEN 'activity_pass'
# MAGIC          WHEN inventory_type = 'POS' THEN 'pos'
# MAGIC          ELSE 'other'
# MAGIC        END AS family,
# MAGIC        updated_at, order_updated_at, pull_date, _source_file
# MAGIC FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY reservation_key ORDER BY pull_date DESC, _source_file DESC) AS rn
# MAGIC       FROM bronze_ridb_reservations)
# MAGIC WHERE rn = 1;

# COMMAND ----------

# Gate ("row counts match"): Silver equals the newest manifest's snapshot of MySQL,
# in total and in every (fiscal_year, park_code) cell
snapshot = manifests[pulls[-1]]["snapshot"]
rows, keys = spark.sql("SELECT COUNT(*), COUNT(DISTINCT reservation_key) FROM silver_reservations").first()
assert rows == keys == snapshot["reservation_rows"], (
    f"silver_reservations: {rows:,} rows, {keys:,} keys; newest manifest: {snapshot['reservation_rows']:,}")

cells = {(r.fiscal_year, r.park_code): r.n for r in spark.sql("""
    SELECT fiscal_year, park_code, COUNT(*) AS n
    FROM silver_reservations
    GROUP BY fiscal_year, park_code""").collect()}
manifest_cells = {(c["fiscal_year"], c["park_code"]): c["rows"] for c in snapshot["rows_by_fy_park"]}
differ = sorted((k for k in cells.keys() | manifest_cells.keys() if cells.get(k) != manifest_cells.get(k)), key=str)
assert not differ, f"(fiscal_year, park_code) counts differ from the newest manifest: {differ}"
print(f"silver_reservations: {rows:,} rows = {keys:,} distinct keys = newest manifest snapshot")
print(f"(fiscal_year, park_code) counts: {len(cells)} of {len(manifest_cells)} equal the newest manifest")

# COMMAND ----------

# For information: the values measured on FY2015-FY2025 (docs/week8/a2_full_checks.txt and the zip reconstruction).
# A later pull changes them, so they are printed, not asserted.
FY2015_2025_BY_YEAR = {2015: 249_467, 2016: 252_277, 2017: 255_657, 2018: 265_056, 2019: 344_820, 2020: 621_336,
                       2021: 1_109_964, 2022: 1_070_554, 2023: 878_807, 2024: 1_290_959, 2025: 1_247_337}
FY2015_2025_BY_PARK = {"GRCA": 749_650, "GRSM": 1_073_559, "YELL": 226_567, "YOSE": 3_621_247, "ZION": 1_915_211}

by_year = {}
by_park = {}
for (fiscal_year, park_code), n in cells.items():
    by_year[fiscal_year] = by_year.get(fiscal_year, 0) + n
    by_park[park_code] = by_park.get(park_code, 0) + n
print("fiscal_year      silver   FY2015-FY2025")
for fiscal_year in sorted(by_year.keys() | FY2015_2025_BY_YEAR.keys()):
    print(f"{fiscal_year:>11} {by_year.get(fiscal_year, 0):>11,} {FY2015_2025_BY_YEAR.get(fiscal_year, 0):>15,}")
print("park_code        silver   FY2015-FY2025")
for park_code in sorted(by_park.keys() | FY2015_2025_BY_PARK.keys(), key=str):
    print(f"{str(park_code):>11} {by_park.get(park_code, 0):>11,} {FY2015_2025_BY_PARK.get(park_code, 0):>15,}")

null_park, other, null_start = spark.sql("""
    SELECT COUNT_IF(park_code IS NULL), COUNT_IF(family = 'other'), COUNT_IF(start_date IS NULL)
    FROM silver_reservations""").first()
print(f"park_code NULL: {null_park:,} (expected 0)")
print(f"family 'other': {other:,} (expected 0: every inventory type has a family)")
print(f"start_date NULL: {null_start:,} (FY2015-FY2025: 48,477 POS and Site rows from FY2015-FY2017; Gold leaves them out)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1: gold_facility_month_reservations
# MAGIC
# MAGIC Counts per facility x family x stay-start month. Each reservation counts once, in the month its stay starts.
# MAGIC - `stays`: all reservations; `paid0_stays`: those with `total_paid = 0` (closures show up here).
# MAGIC - `otb` (on the books): ordered before 00:00 UTC on day 1 of the month.
# MAGIC   `ordered_date_utc` is the UTC date of `ordered_at`, so `ordered_date_utc < day 1` is the same cut,
# MAGIC   and no session time zone enters it.
# MAGIC - `first_order_date`: the earliest order in the group. Gold 2 uses it to find each park's first camping order.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_facility_month_reservations
# MAGIC COMMENT 'Grain: one row per facility x family x stay-start month'
# MAGIC AS
# MAGIC SELECT park_code, facility_id, ANY_VALUE(facility_name) AS facility_name, family,
# MAGIC        YEAR(start_date) * 100 + MONTH(start_date) AS month_key,
# MAGIC        COUNT(*) AS stays,
# MAGIC        COUNT_IF(total_paid = 0) AS paid0_stays,
# MAGIC        COUNT_IF(ordered_date_utc < make_date(YEAR(start_date), MONTH(start_date), 1)) AS otb,
# MAGIC        MIN(ordered_date_utc) AS first_order_date
# MAGIC FROM silver_reservations
# MAGIC WHERE start_date IS NOT NULL
# MAGIC GROUP BY park_code, facility_id, family, YEAR(start_date) * 100 + MONTH(start_date);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2: gold_park_month_reservations, one row per key of gold_park_month
# MAGIC
# MAGIC The rows come from `gold_park_month` itself, so `06_forecast_test` can join the two tables 1:1 on `(park_code, month_key)`.
# MAGIC - `camping_otb`: the test's signal. Overnight camping reservations for stays starting in the month, booked before day 1.
# MAGIC - `camping_stays`, `camping_paid0_stays`: all camping stays starting in the month, and those with `total_paid = 0`.
# MAGIC - `all_otb`, `all_stays`: the same over every family.
# MAGIC - `otb_complete = 1` from the park's first camping order month + 12 months through September of the newest fiscal year.
# MAGIC   Before that, part of the bookings were made before the files begin. The FY2019+ files are split by stay start,
# MAGIC   so the newest file ends with stays starting in September.
# MAGIC   A park with no camping rows keeps its months, with `otb_complete = 0`, so the table always has the 660 keys.
# MAGIC - Months after that September have NULL counts: their stays are in a fiscal-year file that is not published yet.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_park_month_reservations
# MAGIC COMMENT 'Grain: one row per park x month (keys of gold_park_month). A reservation counts once, in its stay-start month. On the books = ordered before 00:00 UTC on day 1'
# MAGIC AS
# MAGIC WITH coverage AS (   -- first complete month = month of the park's first camping order + 12 months
# MAGIC   SELECT park_code, (YEAR(MIN(first_order_date)) + 1) * 100 + MONTH(MIN(first_order_date)) AS first_complete_key
# MAGIC   FROM gold_facility_month_reservations WHERE family = 'camping' GROUP BY park_code
# MAGIC ),
# MAGIC published AS (       -- FY2019+ files are split by start date: the newest file ends with starts in September
# MAGIC   SELECT MAX(fiscal_year) * 100 + 9 AS last_key FROM silver_reservations
# MAGIC ),
# MAGIC by_park AS (
# MAGIC   SELECT park_code, month_key,
# MAGIC          SUM(CASE WHEN family = 'camping' THEN otb ELSE 0 END)         AS camping_otb,
# MAGIC          SUM(CASE WHEN family = 'camping' THEN stays ELSE 0 END)       AS camping_stays,
# MAGIC          SUM(CASE WHEN family = 'camping' THEN paid0_stays ELSE 0 END) AS camping_paid0_stays,
# MAGIC          SUM(otb) AS all_otb, SUM(stays) AS all_stays
# MAGIC   FROM gold_facility_month_reservations GROUP BY park_code, month_key
# MAGIC )
# MAGIC SELECT g.park_code, g.month_key, g.month_start,
# MAGIC        CASE WHEN g.month_key BETWEEN c.first_complete_key AND p.last_key THEN 1 ELSE 0 END AS otb_complete,
# MAGIC        CASE WHEN g.month_key <= p.last_key THEN COALESCE(b.camping_otb, 0) END         AS camping_otb,
# MAGIC        CASE WHEN g.month_key <= p.last_key THEN COALESCE(b.camping_stays, 0) END       AS camping_stays,
# MAGIC        CASE WHEN g.month_key <= p.last_key THEN COALESCE(b.camping_paid0_stays, 0) END AS camping_paid0_stays,
# MAGIC        CASE WHEN g.month_key <= p.last_key THEN COALESCE(b.all_otb, 0) END             AS all_otb,
# MAGIC        CASE WHEN g.month_key <= p.last_key THEN COALESCE(b.all_stays, 0) END           AS all_stays
# MAGIC FROM gold_park_month g
# MAGIC LEFT JOIN coverage c ON c.park_code = g.park_code   -- a park with no camping rows keeps its months, otb_complete 0
# MAGIC CROSS JOIN published p
# MAGIC LEFT JOIN by_park b ON b.park_code = g.park_code AND b.month_key = g.month_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold checkpoints

# COMMAND ----------

# Gate: one row per key of gold_park_month and no key on one side only, so the join in 06 is 1:1
rows = spark.sql("SELECT COUNT(*) FROM gold_park_month_reservations").first()[0]
keys = spark.sql("SELECT COUNT(*) FROM (SELECT DISTINCT park_code, month_key FROM gold_park_month_reservations) AS k").first()[0]
base = spark.table("gold_park_month").count()
joined, only_in_gold_park_month, only_in_reservations = spark.sql("""
    SELECT COUNT(*), COUNT_IF(r.park_code IS NULL), COUNT_IF(g.park_code IS NULL)
    FROM gold_park_month AS g
    FULL OUTER JOIN gold_park_month_reservations AS r
      ON r.park_code = g.park_code AND r.month_key = g.month_key""").first()
assert rows == keys == base == joined, f"rows {rows}, distinct keys {keys}, gold_park_month {base}, joined {joined}"
assert only_in_gold_park_month == only_in_reservations == 0, (
    f"keys on one side only: {only_in_gold_park_month} in gold_park_month, {only_in_reservations} here")
print(f"gold_park_month_reservations: {rows} rows = {keys} distinct keys = the {base} rows of gold_park_month; "
      f"0 keys on one side only")

# Each reservation with a start date counts once in Gold 1, and on-the-books stays are a subset of all stays
stays, dated = spark.sql("""
    SELECT (SELECT SUM(stays) FROM gold_facility_month_reservations),
           (SELECT COUNT(*) FROM silver_reservations WHERE start_date IS NOT NULL)""").first()
assert stays == dated, f"Gold 1 counts {stays:,} stays, Silver has {dated:,} rows with a start date"
over = spark.sql("SELECT COUNT_IF(camping_otb > camping_stays) FROM gold_park_month_reservations").first()[0]
assert over == 0, f"{over} rows with camping_otb > camping_stays"
print(f"gold_facility_month_reservations: {stays:,} stays = Silver rows with a start date; camping_otb <= camping_stays in every row")

# COMMAND ----------

# For information: complete months and NULL rows per park. FY2015-FY2025 gives 120 complete months
# (2015-10..2025-09) each for GRCA, GRSM, YOSE and ZION, 43 for YELL (2022-03..2025-09: its campgrounds
# joined Recreation.gov in 2021), 523 in all, and 15 NULL rows (2025-10..2025-12 in each park).
coverage = spark.sql("""
    SELECT park_code,
           SUM(otb_complete) AS complete_months,
           MIN(CASE WHEN otb_complete = 1 THEN month_key END) AS first_complete,
           MAX(CASE WHEN otb_complete = 1 THEN month_key END) AS last_complete,
           COUNT_IF(camping_otb IS NULL) AS null_rows
    FROM gold_park_month_reservations
    GROUP BY park_code
    ORDER BY park_code""").collect()
print("park_code  complete_months  first_complete  last_complete  null_rows")
for r in coverage:
    print(f"{r.park_code:>9} {r.complete_months:>16} {str(r.first_complete):>15} {str(r.last_complete):>14} {r.null_rows:>10}")
print(f"{'all':>9} {sum(r.complete_months for r in coverage):>16} {'':>15} {'':>14} {sum(r.null_rows for r in coverage):>10}")

# Anchor months from the zip reconstruction of FY2015-FY2025: (camping_otb, camping_stays).
# The first three show that camping counts continue across the vendor change of October 2018.
# MySQL gives 200 rows the order time of an earlier file, so a small difference can come from those rows.
ANCHORS = {("YOSE", 201710): (6_279, 9_078), ("YOSE", 201810): (6_873, 9_218), ("GRCA", 201810): (4_820, 7_286),
           ("GRCA", 202307): (6_121, 8_878), ("GRSM", 202206): (8_038, 11_824), ("YELL", 202307): (2_540, 3_499),
           ("YOSE", 202307): (9_162, 15_849), ("ZION", 202307): (3_773, 5_684)}
gold = {(r.park_code, r.month_key): (r.camping_otb, r.camping_stays) for r in spark.sql(
    "SELECT park_code, month_key, camping_otb, camping_stays FROM gold_park_month_reservations").collect()}


def count_text(value) -> str:
    """A count with thousands separators; NULL for a month after the newest fiscal year."""
    return "NULL" if value is None else f"{value:,}"


print("\npark_code  month_key  camping_otb  reconstruction  camping_stays  reconstruction")
for (park_code, month_key), (otb, stays) in ANCHORS.items():
    got_otb, got_stays = gold[(park_code, month_key)]
    print(f"{park_code:>9} {month_key:>10} {count_text(got_otb):>12} {otb:>15,} {count_text(got_stays):>14} {stays:>15,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export: two aggregate CSVs for the forecast test
# MAGIC
# MAGIC Same rules as `04_export_gold`: one header row, NULL as an empty field, dates as `YYYY-MM-DD`,
# MAGIC whole numbers without `.0`, `\n` line ends, and a read-back check against the table.
# MAGIC - `gold_park_month_reservations.csv`: all 660 rows, ordered by `park_code`, `month_key`.
# MAGIC - `gold_facility_month_camping.csv`: the camping rows of `gold_facility_month_reservations`, one per campground x month,
# MAGIC   without `first_order_date` (in a campground-month with one stay it would be that reservation's order date).
# MAGIC   The fixed-campground secondary check (S1) reads it.
# MAGIC
# MAGIC Both hold counts per park or campground and month, which the RIDB terms allow to publish.
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


export("SELECT * FROM gold_park_month_reservations",
       ["park_code", "month_key"], "gold_park_month_reservations.csv")
export("SELECT park_code, facility_id, facility_name, family, month_key, stays, paid0_stays, otb "
       "FROM gold_facility_month_reservations WHERE family = 'camping'",
       ["park_code", "facility_id", "month_key"], "gold_facility_month_camping.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the files
# MAGIC
# MAGIC 1. In the left sidebar, click **Catalog**, then open **workspace > nps_tourism > Volumes > exports**.
# MAGIC 2. For `gold_park_month_reservations.csv` and `gold_facility_month_camping.csv`, click the kebab menu (three dots),
# MAGIC    then **Download file**. If the browser adds ` (1)` to a name, rename the file back.
# MAGIC
# MAGIC `bronze_ridb_reservations` and `silver_reservations` are row level: they stay in this workspace and are never exported.
