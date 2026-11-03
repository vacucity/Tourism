# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: raw files -> all-string Delta tables
# MAGIC
# MAGIC Reads every pull under `/Volumes/workspace/nps_tourism/landing/<source>/pull_date=YYYY-MM-DD/`.
# MAGIC Spark turns each `pull_date=...` folder name into a `pull_date` column (partition discovery).
# MAGIC Every value is stored as text; two lineage columns record the source file and the load time.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LANDING = "/Volumes/workspace/nps_tourism/landing"
SOURCES = ["nps_visitation", "noaa_weather", "federal_holidays", "google_trends"]
spark.sql("USE workspace.nps_tourism")

# COMMAND ----------

# Pre-flight: each source folder must contain only pull_date=YYYY-MM-DD folders
for src in SOURCES:
    entries = dbutils.fs.ls(f"{LANDING}/{src}/")
    pulls = [e.name.rstrip("/") for e in entries if e.name.startswith("pull_date=")]
    stray = [e.name for e in entries if not e.name.startswith("pull_date=")]
    assert pulls, f"{src}: no pull_date=YYYY-MM-DD folder found"
    assert not stray, f"{src}: move these into a pull_date= folder: {stray}"
    print(src, "->", ", ".join(pulls))

# COMMAND ----------

def write_bronze(raw: DataFrame, table: str) -> None:
    """Every column as string + lineage columns, then overwrite the Delta table."""
    bronze = raw.select(
        *[F.col(f"`{c}`").cast("string").alias(c) for c in raw.columns],
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    (bronze.write.mode("overwrite")
        .option("overwriteSchema", "true")  # columns may change between runs
        .saveAsTable(table))
    print(f"{table}: {spark.table(table).count()} rows")

# COMMAND ----------

# FAILFAST: a broken file stops the run instead of loading as an all-NULL row.
# NPS and NOAA files are JSON arrays spread over many lines, so multiLine is required.
write_bronze(
    spark.read.option("multiLine", True).option("mode", "FAILFAST")
    .json(f"{LANDING}/nps_visitation/"),
    "bronze_nps_visitation",
)
write_bronze(
    spark.read.option("multiLine", True).option("mode", "FAILFAST")
    .json(f"{LANDING}/noaa_weather/"),
    "bronze_weather",
)

# CSV: the first row holds the column names; every value stays text.
write_bronze(
    spark.read.option("header", True).option("mode", "FAILFAST")
    .csv(f"{LANDING}/federal_holidays/"),
    "bronze_holidays",
)
write_bronze(
    spark.read.option("header", True).option("mode", "FAILFAST")
    .csv(f"{LANDING}/google_trends/"),
    "bronze_search_trends",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One row per Bronze table: row count and newest pull
# MAGIC SELECT 'bronze_nps_visitation' AS table_name, COUNT(*) AS row_count, MAX(pull_date) AS latest_pull FROM workspace.nps_tourism.bronze_nps_visitation
# MAGIC UNION ALL SELECT 'bronze_weather', COUNT(*), MAX(pull_date) FROM workspace.nps_tourism.bronze_weather
# MAGIC UNION ALL SELECT 'bronze_holidays', COUNT(*), MAX(pull_date) FROM workspace.nps_tourism.bronze_holidays
# MAGIC UNION ALL SELECT 'bronze_search_trends', COUNT(*), MAX(pull_date) FROM workspace.nps_tourism.bronze_search_trends