# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Export: gold_park_month as one CSV file for Snowflake
# MAGIC
# MAGIC Writes `gold_park_month` to `/Volumes/workspace/nps_tourism/exports/gold_park_month.csv`,
# MAGIC the file that the Snowflake script `02_load_star.sql` loads:
# MAGIC - one header row, then one row per park x month, ordered by `park_code`, `month_key`
# MAGIC - NULL as an empty field, dates as `YYYY-MM-DD`
# MAGIC - whole numbers without `.0`
# MAGIC
# MAGIC Run `03_gold` first. Safe to rerun: the file is overwritten.

# COMMAND ----------

from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

TABLE = "workspace.nps_tourism.gold_park_month"
CSV_PATH = Path("/Volumes/workspace/nps_tourism/exports/gold_park_month.csv")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.nps_tourism.exports")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the CSV
# MAGIC
# MAGIC The table is small (660 rows), so it is converted to pandas and written as a single file.
# MAGIC `toPandas()` turns an integer column that contains NULLs into floats (`191781` becomes `191781.0`).
# MAGIC pandas `Int64` keeps whole numbers and writes NULL as an empty field.

# COMMAND ----------

INTEGER_TYPES = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)

gold = spark.table(TABLE)
pdf = gold.toPandas().sort_values(["park_code", "month_key"], ignore_index=True)

for field in gold.schema.fields:
    if isinstance(field.dataType, INTEGER_TYPES):
        pdf[field.name] = pdf[field.name].astype("Int64")

# NULL is written as an empty field. lineterminator="\n": the same bytes on every operating system.
pdf.to_csv(CSV_PATH, index=False, lineterminator="\n")
print(f"{len(pdf)} rows, {CSV_PATH.stat().st_size:,} bytes: {CSV_PATH}")

# COMMAND ----------

# Read the file back as text: same columns in the same order, same row count,
# and an empty field exactly where the table has NULL
check = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False)
table_nulls = gold.select([(F.count(F.lit(1)) - F.count(c)).alias(c) for c in gold.columns]).first().asDict()
file_nulls = {c: int((check[c] == "").sum()) for c in check.columns}

assert list(check.columns) == gold.columns, "column names or order differ from the table"
assert len(check) == gold.count(), "row count differs from the table"
assert file_nulls == table_nulls, f"NULL counts differ: file {file_nulls}, table {table_nulls}"
print("Columns with NULLs (file = table):", {c: n for c, n in file_nulls.items() if n})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the file
# MAGIC
# MAGIC 1. In the left sidebar, click **Catalog**, then open **workspace > nps_tourism > Volumes > exports**.
# MAGIC 2. Click the kebab menu (three dots) next to `gold_park_month.csv`, then **Download file**.
# MAGIC    If the browser saves it as `gold_park_month (1).csv`, rename it to `gold_park_month.csv`:
# MAGIC    `02_load_star.sql` loads only that name.
# MAGIC 3. Upload the file to the Snowflake stage as described at the end of `01_setup.sql`.
