# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Forecast test: the pre-registered A2 part 2 test, run once
# MAGIC
# MAGIC Do camping reservations on the books at the start of a month lower the error of a seasonal baseline forecast
# MAGIC of monthly visits? The rules are fixed in `docs/week8/a2_part2_spec.md`, and the code is `src/a2_forecast.py`.
# MAGIC This notebook:
# MAGIC 1. checks the gate: Silver must have the row counts in the newest extract's `_manifest.json`, in total and per
# MAGIC    (fiscal_year, park_code);
# MAGIC 2. reads the 660 park x month rows and the campground x month camping counts;
# MAGIC 3. runs the test and prints the settings, training facts, primary table, verdict and secondary checks;
# MAGIC 4. writes the Delta tables `a2_forecast_summary` and `a2_forecast_monthly`, and the CSV files
# MAGIC    `a2_inputs_park_month.csv`, `a2_forecast_summary.csv` and `a2_forecast_monthly.csv` to the exports Volume.
# MAGIC
# MAGIC Run `05_reservations` first, then use **Run all** once. The code is deterministic: a rerun gives the same
# MAGIC numbers.

# COMMAND ----------

import hashlib
import importlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import types as T

# The working directory of a notebook is the folder that contains it: <repo>/databricks/notebooks
ROOT = Path.cwd().parents[1]
MODULE = ROOT / "src" / "a2_forecast.py"
assert MODULE.is_file(), f"Not found: {MODULE}. Open this notebook from the Git folder."
sys.path.insert(0, str(ROOT / "src"))
import a2_forecast  # noqa: E402

importlib.reload(a2_forecast)  # a session opened before the last Pull would otherwise keep the old code

LANDING = Path("/Volumes/workspace/nps_tourism/landing/ridb_reservations")
EXPORTS = Path("/Volumes/workspace/nps_tourism/exports")
spark.sql("USE workspace.nps_tourism")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.nps_tourism.exports")

print("pandas", pd.__version__, "| numpy", np.__version__)
print("a2_forecast.py sha256", hashlib.sha256(MODULE.read_bytes()).hexdigest())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate: row counts match
# MAGIC
# MAGIC The test is scored only if `silver_reservations` holds the reservations of the newest pull: the same total
# MAGIC and the same count in every (fiscal_year, park_code) cell as `snapshot` in that pull's `_manifest.json`.
# MAGIC The extract counted them in MySQL inside the snapshot it exported. If an assertion fails, nothing is scored.

# COMMAND ----------

manifests = sorted(LANDING.glob("pull_date=*/_manifest.json"))  # pull_date=YYYY-MM-DD sorts by date
assert manifests, (f"No pull with a _manifest.json under {LANDING}: the files come from "
                   "src/extract_reservations.py and are private under the RIDB terms")
newest = manifests[-1]
snapshot = json.loads(newest.read_text(encoding="utf-8"))["snapshot"]
in_manifest = {(c["fiscal_year"], c["park_code"]): c["rows"] for c in snapshot["rows_by_fy_park"]}

silver = spark.sql("""
    SELECT fiscal_year, park_code, COUNT(*) AS n
    FROM silver_reservations
    GROUP BY fiscal_year, park_code
""").toPandas()
in_silver = {(int(fy), park): int(n) for fy, park, n in silver.itertuples(index=False)}
differ = {cell: (in_silver.get(cell, 0), in_manifest.get(cell, 0))
          for cell in sorted(in_silver.keys() | in_manifest.keys())
          if in_silver.get(cell, 0) != in_manifest.get(cell, 0)}

print("Newest pull:", newest.parent.name)
print(f"Rows: silver_reservations {sum(in_silver.values()):,}, manifest snapshot {snapshot['reservation_rows']:,}")
print(f"(fiscal_year, park_code) cells: Silver {len(in_silver)}, manifest {len(in_manifest)}, differing {len(differ)}")
print("For information, the FY2015-FY2025 load (docs/week8/a2_full_checks.txt): 7,586,234 reservations in 55 cells")
assert sum(in_silver.values()) == snapshot["reservation_rows"], "Silver total differs from the manifest: not scored"
assert not differ, f"Cells that differ (Silver, manifest): {differ}"
print("GATE PASSED: row counts match")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs
# MAGIC
# MAGIC - Park x month: `gold_park_month` joined with `gold_park_month_reservations`. The reservation table is built on
# MAGIC   the keys of `gold_park_month`, so all 660 rows must come back.
# MAGIC - Campground x month: the camping rows of `gold_facility_month_reservations`, for secondary check S1.
# MAGIC
# MAGIC Both are aggregates. Row-level reservations stay in Silver.

# COMMAND ----------

inputs = spark.sql("""
    SELECT park_code, month_key, g.month_start, g.visitors,
           r.otb_complete, r.camping_otb, r.camping_stays, r.camping_paid0_stays, r.all_otb, r.all_stays
    FROM gold_park_month g
    JOIN gold_park_month_reservations r USING (park_code, month_key)
""")
park_month = inputs.toPandas().sort_values(["park_code", "month_key"], ignore_index=True)
assert len(park_month) == 660, f"{len(park_month)} rows: the join must keep all 660 rows of gold_park_month"

camping_fac = spark.sql("""
    SELECT park_code, facility_id, facility_name, month_key, stays, otb
    FROM gold_facility_month_reservations
    WHERE family = 'camping'
""").toPandas()
print(f"park x month: {len(park_month)} rows; campground x month (camping): {len(camping_fac):,} rows, "
      f"{camping_fac['facility_id'].nunique()} campgrounds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The test
# MAGIC
# MAGIC Settings, training facts, the primary table, the verdict, then the secondary checks (never used for pass/fail).

# COMMAND ----------

result = a2_forecast.run(park_month, camping_fac)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta tables
# MAGIC
# MAGIC - `a2_forecast_summary`: one row per check x measure x park. `check` is `primary` or S1-S8; `error_b` and
# MAGIC   `error_r` are the errors of B and R; `r_lower` applies the tie rule; `months_r_lower` counts the scored
# MAGIC   months in which R's error is lower.
# MAGIC - `a2_forecast_monthly`: actual visits, B and R for every park and month from 2015-01 to 2025-09, all from the
# MAGIC   same fixed parameters. `period` is `train` for the 98 fitted months, `closed` for 2020-03..2020-12 (left out
# MAGIC   of the fit) and `held_out` for the scored months; `ape_b` and `ape_r` are filled for the held-out months only.

# COMMAND ----------

for name in ["summary", "monthly"]:
    table = f"a2_forecast_{name}"
    (spark.createDataFrame(result[name]).write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table))
    print(f"{table}: {spark.table(table).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV files
# MAGIC
# MAGIC The rules of `04_export_gold`: one header row, NULL as an empty field, whole numbers without `.0`, `\n` line
# MAGIC ends, then a read-back check. The summary and monthly files are written from the same pandas frames as the
# MAGIC Delta tables. `a2_inputs_park_month.csv` holds the 660 input rows, so anyone can rerun the test with
# MAGIC `src/a2_forecast.py`.

# COMMAND ----------

INTEGER_TYPES = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)


def write_csv(pdf: pd.DataFrame, name: str) -> None:
    """Write one CSV file, then read it back as text: same columns, same rows, an empty field exactly at NULL."""
    path = EXPORTS / name
    pdf.to_csv(path, index=False, lineterminator="\n")
    check = pd.read_csv(path, dtype=str, keep_default_na=False)
    file_nulls = {c: int((check[c] == "").sum()) for c in check.columns}
    table_nulls = {c: int(pdf[c].isna().sum()) for c in pdf.columns}
    assert list(check.columns) == list(pdf.columns), f"{name}: column names or order differ from the table"
    assert len(check) == len(pdf), f"{name}: row count differs from the table"
    assert file_nulls == table_nulls, f"{name}: NULL counts differ: file {file_nulls}, table {table_nulls}"
    print(f"{len(pdf)} rows, {path.stat().st_size:,} bytes: {path}")
    print("  columns with NULLs (file = table):", {c: n for c, n in file_nulls.items() if n})


# toPandas() turns an integer column that contains NULLs into floats; pandas Int64 keeps whole numbers
inputs_pdf = park_month.copy()
for field in inputs.schema.fields:
    if isinstance(field.dataType, INTEGER_TYPES):
        inputs_pdf[field.name] = inputs_pdf[field.name].astype("Int64")

write_csv(inputs_pdf, "a2_inputs_park_month.csv")
write_csv(result["summary"], "a2_forecast_summary.csv")
write_csv(result["monthly"], "a2_forecast_monthly.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the files
# MAGIC
# MAGIC 1. In the left sidebar, click **Catalog**, then open **workspace > nps_tourism > Volumes > exports**.
# MAGIC 2. For `a2_inputs_park_month.csv`, `a2_forecast_summary.csv` and `a2_forecast_monthly.csv`: click the kebab
# MAGIC    menu (three dots), then **Download file**. Keep the names exactly as they are.
# MAGIC 3. Copy the whole output of this notebook as well: the gate, settings, training facts, primary table,
# MAGIC    verdict and secondary checks.
