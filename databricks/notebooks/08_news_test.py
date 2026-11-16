# Databricks notebook source
# MAGIC %md
# MAGIC # 08 News test: the pre-registered news forecast test, run once
# MAGIC
# MAGIC Does news coverage of the month before lower the error of a seasonal baseline forecast of monthly visits?
# MAGIC The model and the scoring are A2's (`docs/week8/a2_part2_spec.md`) with the news signal swapped in. The code is
# MAGIC `src/news_forecast.py`, which imports `src/a2_forecast.py`. This notebook:
# MAGIC 1. reads the A1 part 2 precision flags from the score file `docs/week8/a1_part2_score.json`, written by a
# MAGIC    guarded `score`; stops without it;
# MAGIC 2. checks the gate: `gold_park_month_news` has one row per key of `gold_park_month`, and NULLs only in 2015-01;
# MAGIC 3. joins `gold_park_month` (visits), `gold_park_month_news` and `gold_park_month_reservations` into the 660
# MAGIC    input rows;
# MAGIC 4. runs the test and prints the settings, the URL rule's yearly pass rate, training facts, primary table,
# MAGIC    verdict, precision marks and secondary checks;
# MAGIC 5. writes the Delta tables `news_forecast_summary` and `news_forecast_monthly`, and the CSV files
# MAGIC    `news_inputs_park_month.csv`, `news_forecast_summary.csv` and `news_forecast_monthly.csv` to the exports
# MAGIC    Volume.
# MAGIC
# MAGIC Run `05_reservations` and `07_gdelt_news` first, and only after the A1 part 2 result is committed and pulled
# MAGIC into this Git folder. Then use **Run all** once. The code is deterministic: a rerun gives the same numbers.

# COMMAND ----------

import hashlib
import importlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import types as T

# The working directory of a notebook is the folder that contains it: <repo>/databricks/notebooks
ROOT = Path.cwd().parents[1]
MODULE = ROOT / "src" / "news_forecast.py"
A2_MODULE = ROOT / "src" / "a2_forecast.py"
assert MODULE.is_file() and A2_MODULE.is_file(), f"Not found: {MODULE}. Open this notebook from the Git folder."
sys.path.insert(0, str(ROOT / "src"))
import a2_forecast  # noqa: E402
import news_forecast  # noqa: E402

# A session opened before the last Pull would otherwise keep the old code. a2_forecast first: news_forecast uses it.
importlib.reload(a2_forecast)
importlib.reload(news_forecast)

# The precision flags are the score file of A1 part 2 (src/gdelt_sample.py score), committed with its result.
# tests/run_notebook_local.py defines FLAGS, EXPORTS and TEST_FLAGS before it runs this cell, so the same notebook
# runs on a laptop with stand-in tables; on Databricks they always take the values below. TEST_FLAGS (tests only)
# also accepts the short test form of the flags or a test score, and then requires synthetic visits from 2024-01.
FLAGS = Path(globals().get("FLAGS", ROOT / "docs" / "week8" / "a1_part2_score.json"))
EXPORTS = Path(globals().get("EXPORTS", "/Volumes/workspace/nps_tourism/exports"))
TEST_FLAGS = globals().get("TEST_FLAGS", False) is True
spark.sql("USE workspace.nps_tourism")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.nps_tourism.exports")

print("pandas", pd.__version__, "| numpy", np.__version__)
print("news_forecast.py sha256", hashlib.sha256(MODULE.read_bytes()).hexdigest())
print("a2_forecast.py sha256  ", hashlib.sha256(A2_MODULE.read_bytes()).hexdigest())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Precision flags from A1 part 2
# MAGIC
# MAGIC `src/gdelt_sample.py score` writes `docs/week8/a1_part2_score.json` from the committed labels. This notebook
# MAGIC reads three of its values: the pooled precision `P` of the URL rule, `pass` (P reaches 0.90), and per park
# MAGIC `parks.<park>.below_bar` (at most 32 of its 40 sampled rows about the park). The file must come from a guarded
# MAGIC score (`git_guard` true, with its `commits`); a test score is refused. The flags mark the result; they never
# MAGIC change it.

# COMMAND ----------

assert FLAGS.is_file(), (
    f"Not found: {FLAGS}. The news test runs only after the A1 part 2 precision result: commit the labels, run "
    "`uv run python src/gdelt_sample.py score`, commit docs/week8/a1_part2_score.json with the result "
    "(docs/week8/a1_part2_result.md), Pull this Git folder, then run this notebook again. Nothing was scored.")
flags = news_forecast.read_flags(FLAGS, score_file=not TEST_FLAGS)
if TEST_FLAGS:
    print(f"TEST FLAGS from {FLAGS} (tests/run_notebook_local.py --test-flags): the precision marks of this run "
          "are not a result")
print(f"{FLAGS.name} sha256", hashlib.sha256(FLAGS.read_bytes()).hexdigest())
print(f"Pooled precision P = {flags['P']:.4f}, meets the {news_forecast.PRECISION_BAR:.2f} bar: {flags['pass']}")
print("Parks below the bar:", [park for park, below in flags["below_bar"].items() if below] or "none")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate: the news table has the 660 keys and the expected NULLs
# MAGIC
# MAGIC The test is scored only if `gold_park_month_news` has one row per key of `gold_park_month`, with no key on one
# MAGIC side only, and its four news columns are NULL exactly in the 5 rows of 2015-01 (20 cells): GDELT has no file
# MAGIC before 2015-02-18. If an assertion fails, nothing is scored.

# COMMAND ----------

NEWS_COLUMNS = ["news_all", "news_url", "news_all_per_million", "news_url_per_million"]

rows, keys = spark.sql(
    "SELECT COUNT(*), COUNT(DISTINCT park_code, month_key) FROM gold_park_month_news").first()
base = spark.table("gold_park_month").count()
joined, only_in_gold_park_month, only_in_news = spark.sql("""
    SELECT COUNT(*), COUNT_IF(n.park_code IS NULL), COUNT_IF(g.park_code IS NULL)
    FROM gold_park_month AS g
    FULL OUTER JOIN gold_park_month_news AS n
      ON n.park_code = g.park_code AND n.month_key = g.month_key""").first()
print(f"gold_park_month_news: {rows} rows, {keys} distinct keys; gold_park_month: {base} rows; "
      f"full outer join: {joined} rows")
assert rows == keys == base == joined == 660, "gold_park_month_news must have the 660 keys of gold_park_month"
assert only_in_gold_park_month == only_in_news == 0, (
    f"keys on one side only: {only_in_gold_park_month} in gold_park_month, {only_in_news} in gold_park_month_news")

nulls = " + ".join(f"CASE WHEN {c} IS NULL THEN 1 ELSE 0 END" for c in NEWS_COLUMNS)
null_cells, null_cells_201501, rows_201501 = spark.sql(f"""
    SELECT SUM(nulls), SUM(CASE WHEN month_key = 201501 THEN nulls ELSE 0 END), COUNT_IF(month_key = 201501)
    FROM (SELECT month_key, {nulls} AS nulls FROM gold_park_month_news) AS t""").first()
print(f"NULL news cells: {null_cells} in all, {null_cells_201501} in the {rows_201501} rows of 201501 (expected 20)")
assert rows_201501 == 5 and null_cells == null_cells_201501 == 20, (
    "the four news columns must be NULL in the 5 rows of 201501 and nowhere else")
print("GATE PASSED: 660 keys, NULLs only in 2015-01")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs
# MAGIC
# MAGIC One row per park x month: visits from `gold_park_month`, the news counts and shares from `gold_park_month_news`,
# MAGIC and `otb_complete` and `camping_otb` from `gold_park_month_reservations` for secondary check N6. The news and
# MAGIC reservation tables are built on the keys of `gold_park_month`, so all 660 rows must come back. The lag (news of
# MAGIC the month before) is taken inside `news_forecast.py`, not here. All columns are aggregates.

# COMMAND ----------

inputs = spark.sql("""
    SELECT park_code, month_key, g.month_start, g.visitors,
           n.news_all, n.news_url, n.news_all_per_million, n.news_url_per_million,
           r.otb_complete, r.camping_otb
    FROM gold_park_month g
    JOIN gold_park_month_news n USING (park_code, month_key)
    JOIN gold_park_month_reservations r USING (park_code, month_key)
""")
park_month = inputs.toPandas().sort_values(["park_code", "month_key"], ignore_index=True)
assert len(park_month) == 660, f"{len(park_month)} rows: the joins must keep all 660 rows of gold_park_month"
print(f"news_inputs_park_month: {len(park_month)} rows, {park_month['park_code'].nunique()} parks, "
      f"months {park_month['month_key'].min()}..{park_month['month_key'].max()}")
# With test flags the visits from 2024-01 must be synthetic: none may equal the committed real value
if TEST_FLAGS:
    news_forecast.require_synthetic_visits(park_month)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The test
# MAGIC
# MAGIC Settings, the URL rule's yearly pass rate, training facts, the primary table, the verdict with the precision
# MAGIC marks, then the secondary checks (never used for pass/fail).

# COMMAND ----------

result = news_forecast.run(park_month, flags)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta tables
# MAGIC
# MAGIC - `news_forecast_summary`: one row per check x measure x park. `check` is `primary` or N1-N6; `error_b` and
# MAGIC   `error_r` are the errors of B and R (in N6, of the compared forecast and of reservations + news); `r_lower`
# MAGIC   applies the tie rule; `months_r_lower` counts the scored months in which R's error is lower; `delta_otb` is
# MAGIC   the reservation coefficient of N6; `below_precision_bar` is the precision mark of the park.
# MAGIC - `news_forecast_monthly`: actual visits, the news of the month before, B and R for every park and month from
# MAGIC   2015-01 to 2025-09, all from the same fixed parameters. `period` is `train` for the 98 fitted months, `closed`
# MAGIC   for 2020-03..2020-12 (left out of the fit) and `held_out` for the scored months; `ape_b` and `ape_r` are
# MAGIC   filled for the held-out months only.

# COMMAND ----------

for name in ["summary", "monthly"]:
    table = f"news_forecast_{name}"
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
# MAGIC Delta tables. `news_inputs_park_month.csv` holds the 660 input rows, so anyone can rerun the test with
# MAGIC `src/news_forecast.py` from this file and the committed score file `docs/week8/a1_part2_score.json`; the
# MAGIC rerun needs both.

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

write_csv(inputs_pdf, "news_inputs_park_month.csv")
write_csv(result["summary"], "news_forecast_summary.csv")
write_csv(result["monthly"], "news_forecast_monthly.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the files
# MAGIC
# MAGIC 1. In the left sidebar, click **Catalog**, then open **workspace > nps_tourism > Volumes > exports**.
# MAGIC 2. For `news_inputs_park_month.csv`, `news_forecast_summary.csv` and `news_forecast_monthly.csv`: click the
# MAGIC    kebab menu (three dots), then **Download file**. Keep the names exactly as they are.
# MAGIC 3. Copy the whole output of this notebook as well: the flags, the gate, settings, training facts, primary
# MAGIC    table, verdict, precision marks and secondary checks.
# MAGIC 4. Commit the three files under `docs/week8/news_inputs/`, together with 07's `gold_month_gkg.csv` and
# MAGIC    `gold_park_month_news.csv` and the result document `docs/week8/news_test_result.md`.
