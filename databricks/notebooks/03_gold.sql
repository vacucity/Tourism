-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 Gold: analysis-ready park x month table
-- MAGIC
-- MAGIC Builds `gold_park_month` from `silver_park_month` only: one row per park x month with park names,
-- MAGIC year-over-year change, a rolling 12-month total and each month's share of the park's year.
-- MAGIC
-- MAGIC Next, this table is loaded into the Snowflake star schema: `park_name` and `states` depend only on `park_code`,
-- MAGIC and the calendar columns depend only on `month_key`, so they split off as a park dimension and a month dimension.
-- MAGIC
-- MAGIC Run `02_silver` first. Always use **Run all**: every cell relies on the `USE` statement below.

-- COMMAND ----------

USE workspace.nps_tourism;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## gold_park_month: one row per park x month
-- MAGIC
-- MAGIC - `visitors_last_year`: the same month one year earlier, found by joining on `month_seq - 12`.
-- MAGIC   A missing month gives NULL instead of silently using the wrong month.
-- MAGIC - `yoy_change_pct`: percent change against `visitors_last_year`.
-- MAGIC - `visitors_12m`: this month plus the 11 before it; NULL until 12 months of history exist.
-- MAGIC - `share_of_year`: this month's visitors divided by the park's calendar-year total.
-- MAGIC - `search_index`: Google Trends 0-100, scaled within each park. Compare months of one park, not parks with each other.
-- MAGIC
-- MAGIC Four months had 0 visitors (Covid closures: GRCA, YELL, YOSE in 2020-04 and YOSE in 2020-05).
-- MAGIC `try_divide` returns NULL instead of failing on division by zero, so `yoy_change_pct` is NULL
-- MAGIC for the same months one year later (2021-04 and 2021-05), and -100.0 for the closed months themselves.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_park_month
COMMENT 'Grain: one row per park x month'
AS
WITH parks AS (
  -- Names as in src/ingest/parks.py, states the park lies in (alphabetical)
  SELECT * FROM VALUES
    ('YELL', 'Yellowstone National Park',           'ID, MT, WY'),
    ('YOSE', 'Yosemite National Park',              'CA'),
    ('GRCA', 'Grand Canyon National Park',          'AZ'),
    ('ZION', 'Zion National Park',                  'UT'),
    ('GRSM', 'Great Smoky Mountains National Park', 'NC, TN')
  AS t(park_code, park_name, states)
),
with_last_year AS (
  -- Join each month to the row 12 months earlier by month_seq, never by row position
  SELECT cur.*, prev.visitors AS visitors_last_year
  FROM silver_park_month AS cur
  LEFT JOIN silver_park_month AS prev
    ON prev.park_code = cur.park_code AND prev.month_seq = cur.month_seq - 12
),
with_windows AS (
  SELECT *,
         -- RANGE uses month_seq values, so a missing month leaves fewer than 12 rows and gives NULL
         CASE WHEN COUNT(visitors) OVER last_12_months = 12
              THEN SUM(visitors) OVER last_12_months END   AS visitors_12m,
         SUM(visitors) OVER (PARTITION BY park_code, year)   AS visitors_year
  FROM with_last_year
  WINDOW last_12_months AS (PARTITION BY park_code ORDER BY month_seq
                            RANGE BETWEEN 11 PRECEDING AND CURRENT ROW)
)
SELECT
  w.park_code,
  p.park_name,
  p.states,
  w.year,
  w.month,
  w.month_key,
  w.month_seq,
  w.month_start,
  w.visitors,
  w.visitors_last_year,
  ROUND(TRY_DIVIDE(w.visitors - w.visitors_last_year, w.visitors_last_year) * 100, 1) AS yoy_change_pct,
  w.visitors_12m,
  TRY_DIVIDE(w.visitors, w.visitors_year) AS share_of_year,
  w.search_index,
  w.tavg_f,
  w.tmax_f,
  w.tmin_f,
  w.prcp_in,
  w.snow_in,
  w.used_backup_station,
  w.holiday_count,
  w.three_day_weekend_count,
  w.weekend_days,
  w.days_in_month
FROM with_windows AS w
LEFT JOIN parks AS p
  ON w.park_code = p.park_code;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Checkpoints

-- COMMAND ----------

-- One row per park x month: row_count = distinct_keys = silver_rows, and every park has a name
SELECT
  COUNT(*)                                AS row_count,
  COUNT(DISTINCT park_code, month_key)    AS distinct_keys,
  (SELECT COUNT(*) FROM silver_park_month) AS silver_rows,
  COUNT(*) - COUNT(park_name)             AS missing_park_names
FROM gold_park_month;

-- COMMAND ----------

-- NULLs in the history columns. Measured:
-- visitors_last_year 60 = the 12 months of 2015 x 5 parks (no earlier year to compare with)
-- yoy_change_pct 64 = those 60 + 4 months whose last-year value is 0
-- visitors_12m 55 = the first 11 months x 5 parks
-- share_of_year 0
SELECT
  COUNT(*) - COUNT(visitors_last_year) AS visitors_last_year_nulls,
  COUNT(*) - COUNT(yoy_change_pct)     AS yoy_change_pct_nulls,
  COUNT(*) - COUNT(visitors_12m)       AS visitors_12m_nulls,
  COUNT(*) - COUNT(share_of_year)      AS share_of_year_nulls
FROM gold_park_month;

-- COMMAND ----------

-- Division by zero in practice, expect 8 rows:
-- the 4 closed months (yoy_change_pct = -100.0) and the same months a year later (yoy_change_pct = NULL)
SELECT park_code, month_key, visitors, visitors_last_year, yoy_change_pct
FROM gold_park_month
WHERE visitors = 0 OR visitors_last_year = 0
ORDER BY park_code, month_key;

-- COMMAND ----------

-- share_of_year must add up to 1 in every park-year (rounded to 6 decimals to ignore floating-point noise)
SELECT
  COUNT(*)                                                  AS park_years,
  SUM(CASE WHEN ROUND(total_share, 6) = 1 THEN 1 ELSE 0 END) AS park_years_summing_to_1,
  MIN(months)                                               AS min_months_per_year,
  MAX(months)                                               AS max_months_per_year
FROM (
  SELECT park_code, year, SUM(share_of_year) AS total_share, COUNT(*) AS months
  FROM gold_park_month
  GROUP BY park_code, year
) AS t;

-- COMMAND ----------

-- In December the last 12 months are January to December, so visitors_12m must equal the year total
SELECT
  COUNT(*)                                                         AS decembers,
  SUM(CASE WHEN g.visitors_12m = y.year_total THEN 1 ELSE 0 END)   AS decembers_matching
FROM gold_park_month AS g
JOIN (
  SELECT park_code, year, SUM(visitors) AS year_total
  FROM gold_park_month
  GROUP BY park_code, year
) AS y
  ON g.park_code = y.park_code AND g.year = y.year
WHERE g.month = 12;

-- COMMAND ----------

-- Dashboard preview: 2025 visitors per park, change against 2024, and the busiest month of 2025
SELECT
  park_name,
  states,
  SUM(visitors)                                                     AS visitors_2025,
  ROUND(TRY_DIVIDE(SUM(visitors) - SUM(visitors_last_year), SUM(visitors_last_year)) * 100, 1)
                                                                    AS change_vs_2024_pct,
  MAX_BY(month_start, visitors)                                     AS peak_month,
  MAX(visitors)                                                     AS peak_visitors,
  ROUND(MAX(share_of_year) * 100, 1)                                AS peak_share_pct
FROM gold_park_month
WHERE year = 2025
GROUP BY park_name, states
ORDER BY visitors_2025 DESC;
