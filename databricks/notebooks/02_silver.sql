-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 Silver: typed, deduplicated tables
-- MAGIC
-- MAGIC Day 3 builds the weather-station map, `silver_visitation` and `silver_weather`.
-- MAGIC Day 4 adds holidays, search interest and the integrated park x month table `silver_park_month`.
-- MAGIC
-- MAGIC Always use **Run all**: every cell relies on the `USE` statement below.

-- COMMAND ----------

USE workspace.nps_tourism;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## silver_visitation: one row per park x month

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_visitation AS
WITH typed AS (
  SELECT
    UPPER(TRIM(UnitCode))                  AS park_code,
    TRY_CAST(Year AS INT)                  AS year,
    TRY_CAST(Month AS INT)                 AS month,
    TRY_CAST(RecreationVisitors AS BIGINT) AS visitors,
    pull_date,
    _source_file
  FROM bronze_nps_visitation
),
deduped AS (
  -- The same park-month can arrive in several pulls: keep the newest one
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY park_code, year, month
                            ORDER BY pull_date DESC, _source_file DESC) AS rn
  FROM typed
)
SELECT
  park_code,
  year,
  month,
  year * 100 + month        AS month_key,   -- 202507: join key
  year * 12 + month - 1     AS month_seq,   -- gap-safe counter for LAG
  make_date(year, month, 1) AS month_start,
  visitors
FROM deduped
WHERE rn = 1
  AND park_code IS NOT NULL AND year IS NOT NULL AND month BETWEEN 1 AND 12;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## silver_weather_stations: which station serves which park (1 = primary, 2 = backup)

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_weather_stations AS
SELECT * FROM VALUES
  ('YELL', 'USC00489905', 1), ('YELL', 'USW00094173', 2),
  ('YOSE', 'USW00053150', 1), ('YOSE', 'USC00049855', 2),
  ('GRCA', 'USW00003195', 1), ('GRCA', 'USC00023582', 2),
  ('ZION', 'USC00429717', 1), ('ZION', 'USC00424968', 2),
  ('GRSM', 'USC00403420', 1), ('GRSM', 'USC00406500', 2)
AS t(park_code, station_id, priority);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## silver_weather: one row per park x month, primary station first, backup second

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_weather AS
WITH typed AS (
  SELECT
    UPPER(TRIM(STATION))                 AS station_id,
    TRY_CAST(SUBSTR(DATE, 1, 4) AS INT)  AS year,      -- DATE looks like 2015-01
    TRY_CAST(SUBSTR(DATE, 6, 2) AS INT)  AS month,
    TRY_CAST(TAVG AS DOUBLE)             AS tavg,
    TRY_CAST(TMAX AS DOUBLE)             AS tmax,
    TRY_CAST(TMIN AS DOUBLE)             AS tmin,
    TRY_CAST(PRCP AS DOUBLE)             AS prcp,
    TRY_CAST(SNOW AS DOUBLE)             AS snow,
    pull_date,
    _source_file
  FROM bronze_weather
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY station_id, year, month
                            ORDER BY pull_date DESC, _source_file DESC) AS rn
  FROM typed
),
filled AS (
  -- NOAA defines TAVG as (TMAX + TMIN) / 2, so rebuild it when only TAVG is missing
  SELECT station_id, year, month,
         COALESCE(tavg, (tmax + tmin) / 2) AS tavg,
         tmax, tmin, prcp, snow
  FROM deduped
  WHERE rn = 1
    AND station_id IS NOT NULL AND year IS NOT NULL AND month BETWEEN 1 AND 12
),
by_priority AS (
  -- Pivot: one column for the primary station value, one for the backup
  SELECT
    s.park_code, f.year, f.month,
    MAX(CASE WHEN s.priority = 1 THEN f.tavg END) AS tavg_1,
    MAX(CASE WHEN s.priority = 2 THEN f.tavg END) AS tavg_2,
    MAX(CASE WHEN s.priority = 1 THEN f.tmax END) AS tmax_1,
    MAX(CASE WHEN s.priority = 2 THEN f.tmax END) AS tmax_2,
    MAX(CASE WHEN s.priority = 1 THEN f.tmin END) AS tmin_1,
    MAX(CASE WHEN s.priority = 2 THEN f.tmin END) AS tmin_2,
    MAX(CASE WHEN s.priority = 1 THEN f.prcp END) AS prcp_1,
    MAX(CASE WHEN s.priority = 2 THEN f.prcp END) AS prcp_2,
    MAX(CASE WHEN s.priority = 1 THEN f.snow END) AS snow_1,
    MAX(CASE WHEN s.priority = 2 THEN f.snow END) AS snow_2
  FROM filled AS f
  JOIN silver_weather_stations AS s ON f.station_id = s.station_id
  GROUP BY s.park_code, f.year, f.month
)
SELECT
  park_code,
  year,
  month,
  year * 100 + month                  AS month_key,
  ROUND(COALESCE(tavg_1, tavg_2), 1)  AS tavg_f,
  -- tmax and tmin come from the same station as tavg, so the three temperatures agree
  CASE WHEN tavg_1 IS NULL AND tavg_2 IS NOT NULL THEN tmax_2 ELSE tmax_1 END AS tmax_f,
  CASE WHEN tavg_1 IS NULL AND tavg_2 IS NOT NULL THEN tmin_2 ELSE tmin_1 END AS tmin_f,
  COALESCE(prcp_1, prcp_2)            AS prcp_in,
  COALESCE(snow_1, snow_2)            AS snow_in,
  CASE WHEN tavg_1 IS NULL AND tavg_2 IS NOT NULL THEN 1 ELSE 0 END AS used_backup_station
FROM by_priority;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Checkpoints

-- COMMAND ----------

-- Row counts and duplicate check (row_count must equal distinct_keys)
SELECT 'silver_visitation' AS table_name, COUNT(*) AS row_count,
       COUNT(DISTINCT park_code, month_key) AS distinct_keys
FROM silver_visitation
UNION ALL
SELECT 'silver_weather', COUNT(*), COUNT(DISTINCT park_code, month_key)
FROM silver_weather;

-- COMMAND ----------

-- Weather coverage per park, for the months we have visitors for
SELECT
  v.park_code,
  COUNT(*)                                                         AS park_months,
  ROUND(AVG(CASE WHEN w.tavg_f  IS NOT NULL THEN 1 ELSE 0 END), 3) AS tavg_rate,
  ROUND(AVG(CASE WHEN w.prcp_in IS NOT NULL THEN 1 ELSE 0 END), 3) AS prcp_rate,
  ROUND(AVG(CASE WHEN w.snow_in IS NOT NULL THEN 1 ELSE 0 END), 3) AS snow_rate,
  SUM(w.used_backup_station)                                       AS backup_months
FROM silver_visitation AS v
LEFT JOIN silver_weather AS w
  ON v.park_code = w.park_code AND v.month_key = w.month_key
GROUP BY v.park_code
ORDER BY v.park_code;

-- COMMAND ----------

-- Months with zero visitors are real Covid closures.
-- Expect 4 rows: GRCA 202004, YELL 202004, YOSE 202004, YOSE 202005
SELECT park_code, month_key, visitors
FROM silver_visitation
WHERE visitors = 0
ORDER BY park_code, month_key;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## silver_holidays: one row per observed federal holiday date
-- MAGIC
-- MAGIC OPM publishes one holiday table per year (`schedule_year`). Cleaning rules:
-- MAGIC - Footnote stars (`*`, `**`, `***`) are removed from `date_text`.
-- MAGIC - Curly apostrophes become straight ones, so every year spells `New Year's Day` the same way.
-- MAGIC - Inauguration Day is dropped: only federal employees in the Washington, DC area get that day off.
-- MAGIC - When January 1 is a Saturday, the holiday is observed on Friday, December 31 and OPM lists it in the
-- MAGIC   next year's table: the 2022 table holds 2021-12-31 and the 2028 table holds 2027-12-31.
-- MAGIC   So `year` comes from the holiday date, not from `schedule_year`.
-- MAGIC - Every pull holds whole year tables, so each `schedule_year` is taken from the newest pull that contains it.

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_holidays AS
WITH typed AS (
  SELECT
    TRY_CAST(schedule_year AS INT)                      AS schedule_year,
    TRIM(REPLACE(date_text, '*', ''))                   AS date_clean,     -- "Friday, June 18 ***" -> "Friday, June 18"
    -- r'...' is a raw string, so \u2019 reaches the regex engine, which reads it as the curly apostrophe
    TRIM(REGEXP_REPLACE(holiday_name, r'\u2019', '\'')) AS holiday_name,   -- curly apostrophe -> straight one
    pull_date,
    _source_file
  FROM bronze_holidays
),
split_text AS (
  SELECT
    schedule_year,
    holiday_name,
    SUBSTRING_INDEX(date_clean, ',', 1)         AS weekday_listed, -- "Friday"
    TRIM(SUBSTRING_INDEX(date_clean, ',', -1))  AS month_day,      -- "June 18"
    -- A New Year's Day in December belongs to the year before its table
    CASE WHEN holiday_name LIKE 'New Year%' AND date_clean LIKE '%December%'
         THEN schedule_year - 1 ELSE schedule_year END AS holiday_year,
    pull_date,
    _source_file
  FROM typed
  WHERE holiday_name <> 'Inauguration Day'
),
dated AS (
  -- "2021 December 31" -> DATE 2021-12-31, NULL if the text does not parse
  SELECT *,
         CAST(TRY_TO_TIMESTAMP(CONCAT(CAST(holiday_year AS STRING), ' ', month_day), 'yyyy MMMM d') AS DATE)
           AS holiday_date
  FROM split_text
),
deduped AS (
  -- Every pull holds whole year tables. DENSE_RANK gives rn = 1 to all rows of the newest pull for each
  -- schedule_year, so a date that a newer pull moved or removed does not survive from an older pull
  SELECT *,
         DENSE_RANK() OVER (PARTITION BY schedule_year
                            ORDER BY pull_date DESC, _source_file DESC) AS rn
  FROM dated
)
SELECT
  holiday_date,
  holiday_name,
  YEAR(holiday_date)                              AS year,
  MONTH(holiday_date)                             AS month,
  YEAR(holiday_date) * 100 + MONTH(holiday_date)  AS month_key,
  DAYOFWEEK(holiday_date)                         AS day_of_week,     -- 1 = Sunday ... 7 = Saturday
  DATE_FORMAT(holiday_date, 'EEEE')               AS weekday_name,
  -- Monday or Friday off: the holiday and the weekend make a three-day weekend
  CASE WHEN DAYOFWEEK(holiday_date) IN (2, 6) THEN 1 ELSE 0 END AS three_day_weekend,
  weekday_listed,                                 -- as printed by OPM, compared in the checkpoints
  schedule_year
FROM deduped
WHERE rn = 1 AND holiday_date IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## silver_search_interest: one row per park x month
-- MAGIC
-- MAGIC `google_trends.py` sends one request per park, and Google scales every request to 0-100,
-- MAGIC so 100 is that park's own busiest search month. Compare months within a park, not parks with each other.
-- MAGIC Every pull re-downloads the full history, so all rows of the newest pull share one scale.

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_search_interest AS
WITH unpivoted AS (
  -- UNPIVOT turns the five park columns into rows, and park_code receives the column name.
  -- INCLUDE NULLS keeps an empty cell as a NULL row (by default UNPIVOT drops it), so the newest pull still wins
  SELECT date, park_code, search_text, pull_date, _source_file
  FROM bronze_search_trends
  UNPIVOT INCLUDE NULLS (search_text FOR park_code IN (YELL, YOSE, GRCA, ZION, GRSM))
),
typed AS (
  SELECT
    park_code,
    TRY_CAST(SUBSTR(date, 1, 4) AS INT) AS year,      -- date looks like 2015-01-01
    TRY_CAST(SUBSTR(date, 6, 2) AS INT) AS month,
    -- Through DOUBLE: pandas writes a column with an empty cell as floats ("19.0"), which a direct INT cast turns into NULL
    TRY_CAST(TRY_CAST(search_text AS DOUBLE) AS INT) AS search_index,
    pull_date,
    _source_file
  FROM unpivoted
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY park_code, year, month
                            ORDER BY pull_date DESC, _source_file DESC) AS rn
  FROM typed
)
SELECT
  park_code,
  year,
  month,
  year * 100 + month AS month_key,
  search_index
FROM deduped
WHERE rn = 1
  AND year IS NOT NULL AND month BETWEEN 1 AND 12;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## silver_park_month: the integrated park x month table
-- MAGIC
-- MAGIC `silver_visitation` is the backbone: every park-month with a visitor count is kept, and weather,
-- MAGIC search interest and holiday counts are LEFT JOINed onto it.
-- MAGIC A month with no federal holiday gets 0, because `silver_holidays` covers every year of the backbone.

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_park_month AS
WITH holiday_months AS (
  SELECT
    month_key,
    COUNT(*)               AS holiday_count,
    SUM(three_day_weekend) AS three_day_weekend_count
  FROM silver_holidays
  GROUP BY month_key
)
SELECT
  v.park_code,
  v.year,
  v.month,
  v.month_key,
  v.month_seq,
  v.month_start,
  v.visitors,
  w.tavg_f,
  w.tmax_f,
  w.tmin_f,
  w.prcp_in,
  w.snow_in,
  w.used_backup_station,
  s.search_index,
  COALESCE(h.holiday_count, 0)           AS holiday_count,
  COALESCE(h.three_day_weekend_count, 0) AS three_day_weekend_count,
  -- List every day of the month, keep Sundays (1) and Saturdays (7), count them
  SIZE(FILTER(SEQUENCE(v.month_start, LAST_DAY(v.month_start)),
              d -> DAYOFWEEK(d) IN (1, 7)))  AS weekend_days,
  DAY(LAST_DAY(v.month_start))           AS days_in_month
FROM silver_visitation AS v
LEFT JOIN silver_weather AS w
  ON v.park_code = w.park_code AND v.month_key = w.month_key
LEFT JOIN silver_search_interest AS s
  ON v.park_code = s.park_code AND v.month_key = s.month_key
LEFT JOIN holiday_months AS h
  ON v.month_key = h.month_key;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Day 4 checkpoints

-- COMMAND ----------

-- Row counts and duplicate check for the Day 4 tables (row_count must equal distinct_keys)
SELECT 'silver_holidays' AS table_name, COUNT(*) AS row_count,
       COUNT(DISTINCT holiday_date) AS distinct_keys
FROM silver_holidays
UNION ALL
SELECT 'silver_search_interest', COUNT(*), COUNT(DISTINCT park_code, month_key)
FROM silver_search_interest
UNION ALL
SELECT 'silver_park_month', COUNT(*), COUNT(DISTINCT park_code, month_key)
FROM silver_park_month;

-- COMMAND ----------

-- Holiday parsing check, expect one row: 0, 11, 2
-- weekday_mismatches: parsed dates that do not fall on the weekday OPM printed
-- distinct_names: 11 holiday names once apostrophes are normalized and Inauguration Day is dropped
-- next_year_table_rows: 2021-12-31 and 2027-12-31, listed in the 2022 and 2028 tables
SELECT
  SUM(CASE WHEN weekday_name <> weekday_listed THEN 1 ELSE 0 END) AS weekday_mismatches,
  COUNT(DISTINCT holiday_name)                                    AS distinct_names,
  SUM(CASE WHEN year <> schedule_year THEN 1 ELSE 0 END)          AS next_year_table_rows
FROM silver_holidays;

-- COMMAND ----------

-- Observed federal holidays per calendar year. Expect 170 holidays in 16 rows:
-- 2015-2020: 10 each (Juneteenth is first observed in 2021)
-- 2021: 12 (includes 2021-12-31), 2022: 10, 2023-2026: 11 each
-- 2027: 12 (includes 2027-12-31), 2028: 10, 2029-2030: 11 each
SELECT year, COUNT(*) AS holidays, SUM(three_day_weekend) AS three_day_weekends
FROM silver_holidays
GROUP BY year
ORDER BY year;

-- COMMAND ----------

-- Search coverage per park: every park has 2015-01 to 2025-12 with no NULLs,
-- and max_index is 100 in every park because each park is scaled to its own busiest month.
-- null_values counts empty cells and values that TRY_CAST cannot read as INT
SELECT
  park_code,
  COUNT(*)                       AS months,
  MIN(month_key)                 AS first_month,
  MAX(month_key)                 AS last_month,
  COUNT(*) - COUNT(search_index) AS null_values,
  MIN(search_index)              AS min_index,
  MAX(search_index)              AS max_index
FROM silver_search_interest
GROUP BY park_code
ORDER BY park_code;

-- COMMAND ----------

-- silver_park_month keeps every backbone row (row_count = backbone_rows) and shows NULLs per column.
-- Measured: tavg_f, tmax_f, tmin_f 1 each (YOSE 201808), prcp_in 4 (all YOSE),
-- snow_in 138 (YOSE 107, GRCA 25, YELL 4, ZION 2), every other column 0.
-- These NULLs come from silver_weather (no station value for that month).
-- used_backup_station is 0 or 1 in every silver_weather row, so used_backup_station_nulls = 0
-- proves that every park-month has a weather row.
SELECT
  (SELECT COUNT(*) FROM silver_visitation)  AS backbone_rows,
  COUNT(*)                                  AS row_count,
  COUNT(*) - COUNT(visitors)                AS visitors_nulls,
  COUNT(*) - COUNT(tavg_f)                  AS tavg_f_nulls,
  COUNT(*) - COUNT(tmax_f)                  AS tmax_f_nulls,
  COUNT(*) - COUNT(tmin_f)                  AS tmin_f_nulls,
  COUNT(*) - COUNT(prcp_in)                 AS prcp_in_nulls,
  COUNT(*) - COUNT(snow_in)                 AS snow_in_nulls,
  COUNT(*) - COUNT(used_backup_station)     AS used_backup_station_nulls,
  COUNT(*) - COUNT(search_index)            AS search_index_nulls,
  COUNT(*) - COUNT(holiday_count)           AS holiday_count_nulls,
  COUNT(*) - COUNT(three_day_weekend_count) AS three_day_weekend_count_nulls,
  COUNT(*) - COUNT(weekend_days)            AS weekend_days_nulls,
  COUNT(*) - COUNT(days_in_month)           AS days_in_month_nulls
FROM silver_park_month;

-- COMMAND ----------

-- Preview: one park-year of the integrated table
SELECT *
FROM silver_park_month
WHERE park_code = 'YELL' AND year = 2025
ORDER BY month;
