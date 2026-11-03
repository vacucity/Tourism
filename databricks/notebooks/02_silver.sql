-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 Silver: typed, deduplicated, one row per park x month
-- MAGIC
-- MAGIC Day 3 builds the weather-station map, `silver_visitation` and `silver_weather`.
-- MAGIC Day 4 adds holidays, search interest and the integrated demand table.
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