-- 03 Checks: read-only. Compares the star schema with staging (the CSV) and with Gold in Databricks.
-- Run 02_load_star.sql first. Run each statement with Run to see its result.

USE ROLE SYSADMIN;
USE WAREHOUSE CLASS_WH;
USE DATABASE NPS_TOURISM;

-- 1. Row counts, duplicate keys and orphan keys: every row says PASS.
-- Snowflake does not enforce PRIMARY KEY and FOREIGN KEY, so these queries are the actual test.
WITH results AS (
  SELECT 1 AS n, 'dim_park rows' AS check_name, 5 AS expected,
         (SELECT COUNT(*) FROM DW.DIM_PARK) AS actual
  UNION ALL
  SELECT 2, 'dim_month rows', 132,
         (SELECT COUNT(*) FROM DW.DIM_MONTH)
  UNION ALL
  SELECT 3, 'fact rows', 660,
         (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH)
  UNION ALL
  SELECT 4, 'fact rows minus staging rows', 0,
         (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH) - (SELECT COUNT(*) FROM STAGING.GOLD_PARK_MONTH)
  UNION ALL
  SELECT 5, 'duplicate park_key in dim_park', 0,
         (SELECT COUNT(*) - COUNT(DISTINCT park_key) FROM DW.DIM_PARK)
  UNION ALL
  SELECT 6, 'duplicate park_code in dim_park', 0,
         (SELECT COUNT(*) - COUNT(DISTINCT park_code) FROM DW.DIM_PARK)
  UNION ALL
  SELECT 7, 'duplicate month_key in dim_month', 0,
         (SELECT COUNT(*) - COUNT(DISTINCT month_key) FROM DW.DIM_MONTH)
  UNION ALL
  SELECT 8, 'duplicate park x month in fact', 0,
         (SELECT COUNT(*) - COUNT(DISTINCT park_key, month_key) FROM DW.FACT_PARK_MONTH)
  UNION ALL
  SELECT 9, 'fact rows whose park_key is not in dim_park', 0,
         (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH f
          LEFT JOIN DW.DIM_PARK p ON p.park_key = f.park_key
          WHERE p.park_key IS NULL)
  UNION ALL
  SELECT 10, 'fact rows whose month_key is not in dim_month', 0,
         (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH f
          LEFT JOIN DW.DIM_MONTH m ON m.month_key = f.month_key
          WHERE m.month_key IS NULL)
  UNION ALL
  -- 132 months spanned and 132 rows (row 2): no month is missing between 2015-01 and 2025-12
  SELECT 11, 'months spanned by dim_month, first to last', 132,
         (SELECT MAX(month_seq) - MIN(month_seq) + 1 FROM DW.DIM_MONTH)
)
SELECT n, check_name, expected, actual, IFF(actual = expected, 'PASS', 'FAIL') AS result
FROM results
ORDER BY n;

-- 2. The star holds exactly the CSV: join it back to the gold_park_month layout and compare
-- every column with staging. IS DISTINCT FROM treats two NULLs as equal. Expect rows_differing = 0.
WITH star AS (
  SELECT
    p.park_code, p.park_name, p.states,
    m.year, m.month, m.month_key, m.month_seq, m.month_start,
    f.visitors, f.visitors_last_year, f.yoy_change_pct, f.visitors_12m, f.share_of_year, f.search_index,
    f.tavg_f, f.tmax_f, f.tmin_f, f.prcp_in, f.snow_in, f.used_backup_station,
    m.holiday_count, m.three_day_weekend_count, m.weekend_days, m.days_in_month
  FROM DW.FACT_PARK_MONTH f
  JOIN DW.DIM_PARK p  ON p.park_key = f.park_key
  JOIN DW.DIM_MONTH m ON m.month_key = f.month_key
)
SELECT COUNT(*) AS rows_differing
FROM STAGING.GOLD_PARK_MONTH g
FULL OUTER JOIN star s
  ON s.park_code = g.park_code AND s.month_key = g.month_key
WHERE g.park_code IS NULL OR s.park_code IS NULL
   OR g.park_name               IS DISTINCT FROM s.park_name
   OR g.states                  IS DISTINCT FROM s.states
   OR g.year                    IS DISTINCT FROM s.year
   OR g.month                   IS DISTINCT FROM s.month
   OR g.month_seq               IS DISTINCT FROM s.month_seq
   OR g.month_start             IS DISTINCT FROM s.month_start
   OR g.visitors                IS DISTINCT FROM s.visitors
   OR g.visitors_last_year      IS DISTINCT FROM s.visitors_last_year
   OR g.yoy_change_pct          IS DISTINCT FROM s.yoy_change_pct
   OR g.visitors_12m            IS DISTINCT FROM s.visitors_12m
   OR g.share_of_year           IS DISTINCT FROM s.share_of_year
   OR g.search_index            IS DISTINCT FROM s.search_index
   OR g.tavg_f                  IS DISTINCT FROM s.tavg_f
   OR g.tmax_f                  IS DISTINCT FROM s.tmax_f
   OR g.tmin_f                  IS DISTINCT FROM s.tmin_f
   OR g.prcp_in                 IS DISTINCT FROM s.prcp_in
   OR g.snow_in                 IS DISTINCT FROM s.snow_in
   OR g.used_backup_station     IS DISTINCT FROM s.used_backup_station
   OR g.holiday_count           IS DISTINCT FROM s.holiday_count
   OR g.three_day_weekend_count IS DISTINCT FROM s.three_day_weekend_count
   OR g.weekend_days            IS DISTINCT FROM s.weekend_days
   OR g.days_in_month           IS DISTINCT FROM s.days_in_month;

-- 3. Total visitors per park, staging vs star: 5 rows, all MATCH
SELECT
  COALESCE(g.park_code, s.park_code)                    AS park_code,
  g.staging_visitors,
  s.star_visitors,
  IFF(g.staging_visitors = s.star_visitors, 'MATCH', 'DIFF') AS result
FROM (
  SELECT park_code, SUM(visitors) AS staging_visitors
  FROM STAGING.GOLD_PARK_MONTH
  GROUP BY park_code
) g
FULL OUTER JOIN (
  SELECT p.park_code, SUM(f.visitors) AS star_visitors
  FROM DW.FACT_PARK_MONTH f
  JOIN DW.DIM_PARK p ON p.park_key = f.park_key
  GROUP BY p.park_code
) s
  ON s.park_code = g.park_code
ORDER BY 1;

-- 4. NULLs per measure: the three rows must be identical.
-- Row 1 holds the counts measured in Databricks Gold: visitors_last_year 60 = 2015 (no earlier year),
-- yoy_change_pct 64 = those 60 + 4 months whose last-year value is 0, visitors_12m 55 = the first 11 months per park.
SELECT 1 AS n, 'Gold in Databricks' AS source,
       0 AS visitors, 60 AS visitors_last_year, 64 AS yoy_change_pct, 55 AS visitors_12m, 0 AS share_of_year,
       0 AS search_index, 1 AS tavg_f, 1 AS tmax_f, 1 AS tmin_f, 4 AS prcp_in, 138 AS snow_in,
       0 AS used_backup_station
UNION ALL
SELECT 2, 'STAGING.GOLD_PARK_MONTH',
       COUNT(*) - COUNT(visitors), COUNT(*) - COUNT(visitors_last_year), COUNT(*) - COUNT(yoy_change_pct),
       COUNT(*) - COUNT(visitors_12m), COUNT(*) - COUNT(share_of_year), COUNT(*) - COUNT(search_index),
       COUNT(*) - COUNT(tavg_f), COUNT(*) - COUNT(tmax_f), COUNT(*) - COUNT(tmin_f),
       COUNT(*) - COUNT(prcp_in), COUNT(*) - COUNT(snow_in), COUNT(*) - COUNT(used_backup_station)
FROM STAGING.GOLD_PARK_MONTH
UNION ALL
SELECT 3, 'DW.FACT_PARK_MONTH',
       COUNT(*) - COUNT(visitors), COUNT(*) - COUNT(visitors_last_year), COUNT(*) - COUNT(yoy_change_pct),
       COUNT(*) - COUNT(visitors_12m), COUNT(*) - COUNT(share_of_year), COUNT(*) - COUNT(search_index),
       COUNT(*) - COUNT(tavg_f), COUNT(*) - COUNT(tmax_f), COUNT(*) - COUNT(tmin_f),
       COUNT(*) - COUNT(prcp_in), COUNT(*) - COUNT(snow_in), COUNT(*) - COUNT(used_backup_station)
FROM DW.FACT_PARK_MONTH
ORDER BY n;

-- 5. Declared constraints: 6 rows (2 on DIM_PARK, 1 on DIM_MONTH, 3 on FACT_PARK_MONTH), none enforced
SELECT table_name, constraint_name, constraint_type, enforced
FROM NPS_TOURISM.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE table_schema = 'DW'
ORDER BY table_name, constraint_type, constraint_name;

-- 6. Business query 1: the dashboard preview from the last cell of 03_gold in Databricks, answered from the star.
-- Same result as Databricks: 2025 visitors per park, change against 2024, and the busiest month of 2025.
-- DOUBLE division as in Spark's TRY_DIVIDE; NULLIF gives NULL instead of dividing by zero.
SELECT
  p.park_name,
  p.states,
  SUM(f.visitors)                                                AS visitors_2025,
  ROUND(CAST(SUM(f.visitors) - SUM(f.visitors_last_year) AS DOUBLE)
        / NULLIF(SUM(f.visitors_last_year), 0) * 100, 1)         AS change_vs_2024_pct,
  MAX_BY(m.month_start, f.visitors)                              AS peak_month,
  MAX(f.visitors)                                                AS peak_visitors,
  ROUND(MAX(f.share_of_year) * 100, 1)                           AS peak_share_pct
FROM DW.FACT_PARK_MONTH f
JOIN DW.DIM_PARK p  ON p.park_key = f.park_key
JOIN DW.DIM_MONTH m ON m.month_key = f.month_key
WHERE m.year = 2025
GROUP BY p.park_name, p.states
ORDER BY visitors_2025 DESC;

-- 7. Business query 2: the busiest month of 2025 in each park, with its weather, search interest and holidays.
-- QUALIFY keeps the first row per park after ordering by visitors; the peak months equal peak_month in query 6.
SELECT
  p.park_code,
  m.month_name,
  m.month_start,
  f.visitors,
  ROUND(f.share_of_year * 100, 1) AS share_of_year_pct,
  f.search_index,
  f.tavg_f,
  f.prcp_in,
  m.holiday_count
FROM DW.FACT_PARK_MONTH f
JOIN DW.DIM_PARK p  ON p.park_key = f.park_key
JOIN DW.DIM_MONTH m ON m.month_key = f.month_key
WHERE m.year = 2025
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.park_code ORDER BY f.visitors DESC, m.month_key) = 1
ORDER BY f.visitors DESC;
