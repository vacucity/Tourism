-- 05 Checks for the reservation fact: read-only. Compares DW.FACT_PARK_MONTH_RESERVATIONS with staging (the CSV),
-- with FACT_PARK_MONTH and with the values of the CSV exported by 05_reservations.
-- Run 04_load_reservations.sql first. Run each statement with Run to see its result.

USE ROLE SYSADMIN;
USE WAREHOUSE CLASS_WH;
USE DATABASE NPS_TOURISM;

-- 1. Rows, keys, orphans and the rules of the counts: every row says PASS.
-- Snowflake does not enforce PRIMARY KEY and FOREIGN KEY, so these queries are the actual test.
-- Expected values from gold_park_month_reservations in Databricks (05_reservations): 660 rows, otb_complete = 1 in
-- 523 rows (120 each in GRCA, GRSM, YOSE, ZION; 43 in YELL), and NULL counts in 15 rows (2025-10..2025-12 in each park).
WITH results AS (
  SELECT 1 AS n, 'fact rows' AS check_name, 660 AS expected,
         (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH_RESERVATIONS) AS actual
  UNION ALL
  SELECT 2, 'fact rows minus staging rows', 0,
         (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH_RESERVATIONS)
         - (SELECT COUNT(*) FROM STAGING.GOLD_PARK_MONTH_RESERVATIONS)
  UNION ALL
  SELECT 3, 'duplicate park x month', 0,
         (SELECT COUNT(*) - COUNT(DISTINCT park_key, month_key) FROM DW.FACT_PARK_MONTH_RESERVATIONS)
  UNION ALL
  SELECT 4, 'rows whose park_key is not in dim_park', 0,
         (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH_RESERVATIONS f
          LEFT JOIN DW.DIM_PARK p ON p.park_key = f.park_key
          WHERE p.park_key IS NULL)
  UNION ALL
  SELECT 5, 'rows whose month_key is not in dim_month', 0,
         (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH_RESERVATIONS f
          LEFT JOIN DW.DIM_MONTH m ON m.month_key = f.month_key
          WHERE m.month_key IS NULL)
  UNION ALL
  -- Same grain as FACT_PARK_MONTH: every key is in both facts, so the two join 1:1
  SELECT 6, 'keys in one fact only (visits or reservations)', 0,
         (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH v
          FULL OUTER JOIN DW.FACT_PARK_MONTH_RESERVATIONS r
            ON r.park_key = v.park_key AND r.month_key = v.month_key
          WHERE v.park_key IS NULL OR r.park_key IS NULL)
  UNION ALL
  SELECT 7, 'rows with otb_complete = 1', 523,
         (SELECT COUNT_IF(otb_complete = 1) FROM DW.FACT_PARK_MONTH_RESERVATIONS)
  UNION ALL
  SELECT 8, 'rows with otb_complete not 0 or 1', 0,
         (SELECT COUNT_IF(otb_complete NOT IN (0, 1)) FROM DW.FACT_PARK_MONTH_RESERVATIONS)
  UNION ALL
  -- NULL means "file not published yet": all five counts are NULL together, only after 2025-09, never in a complete month
  SELECT 9, 'rows with NULL counts', 15,
         (SELECT COUNT_IF(camping_otb IS NULL) FROM DW.FACT_PARK_MONTH_RESERVATIONS)
  UNION ALL
  SELECT 10, 'NULL rows before 2025-10, or with only some counts NULL, or complete', 0,
         (SELECT COUNT_IF((camping_otb IS NULL AND (month_key <= 202509 OR otb_complete = 1))
                          OR (camping_otb IS NULL) <> (camping_stays IS NULL)
                          OR (camping_otb IS NULL) <> (camping_paid0_stays IS NULL)
                          OR (camping_otb IS NULL) <> (all_otb IS NULL)
                          OR (camping_otb IS NULL) <> (all_stays IS NULL))
          FROM DW.FACT_PARK_MONTH_RESERVATIONS)
  UNION ALL
  -- On the books is a part of all stays; camping is a part of all families; paid-0 stays are a part of camping stays
  SELECT 11, 'rows breaking otb <= stays, camping <= all, paid0 <= stays', 0,
         (SELECT COUNT_IF(camping_otb > camping_stays OR all_otb > all_stays
                          OR camping_otb > all_otb OR camping_stays > all_stays
                          OR camping_paid0_stays > camping_stays
                          OR LEAST(camping_otb, camping_stays, camping_paid0_stays, all_otb, all_stays) < 0)
          FROM DW.FACT_PARK_MONTH_RESERVATIONS)
)
SELECT n, check_name, expected, actual, IFF(actual = expected, 'PASS', 'FAIL') AS result
FROM results
ORDER BY n;

-- 2. The fact holds exactly the CSV: join it back to the CSV layout and compare every column with staging.
-- IS DISTINCT FROM treats two NULLs as equal. Expect rows_differing = 0.
WITH star AS (
  SELECT p.park_code, f.month_key, m.month_start, f.otb_complete, f.camping_otb, f.camping_stays,
         f.camping_paid0_stays, f.all_otb, f.all_stays
  FROM DW.FACT_PARK_MONTH_RESERVATIONS f
  JOIN DW.DIM_PARK p  ON p.park_key = f.park_key
  JOIN DW.DIM_MONTH m ON m.month_key = f.month_key
)
SELECT COUNT(*) AS rows_differing
FROM STAGING.GOLD_PARK_MONTH_RESERVATIONS g
FULL OUTER JOIN star s
  ON s.park_code = g.park_code AND s.month_key = g.month_key
WHERE g.park_code IS NULL OR s.park_code IS NULL
   OR g.month_start         IS DISTINCT FROM s.month_start
   OR g.otb_complete        IS DISTINCT FROM s.otb_complete
   OR g.camping_otb         IS DISTINCT FROM s.camping_otb
   OR g.camping_stays       IS DISTINCT FROM s.camping_stays
   OR g.camping_paid0_stays IS DISTINCT FROM s.camping_paid0_stays
   OR g.all_otb             IS DISTINCT FROM s.all_otb
   OR g.all_stays           IS DISTINCT FROM s.all_stays;

-- 3. Totals: the three rows must be identical.
-- Row 1 holds the sums of gold_park_month_reservations.csv as exported by 05_reservations (2015-01..2025-09).
SELECT 1 AS n, 'gold_park_month_reservations.csv' AS source, 523 AS complete_months,
       1903377 AS camping_otb, 2958004 AS camping_stays, 118103 AS camping_paid0_stays,
       4292972 AS all_otb, 7521456 AS all_stays
UNION ALL
SELECT 2, 'STAGING.GOLD_PARK_MONTH_RESERVATIONS', SUM(otb_complete),
       SUM(camping_otb), SUM(camping_stays), SUM(camping_paid0_stays), SUM(all_otb), SUM(all_stays)
FROM STAGING.GOLD_PARK_MONTH_RESERVATIONS
UNION ALL
SELECT 3, 'DW.FACT_PARK_MONTH_RESERVATIONS', SUM(otb_complete),
       SUM(camping_otb), SUM(camping_stays), SUM(camping_paid0_stays), SUM(all_otb), SUM(all_stays)
FROM DW.FACT_PARK_MONTH_RESERVATIONS
ORDER BY n;

-- 4. Declared constraints: 3 rows on FACT_PARK_MONTH_RESERVATIONS (1 primary key, 2 foreign keys), none enforced
SELECT table_name, constraint_name, constraint_type, enforced
FROM NPS_TOURISM.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE table_schema = 'DW' AND table_name = 'FACT_PARK_MONTH_RESERVATIONS'
ORDER BY constraint_type, constraint_name;

-- 5. Business query: visits and camping bookings per park in 2024, across the two facts.
-- Each fact is summed to park x year on its own, then the two are joined (a drill-across through the shared
-- dimensions), so neither fact repeats rows of the other. camping_otb_2024 per park, from the CSV:
-- GRCA 47,080, GRSM 43,157, YELL 13,850, YOSE 73,801, ZION 29,795. YELL has otb_complete = 1 in all of 2024.
WITH visits AS (
  SELECT f.park_key, SUM(f.visitors) AS visitors_2024
  FROM DW.FACT_PARK_MONTH f
  JOIN DW.DIM_MONTH m ON m.month_key = f.month_key
  WHERE m.year = 2024
  GROUP BY f.park_key
),
bookings AS (
  SELECT r.park_key, SUM(r.camping_otb) AS camping_otb_2024, SUM(r.camping_stays) AS camping_stays_2024,
         SUM(r.otb_complete) AS complete_months_2024
  FROM DW.FACT_PARK_MONTH_RESERVATIONS r
  JOIN DW.DIM_MONTH m ON m.month_key = r.month_key
  WHERE m.year = 2024
  GROUP BY r.park_key
)
SELECT
  p.park_code,
  p.park_name,
  v.visitors_2024,
  b.camping_otb_2024,
  b.camping_stays_2024,
  b.complete_months_2024,
  -- DOUBLE division as in 03_checks.sql; NULLIF gives NULL instead of dividing by zero
  ROUND(CAST(b.camping_otb_2024 AS DOUBLE) / NULLIF(v.visitors_2024, 0) * 1000, 1) AS camping_otb_per_1000_visits
FROM DW.DIM_PARK p
JOIN visits v   ON v.park_key = p.park_key
JOIN bookings b ON b.park_key = p.park_key
ORDER BY v.visitors_2024 DESC;
