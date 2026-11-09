-- 02 Load: gold_park_month.csv -> STAGING.GOLD_PARK_MONTH -> star schema in DW
-- Rerunnable. Staging is rebuilt from the staged file on every run; the star is loaded with MERGE,
-- which inserts new rows and updates changed ones, so a rerun with the same file changes nothing.
-- Run 01_setup.sql and upload gold_park_month.csv to @STAGING.NPS_STAGE first.
-- Run each statement in order with Run (cursor in the statement) to see its result.

USE ROLE SYSADMIN;
USE WAREHOUSE CLASS_WH;
USE DATABASE NPS_TOURISM;

-- 1. Staging -------------------------------------------------------------------------------------

-- Same columns, order and types as gold_park_month in Databricks.
-- INTEGER and BIGINT are both NUMBER(38,0) in Snowflake; DOUBLE is Snowflake's FLOAT, a 64-bit float like Spark DOUBLE.
-- CREATE OR REPLACE starts every run with an empty table, so a reload never adds rows twice.
CREATE OR REPLACE TABLE STAGING.GOLD_PARK_MONTH (
  park_code               VARCHAR(4)   NOT NULL,
  park_name               VARCHAR(100),
  states                  VARCHAR(20),
  year                    INTEGER,
  month                   INTEGER,
  month_key               INTEGER      NOT NULL,
  month_seq               INTEGER,
  month_start             DATE,
  visitors                BIGINT,
  visitors_last_year      BIGINT,
  yoy_change_pct          DOUBLE,
  visitors_12m            BIGINT,
  share_of_year           DOUBLE,
  search_index            INTEGER,
  tavg_f                  DOUBLE,
  tmax_f                  DOUBLE,
  tmin_f                  DOUBLE,
  prcp_in                 DOUBLE,
  snow_in                 DOUBLE,
  used_backup_station     INTEGER,
  holiday_count           BIGINT,
  three_day_weekend_count BIGINT,
  weekend_days            INTEGER,
  days_in_month           INTEGER
)
COMMENT = 'Grain: one row per park x month. gold_park_month.csv loaded as is, reloaded on every run';

-- FORCE = TRUE loads the file even if Snowflake's load history lists it as loaded before.
-- ON_ERROR = ABORT_STATEMENT: one bad row (or a missing file) stops the COPY and loads nothing.
-- Result: one row for gold_park_month.csv with status LOADED and rows_parsed = rows_loaded.
COPY INTO STAGING.GOLD_PARK_MONTH
  FROM @STAGING.NPS_STAGE
  FILES = ('gold_park_month.csv')
  FILE_FORMAT = (FORMAT_NAME = 'STAGING.NPS_CSV')
  ON_ERROR = ABORT_STATEMENT
  FORCE = TRUE;

-- Every row of the file is in staging, once: staging_rows = rows_loaded above = distinct_keys (660 for 2015-2025)
SELECT
  COUNT(*)                             AS staging_rows,
  COUNT(DISTINCT park_code, month_key) AS distinct_keys,
  COUNT(DISTINCT park_code)            AS parks,
  COUNT(DISTINCT month_key)            AS months
FROM STAGING.GOLD_PARK_MONTH;

-- 2. Star schema ---------------------------------------------------------------------------------

-- Snowflake stores PRIMARY KEY, UNIQUE and FOREIGN KEY constraints but does not enforce them on standard tables
-- (NOT NULL is enforced). They document the model; 03_checks.sql tests keys and orphans with queries.

CREATE TABLE IF NOT EXISTS DW.DIM_PARK (
  park_key  INTEGER      NOT NULL IDENTITY(1, 1) ORDER,   -- surrogate key, assigned by Snowflake on insert
  park_code VARCHAR(4)   NOT NULL,                        -- natural key: NPS unit code
  park_name VARCHAR(100) NOT NULL,
  states    VARCHAR(20)  NOT NULL,                        -- states the park lies in, alphabetical
  CONSTRAINT pk_dim_park PRIMARY KEY (park_key),
  CONSTRAINT uq_dim_park_code UNIQUE (park_code)
)
COMMENT = 'Grain: one row per national park. Type 1: changes are overwritten in place';

CREATE TABLE IF NOT EXISTS DW.DIM_MONTH (
  month_key               INTEGER    NOT NULL,   -- 202507 = July 2025
  year                    INTEGER    NOT NULL,
  month                   INTEGER    NOT NULL,
  month_name              VARCHAR(3) NOT NULL,   -- Jan ... Dec
  quarter                 INTEGER    NOT NULL,
  month_seq               INTEGER    NOT NULL,   -- year * 12 + month - 1: consecutive months differ by 1
  month_start             DATE       NOT NULL,
  days_in_month           INTEGER    NOT NULL,
  weekend_days            INTEGER    NOT NULL,
  holiday_count           BIGINT     NOT NULL,   -- observed federal holidays in the month
  three_day_weekend_count BIGINT     NOT NULL,   -- holidays on a Monday or a Friday
  CONSTRAINT pk_dim_month PRIMARY KEY (month_key)
)
COMMENT = 'Grain: one row per calendar month. Type 1: changes are overwritten in place';

-- Measures: additive = sums over parks and months; semi-additive = sums over one of them only;
-- non-additive = do not sum. visitors_last_year, yoy_change_pct, visitors_12m and share_of_year are
-- computed from visitors in Databricks Gold and stored here, so the star holds every Gold column.
CREATE TABLE IF NOT EXISTS DW.FACT_PARK_MONTH (
  park_key            INTEGER NOT NULL,   -- DIM_PARK
  month_key           INTEGER NOT NULL,   -- DIM_MONTH
  visitors            BIGINT,             -- additive: recreation visitors
  visitors_last_year  BIGINT,             -- additive: same month one year earlier
  yoy_change_pct      DOUBLE,             -- non-additive: percent change against visitors_last_year
  visitors_12m        BIGINT,             -- semi-additive (parks, not months): this month plus the 11 before it
  share_of_year       DOUBLE,             -- non-additive: this month / the park's calendar-year total
  search_index        INTEGER,            -- non-additive: Google Trends 0-100, scaled within each park
  tavg_f              DOUBLE,             -- non-additive
  tmax_f              DOUBLE,             -- non-additive
  tmin_f              DOUBLE,             -- non-additive
  prcp_in             DOUBLE,             -- semi-additive (months, not parks): monthly total
  snow_in             DOUBLE,             -- semi-additive (months, not parks): monthly total
  used_backup_station INTEGER,            -- 0/1 flag, 1 = tavg_f, tmax_f and tmin_f come from the park's backup station
  CONSTRAINT pk_fact_park_month PRIMARY KEY (park_key, month_key),
  CONSTRAINT fk_fact_park FOREIGN KEY (park_key) REFERENCES DW.DIM_PARK (park_key),
  CONSTRAINT fk_fact_month FOREIGN KEY (month_key) REFERENCES DW.DIM_MONTH (month_key)
)
COMMENT = 'Grain: one row per park x month';

-- 3. Load the star: dimensions first, then the fact --------------------------------------------

-- WHEN MATCHED AND ... IS DISTINCT FROM ...: update a row only if a value changed.
-- IS DISTINCT FROM treats two NULLs as equal and NULL vs a value as different.
-- First run: inserts 5, updates 0. Rerun with the same file: inserts 0, updates 0.
MERGE INTO DW.DIM_PARK t
USING (
  SELECT DISTINCT park_code, park_name, states
  FROM STAGING.GOLD_PARK_MONTH
) s
ON t.park_code = s.park_code
WHEN MATCHED AND (t.park_name IS DISTINCT FROM s.park_name
               OR t.states    IS DISTINCT FROM s.states) THEN
  UPDATE SET park_name = s.park_name,
             states    = s.states
WHEN NOT MATCHED THEN
  INSERT (park_code, park_name, states)
  VALUES (s.park_code, s.park_name, s.states);

-- Calendar columns depend only on month_key, so SELECT DISTINCT gives one row per month.
-- First run: inserts 132, updates 0. Rerun: inserts 0, updates 0.
MERGE INTO DW.DIM_MONTH t
USING (
  SELECT DISTINCT
    month_key,
    year,
    month,
    MONTHNAME(month_start) AS month_name,
    QUARTER(month_start)   AS quarter,
    month_seq,
    month_start,
    days_in_month,
    weekend_days,
    holiday_count,
    three_day_weekend_count
  FROM STAGING.GOLD_PARK_MONTH
) s
ON t.month_key = s.month_key
WHEN MATCHED AND (t.year                    IS DISTINCT FROM s.year
               OR t.month                   IS DISTINCT FROM s.month
               OR t.month_name              IS DISTINCT FROM s.month_name
               OR t.quarter                 IS DISTINCT FROM s.quarter
               OR t.month_seq               IS DISTINCT FROM s.month_seq
               OR t.month_start             IS DISTINCT FROM s.month_start
               OR t.days_in_month           IS DISTINCT FROM s.days_in_month
               OR t.weekend_days            IS DISTINCT FROM s.weekend_days
               OR t.holiday_count           IS DISTINCT FROM s.holiday_count
               OR t.three_day_weekend_count IS DISTINCT FROM s.three_day_weekend_count) THEN
  UPDATE SET year                    = s.year,
             month                   = s.month,
             month_name              = s.month_name,
             quarter                 = s.quarter,
             month_seq               = s.month_seq,
             month_start             = s.month_start,
             days_in_month           = s.days_in_month,
             weekend_days            = s.weekend_days,
             holiday_count           = s.holiday_count,
             three_day_weekend_count = s.three_day_weekend_count
WHEN NOT MATCHED THEN
  INSERT (month_key, year, month, month_name, quarter, month_seq, month_start,
          days_in_month, weekend_days, holiday_count, three_day_weekend_count)
  VALUES (s.month_key, s.year, s.month, s.month_name, s.quarter, s.month_seq, s.month_start,
          s.days_in_month, s.weekend_days, s.holiday_count, s.three_day_weekend_count);

-- park_key comes from DIM_PARK. LEFT JOIN: a park missing from DIM_PARK would give park_key NULL,
-- which NOT NULL rejects, so the MERGE fails instead of silently dropping rows.
-- First run: inserts 660, updates 0. Rerun: inserts 0, updates 0. One changed value in the CSV: updates 1.
MERGE INTO DW.FACT_PARK_MONTH t
USING (
  SELECT
    p.park_key,
    g.month_key,
    g.visitors,
    g.visitors_last_year,
    g.yoy_change_pct,
    g.visitors_12m,
    g.share_of_year,
    g.search_index,
    g.tavg_f,
    g.tmax_f,
    g.tmin_f,
    g.prcp_in,
    g.snow_in,
    g.used_backup_station
  FROM STAGING.GOLD_PARK_MONTH g
  LEFT JOIN DW.DIM_PARK p
    ON p.park_code = g.park_code
) s
ON t.park_key = s.park_key AND t.month_key = s.month_key
WHEN MATCHED AND (t.visitors            IS DISTINCT FROM s.visitors
               OR t.visitors_last_year  IS DISTINCT FROM s.visitors_last_year
               OR t.yoy_change_pct      IS DISTINCT FROM s.yoy_change_pct
               OR t.visitors_12m        IS DISTINCT FROM s.visitors_12m
               OR t.share_of_year       IS DISTINCT FROM s.share_of_year
               OR t.search_index        IS DISTINCT FROM s.search_index
               OR t.tavg_f              IS DISTINCT FROM s.tavg_f
               OR t.tmax_f              IS DISTINCT FROM s.tmax_f
               OR t.tmin_f              IS DISTINCT FROM s.tmin_f
               OR t.prcp_in             IS DISTINCT FROM s.prcp_in
               OR t.snow_in             IS DISTINCT FROM s.snow_in
               OR t.used_backup_station IS DISTINCT FROM s.used_backup_station) THEN
  UPDATE SET visitors            = s.visitors,
             visitors_last_year  = s.visitors_last_year,
             yoy_change_pct      = s.yoy_change_pct,
             visitors_12m        = s.visitors_12m,
             share_of_year       = s.share_of_year,
             search_index        = s.search_index,
             tavg_f              = s.tavg_f,
             tmax_f              = s.tmax_f,
             tmin_f              = s.tmin_f,
             prcp_in             = s.prcp_in,
             snow_in             = s.snow_in,
             used_backup_station = s.used_backup_station
WHEN NOT MATCHED THEN
  INSERT (park_key, month_key, visitors, visitors_last_year, yoy_change_pct, visitors_12m, share_of_year,
          search_index, tavg_f, tmax_f, tmin_f, prcp_in, snow_in, used_backup_station)
  VALUES (s.park_key, s.month_key, s.visitors, s.visitors_last_year, s.yoy_change_pct, s.visitors_12m, s.share_of_year,
          s.search_index, s.tavg_f, s.tmax_f, s.tmin_f, s.prcp_in, s.snow_in, s.used_backup_station);

-- Row counts after the load: 660, 5, 132, 660
SELECT
  (SELECT COUNT(*) FROM STAGING.GOLD_PARK_MONTH) AS staging_rows,
  (SELECT COUNT(*) FROM DW.DIM_PARK)             AS dim_park_rows,
  (SELECT COUNT(*) FROM DW.DIM_MONTH)            AS dim_month_rows,
  (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH)      AS fact_rows;
