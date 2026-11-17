-- 04 Load reservations: gold_park_month_reservations.csv -> STAGING.GOLD_PARK_MONTH_RESERVATIONS
-- -> DW.FACT_PARK_MONTH_RESERVATIONS, a second fact at the grain of FACT_PARK_MONTH (one row per park x month).
-- Rerunnable: staging is rebuilt from the staged file on every run, and the fact is loaded with MERGE, so a rerun
-- with the same file changes nothing.
-- Run 01_setup.sql and 02_load_star.sql first: the fact takes park_key from DW.DIM_PARK and its months must be in
-- DW.DIM_MONTH.
-- Run each statement in order with Run (cursor in the statement) to see its result.
--
-- The file: gold_park_month_reservations.csv, written by the Databricks notebook 05_reservations and committed in the
-- repository as docs/week8/a2_inputs/gold_park_month_reservations.csv (660 rows, aggregates only: counts of
-- reservations per park x month, no row-level reservation data).
-- Upload it to the stage in Snowsight, as for gold_park_month.csv (steps at the end of 01_setup.sql):
-- 1. In the navigation menu, select Ingestion > Add Data, then Load files into a Stage.
-- 2. Select docs/week8/a2_inputs/gold_park_month_reservations.csv from the repository. The file name must be exactly
--    gold_park_month_reservations.csv.
-- 3. Select the database and schema NPS_TOURISM.STAGING, then the stage NPS_STAGE. Leave the path empty. Upload.
-- To replace it with a newer export, run this REMOVE first (without the leading --), then upload again:
-- REMOVE @NPS_TOURISM.STAGING.NPS_STAGE/gold_park_month_reservations.csv;

USE ROLE SYSADMIN;
USE WAREHOUSE CLASS_WH;
USE DATABASE NPS_TOURISM;

-- The file must be on the stage: one row for gold_park_month_reservations.csv
LIST @STAGING.NPS_STAGE PATTERN = '.*gold_park_month_reservations[.]csv';

-- 1. Staging -------------------------------------------------------------------------------------

-- Same columns, order and types as gold_park_month_reservations in Databricks. The five counts are NULL from
-- 2025-10: those stays are in the FY2026 file, which is not published yet. NULL is an empty field in the CSV.
CREATE OR REPLACE TABLE STAGING.GOLD_PARK_MONTH_RESERVATIONS (
  park_code           VARCHAR(4) NOT NULL,
  month_key           INTEGER    NOT NULL,
  month_start         DATE       NOT NULL,
  otb_complete        INTEGER    NOT NULL,
  camping_otb         BIGINT,
  camping_stays       BIGINT,
  camping_paid0_stays BIGINT,
  all_otb             BIGINT,
  all_stays           BIGINT
)
COMMENT = 'Grain: one row per park x month. gold_park_month_reservations.csv loaded as is, reloaded on every run';

-- TRUNCATE empties the table and the record of loaded files, so the COPY below loads the file again
-- (see 02_load_star.sql). ON_ERROR = ABORT_STATEMENT: one bad row or a missing file loads nothing.
-- Result: one row for gold_park_month_reservations.csv with status LOADED and rows_parsed = rows_loaded = 660.
TRUNCATE TABLE STAGING.GOLD_PARK_MONTH_RESERVATIONS;

COPY INTO STAGING.GOLD_PARK_MONTH_RESERVATIONS
  FROM @STAGING.NPS_STAGE
  FILES = ('gold_park_month_reservations.csv')
  FILE_FORMAT = (FORMAT_NAME = 'STAGING.NPS_CSV')
  ON_ERROR = ABORT_STATEMENT;

-- Every row of the file is in staging, once: staging_rows = distinct_keys = 660, 5 parks, 132 months.
-- If staging_rows is larger than distinct_keys, do not run the MERGE: run again from the TRUNCATE above.
SELECT
  COUNT(*)                             AS staging_rows,
  COUNT(DISTINCT park_code, month_key) AS distinct_keys,
  COUNT(DISTINCT park_code)            AS parks,
  COUNT(DISTINCT month_key)            AS months
FROM STAGING.GOLD_PARK_MONTH_RESERVATIONS;

-- 2. The fact ------------------------------------------------------------------------------------

-- Grain: one row per park x month, the same grain and keys as FACT_PARK_MONTH, so the two facts join 1:1 through
-- DIM_PARK and DIM_MONTH. A reservation counts once, in the month its stay starts; on the books = ordered before
-- 00:00 UTC on day 1 of that month. Counts are gross: the source files have no cancellation status.
-- The counts are additive: each reservation is in exactly one park and one month, so they sum over parks and months.
-- PRIMARY KEY and FOREIGN KEY are not enforced by Snowflake; 05_checks_reservations.sql tests them.
CREATE TABLE IF NOT EXISTS DW.FACT_PARK_MONTH_RESERVATIONS (
  park_key            INTEGER NOT NULL,   -- DIM_PARK
  month_key           INTEGER NOT NULL,   -- DIM_MONTH
  otb_complete        INTEGER NOT NULL,   -- 0/1 flag, 1 = camping_otb holds every booking made before day 1
  camping_otb         BIGINT,             -- additive: camping stays starting in the month, booked before day 1
  camping_stays       BIGINT,             -- additive: all camping stays starting in the month
  camping_paid0_stays BIGINT,             -- additive: camping stays with total_paid = 0 (closures show up here)
  all_otb             BIGINT,             -- additive: camping_otb over every reservation family
  all_stays           BIGINT,             -- additive: camping_stays over every reservation family
  CONSTRAINT pk_fact_park_month_reservations PRIMARY KEY (park_key, month_key),
  CONSTRAINT fk_fact_res_park FOREIGN KEY (park_key) REFERENCES DW.DIM_PARK (park_key),
  CONSTRAINT fk_fact_res_month FOREIGN KEY (month_key) REFERENCES DW.DIM_MONTH (month_key)
)
COMMENT = 'Grain: one row per park x month. Reservations by stay-start month; on the books = ordered before 00:00 UTC on day 1. NULL counts from 2025-10 (FY2026 file not published)';

-- park_key comes from DIM_PARK. LEFT JOIN: a park missing from DIM_PARK gives park_key NULL, which NOT NULL rejects,
-- so the MERGE fails instead of silently dropping rows.
-- First run: inserts 660, updates 0. Rerun with the same file: inserts 0, updates 0.
MERGE INTO DW.FACT_PARK_MONTH_RESERVATIONS t
USING (
  SELECT
    p.park_key,
    g.month_key,
    g.otb_complete,
    g.camping_otb,
    g.camping_stays,
    g.camping_paid0_stays,
    g.all_otb,
    g.all_stays
  FROM STAGING.GOLD_PARK_MONTH_RESERVATIONS g
  LEFT JOIN DW.DIM_PARK p
    ON p.park_code = g.park_code
) s
ON t.park_key = s.park_key AND t.month_key = s.month_key
WHEN MATCHED AND (t.otb_complete        IS DISTINCT FROM s.otb_complete
               OR t.camping_otb         IS DISTINCT FROM s.camping_otb
               OR t.camping_stays       IS DISTINCT FROM s.camping_stays
               OR t.camping_paid0_stays IS DISTINCT FROM s.camping_paid0_stays
               OR t.all_otb             IS DISTINCT FROM s.all_otb
               OR t.all_stays           IS DISTINCT FROM s.all_stays) THEN
  UPDATE SET otb_complete        = s.otb_complete,
             camping_otb         = s.camping_otb,
             camping_stays       = s.camping_stays,
             camping_paid0_stays = s.camping_paid0_stays,
             all_otb             = s.all_otb,
             all_stays           = s.all_stays
WHEN NOT MATCHED THEN
  INSERT (park_key, month_key, otb_complete, camping_otb, camping_stays, camping_paid0_stays, all_otb, all_stays)
  VALUES (s.park_key, s.month_key, s.otb_complete, s.camping_otb, s.camping_stays, s.camping_paid0_stays,
          s.all_otb, s.all_stays);

-- Row counts after the load: 660 and 660
SELECT
  (SELECT COUNT(*) FROM STAGING.GOLD_PARK_MONTH_RESERVATIONS) AS staging_rows,
  (SELECT COUNT(*) FROM DW.FACT_PARK_MONTH_RESERVATIONS)      AS fact_rows;
