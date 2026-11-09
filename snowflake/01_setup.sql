-- 01 Setup: warehouse, database, schemas, CSV file format and stage for the Gold export
-- Snowsight worksheet: Run All (in the More options menu next to Run). Safe to rerun.
-- Then upload the file (steps at the end) and run the LIST statement again with Run.

USE ROLE SYSADMIN;

-- Creates CLASS_WH only if it does not exist yet; AUTO_SUSPEND = 60 stops it after 60 idle seconds
CREATE WAREHOUSE IF NOT EXISTS CLASS_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
ALTER WAREHOUSE CLASS_WH SET AUTO_SUSPEND = 60;
USE WAREHOUSE CLASS_WH;

CREATE DATABASE IF NOT EXISTS NPS_TOURISM;
CREATE SCHEMA IF NOT EXISTS NPS_TOURISM.STAGING;   -- the CSV as exported from Databricks
CREATE SCHEMA IF NOT EXISTS NPS_TOURISM.DW;        -- the star schema
USE SCHEMA NPS_TOURISM.STAGING;

-- Matches the file written by the Databricks notebook 04_export_gold:
-- comma-separated, one header row, text that contains a comma is in double quotes ("ID, MT, WY"),
-- NULL is an empty field (two commas in a row), dates are YYYY-MM-DD, UTF-8.
CREATE OR REPLACE FILE FORMAT NPS_CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE
  DATE_FORMAT = 'YYYY-MM-DD'
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  ENCODING = 'UTF8'
  COMMENT = 'gold_park_month.csv from the Databricks notebook 04_export_gold';

-- Internal named stage: uploaded files are stored inside Snowflake.
-- The file format is named in COPY INTO (02_load_star.sql), so it can be replaced without touching the stage.
CREATE STAGE IF NOT EXISTS NPS_STAGE
  COMMENT = 'Uploads of gold_park_month.csv';

-- Upload gold_park_month.csv to the stage in Snowsight:
-- 1. In the navigation menu, select Ingestion > Add Data.
-- 2. Select Load files into a Stage.
-- 3. Select gold_park_month.csv (downloaded from the Databricks Volume exports). The file name must be exactly
--    gold_park_month.csv, because 02_load_star.sql loads only that name. If the browser saved it as
--    gold_park_month (1).csv, rename it first.
-- 4. Select the database and schema NPS_TOURISM.STAGING, then the stage NPS_STAGE. Leave the path empty.
-- 5. Select Upload, then run the LIST below with Run: it shows one row for gold_park_month.csv.
--
-- To replace the file with a newer export, run this REMOVE first (without the leading --), then upload again:
-- REMOVE @NPS_TOURISM.STAGING.NPS_STAGE/gold_park_month.csv;

LIST @NPS_STAGE;
