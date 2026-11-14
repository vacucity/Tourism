-- OLTP system of record for Recreation.gov reservations at the five parks (3NF).
--
-- Every statement is idempotent, so the file can be run again at any time:
--   "C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u nps_app -p nps_tourism < mysql\01_schema.sql
-- src/load_mysql.py also applies it before each load. No semicolons inside comments:
-- the loader splits the file on them.
--
-- Dependencies checked on FY2015-FY2025 (five-park subset) that shape the tables:
--   facility_id -> park                                           (held for all 90 facilities, in every file and across files)
--   facility_id -> name, zip, state, latitude, longitude          (held within FY2015-FY2024 files. Across files they change,
--     and FY2025 changes them for 19 facilities during the year: the newest file, then the latest order, wins)
--   product_id alone does not identify a product: the same id appears at several
--     facilities (17 ids in FY2015-FY2025, 12 in FY2017, 1 in FY2024), so a product is (facility_id, product_id)
--   (facility_id, product_id) -> use_type                         (held in every file and across files)
--   site_type and inventory_type vary between reservations of the same product
--     (permits: LOTTERY_PERMIT vs PERMIT), so they belong to the reservation
--   order number -> order time and customer location: an order is one checkout and
--     can hold several reservations (FY2024 "0610465289-3" = order 0610465289, line 3)
--   nights = end_date - start_date in all 528,886 FY2024 rows that have an end date (nights has no number
--     exactly when end_date is empty: blank, or the bare text ' days' in 80,526 rows), so nights is not stored
--   customer ZIP does not determine customer state (FY2017: 441 ZIPs appear with two or more states),
--     and state does not determine country (26 states appear with two or more countries), so both stay
--     on booking_order as entered
--   facility ZIP and state are the facility's published address. ZIP is not used as a determinant:
--     FY2024 lists ZIPs such as 100, 4, 434 and 8308, and ZIP 84767 once with no state
--   site_type does not determine inventory_type: each layout has its own words ('Entry Point' is
--     Entrance in FY2017 and PERMIT in FY2024)

CREATE TABLE IF NOT EXISTS park (
    park_code CHAR(4)     NOT NULL,
    park_name VARCHAR(64) NOT NULL,
    PRIMARY KEY (park_code),
    UNIQUE KEY uq_park_name (park_name),
    CONSTRAINT ck_park_code CHECK (CHAR_LENGTH(park_code) = 4)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Same codes and names as src/ingest/parks.py
INSERT INTO park (park_code, park_name) VALUES
    ('YELL', 'Yellowstone National Park'),
    ('YOSE', 'Yosemite National Park'),
    ('GRCA', 'Grand Canyon National Park'),
    ('ZION', 'Zion National Park'),
    ('GRSM', 'Great Smoky Mountains National Park')
AS new ON DUPLICATE KEY UPDATE park_name = new.park_name;

-- One row per fiscal-year file that has been loaded
CREATE TABLE IF NOT EXISTS source_file (
    fiscal_year SMALLINT        NOT NULL,
    zip_name    VARCHAR(64)     NOT NULL,
    csv_name    VARCHAR(128)    NOT NULL,
    csv_bytes   BIGINT UNSIGNED NOT NULL,
    rows_read   INT UNSIGNED    NOT NULL,  -- every data row in the CSV, all agencies
    loaded_at    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP       NULL,  -- set after the last replay transaction, NULL while a load runs or after a crash
    PRIMARY KEY (fiscal_year),
    CONSTRAINT ck_source_fy CHECK (fiscal_year BETWEEN 2006 AND 2100)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- What the loader counted in the file for each park, to reconcile against the tables
CREATE TABLE IF NOT EXISTS source_file_park (
    fiscal_year    SMALLINT     NOT NULL,
    park_code      CHAR(4)      NOT NULL,
    source_rows    INT UNSIGNED NOT NULL,  -- rows in the file that name this park (any agency)
    dropped_rows   INT UNSIGNED NOT NULL,  -- rows of an agency other than NPS, and DONATION rows
    rejected_rows  INT UNSIGNED NOT NULL,  -- rows missing a required value or breaking a CHECK below, written to a rejects file
    duplicate_rows INT UNSIGNED NOT NULL,  -- extra rows that repeat a reservation id within the file
    -- rows already stored from an earlier fiscal-year file: the same reservation id, or (FY2019+) an
    -- old-system order number that a reservation from an earlier file already uses
    other_file_rows INT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (fiscal_year, park_code),
    CONSTRAINT fk_sfp_file FOREIGN KEY (fiscal_year) REFERENCES source_file (fiscal_year),
    CONSTRAINT fk_sfp_park FOREIGN KEY (park_code) REFERENCES park (park_code),
    CONSTRAINT ck_sfp_counts CHECK (dropped_rows + rejected_rows + duplicate_rows + other_file_rows <= source_rows)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- A database created before FY2015-FY2025 support has source_file_park without other_file_rows (CREATE TABLE
-- IF NOT EXISTS leaves an existing table as it is). The next four statements add the column and widen the
-- CHECK when the column is missing, and do nothing otherwise, so they can be run again at any time.
SET @migrate_sfp = IF((SELECT COUNT(*) FROM information_schema.columns
                       WHERE table_schema = DATABASE() AND table_name = 'source_file_park'
                         AND column_name = 'other_file_rows') = 0,
    'ALTER TABLE source_file_park ADD COLUMN other_file_rows INT UNSIGNED NOT NULL DEFAULT 0 AFTER duplicate_rows, DROP CHECK ck_sfp_counts, ADD CONSTRAINT ck_sfp_counts CHECK (dropped_rows + rejected_rows + duplicate_rows + other_file_rows <= source_rows)',
    'DO 0');
PREPARE migrate_sfp FROM @migrate_sfp;
EXECUTE migrate_sfp;
DEALLOCATE PREPARE migrate_sfp;

-- A campground, permit program or ticketed entry. Attributes are those of the newest file loaded
-- (within a file: of the row with the latest order time).
CREATE TABLE IF NOT EXISTS facility (
    facility_id   VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,  -- numeric, AP2510, AP22308 or a UUID
    park_code     CHAR(4)      NOT NULL,
    facility_name VARCHAR(128) NOT NULL,
    facility_zip  VARCHAR(10)  NULL,
    state_code    CHAR(2)      NULL,
    -- NULL when the file gives none, or a negative latitude with a positive longitude (300015 and 300016 in
    -- FY2020-FY2021, 4675323 in FY2022-FY2025): every five-park facility is north of the equator and west of Greenwich
    latitude      DECIMAL(9,6) NULL,
    longitude     DECIMAL(9,6) NULL,
    updated_fy    SMALLINT     NOT NULL,  -- fiscal-year file these attributes come from
    PRIMARY KEY (facility_id),
    KEY ix_facility_park (park_code),
    CONSTRAINT fk_facility_park FOREIGN KEY (park_code) REFERENCES park (park_code),
    CONSTRAINT fk_facility_file FOREIGN KEY (updated_fy) REFERENCES source_file (fiscal_year),
    CONSTRAINT ck_facility_lat CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT ck_facility_lon CHECK (longitude BETWEEN -180 AND 180)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- A bookable unit (campsite, permit, ticket, pass) inside one facility
CREATE TABLE IF NOT EXISTS product (
    facility_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    product_id  VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    -- Overnight, Day, Activity. A file without a use type keeps the stored one (FY2018 gives none for
    -- 6 products that FY2015-FY2017 list as Day)
    use_type    VARCHAR(16) NULL,
    PRIMARY KEY (facility_id, product_id),
    CONSTRAINT fk_product_facility FOREIGN KEY (facility_id) REFERENCES facility (facility_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- One checkout. The source has no customer names, only location. An order listed in two files (lines that
-- start in two fiscal years) keeps the values of the earlier file.
CREATE TABLE IF NOT EXISTS booking_order (
    order_number     VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,  -- '0610465289' (FY2019+) or '2-36409369' (old system)
    -- UTC. FY2015-FY2017 give only the order date: stored at 00:00. FY2018 gives Eastern local time,
    -- converted to UTC. FY2019-FY2021 times have no zone marker and are UTC, FY2022+ times end in Z.
    ordered_at       DATETIME(6) NOT NULL,
    customer_zip     VARCHAR(10) NULL,
    customer_state   VARCHAR(8)  NULL,      -- FY2015-FY2020 files only
    customer_country VARCHAR(8)  NULL,      -- FY2015-FY2020 files only
    updated_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_number),
    KEY ix_order_ordered_at (ordered_at, order_number),  -- order-time range queries
    KEY ix_order_updated_at (updated_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- reservation_id is the natural key (UNIQUE). FY2019+ ids are mostly UUIDs (time-based version 1, random version 4,
-- hash-based version 5), so as the primary key
-- every insert would land on a random page of the clustered index: a surrogate key that grows in
-- replay order keeps inserts at the end of the table, and the secondary indexes small.
-- The incremental extract uses reservation_key as its watermark: keys grow in load order, while a later
-- fiscal-year file adds orders older than ones already loaded (FY2024 holds 54,929 rows ordered before Oct 2023).
-- A reservation is stored once, from the first file that lists it (fiscal years load oldest first).
CREATE TABLE IF NOT EXISTS reservation (
    reservation_key  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    reservation_id   VARCHAR(48) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,  -- historicalreservationid, up to 45 characters
    order_number     VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    order_line       SMALLINT UNSIGNED NULL,  -- FY2019+ only: the "-3" in order number 0610465289-3. NULL for old-system numbers
    facility_id      VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    product_id       VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    fiscal_year      SMALLINT    NOT NULL,  -- the file it came from
    inventory_type   VARCHAR(20) NOT NULL,  -- FY2019+: CAMPING, PERMIT, TIMED_ENTRY, ... FY2015-FY2018: Site, Tour, Entrance, POS, LOTTERY (FY2015), ERR (FY2018)
    site_type        VARCHAR(40) NULL,
    start_date       DATE        NULL,      -- empty only in FY2015-FY2017 rows (FY2017: POS rows and 95 Elkmont campsite rows)
    end_date         DATE        NULL,      -- empty for TIMED_ENTRY, LOTTERY_*, QUEUE_LOTTERY and most ACTIVITYPASS rows (FY2024) and Entrance, Tour, POS rows (FY2017)
    number_of_people SMALLINT UNSIGNED NULL,
    tax              DECIMAL(10,2) NOT NULL,
    use_fee          DECIMAL(10,2) NULL,     -- fee breakdown: FY2015-FY2017 files only (the columns are empty from FY2018)
    tran_fee         DECIMAL(10,2) NULL,
    attr_fee         DECIMAL(10,2) NULL,
    total_before_tax DECIMAL(10,2) NOT NULL,
    discount         DECIMAL(10,2) NULL,     -- FY2019+ only, stored as a positive amount
    total_paid       DECIMAL(10,2) NOT NULL,
    updated_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (reservation_key),
    UNIQUE KEY uq_reservation_id (reservation_id),
    -- order_number alone, not (order_number, order_line): MySQL 26.7 skips the foreign key check
    -- when the index behind it has a further column that is NULL (order_line is NULL before FY2019).
    -- (order_number, order_line) is not UNIQUE either: with a second unique key an upsert could
    -- merge two reservations. mysql/02_checks.sql counts repeated pairs instead.
    KEY ix_reservation_order (order_number),
    KEY ix_reservation_product (facility_id, product_id),
    KEY ix_reservation_start (start_date),
    KEY ix_reservation_fy (fiscal_year, facility_id),  -- covers counts per fiscal year and park
    KEY ix_reservation_updated_at (updated_at),
    CONSTRAINT fk_reservation_order FOREIGN KEY (order_number) REFERENCES booking_order (order_number),
    CONSTRAINT fk_reservation_product FOREIGN KEY (facility_id, product_id) REFERENCES product (facility_id, product_id),
    CONSTRAINT fk_reservation_file FOREIGN KEY (fiscal_year) REFERENCES source_file (fiscal_year),
    -- A CHECK passes when its expression is NULL, so empty dates and fees are allowed
    CONSTRAINT ck_reservation_dates CHECK (end_date >= start_date),
    -- total_before_tax is stored as given: 5 FY2024 backcountry permits have a negative value and a positive total_paid
    CONSTRAINT ck_reservation_money CHECK (tax >= 0 AND use_fee >= 0 AND tran_fee >= 0 AND attr_fee >= 0
                                           AND discount >= 0 AND total_paid >= 0)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Equipment brought on a reservation. FY2015-FY2018 files have one count column per type
-- (Tent, Popup, Trailer, ...), FY2019+ files one description and a length: both become rows here.
CREATE TABLE IF NOT EXISTS reservation_equipment (
    reservation_id VARCHAR(48) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    equipment_type VARCHAR(32)  NOT NULL,
    quantity       SMALLINT UNSIGNED NULL,  -- FY2015-FY2018 only
    length_ft      DECIMAL(8,2) NULL,       -- FY2019+ only. NULL when the value does not fit (12 FY2020-FY2023 rows, up to 3.4e10)
    PRIMARY KEY (reservation_id, equipment_type),
    CONSTRAINT fk_equipment_reservation FOREIGN KEY (reservation_id) REFERENCES reservation (reservation_id),
    CONSTRAINT ck_equipment_quantity CHECK (quantity > 0),
    CONSTRAINT ck_equipment_length CHECK (length_ft >= 0)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
