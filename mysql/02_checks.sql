-- Checks after a load. Read-only, run any time:
--   "C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u nps_app -p nps_tourism --table < mysql\02_checks.sql
-- Every query named "should be 0" must return 0 (or no rows).

-- 1. Rows per table
SELECT 'park' AS table_name, COUNT(*) AS n FROM park
UNION ALL SELECT 'source_file', COUNT(*) FROM source_file
UNION ALL SELECT 'source_file_park', COUNT(*) FROM source_file_park
UNION ALL SELECT 'facility', COUNT(*) FROM facility
UNION ALL SELECT 'product', COUNT(*) FROM product
UNION ALL SELECT 'booking_order', COUNT(*) FROM booking_order
UNION ALL SELECT 'reservation', COUNT(*) FROM reservation
UNION ALL SELECT 'reservation_equipment', COUNT(*) FROM reservation_equipment;

-- 2. Reconciliation against the source files: rows the loader counted in each file versus rows in
--    the table, per fiscal year and park. diff should be 0 on every row.
SELECT s.fiscal_year, s.park_code, s.source_rows, s.dropped_rows, s.rejected_rows, s.duplicate_rows,
       s.source_rows - s.dropped_rows - s.rejected_rows - s.duplicate_rows AS expected_rows,
       COALESCE(m.n, 0) AS mysql_rows,
       COALESCE(m.n, 0) - (s.source_rows - s.dropped_rows - s.rejected_rows - s.duplicate_rows) AS diff
FROM source_file_park s
LEFT JOIN (SELECT c.fiscal_year, f.park_code, SUM(c.n) AS n
           -- count per facility first: the index (fiscal_year, facility_id) answers it without
           -- reading the reservation rows
           FROM (SELECT fiscal_year, facility_id, COUNT(*) AS n FROM reservation
                 GROUP BY fiscal_year, facility_id) c
           JOIN facility f ON f.facility_id = c.facility_id
           GROUP BY c.fiscal_year, f.park_code) m
       ON m.fiscal_year = s.fiscal_year AND m.park_code = s.park_code
ORDER BY s.fiscal_year, s.park_code;

-- File-level totals: rows read from each CSV and how many named one of the five parks
SELECT f.fiscal_year, f.csv_name, f.rows_read, SUM(s.source_rows) AS five_park_rows,
       (SELECT COUNT(*) FROM reservation r WHERE r.fiscal_year = f.fiscal_year) AS mysql_rows,
       f.loaded_at, f.completed_at
FROM source_file f JOIN source_file_park s ON s.fiscal_year = f.fiscal_year
GROUP BY f.fiscal_year, f.csv_name, f.rows_read, f.loaded_at, f.completed_at
ORDER BY f.fiscal_year;

-- Files whose replay did not finish (crash or Ctrl+C): run the loader again. Should return no rows.
SELECT fiscal_year, csv_name FROM source_file WHERE completed_at IS NULL;

-- 3. Orphans (the foreign keys prevent them, checked anyway). Should be 0.
SELECT 'reservation without order' AS orphan_check, COUNT(*) AS n
FROM reservation r LEFT JOIN booking_order o ON o.order_number = r.order_number WHERE o.order_number IS NULL
UNION ALL
SELECT 'reservation without product', COUNT(*)
FROM reservation r LEFT JOIN product p ON p.facility_id = r.facility_id AND p.product_id = r.product_id
WHERE p.product_id IS NULL
UNION ALL
SELECT 'reservation without source file', COUNT(*)
FROM reservation r LEFT JOIN source_file s ON s.fiscal_year = r.fiscal_year WHERE s.fiscal_year IS NULL
UNION ALL
SELECT 'product without facility', COUNT(*)
FROM product p LEFT JOIN facility f ON f.facility_id = p.facility_id WHERE f.facility_id IS NULL
UNION ALL
SELECT 'facility without park', COUNT(*)
FROM facility f LEFT JOIN park k ON k.park_code = f.park_code WHERE k.park_code IS NULL
UNION ALL
SELECT 'equipment without reservation', COUNT(*)
FROM reservation_equipment e LEFT JOIN reservation r ON r.reservation_id = e.reservation_id
WHERE r.reservation_id IS NULL
UNION ALL
-- Not enforced by a key: every order should hold at least one reservation
SELECT 'order without reservation', COUNT(*)
FROM booking_order o WHERE NOT EXISTS (SELECT 1 FROM reservation r WHERE r.order_number = o.order_number)
UNION ALL
SELECT 'product without reservation', COUNT(*)
FROM product p WHERE NOT EXISTS (SELECT 1 FROM reservation r
                                 WHERE r.facility_id = p.facility_id AND r.product_id = p.product_id);

-- 4. Duplicates. Should be 0. The keys prevent all but (order_number, order_line), which is
--    deliberately not a UNIQUE key (see mysql/01_schema.sql).
SELECT 'reservation_id' AS duplicate_check, COUNT(*) AS n
FROM (SELECT reservation_id FROM reservation GROUP BY reservation_id HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'order_number + order_line', COUNT(*)
FROM (SELECT order_number, order_line FROM reservation WHERE order_line IS NOT NULL
      GROUP BY order_number, order_line HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'order_number', COUNT(*)
FROM (SELECT order_number FROM booking_order GROUP BY order_number HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'facility_id', COUNT(*)
FROM (SELECT facility_id FROM facility GROUP BY facility_id HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'facility_id + product_id', COUNT(*)
FROM (SELECT facility_id, product_id FROM product GROUP BY facility_id, product_id HAVING COUNT(*) > 1) d;

-- 5. Constraint sanity (the CHECKs prevent them, checked anyway). Should be 0.
SELECT 'negative amount' AS sanity_check, COUNT(*) AS n FROM reservation
WHERE tax < 0 OR use_fee < 0 OR tran_fee < 0 OR attr_fee < 0 OR discount < 0 OR total_paid < 0
UNION ALL
SELECT 'end_date before start_date', COUNT(*) FROM reservation WHERE end_date < start_date
UNION ALL
SELECT 'latitude or longitude out of range', COUNT(*) FROM facility
WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180
UNION ALL
SELECT 'equipment quantity 0 or length negative', COUNT(*) FROM reservation_equipment
WHERE quantity = 0 OR length_ft < 0;

-- Allowed but worth knowing: empty dates and people counts, per fiscal year and inventory type
SELECT fiscal_year, inventory_type, COUNT(*) AS reservations,
       SUM(start_date IS NULL) AS no_start_date, SUM(end_date IS NULL) AS no_end_date,
       SUM(number_of_people IS NULL) AS no_people_count
FROM reservation GROUP BY fiscal_year, inventory_type ORDER BY fiscal_year, reservations DESC;

-- 6. Date ranges per fiscal year. A fiscal year runs October to September: FY2024 = 2023-10-01 to
--    2024-09-30. The FY2024 file is split by start date, the FY2017 file by order date.
--    ordered_after_start_utc_date: the order's UTC date is after the start date (e.g. walk-ups), allowed and
--    only counted. An evening order in the park's time zone falls on the next UTC day, so this counts more
--    than local time would (FY2024: 7,709 by UTC date, 3,911 by park time).
SELECT r.fiscal_year, MIN(r.start_date) AS first_start, MAX(r.start_date) AS last_start,
       MIN(o.ordered_at) AS first_order, MAX(o.ordered_at) AS last_order,
       SUM(DATE(o.ordered_at) > r.start_date) AS ordered_after_start_utc_date
FROM reservation r JOIN booking_order o ON o.order_number = r.order_number
GROUP BY r.fiscal_year ORDER BY r.fiscal_year;

-- 7. Replay order. Each replay transaction writes its reservations in one INSERT, so they share one
--    updated_at. Ordered by that time, no transaction should hold an order older than the newest order
--    of the transaction before it in the same file. Holds after a replay into an empty database (a rerun
--    changes nothing).
SELECT COUNT(*) AS transactions, SUM(first_order < prev_last_order) AS out_of_order_transactions
FROM (SELECT r.fiscal_year, r.updated_at, MIN(o.ordered_at) AS first_order,
             LAG(MAX(o.ordered_at)) OVER (PARTITION BY r.fiscal_year ORDER BY r.updated_at) AS prev_last_order
      FROM reservation r JOIN booking_order o ON o.order_number = r.order_number
      GROUP BY r.fiscal_year, r.updated_at) t;

-- 8. Facilities and reservations per park, all years
SELECT k.park_code, k.park_name, COUNT(f.facility_id) AS facilities, COALESCE(SUM(c.n), 0) AS reservations
FROM park k
LEFT JOIN facility f ON f.park_code = k.park_code
LEFT JOIN (SELECT facility_id, COUNT(*) AS n FROM reservation GROUP BY facility_id) c
       ON c.facility_id = f.facility_id
GROUP BY k.park_code, k.park_name ORDER BY k.park_code;
