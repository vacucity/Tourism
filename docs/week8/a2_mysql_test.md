# Week 8 test A2, part 1: Recreation.gov reservations into MySQL

Run on 2026-11-08 against the local MySQL 26.7 server (database `nps_tourism`, user `nps_app`, default
128 MB buffer pool) with `src/ingest/ridb_download.py`, `src/load_mysql.py` and `mysql/02_checks.sql`.
The full check output is in `a2_checks.txt`.

| Pass condition (from the proposal) | Result |
| --- | --- |
| Load FY2017 and FY2024 into MySQL | 3,303,553 and 9,777,935 CSV rows read; 255,657 and 1,290,959 reservations stored |
| Map facilities to the five parks | 79 facilities: GRCA 5, GRSM 34, YELL 7, YOSE 17, ZION 16 |
| Check keys | 0 orphans (8 checks), 0 duplicate keys (5 checks), 0 constraint violations (4 checks) |
| Row counts match the source files | Reconciliation difference 0 for all 10 (fiscal year, park) rows |
| Reservations lower held-out error in at least three parks | Part 2, not run yet: needs FY2015–FY2025 loaded |

## Row accounting

Source rows are the rows in the file that name the park. Stored = source − dropped − rejected − repeated ids.

| Fiscal year | Park | Source rows | Rejected | Repeated ids | Stored |
| --- | --- | ---: | ---: | ---: | ---: |
| 2017 | GRCA | 65,179 | 0 | 1 | 65,178 |
| 2017 | GRSM | 46,088 | 0 | 39 | 46,049 |
| 2017 | YELL | 234 | 0 | 0 | 234 |
| 2017 | YOSE | 112,721 | 0 | 28 | 112,693 |
| 2017 | ZION | 31,506 | 0 | 3 | 31,503 |
| 2024 | GRCA | 103,438 | 1 | 0 | 103,437 |
| 2024 | GRSM | 196,564 | 4 | 0 | 196,560 |
| 2024 | YELL | 52,062 | 6 | 0 | 52,056 |
| 2024 | YOSE | 606,943 | 2 | 0 | 606,941 |
| 2024 | ZION | 331,966 | 1 | 0 | 331,965 |
| | **Total** | **1,546,701** | **14** | **71** | **1,546,616** |

- FY2017 files hold the park name in RegionDescription, FY2024 files in ParentLocation. In FY2017,
  Yellowstone appears only as "Yellowstone Non-Commercially Guided Snowmobile Access Program" (facility
  250849, 234 rows). FY2024 lists the same facility under Yellowstone National Park.
- 0 rows were dropped: every five-park row in both files is an NPS row, and none is a DONATION row.
- 71 FY2017 reservation ids appear twice. 70 are listed under two products and 1 under one product with
  two customer ZIPs. The row with the lowest product id is kept.
- 14 FY2024 rows went to `data/work/ridb/rejects_FY2024.csv`: 13 end before they start, and 1 is a Half
  Dome permit refund with a total paid of −20.

## Schema

`mysql/01_schema.sql` defines 8 InnoDB tables in third normal form, with primary keys, foreign keys and
CHECK constraints: `park`, `facility`, `product`, `booking_order`, `reservation`,
`reservation_equipment`, plus `source_file` and `source_file_park` for lineage and reconciliation.
Stored rows: 1,493,020 orders, 1,546,616 reservations, 555,301 equipment rows, 5,136 products.

The dependencies behind the tables were checked on both files:

- A product is (facility_id, product_id): the same product id appears at several facilities (12 ids in
  FY2017, 1 in FY2024).
- An FY2024 order number such as `0610465289-3` is line 3 of order 0610465289. Order time and customer
  ZIP depend on the order alone, so orders are their own table (1,237,363 FY2024 orders for 1,290,959
  reservations).
- FY2017's 22 equipment count columns are a repeating group and become rows of `reservation_equipment`.
- Nights equals end date minus start date in every FY2024 row with an end date, so it is not stored.

## Replay and reruns

- Each file is replayed in order-time order, in 774 transactions (128 for FY2017, 646 for FY2024) of
  about 2,000 reservations, never splitting an order. 0 transactions are out of order within a file.
- The load took 1,227 s. A rerun took 1,282 s and inserted or changed 0 rows; every table count was
  unchanged.
- Crash test on a separate test server: the loader was killed during the FY2024 replay. The database
  held exactly the first 81 transactions, with no split orders. `02_checks.sql` listed FY2024 as
  unfinished (`source_file.completed_at` is NULL). A rerun restored contents identical to a clean load.

## Findings for later steps

- The FY2017 file is split by order date (orders 2016-10-01 to 2017-09-30, starts 2016-09-29 to
  2018-09-30). The FY2024 file is split by start date (starts 2023-10-01 to 2024-09-30, orders
  2020-12-16 to 2025-01-06).
- A later fiscal-year file adds orders older than ones already loaded: 54,929 FY2024 reservations were
  ordered before 2023-10-01. The incremental extract therefore uses `reservation_key`, which grows in
  load order, as its watermark instead of order time.
- MySQL 26.7 skips a foreign key check when the index behind the key has a further column that is NULL.
  The order foreign key therefore uses an index on `order_number` alone.

## Reproduce

Needs `MYSQL_URL` in `.env` (see the docstring of `src/load_mysql.py`).

```bat
uv run python src/ingest/ridb_download.py --fiscal-years 2017 2024
uv run python src/load_mysql.py --fiscal-years 2017 2024
"C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u nps_app -p nps_tourism --table < mysql\02_checks.sql
```
