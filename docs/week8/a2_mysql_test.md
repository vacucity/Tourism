# Week 8 test A2: Recreation.gov reservations into MySQL

Part 1 loaded FY2017 and FY2024 on 2026-11-08 into the database `nps_tourism`. On 2026-11-13 and 2026-11-14 all
eleven published fiscal years, FY2015 to FY2025, were loaded into a new database `nps_tourism_full` on a test
server, then copied to the local server. Both databases are now on the local MySQL 26.7 server (user `nps_app`,
default 128 MB buffer pool). Both loads use `src/ingest/ridb_download.py`, `src/load_mysql.py` and
`mysql/02_checks.sql`. The full check outputs are in `a2_checks.txt` (part 1) and `a2_full_checks.txt` (all years).

| Pass condition (from the proposal) | Part 1: FY2017 + FY2024 | All years: FY2015–FY2025 |
| --- | --- | --- |
| Load the reservation files into MySQL | 3,303,553 and 9,777,935 CSV rows read; 255,657 and 1,290,959 reservations stored | 66,730,835 CSV rows read; 7,586,234 reservations stored |
| Map facilities to the five parks | 79 facilities: GRCA 5, GRSM 34, YELL 7, YOSE 17, ZION 16 | 90 facilities: GRCA 5, GRSM 36, YELL 10, YOSE 22, ZION 17 |
| Check keys | 0 orphans (8 checks), 0 duplicate keys (5 checks), 0 constraint violations (4 checks) | The same 17 checks: all 0 |
| Row counts match the source files | Reconciliation difference 0 for all 10 (fiscal year, park) rows | Reconciliation difference 0 for all 55 rows |
| Reservations lower held-out error in at least three parks | Part 2 uses the all-years load | PASS on 2026-11-15: lower in 3 of 5 parks (GRCA, YOSE, ZION), `a2_part2_result.md` |

## Part 1: row accounting

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
Stored rows: 1,493,020 orders, 1,546,616 reservations, 555,301 equipment rows, 5,136 products in part 1;
7,243,035 orders, 7,586,234 reservations, 2,906,181 equipment rows, 6,090 products for all years.

The dependencies behind the tables were checked on both part 1 files:

- A product is (facility_id, product_id): the same product id appears at several facilities (12 ids in
  FY2017, 1 in FY2024).
- An FY2024 order number such as `0610465289-3` is line 3 of order 0610465289. Order time and customer
  ZIP depend on the order alone, so orders are their own table (1,237,363 FY2024 orders for 1,290,959
  reservations).
- FY2017's 22 equipment count columns are a repeating group and become rows of `reservation_equipment`.
- Nights equals end date minus start date in every FY2024 row with an end date, so it is not stored.

For all years, `source_file_park` gained the column `other_file_rows`. The schema script adds it with a
rerunnable step to a database created before (it did so on `nps_tourism`).

## Part 1: replay and reruns

- Each file is replayed in order-time order, in 774 transactions (128 for FY2017, 646 for FY2024) of
  about 2,000 reservations, never splitting an order. 0 transactions are out of order within a file.
- The load took 1,227 s. A rerun took 1,282 s and inserted or changed 0 rows; every table count was
  unchanged.
- Crash test on a separate test server: the loader was killed during the FY2024 replay. The database
  held exactly the first 81 transactions, with no split orders. `02_checks.sql` listed FY2024 as
  unfinished (`source_file.completed_at` is NULL). A rerun restored contents identical to a clean load.

## All years: FY2015–FY2025

### Row accounting

Stored = source − dropped − rejected − repeated ids − rows already stored from another file. 0 rows were
dropped in any year: every five-park row is an NPS row, and none is a DONATION row. The 55 (fiscal year, park)
rows are in `a2_full_checks.txt`.

| Fiscal year | Rows read | Source rows | Rejected | Repeated ids | From another file | Stored | Transactions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2015 | 2,949,219 | 264,700 | 0 | 15,233 | 0 | 249,467 | 125 |
| 2016 | 2,980,714 | 252,339 | 0 | 62 | 0 | 252,277 | 127 |
| 2017 | 3,303,553 | 255,728 | 0 | 71 | 0 | 255,657 | 128 |
| 2018 | 3,299,805 | 270,580 | 0 | 5,089 | 435 | 265,056 | 133 |
| 2019 | 3,479,643 | 349,485 | 203 | 29 | 4,433 | 344,820 | 173 |
| 2020 | 5,114,789 | 621,470 | 119 | 0 | 15 | 621,336 | 311 |
| 2021 | 8,354,633 | 1,110,180 | 215 | 0 | 1 | 1,109,964 | 555 |
| 2022 | 9,050,916 | 1,070,603 | 48 | 0 | 1 | 1,070,554 | 536 |
| 2023 | 8,984,097 | 878,820 | 13 | 0 | 0 | 878,807 | 440 |
| 2024 | 9,777,935 | 1,290,973 | 14 | 0 | 0 | 1,290,959 | 646 |
| 2025 | 9,435,531 | 1,248,058 | 6 | 715 | 0 | 1,247,337 | 624 |
| **Total** | **66,730,835** | **7,612,936** | **618** | **21,199** | **4,885** | **7,586,234** | **3,798** |

Stored per park: GRCA 749,650, GRSM 1,073,559, YELL 226,567, YOSE 3,621,247, ZION 1,915,211.

### How the files differ

| Files | Columns | Park name in | Order number | Order time as written | Stored as | File holds |
| --- | ---: | --- | --- | --- | --- | --- |
| FY2015–FY2017 | 56 | RegionDescription | `2-36681763` | `2016-10-02` | that date at 00:00 | orders placed in the fiscal year |
| FY2018 | 57 | RegionDescription | `2-36681763` | `2017-10-01 00:08:51` | Eastern time converted to UTC | orders placed in the fiscal year |
| FY2019–FY2020 | 36, 37 | ParentLocation | `0100000570-1` | `2018-10-05 14:21:14.491`, 0–3 decimals | UTC as written | stays that start in the fiscal year |
| FY2021 | 35 | ParentLocation | `0100000570-1` | `2020-10-23 14:02:08.611` | UTC as written | stays that start in the fiscal year |
| FY2022–FY2025 | 35 | ParentLocation | `0100000570-1` | `2020-10-23T14:02:08.611187Z` | UTC | stays that start in the fiscal year |

- The loader tells the two layouts apart by their columns. FY2015–FY2018 have EntityType and 22 equipment
  count columns; FY2019+ have inventorytype, discount, equipmentdescription and equipmentlength. FY2018 adds
  LatLongPoint, FY2020 adds nights, and FY2021 drops customer state and country.
- FY2015–FY2018 list facility 250849, the only Yellowstone facility in those files, under "Yellowstone
  Non-Commercially Guided Snowmobile Access Program"; FY2019+ list it under Yellowstone National Park.
- FY2018 times are Eastern local time. On the 15th of the month, orders at the Upper, Lower and North Pines
  campgrounds in Yosemite peak at 10:00 in FY2018, and at 14:00 and 15:00 UTC in FY2019 and FY2022. The peak
  moves by 4 to 5 hours, the offset of Eastern time from UTC.
- FY2019–FY2021 times carry no zone but are UTC. 177 orders appear in both the FY2021 file and the FY2022 file,
  whose times end in `Z`; their times differ by less than 1 ms. Orders that appear in both FY2019 and FY2020, or
  in both FY2020 and FY2021, have identical times.
- An old-system order number such as `2-36409369` is stored whole, with no order line. FY2019+ numbers such
  as `0100000570-1` are order 0100000570, line 1.
- The FY2018 zip holds a README: "Due to the transition between Rec.gov vendors the FY 2018 historical
  reservation data has not been fully verified for accuracy and completeness."

### Rules for the other years

- A reservation is stored once, from the first file that lists it; the skipped rows are counted in
  `other_file_rows`. FY2018 lists 435 FY2016 and FY2017 bookings again with a later order time. FY2019 repeats
  4,433 FY2015–FY2018 bookings under new reservation ids, with the old order number and the same start date.
  FY2020 lists 13 FY2016–FY2018 orders again under new reservation ids, with start dates in May to July 2020.
  These 4,446 rows are skipped because their order number is already stored. FY2020–FY2022 skip 4 more rows
  whose reservation id is already stored: bookings of the previous file that run into the next fiscal year.
- An order that already has lines from an earlier file keeps that file's order time and customer values.
- A reservation id repeated within a file keeps the row with the earliest order time, then the lowest product
  id, then the first row in the file. FY2015: 15,123 Entrance + LOTTERY pairs for the Half Dome cables and the
  snowmobile program, where the Entrance row is kept. FY2018: 5,089 extra rows, mostly site changes in which the
  later row carries the time of the change. FY2019: 29 ids `<id>__250849` that repeat a reservation `<id>`.
  FY2025: 715 extra rows, permits and campsites listed with two start dates.
- Rejected, 618 rows in `data/work/ridb/rejects_FY*.csv`: 522 end before they start, 65 have a negative amount,
  27 FY2021 rows have the order date `0001-01-01`, and 4 FY2022–FY2023 rows have a stray quote in the customer ZIP
  that shifts the later columns.
- Facility attributes come from the newest file and, within a file, from the row with the latest order time
  (19 facilities change during FY2025). Every five-park facility lies north of the equator and west of
  Greenwich, so coordinates without a positive latitude and a negative longitude are stored as NULL. 300015
  and 300016 give a negative latitude and a positive longitude in every FY2020–FY2021 row. 4675323 does so in
  every FY2022–FY2024 row and in 4,834 of its 10,422 FY2025 rows; its latest FY2025 orders give 44.598244 /
  −110.547170, which are stored. 7 of the 90 facilities have no coordinates: 300015, 300016 and five whose
  newest file gives none.
- A product keeps its stored use type when a newer file gives none: FY2018 gives none for 6 products that
  FY2015–FY2017 list as Day.
- Customer ZIPs over 10 characters and 12 equipment lengths over 999,999.99 are stored as NULL.
- The loader stops if asked to add a fiscal year older than one already stored; reruns of stored years are
  allowed. `nps_tourism` already held FY2017 and FY2024, so the eleven years went into the empty database
  `nps_tourism_full`.

### Part 1 rows in the full load

Part 1 was loaded again on the test server with the part 1 code; its `02_checks.sql` output equals
`a2_checks.txt` apart from the load times. Compared row by row with that copy, `nps_tourism_full` holds the same
1,546,616 FY2017 and FY2024 reservations (0 differ), the same 555,301 equipment rows and the same 5,136
products. The differences come from the rules above:

- 307 FY2024 orders also have a line in the FY2023 file and keep its values. 13 of them get a different
  customer ZIP. 5 of these 13 have none in FY2023, so their ZIP is NULL.
- 76 of the 79 facilities take their attributes from a newer file (75 from FY2025): 2 names, 6 ZIPs, 2 states
  and 21 coordinate pairs differ.

### Load, rerun and copy

- The load ran on a test MySQL 26.7 server on the same laptop, with its own data folder on port 3397.
  FY2015–FY2019 took 6.6 minutes on 2026-11-13 with a 256 MB buffer pool. FY2020–FY2025 took 75 minutes on
  2026-11-14 with 512 MB, lowered to 256 MB from partway through the FY2021 replay until the start of the FY2024
  replay. The second run started with FY2015–FY2019 again and wrote 0 orders and 0 reservations for them.
- 0 transactions are out of order within a file. The loader's working set peaks at 1.1 GB, during FY2024.
- A rerun of all eleven years took 3,016 s and inserted or changed 0 orders and 0 reservations; every table
  count was unchanged. The use-type rule was added after the load, so this rerun set the use type of the 6
  FY2018 products back to Day.
- The test database was copied to the project server with `mysqldump`. The dump file has 2,328,294,553 bytes
  and was written in 56 s. It drops and recreates each table and holds no CREATE DATABASE, USE, DEFINER or GTID
  statement, so `nps_app` imports it with its rights on `nps_tourism_full` alone. The import ended with exit
  code 0; a test import of the same file at a 128 MB buffer pool took 30 min 51 s.
- The copy is identical. CHECKSUM TABLE gives the same values on both servers, shown below, and
  `02_checks.sql` gives byte-identical output (`a2_full_checks.txt`).

| Table | park | source_file | source_file_park | facility | product | booking_order | reservation | reservation_equipment |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CHECKSUM | 2415391416 | 1273965251 | 2781889666 | 1805411763 | 534672949 | 1314699666 | 3515714462 | 3426894241 |

## Findings for later steps

- FY2015–FY2018 files are split by order date, FY2019+ files by start date. FY2017 holds orders from
  2016-10-01 to 2017-09-30 with starts from 2016-09-29 to 2018-09-30. FY2024 holds starts from 2023-10-01 to
  2024-09-30 with orders from 2020-12-16 to 2025-01-06. The FY2018 file holds stays up to 2019-09-29. FY2019
  repeats 4,433 bookings of the FY2015–FY2018 files, and FY2020 lists 13 of their orders again with 2020 start
  dates. The loader keeps the older rows in both cases.
- A later fiscal-year file adds orders older than ones already loaded. From FY2020 on, every file starts with
  orders placed before its fiscal year (FY2025: from 2022-02-16). The incremental extract therefore uses
  `reservation_key`, which grows in load order, as its watermark instead of order time.
- `reservation_key` has gaps: an upsert of a row that already exists uses up a key value. After the reruns,
  the 7,586,234 reservations hold keys from 1 to 8,953,544, and the next key is 16,786,469.
- MySQL 26.7 skips a foreign key check when the index behind the key has a further column that is NULL.
  The order foreign key therefore uses an index on `order_number` alone.

## Reproduce

Needs `MYSQL_URL` in `.env` pointing at an empty database (see the docstring of `src/load_mysql.py`).
Part 1 is the same with `--fiscal-years 2017 2024` and the database `nps_tourism`.

```bat
uv run python src/ingest/ridb_download.py --fiscal-years 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025
uv run python src/load_mysql.py --fiscal-years 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025
"C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u nps_app -p nps_tourism_full --table < mysql\02_checks.sql
```

Copy from the test server (port 3397) to the project server, then compare the checksums with the table above:

```bat
"C:\Program Files\MySQL\MySQL Server 26.7\bin\mysqldump.exe" -h 127.0.0.1 -P 3397 -u root --single-transaction --no-tablespaces --set-gtid-purged=OFF --default-character-set=utf8mb4 nps_tourism_full > data\work\nps_tourism_full.sql
"C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u nps_app -p -e "CREATE DATABASE IF NOT EXISTS nps_tourism_full"
"C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u nps_app -p nps_tourism_full < data\work\nps_tourism_full.sql
"C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u nps_app -p nps_tourism_full --table -e "CHECKSUM TABLE park, source_file, source_file_park, facility, product, booking_order, reservation, reservation_equipment"
```
