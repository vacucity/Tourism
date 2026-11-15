# Week 8 test A2 part 2: pre-registered forecast test

This document fixes the rules of A2 part 2 before the reservations are extracted for the test. It is committed in one
commit with the four code files that run the test, before the extract reads MySQL. When it was committed, no one had
computed a held-out error with real visits. The result document gives this commit's hash.

The proposal (`docs/Project_proposal.pdf`, Week 8 test 2) sets the test:

> "... computes the reservations on the books at the start of each month from the FY2015–FY2025 files and compares a
> seasonal baseline with and without them on held-out months from January 2024 to September 2025, training without
> the 2020 closures. Pass if row counts match and reservations lower held-out error in at least three parks;
> otherwise the result is descriptive, not a forecast."

| Proposal wording | Rule in this test |
| --- | --- |
| reservations on the books at the start of each month | camping reservations ordered before 00:00 UTC on day 1, for stays that start in that month |
| from the FY2015–FY2025 files | the MySQL load `nps_tourism_full` (7,586,234 reservations, `a2_mysql_test.md`) |
| a seasonal baseline with and without them | B: calendar-month means of log visits; R: B plus a reservation term |
| held-out months from January 2024 to September 2025 | 2024-01 to 2025-09, 21 months per park |
| training without the 2020 closures | 2015-01 to 2023-12 without 2020-03 to 2020-12, 98 months per park |
| row counts match | the gate below: row counts agree at every step from the source files to Gold |
| lower held-out error in at least three parks | lower MAPE after rounding to 2 decimals, in at least 3 of the 5 parks |

## The signal

`camping_otb` for park p and month m counts the reservations in `silver_reservations` that meet all four rules:

1. `park_code` is p.
2. `family` is `camping`: `inventory_type` is `Site` (FY2015–FY2018 files) or `CAMPING` (FY2019+), and the
   product's `use_type` is not `Day`. A NULL use type counts as overnight. 05 first gives five ticket and parking
   facilities (274348, 10112471, 233374, 258488, 255304) their own families; none of them has a `Site` or `CAMPING`
   row.
3. `start_date` falls in month m. A reservation counts once, in the month its stay starts, whatever its length.
4. `ordered_date_utc` is before day 1 of m. `ordered_date_utc` is `DATE(booking_order.ordered_at)`, computed in
   MySQL, and `ordered_at` is stored in UTC, so the rule is "ordered before 00:00 UTC on day 1".

- The unit is reservations. Each counts once, so the counts add up over campgrounds, parks and months.
- FY2015–FY2017 give order dates only, stored at 00:00, so an order dated day 1 of m is not on the books for m.
  FY2018 Eastern times were converted to UTC by the loader. FY2019+ times are UTC.
- The counts are gross. The files have no status and no cancellation date, so a booking made before the cut counts
  even if it was cancelled or refunded later. The signal does not use `total_paid`, because a refund can come
  after the cut.

**Why camping only.** Camping is the only family that is in every file FY2015–FY2025 in all four of GRCA, GRSM,
YOSE and ZION, and every overnight `Site` campground id continues as `CAMPING`. Other programs start or stop within
the period: YOSE timed entry runs in the summers of 2021, 2022, 2024 and 2025 but not 2023, ZION shuttle tickets run
2020-07 to 2021-06, and YOSE wilderness permits and ZION Angels Landing start in 2022-04. Day-use sites and the
48,477 FY2015–FY2017 rows without a start date are left out.

**Complete months.** `otb_complete` is 1 from the month of the park's first camping order plus 12 months, through
September of the newest fiscal year. Before that, some bookings for a month could predate the files.
- The first camping order is 2014-10-01 for GRCA, GRSM, YOSE and ZION, where the files begin, so their counts are
  complete from 2015-10. For YELL it is 2021-03-24, so 2022-03. The files hold no YELL camping reservations before
  2021, so its earlier camping counts are 0.
- The FY2019+ files are split by stay start date, and FY2025 ends with stays starting on 2025-09-30. Every held-out
  month is complete. Months from 2025-10 have NULL counts, because FY2026 is not published.

## The two models

Each park is fitted on its own. V(m) is `visitors` in `gold_park_month` (NPS recreation visits), k is the calendar
month 1–12 of m, x(m) is `camping_otb`, and T is the 98 training months.

```
y(m)  = ln V(m)
a(k)  = mean of y over the months of T in calendar month k
B:      V_B(m) = exp(a(k))
R:      V_R(m) = exp(a(k) + delta * d(m)) = V_B(m) * exp(delta * d(m))
d(m)  = (x(m) - xbar(k)) / 1000   if otb_complete(m) = 1 and calendar month k carries the term
d(m)  = 0                         otherwise
delta = sum(e * d) / sum(d * d) over T, with e = y - a(k);  delta = 0 if sum(d * d) = 0
```

- C is the months of T with `otb_complete` = 1. A calendar month carries the term when C has at least 2 months in
  it. xbar(k), the usual bookings, is the mean of x over the months of C in calendar month k.
- B is least squares on 12 month dummies, with no intercept and no trend. R adds d. Within each calendar month d
  sums to 0 over T, so R's month terms equal B's and delta is the regression of B's residuals on d. The code
  checks delta against `numpy.linalg.lstsq` to 1e-9.
- In words: R moves B by 100 × (e^delta − 1) percent for every 1,000 camping reservations above the usual count for
  that calendar month. It equals B when bookings are at the usual level, and wherever d = 0.
- Bookings enter in thousands, not logs. Winter counts jump from 0 to hundreds when campgrounds become reservable
  (GRCA December: 0 through 2021, 378 in 2022; ZION January: 0 through 2020, 483 in 2021). On a log scale those
  jumps would dominate delta.
- Neither model has a bias correction on exp, regularisation or a refit. SE(delta) = sqrt(RSS / (n − 13) / sum(d²)),
  with RSS the sum of squared residuals of R and n the number of training months, is reported as a description.

## Training, held-out months and the forecast origin

| | GRCA | GRSM | YELL | YOSE | ZION |
| --- | ---: | ---: | ---: | ---: | ---: |
| Training months (both models) | 98 | 98 | 98 | 98 | 98 |
| Complete from | 2015-10 | 2015-10 | 2022-03 | 2015-10 | 2015-10 |
| Term months (complete training months) | 89 | 89 | 22 | 89 | 89 |
| Calendar months with the term | 12 | 12 | 10 | 12 | 12 |
| Held-out months | 21 | 21 | 21 | 21 | 21 |

- 2015-01 to 2023-12 has four months with 0 visits, all inside the excluded window: GRCA, YELL and YOSE in 2020-04,
  and YOSE in 2020-05. No other month is dropped. S2 and S3 show the effect of dropping more.
- YELL's January and February have only 2023 in C, so they carry no term: R = B in 4 of YELL's 21 held-out months.
- All 105 held-out months are scored, with no exclusions.

**Forecast origin.** Each held-out month m is forecast at 00:00 UTC on day 1 of m. a, xbar and delta are fitted
once on the training months and never refit.
- Used: visits through 2023-12 (through the fixed parameters), the calendar month, and `camping_otb(m)`. The count
  is rebuilt from the FY2024 and FY2025 files, which were published later, from orders placed before the cut only.
- Not used: visits from 2024-01 on (they only score the forecasts), orders on or after the cut, `total_paid` and
  final stay counts of held-out months, and weather.
- Both models get the same visit information. They differ only by the reservation term.

## Error measure and pass rule

```
MAPE = (100 / 21) * sum over the 21 held-out months of |V_hat(m) - V(m)| / V(m)
```

MAPE is computed per park for B and for R. It is scale-free, so each park is judged against its own baseline.

**Gate: row counts match.** The MySQL load reconciled the source files with MySQL (`a2_mysql_test.md`). The code
asserts every later step below and stops on the first difference. If the gate fails, nothing is scored.

| Step | Assertion | Where |
| --- | --- | --- |
| Source files to MySQL | reconciliation difference 0 for all 55 (fiscal year, park) rows | `mysql/02_checks.sql`, output in `a2_full_checks.txt` |
| MySQL to Parquet | on a full pull, rows written per (fiscal_year, park_code) and in total = the MySQL counts in the snapshot | `extract_reservations.py` |
| Parquet to Bronze | each pull folder holds exactly the files its manifest lists, with the listed byte sizes; rows per file = the manifest's `files[].rows`; rows per pull = `rows_new + rows_changed` | `05_reservations` |
| Bronze to Silver | Silver rows = distinct keys = the newest manifest's `snapshot.reservation_rows`, and every (fiscal_year, park_code) count = its `rows_by_fy_park` | `05_reservations`; 06 checks the total and the counts again |
| Silver to Gold | the `stays` of `gold_facility_month_reservations` add up to the Silver rows with a start date; `gold_park_month_reservations` has one row per key of `gold_park_month`, 0 keys on one side only, and `camping_otb` ≤ `camping_stays` in every row; the join in 06 returns 660 rows | `05_reservations`, `06_forecast_test` |

The gates compare with the newest pull's `_manifest.json`, not with fixed totals, because a later pull changes the
totals. 05 and 06 print the FY2015–FY2025 values for comparison.

`a2_forecast.run()` also stops, before it prints any error or the verdict, if the input does not have exactly the
five parks with one row per park and month, if a park lacks one of its 98 training months or 21 held-out months, if
a calendar month is missing from the training months, if visits are not above 0 in every training and held-out
month, if the bookings count is NULL in a complete training month or a held-out month, if a held-out month has
`otb_complete` = 0, or if a fitted coefficient differs from `numpy.linalg.lstsq` by more than 1e-9.

**Pass rule.**
- Park p counts when `round(MAPE_R, 2) < round(MAPE_B, 2)` (Python `round`, on percent values).
- Values equal after rounding count as not lower. delta = 0 gives identical forecasts, which is a tie.
- The sign of delta is not part of the rule, because the proposal asks only for lower error. S8 adds a sign rule.
- PASS if the gate passes and at least 3 of the 5 parks count. 06 prints exactly one of these two sentences, with
  N the number of parks that count:
  - PASS: "camping reservations on the books at the start of the month lower held-out error in N of 5 parks;
    reservations are usable leading signals"
  - DESCRIPTIVE: "reservations lower held-out error in N of 5 parks; the result is descriptive, not a forecast"

## Secondary checks

The same 06 run computes 8 secondary checks. They are printed under "SECONDARY CHECKS, not used for pass/fail",
each as "parks where R is lower: k of 5" with per-park values. Each uses the primary rules unless stated.

| Check | What changes |
| --- | --- |
| S1 fixed campgrounds | x sums the on-the-books counts of the campgrounds with at least 1 camping stay starting in every calendar year from max(2016, the park's first camping stay year + 1) through 2023, except 2020. Input: the camping rows of `gold_facility_month_reservations`, exported as `gold_facility_month_camping.csv` |
| S2 wider closure window | training also without 2020-03 to 2021-06 (92 months per park), which also leaves out the ZION shuttle-ticket period |
| S3 other closure months | training also without the months that have at least 50 camping stays starting in the month, of which at least 30% have `total_paid` = 0. Reservations of training months only |
| S4 all types | x = `all_otb`, every family with a start date |
| S5 other error measures | on the primary forecasts: mean absolute log error and RMSE of the log error, both in log points (100 × ln), and MAE in visitors, each with the 2-decimal tie rule |
| S6 stronger baseline | month means plus a post-2020 level POST (1 from 2021-01): B+POST against R+POST with the same d, and B+POST against primary R |
| S7 by year | MAPE over 2024-01 to 2024-12 (12 months) and over 2025-01 to 2025-09 (9 months) |
| S8 sign and stability | a park counts only if R is lower and delta > 0; `months_r_lower` (below) shows the stability |

- Every summary row also has `months_r_lower`: the number of scored months in which R's error is lower, with both
  errors rounded to 2 decimals. It shows how stable each result is.
- Reported with no test: per park delta, SE(delta), the term months, the effect per 1,000 bookings, the share of
  camping stays on the books by day 1, and actual visits, B and R for every month 2015-01 to 2025-09
  (`a2_forecast_monthly`).

`a2_forecast.run()` prints its settings first, so the output shows the rules it ran with:
`TRAIN = 201501..202312`, `CLOSED = 202003..202012`, `HELD_OUT = 202401..202509`, `SCALE = 1000`,
`MIN_YEARS_PER_MONTH = 2`, `METRIC = MAPE`, `TIE_DECIMALS = 2`, `PARKS_NEEDED = 3`, and for the secondary checks
`FIXED_FIRST_YEAR = 2016`, `WIDE_CLOSED = 202003..202106`, `FLAG_MIN_STAYS = 50`, `FLAG_PAID0_SHARE = 0.3`,
`POST_FROM = 202101`.

## Run discipline

1. This document, `src/extract_reservations.py`, `databricks/notebooks/05_reservations.py`, `src/a2_forecast.py`
   and `databricks/notebooks/06_forecast_test.py` are committed together and pushed before the extract runs, with
   `pyproject.toml` and `uv.lock` after `uv add pyarrow`.
2. 06 runs once, with Run all, after 05. The code is deterministic, so a rerun gives the same numbers. 06 prints the
   sha256 of `src/a2_forecast.py`, which must equal the committed file's.
3. If an assertion stops 05 or 06 before the verdict, nothing is scored. The cause and the fix are reported, and
   the notebook runs again.
4. A change to the code or the rules after the verdict is reported as a deviation next to the original result,
   never in its place.
5. Before this commit the code ran only on test data: FY2017 and FY2018 loaded into a throwaway MySQL server, a
   full-size stand-in of the Parquet built from the zip reconstruction, a synthetic FY2019–FY2025 pull, and
   synthetic visits for every month from 2024-01. The tests of `src/a2_forecast.py` used real visits through 2023-12.

## Pipeline

| Step | Where | Code | Output |
| --- | --- | --- | --- |
| 1. Extract | laptop, `uv run python src/extract_reservations.py` | `src/extract_reservations.py` | `data/work/landing/ridb_reservations/pull_date=YYYY-MM-DD/`: one Parquet file per block of 1,000,000 reservation keys, and `_manifest.json` |
| 2. Upload | Databricks Catalog, by hand | | `/Volumes/workspace/nps_tourism/landing/ridb_reservations/pull_date=YYYY-MM-DD/` |
| 3. Tables | Databricks, Run all after `03_gold` | `databricks/notebooks/05_reservations.py` | `bronze_ridb_reservations`, `silver_reservations`, `gold_facility_month_reservations`, `gold_park_month_reservations`; `gold_park_month_reservations.csv`, `gold_facility_month_camping.csv` |
| 4. Test | Databricks, Run all once | `databricks/notebooks/06_forecast_test.py`, which imports `src/a2_forecast.py` | `a2_forecast_summary` (65 rows), `a2_forecast_monthly` (645 rows); `a2_inputs_park_month.csv`, `a2_forecast_summary.csv`, `a2_forecast_monthly.csv` |

- The extract reads MySQL in one read-only REPEATABLE READ transaction and writes 16 typed columns per
  reservation. New rows are found by `reservation_key`, and changed rows by the reservation's or the order's
  `updated_at`. Order time cannot be the watermark, because a later fiscal-year file adds older orders. A second
  run prints "no new or changed reservations". The extract imports pyarrow, which `pyproject.toml` does not list
  yet; `uv add pyarrow` adds it.
- `silver_reservations` keeps one row per `reservation_key`, from the newest pull that holds it. It removes the
  backslashes and line breaks from facility names; the FY2018 file gives two names these characters (Mather and
  Big Creek campgrounds).
- `gold_park_month_reservations` has 9 columns: `park_code`, `month_key`, `month_start`, `otb_complete`,
  `camping_otb`, `camping_stays`, `camping_paid0_stays`, `all_otb`, `all_stays`. Notebooks 00–04 do not change.
- Anyone can rerun the test from the two input CSVs:
  `uv run python src/a2_forecast.py --park-month a2_inputs_park_month.csv --camping-facility-month gold_facility_month_camping.csv`.

## What stays private

The RIDB terms allow publishing aggregates only.
- Row-level reservations stay in MySQL, in `data/work` (git-ignored), in the landing Volume and in
  `bronze_ridb_reservations` and `silver_reservations`. The notebooks print no reservation rows.
- The Parquet leaves out `reservation_id`, `order_number`, customer ZIP, state and country, the fee columns and
  equipment.
- Only the five CSV files and the printed output of 05 and 06 leave Databricks. They hold counts and other
  aggregates, never a reservation row. The campground CSV leaves out `first_order_date`, which in a campground-month
  with one stay would be that reservation's order date.

## Expected checkpoints

The zip reconstruction rebuilt the loader's rules in DuckDB from the eleven zip files. It equals MySQL in all 55
(fiscal year, park) counts. It differs in one rule that changes counts: it uses each row's own order time, while
MySQL gives the lines of an order already stored from an earlier file that file's order time. This affects 200 rows.
It also takes each row's own use type, where MySQL keeps one per product; this changes the family of no row. The
numbers in this section come from the reconstruction. Any difference in the run is reported and traced to those 200
rows, never tuned away.

**Silver** (these counts are also MySQL's, `a2_full_checks.txt`): 7,586,234 rows and as many distinct keys.

| Fiscal year | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Reservations | 249,467 | 252,277 | 255,657 | 265,056 | 344,820 | 621,336 | 1,109,964 | 1,070,554 | 878,807 | 1,290,959 | 1,247,337 |

- Per park: GRCA 749,650, GRSM 1,073,559, YELL 226,567, YOSE 3,621,247, ZION 1,915,211.
- `park_code` NULL 0, family `other` 0. `start_date` NULL 48,477 (46,069 POS and 2,408 Site, all FY2015–FY2017).

**Gold**
- `gold_park_month_reservations`: 660 rows. `otb_complete` = 1 in 523: 120 each for GRCA, GRSM, YOSE and ZION
  (2015-10 to 2025-09) and 43 for YELL (2022-03 to 2025-09).
- 15 rows (2025-10 to 2025-12 in each park) have NULL counts. `camping_otb` ≤ `camping_stays` in every row.

| Park | Month | camping_otb | camping_stays |
| --- | --- | ---: | ---: |
| YOSE | 2017-10 | 6,279 | 9,078 |
| YOSE | 2018-10 | 6,873 | 9,218 |
| GRCA | 2018-10 | 4,820 | 7,286 |
| GRCA | 2023-07 | 6,121 | 8,878 |
| GRSM | 2022-06 | 8,038 | 11,824 |
| YELL | 2023-07 | 2,540 | 3,499 |
| YOSE | 2023-07 | 9,162 | 15,849 |
| ZION | 2023-07 | 3,773 | 5,684 |

The first three rows check the October 2018 vendor change. No file has an order between 2018-10-01 03:58 and
2018-10-05 14:21 UTC, and YOSE's October count is 6,279 in 2017 and 6,873 in 2018, so camping counts continue across
the gap.

**Secondary inputs**
- S1 campgrounds (share of the camping stays of those years): GRCA 2, Mather and North Rim (95.7%); GRSM 17
  (88.8%); YELL 3, Indian Creek, Lewis Lake and Slough Creek, years 2022–2023 (75.3%); YOSE 4, Upper, Lower and
  North Pines and Wawona (70.2%); ZION 1, Watchman (78.0%).
- S3 months dropped: YELL 2022-06 to 2022-10; YOSE 2019-02, 2019-03, 2021-01 and 2023-03; none in GRCA, GRSM or
  ZION. Training months become 98, 98, 93, 94 and 98.

**Training-only fit** of the primary model, with visits through 2023-12 and the reconstruction's camping counts:

| Park | delta | SE(delta) | Per 1,000 bookings | Term months | Calendar months with the term |
| --- | ---: | ---: | ---: | ---: | ---: |
| GRCA | 0.0318 | 0.0352 | +3.23% | 89 | 12 |
| GRSM | 0.0560 | 0.0108 | +5.75% | 89 | 12 |
| YELL | −0.2511 | 0.1346 | −22.21% | 22 | 10 |
| YOSE | 0.0346 | 0.0254 | +3.52% | 89 | 12 |
| ZION | 0.1464 | 0.0346 | +15.76% | 89 | 12 |

## Known limits

- **Supply, not demand.** The proposal warns that "booked slots can reflect permit supply rather than demand".
  Camping counts follow which campgrounds take reservations. GRSM campgrounds first have stays in 2018 (Balsam
  Mountain, Big Creek, Abrams Creek), 2020 (Deep Creek) and 2022 (Look Rock), and its camping reservations on the
  books rose from 23,978 in 2018 to 51,199 in 2021. ZION's South Campground has stays from 2018-03 to 2023-10. YOSE
  has no stays at Hodgdon in 2021, at Crane Flat in 2021–2023 or at Tuolumne Meadows in 2022–2024. A full campground
  caps the count. The primary test scores the signal as it is, and S1 holds the campgrounds fixed. A win can come
  from capacity that moves with visits.
- **YELL.** Its term rests on 22 months, 2022-03 to 2023-12, which include the June 2022 flood. 50.1% of YELL's
  camping stays starting in 2022 have `total_paid` = 0. Bookings made before the flood stayed on the books while
  visits fell, and the training delta is −0.25 per 1,000 bookings. S3 drops 2022-06 to 2022-10.
- **Gross bookings.** A cancellation made before the month starts cannot be removed without information from after
  the origin. After the July 2025 fire closed the North Rim, 79% of North Rim Campground stays starting in 2025-08
  and 81% in 2025-09 have `total_paid` = 0, against 0.8% in 2025-06. They count, and those months are scored. The
  FY2018 zip's README says its data "has not been fully verified for accuracy and completeness".
- **A small test.** 5 parks, 21 months each, and parameters fixed at one origin, the end of 2023. S7 and
  `months_r_lower` show how stable each result is.

## Visit data seen before this commit

The Claude agents that drafted and reviewed these rules were shown a facts report that included some 2024–2025 visit
summaries: GRCA's annual totals for 2024 and 2025, the range of its 2021–2025 monthly visits and its July–September
2025 visits relative to 2015–2019, ZION and YELL winter months relative to 2015–2019, and that every month 2015–2025
has a visit count and the only months with 0 visits are in 2020. No rule uses them. Each rule follows the
proposal's wording, reservation facts, or visits through 2023. The training-only fit above was computed after the
rules were fixed. Every test of the code replaced the visits from 2024-01 on with synthetic values. When this
document was committed, no held-out error with real visits had been computed.
