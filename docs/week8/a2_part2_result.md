# Week 8 test A2 part 2: result

The pre-registered forecast test ran once on 2026-11-15. Its rules are in `a2_part2_spec.md`. The rules and the four
code files were committed and pushed in `6fc8f112381b19fe49e7ba55e430ed69bb4ce93a` before the reservations were
extracted from MySQL.

The two models, in short (the spec has the full definitions):
- **B** forecasts a month's visits as exp of the mean log visits of that calendar month over the 98 training months,
  2015-01 to 2023-12 without 2020-03 to 2020-12.
- **R** multiplies B by exp(delta × d).
  - d is the number of camping reservations on the books at 00:00 UTC on day 1 of the month, minus the usual count
    for that calendar month, in thousands.
  - The usual count is the mean over the complete training months of that calendar month.
  - delta is fitted once on the training months.
- Both models are scored on the 21 held-out months, 2024-01 to 2025-09.

## Verdict

**PASS: R is lower in 3 of 5 parks (GRCA, YOSE, ZION).** 06 printed:

> camping reservations on the books at the start of the month lower held-out error in 3 of 5 parks; reservations are
> usable leading signals

| Park | delta | SE(delta) | Per 1,000 bookings | MAPE B | MAPE R | R lower | Months R lower |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| GRCA | 0.0318 | 0.0352 | +3.23% | 16.39 | 15.89 | yes | 9 of 21 |
| GRSM | 0.0560 | 0.0108 | +5.75% | 12.39 | 13.09 | no | 8 of 21 |
| YELL | −0.2511 | 0.1346 | −22.21% | 16.97 | 21.93 | no | 3 of 21 |
| YOSE | 0.0346 | 0.0254 | +3.52% | 8.19 | 8.06 | yes | 11 of 21 |
| ZION | 0.1464 | 0.0346 | +15.76% | 12.40 | 10.12 | yes | 15 of 21 |

- MAPE is over the 21 held-out months, in percent, rounded to 2 decimals as the pass rule compares it.
- delta and SE(delta) were fitted on the 98 training months. They equal the training-only table in the spec.
- Months R lower: the held-out months in which R's absolute percentage error is lower, both rounded to 2 decimals.

## What the PASS rests on

- **Margins.** B minus R is 2.28 MAPE points in ZION, 0.50 in GRCA and 0.13 in YOSE. Compared without rounding, the
  same three parks have the lower MAPE.
- **Training fit.** delta / SE(delta) is 5.16 in GRSM, 4.23 in ZION, 1.37 in YOSE, 0.90 in GRCA and −1.87 in YELL. Of
  the three parks that count, only ZION has a delta more than twice its SE.
- **ZION.** R is lower in the primary test and in every secondary row except one: B+POST has a lower MAPE than
  primary R (9.19 against 10.12).
- **GRCA.** The margin comes from 2025-07 to 2025-09.
  - In those months visits were 459,269, 423,811 and 369,315, and B forecast 689,859, 633,260 and 516,593. R's
    absolute percentage error is lower by 7.27, 6.29 and 4.16 points, which is 0.84 MAPE points together.
  - Over the other 18 held-out months, R's mean absolute percentage error is 11.77 and B's is 11.37.
  - Camping bookings on the books were below the usual count in all three months (4,884 against 6,446 in July). In
    July the drop is at Mather Campground: 2,941 against a mean of 4,638 in the seven complete training Julys that
    set the usual count (2016–2019, 2021–2023). North Rim Campground had 1,496 against 1,477.
- **YOSE.** R is not lower in 4 of the 12 secondary rows: S2 (7.91 against 7.42), S4 (14.02 against 8.19), S7 for
  2025 (9.87 against 8.99) and the RMSE of the log error (9.71 against 9.59). A check made after the run left out
  each held-out month in turn. Leaving out any one of 6 YOSE months (2024-04, 2024-06, 2024-10, 2025-05, 2025-06,
  2025-07) makes R not lower. In the other four parks no single month changes the outcome.
- **GRSM.** Its delta / SE(delta) is the highest of the five, but R's held-out MAPE is higher (13.09 against 12.39).
  With the 17 campgrounds that had camping stays in every year 2016–2019 and 2021–2023 held fixed (S1), R is lower
  (12.14 against 12.39). The spec's known limits list GRSM campgrounds that first have stays in 2018, 2020 and 2022.
- **YELL.** delta is negative.
  - It comes from the 22 complete training months, 2022-03 to 2023-12. d is not 0 in 14 of them, April to October of
    2022 and of 2023, and 5 of those are the June 2022 flood months 2022-06 to 2022-10 that S3 flags.
  - B is below the actual visits in all 21 held-out months. d is above 0 in 10 of them, so R lowers a forecast that
    is already too low.
  - With the flagged months left out of training (S3), delta is +0.1522 and R is still not lower (15.54 against
    15.27).
- **A post-2020 level.** In S6, month means plus a post-2020 level (B+POST) have a lower MAPE than primary R in GRCA
  (11.93 against 15.89), YELL (7.92 against 21.93) and ZION (9.19 against 10.12). With the level in both models, R+POST
  is lower than B+POST in GRCA, YOSE and ZION.

## Secondary checks

They are not used for pass/fail. Each row counts the parks where R is lower under the primary tie rule.

| Check | Parks where R is lower | Parks |
| --- | ---: | --- |
| S1 fixed campgrounds | 4 of 5 | GRCA, GRSM, YOSE, ZION |
| S2 training also without 2020-03 to 2021-06 | 2 of 5 | GRCA, ZION |
| S3 training also without flagged closure months | 3 of 5 | GRCA, YOSE, ZION |
| S4 all reservation types | 1 of 5 | ZION |
| S5 mean absolute log error | 3 of 5 | GRCA, YOSE, ZION |
| S5 MAE in visitors | 3 of 5 | GRCA, YOSE, ZION |
| S5 RMSE of the log error | 2 of 5 | GRCA, ZION |
| S6 B+POST against R+POST | 3 of 5 | GRCA, YOSE, ZION |
| S6 B+POST against primary R | 2 of 5 | GRSM, YOSE |
| S7 2024-01 to 2024-12 | 2 of 5 | YOSE, ZION |
| S7 2025-01 to 2025-09 | 2 of 5 | GRCA, ZION |
| S8 R lower and delta > 0 | 3 of 5 | GRCA, YOSE, ZION |

S3 drops no month in GRCA, GRSM or ZION, and delta > 0 in all three parks that count. In those parks the S3 and S8
rows therefore repeat the primary result. Every value is in `a2_inputs/a2_forecast_summary.csv`.

## How the run went

1. 2026-11-15 11:07:58 −06:00: commit `6fc8f11`, with the spec, the four code files, and `pyproject.toml` and
   `uv.lock` with pyarrow 26.0.1. It was pushed before the extract.
2. 11:09 to 11:14: `src/extract_reservations.py` wrote the full pull `pull_date=2026-11-15`. It holds 7,586,234
   reservations in 9 Parquet files, 110.5 MB, and took 283 s. A second run printed "no new or changed reservations".
3. Before the upload, the pull was checked on the laptop.
   - The 55 (fiscal year, park) counts equal `a2_full_checks.txt`.
   - 05 ran on the pull on the laptop, with DuckDB in place of Spark, and every assertion passed.
   - The camping signal computed straight from the spec equals the zip reconstruction in all 645 park-months.
   - Every number in the spec's expected checkpoints that does not need visits matched exactly.
   - The spec asks that any difference be traced to the 200 rows whose order time differs between MySQL and the zip
     reconstruction. They are FY2022 rows with the same order date in both, and they change 0 counts.
   - No check between the commit and the 06 run read real visits or computed a held-out error. The local 05 run
     used synthetic visits, and the other checks read only the pull and the zip reconstruction.
4. The pull was uploaded by hand to the landing Volume, and 05 ran on Databricks.
   - Its two CSVs are byte-identical to the local run's.
   - `gold_park_month_reservations` has 660 rows: `otb_complete` = 1 in 523 and NULL counts in 15.
5. 06 ran once with Run all at 12:19, after 05, and every cell finished without an error. Its export,
   `a2_inputs/06_forecast_test.html`, holds the same 13 cells as the committed notebook, with all of the printed
   output.
   - It printed `a2_forecast.py sha256 fa6960adfe4533890b158c2804c1a135617af3027abf7dd80d30f1dddd2fb47e`, the
     sha256 of the file in `6fc8f11`. It ran under pandas 2.3.3 and numpy 2.3.4.
   - Its gate printed 7,586,234 Silver rows against the manifest's 7,586,234, 0 of 55 cells differing, and
     `GATE PASSED: row counts match`.
   - No code or rule changed after `6fc8f11`, so there are no deviations.

| Gate step | Evidence |
| --- | --- |
| Source files to MySQL | `a2_full_checks.txt`: reconciliation difference 0 in all 55 rows |
| MySQL to Parquet | The extract renames the pull folder only after the rows written per (fiscal_year, park_code) and in total equal MySQL's. Read back: 7,586,234 rows, as many distinct keys, 55 counts equal to `a2_full_checks.txt` |
| Parquet to Bronze, Bronze to Silver, Silver to Gold | 05 reached its export cells, which come after all of its assertions. Its two CSVs are byte-identical to the local run's |
| Silver against the manifest again; the join returns 660 rows | 06 printed `GATE PASSED: row counts match` (Silver total and all 55 counts equal to the manifest) and `park x month: 660 rows`, which its join assertion requires |

## Reproduction

- **The committed code.** `src/a2_forecast.py` (sha256 `fa6960adfe4533890b158c2804c1a135617af3027abf7dd80d30f1dddd2fb47e`,
  the file in `6fc8f11`) was run on the downloaded CSVs on the laptop. It ran under three pairs of versions: pandas
  2.3.3 with numpy 2.3.4 (as on Databricks), pandas 3.0.6 with numpy 2.5.4, and pandas 2.2.3 with numpy 2.1.3.
  - Each run prints the same settings, tables and verdict as 06. The one exception is the line comparing delta with
    lstsq: the largest difference is 2.2e−15 on Databricks and 2.7e−15 on the laptop.
  - The monthly CSV is byte-identical to 06's.
  - The summary CSV is identical except one value, S2 ZION `error_r`: 11.454510446089563 on the laptop and
    11.45451044608956 on Databricks, a difference of 1.8e−15.
    - The difference is the same under all three pairs of versions.
    - Both values are 11.45 after rounding, and `r_lower` and `months_r_lower` are the same.
- **An independent version.** B, R and every check were rebuilt from the spec text alone, without the code. The
  exception is the S6 R+POST SE, which uses the code's formula (see below).
  - All 65 summary rows agree within 4e−12 relative. The largest gap is the S6 GRSM delta, and the errors agree
    within 1.3e−13.
  - All 645 monthly rows agree within 5.2e−13, and every count of parks is the same.
- **The inputs.** In `a2_inputs_park_month.csv`, `visitors` equals the 2026-11-09 export of `gold_park_month` in all
  660 rows. The reservation columns equal 05's `gold_park_month_reservations.csv`.

From the repository root, this command reruns the test from the files in `docs/week8/a2_inputs/`:

```
uv run python src/a2_forecast.py --park-month docs/week8/a2_inputs/a2_inputs_park_month.csv --camping-facility-month docs/week8/a2_inputs/gold_facility_month_camping.csv --out <folder>
```

## Spec statements that differ from the run

None of these changes a count used by the test or the verdict.
- The spec says "R = B in 4 of YELL's 21 held-out months", counting January and February. R = B in 8 months, because
  `camping_otb` and its usual count are both 0 in 2024-03, 2024-11, 2024-12 and 2025-03, so d = 0.
- The spec gives SE(delta) = sqrt(RSS / (n − 13) / sum(d²)) and says each secondary check "uses the primary rules
  unless stated".
  - For R+POST in S6, the code instead reports the least-squares SE of delta in the 14-coefficient model, with
    n − 14 degrees of freedom.
  - For example, it gives 0.0206 in GRCA against 0.0201 by the spec's formula, and 0.0471 against 0.0319 in ZION.
  - SE is descriptive, and no count of parks depends on it.
- The spec says Silver removes backslashes and line breaks that the FY2018 file gives the Mather and Big Creek names.
  No facility name in the pull has either, because MySQL keeps each facility's name from its newest file. The
  cleaning changes nothing.
- The spec says YOSE timed entry runs "in the summers of 2021, 2022, 2024 and 2025 but not 2023". The pull has YOSE
  timed-entry stays in 2023: 9,096 in February and 1 in July, all at facility 10086745. They are entry tickets, not
  camping, so no camping count changes.

## Files

`docs/week8/a2_inputs/` holds the five CSV files that left Databricks, and 06 exported as HTML with its output:

| File | Rows | From |
| --- | ---: | --- |
| `06_forecast_test.html` | | 06 |
| `gold_park_month_reservations.csv` | 660 | 05 |
| `gold_facility_month_camping.csv` | 2,760 | 05 |
| `a2_inputs_park_month.csv` | 660 | 06 |
| `a2_forecast_summary.csv` | 65 | 06 |
| `a2_forecast_monthly.csv` | 645 | 06 |

The CSV files hold counts per park-month or campground-month, NPS visits, forecasts and errors. The HTML holds 06's
code and its printed output, which is counts and other aggregates. No reservation row left Databricks. The campground file leaves out `first_order_date`, which in its 39 campground-months with one stay would
be that reservation's order date.
