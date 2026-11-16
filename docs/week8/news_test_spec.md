# News forecast test: pre-registered rules

This document fixes the rules of the news forecast test before the news tables are built on Databricks and before
any news model is scored. It is committed in one commit (commit 1) with `a1_part2_spec.md` and the code that builds
the tables and runs the test. When it was committed, no one had computed a held-out error of a news model with real
visits. The result document, `docs/week8/news_test_result.md`, gives this commit's hash.

The proposal (`docs/Project_proposal.pdf`, Week 8 test 2) sets the reservation test and ends:

> "Pass if row counts match and reservations lower held-out error in at least three parks; otherwise the result is
> descriptive, not a forecast. News coverage joins the comparison after the full scan."

The README plan for Nov 23-29 adds: "Count it as a share of all GKG records per month, because GDELT has no files from
2025-06-15 to 2025-07-01."

| Wording | Rule in this test |
| --- | --- |
| News coverage | `news_url_per_million`: the park's GDELT match rows that pass URL rule R0, per million GKG records, in the GDELT files of one UTC month |
| joins the comparison | A2's test (`a2_part2_spec.md`) with only the signal swapped: B against R on the same training and held-out months, by MAPE, with A2's tie rule and 3 of 5 parks |
| after the full scan | the full scan is done: 403,694 GKG files from 2015-02-18 to 2026-11-08 were listed, and 403,565 of them were read ok. The test also waits for the precision check of R0 (`a1_part2_spec.md`) |
| a share of all GKG records per month | 1e6 x the count / the records of the same UTC month. The share is the only treatment of missing GDELT files |
| A2's forecast origin, 00:00 UTC on day 1 | the forecast of month m uses the news of UTC month m - 1 |

## The signal

For park p and UTC month m:

```
news_url(p, m)             = match rows of park p with url_rule = 1 whose GDELT file is in UTC month m
records(m)                 = well-formed GKG records in the ok GDELT files of UTC month m
news_url_per_million(p, m) = 1e6 * news_url(p, m) / records(m)
x(m)                       = news_url_per_million(p, m - 1)
```

1. A match row is one row (record_id, url, park_code) of `silver_gdelt_matches`: a GKG record that names the park in
   full in V1Locations, V2EnhancedLocations, V1Organizations or V2.1AllNames (`a1_gdelt_test.md`, Matching).
2. `url_rule` = 1 when the URL path holds the park's own name: rule R0 of `a1_part2_spec.md`, taken from
   `src/gdelt_url_rule.py`.
3. The month is the UTC month of the GDELT file, cut from the text of `file_ts`.
4. **Lag of one month.** Every file of UTC month m - 1 has a file_ts before 00:00 UTC on day 1 of m, A2's forecast
   origin. So R uses no news from after the origin, the same information set as A2's bookings.

- The counts are match rows. A record that names two parks counts once in each, and a URL seen again in a later
  record counts again. That is the basis of `records` too.
- The first GKG file of the scan is from 2015-02-18 23:00 UTC, so the news columns are NULL in 2015-01. x(2015-01)
  (news of 2014-12, not in the table) and x(2015-02) (news of 2015-01) are NULL. There d = 0, as for
  `otb_complete` = 0 in A2.
- **No coverage threshold.** 2015-02 has an ok file in 35.5% of its 15-minute slots (the scan starts on 2015-02-18),
  and 2025-06 in 45.5% (the outage). Both are used, as shares. 2025-06 is the news of held-out month 2025-07.
- **UTC months, not local months.** A local month would move the files of the first hours of day 1 (UTC) into month
  m - 1, and they come after the origin. Assigning rows to each park's local month would move 0.48% (YOSE) to 1.47%
  (GRCA) of a park's R0 passes; among the park-months with at least 50 R0 passes, the largest share moved is GRCA
  2020-04, 71 of 297 (23.9%).
- The primary signal is `news_url_per_million`. Secondary check N1 uses `news_all_per_million`, all full-name
  matches, without the URL rule.

## The two models

Each park is fitted on its own. The models are A2's (`a2_part2_spec.md`, "The two models"), with x swapped. V(m) is
`visitors` in `gold_park_month` (NPS recreation visits), k is the calendar month 1-12 of m, and T is the 98 training
months.

```
y(m)  = ln V(m)
a(k)  = mean of y over the months of T in calendar month k
B:      V_B(m) = exp(a(k))
R:      V_R(m) = exp(a(k) + delta * d(m)) = V_B(m) * exp(delta * d(m))
d(m)  = (x(m) - xbar(k)) / 100   if x(m) is not NULL and calendar month k carries the term
d(m)  = 0                        otherwise
delta = sum(e * d) / sum(d * d) over T, with e = y - a(k);  delta = 0 if sum(d * d) = 0
```

- C is the months of T where x is not NULL: the term months, 2015-03 on, 96 per park. A calendar month carries the
  term when C has at least 2 months in it; all 12 do. xbar(k), the usual news level, is the mean of x over the months
  of C in calendar month k.
- B is least squares on 12 month dummies, with no intercept and no trend. Within each calendar month d sums to 0 over
  T, so R's month terms equal B's and delta is the regression of B's residuals on d. The code checks delta against
  `numpy.linalg.lstsq` to 1e-9.
- SE(delta) = sqrt(RSS / (n - 13) / sum(d^2)), with RSS the residual sum of squares of R and n the training months,
  is reported as a description.
- **Scale.** d is in units of 100 URL-rule matches per million GKG records (1 per 10,000 records). In words: R moves B
  by 100 x (e^delta - 1) percent for every 100 matches per million above the usual level of that calendar month. The
  scale sets only the unit of delta and SE(delta); the forecasts do not depend on it.
- **B is A2's B.** The same training months and the same visits give the same a(k). Its 645 forecasts equal
  `forecast_b` of `docs/week8/a2_inputs/a2_forecast_monthly.csv`.
- Neither model has a bias correction on exp, regularisation or a refit. There are no spike-month exclusions.

**Code.** `src/news_forecast.py` imports `src/a2_forecast.py`, which this test does not change, and uses its `fit()`,
`forecast()`, `score()` and error measures. A2's `term()` divides by 1,000 bookings, so `news_forecast.py` passes it
x times 10, which gives d = (x - xbar(k)) / 100. The module stops if A2's settings in `a2_forecast.py` differ from its
own copy.

## Training, held-out months and the forecast origin

| | GRCA | GRSM | YELL | YOSE | ZION |
| --- | ---: | ---: | ---: | ---: | ---: |
| Training months (both models): 2015-01..2023-12 without 2020-03..2020-12 | 98 | 98 | 98 | 98 | 98 |
| Term months (training months with x not NULL) | 96 | 96 | 96 | 96 | 96 |
| Calendar months with the term | 12 | 12 | 12 | 12 | 12 |
| Held-out months: 2024-01..2025-09 | 21 | 21 | 21 | 21 | 21 |

- The held-out forecasts use the news of 2023-12 to 2025-08.
- All 105 held-out months are scored, with no exclusions.

**Forecast origin.** Each held-out month m is forecast at 00:00 UTC on day 1 of m. a, xbar and delta are fitted once
on the training months and never refit.
- Used: visits through 2023-12 (through the fixed parameters), the calendar month, and the news of UTC month m - 1.
- Not used: visits from 2024-01 on (they only score the forecasts), news of month m or later, reservations (except
  in N6) and weather.
- Both models get the same visit information. They differ only by the news term.

## Error measure, gates and pass rule

```
MAPE = (100 / 21) * sum over the 21 held-out months of |V_hat(m) - V(m)| / V(m)
```

MAPE is computed per park for B and for R.

**Gates.** The code asserts every step below and stops on the first difference. If a gate fails, nothing is scored.

| Step | Assertion | Where |
| --- | --- | --- |
| Exports to the Volume | `gdelt_matches`, `gdelt_daily` and `gdelt_audit` hold only `pull_date=` folders, and every file is `<dataset>_<shard>.csv` or `.csv.gz` with a shard s1..s4. (a) Each shard is in at least one pull of each dataset; (b) within a pull the three datasets hold the same shards; (c) one file per (dataset, pull, shard); (d) all files of a dataset have the same header, the same column names in the same order, read with Python's csv module. The pull chosen for each shard is printed | `07_gdelt_news` pre-flight |
| Volume to Bronze | each file loads as many rows as Python's csv module reads from it, and no value ends in a carriage return. Bronze is read with `enforceSchema` false, so Spark checks the header of every file against the column names it uses and stops at a file that differs | `07_gdelt_news` |
| Bronze to Silver | each Silver table keeps the newest pull of each shard (DENSE_RANK), one pull per shard, the pull the pre-flight chose; unique keys: (record_id, url, park_code), file_ts, day; after TRY_CAST, 0 NULLs in every column except `source` and `themes` (`silver_gdelt_matches`) and `error` (`silver_gdelt_files`); file_ts has 14 digits on a quarter hour and parses as a time; `url_rule` and `md5_ok` are 0 or 1; `status` is ok, missing_on_server, md5_mismatch or error | `07_gdelt_news` |
| Silver against Silver | every file with matches has an audit row, status ok and one shard, and every file's match rows = the audit's `matched_rows`; per day, records = the audit's `rows_read` of ok files, and the files by status = the audit; each day is in one shard, and the days run without a gap; Python `passes()` = Spark `url_rule` on every row, counted inside Spark with a UDF | `07_gdelt_news` |
| Silver to Gold, months | `gold_month_gkg` has one row per month of `gold_park_month`, none on one side only; 0 NULLs; files_ok <= files_listed <= slots; coverage in [0, 1]; files listed, files ok and records = `silver_gdelt_days` in the same months, and files ok and records = `silver_gdelt_files` | `07_gdelt_news` |
| Silver to Gold, park-months | `gold_park_month_news`: rows = distinct keys = the rows of `gold_park_month` = the rows of their full outer join, 0 keys on one side only; NULLs only in the four news columns and exactly in the months with no ok GDELT file, which must be [201501], 20 cells; news_url <= news_all; the sums of news_all and news_url = the Silver rows (and those with `url_rule` = 1) in the months of `gold_park_month` | `07_gdelt_news` |
| Gold to CSV | read back: the same columns in the same order, the same row count, and in each column as many empty fields as the table has NULLs | `07_gdelt_news`, `08_news_test` |
| Gold to the test | the precision flags file exists, is the score file of a guarded `score` and holds valid flags (below; with test flags, the no-peek check too); `gold_park_month_news` has 660 rows = 660 distinct keys = the rows of `gold_park_month` = the full outer join, 0 keys on one side only; the four news columns hold 20 NULL cells, all in the 5 rows of 201501; the join with visits and reservations keeps all 660 rows | `08_news_test` |

The gates compare tables with sources counted in the same run, not with fixed totals, because a later export changes
the totals. 07 prints the totals of the 2026-11-13 exports for comparison. Two values are fixed by the 660-key grid of
`gold_park_month`: 660 rows, and NULL news only in the 20 cells of 201501.

`news_forecast.run()` also stops, before it prints any error or the verdict, if:
- the input does not have exactly the five parks with one row per park and month, `month_start` on day 1, and every
  month from 2015-01 to 2025-09;
- a park lacks one of its 98 training months or 21 held-out months, or a calendar month is missing from the training
  months;
- visits are not above 0 in every training and held-out month;
- the lagged signal is NULL in a held-out month, or a park has other than 96 term months;
- a fitted coefficient differs from `numpy.linalg.lstsq` by more than 1e-9 (primary, N1, N2 and both N6
  coefficients);
- for N6, a held-out month has `otb_complete` = 0 or a NULL `camping_otb`;
- the precision flags are not valid (below). `run()` has no default for them.

**Pass rule.**
- Park p counts when `round(MAPE_R, 2) < round(MAPE_B, 2)` (Python `round`, on percent values).
- Values equal after rounding count as not lower. delta = 0 gives identical forecasts, which is a tie.
- The sign of delta is not part of the rule, because the proposal asks only for lower error. N5 adds a sign rule.
- PASS if the gates pass and at least 3 of the 5 parks count. 08 prints exactly one of these two sentences, with N the
  number of parks that count:
  - PASS: "news coverage of the month before lowers held-out error in N of 5 parks; news coverage is a usable leading
    signal"
  - DESCRIPTIVE: "news coverage lowers held-out error in N of 5 parks; the result is descriptive, not a forecast"

## Precision flags from A1 part 2

- 08 reads `docs/week8/a1_part2_score.json`, which a guarded `src/gdelt_sample.py score` writes and A1 part 2 commits
  with its result (commit 3b). It takes three values: the pooled precision `P`, `pass` (P >= 0.90) and per park
  `parks.<park>.below_bar` (at most 32 of its 40 sampled rows about the park).
- Without the file, 08 stops before its gate and nothing is scored.
- The file must be the score file of a guarded score: `git_guard` is true, `commits` is an object, `parks` is an
  object and there is no top-level `below_bar`. Otherwise 08 and the CLI stop with "`<path>` is not the score file of a
  guarded score (git_guard is not true); a test score or the short test form is read only with test flags" (or a
  message naming the missing commits or the per-park flags).
- The flags must be valid: P between 0 and 1; `pass` true or false and equal to (P >= 0.90), not checked when P is
  within 1e-6 of 0.90, because `score` compares the exact fraction; and `below_bar` true or false for each of the five
  parks and no other.
- The flags mark the result. They never change it. After the verdict, 08 prints one PRECISION line, with `<P>` to 4
  decimals, following the flag `pass`:
  - if `pass` is true: "PRECISION: pooled precision P = `<P>` meets the 0.90 bar";
  - if `pass` is false: "PRECISION: MARKED BELOW THE PRECISION BAR: the URL rule's pooled precision P = `<P>` is below
    0.90, so the whole news result is marked below the bar";
  - then, when parks are marked below the bar, "; parks marked below the bar: `<parks>`; R is lower in `<k>` of them",
    followed by ": `<the parks where R is lower>`" when k > 0; when no park is, "; no park is marked below the bar".
- Every summary row has `below_precision_bar`, true or false: true for a park marked below the bar, and true for
  every park when `pass` is false.

## Test flags and the no-peek guards

Tests use the same code with test flags: `--test-flags` in the CLI, and `TEST_FLAGS` in 08, which is true only when
`tests/run_notebook_local.py --test-flags` defines it before the first cell.
- With test flags, the flags may also come from the score file of a test score (`git_guard` false) or from the short
  form `{"P": ..., "pass": ..., "below_bar": {"GRCA": ..., ...}}`. 08 and the CLI then print "TEST FLAGS from `<path>`
  ...: the precision marks of this run are not a result".
- With test flags, every visitors value from 202401 on in the input must differ from the committed real value of the
  same park and month in `docs/week8/a2_inputs/a2_inputs_park_month.csv`. The check compares for equality only and
  prints only "no-peek check: `<n>` visitors values from 202401 equal the committed real values", or "could not be
  compared" when the files cannot be compared, never a value or an error message. Any equal value, or no comparison,
  stops the run before anything is fitted. The CLI then prints "REFUSED: test flags need synthetic visits from 202401
  in every row of the input. Nothing was scored." and exits with code 1. 08 runs the check after the join, before
  `run()`.
- Without test flags, the CLI refuses a test score or the short form: "REFUSED: `<reason>`. Nothing was scored.", exit
  code 1. `--precision-flags` is required; without it the CLI stops with "the following arguments are required:
  --precision-flags" (exit code 2).
- Without test flags, the CLI runs the same visits check and prints the same line. When a value is equal or the
  files cannot be compared, it runs only if `--precision-flags` is `docs/week8/a1_part2_score.json` of the git
  repository that holds `src/news_forecast.py`, committed at HEAD and unchanged since (its blob equals the blob at
  HEAD), and prints "no-peek check: docs/week8/a1_part2_score.json is committed at HEAD and unchanged, so real visits
  may be used". Otherwise it stops before anything is fitted with "REFUSED: the input may hold real visits from
  202401, so --precision-flags must be docs/week8/a1_part2_score.json of the git repository `<root>`, committed at
  HEAD and unchanged: ...", exit code 1. A score file that is only staged, edited after its commit, or outside the
  repository is refused. So the CLI computes no held-out error with real visits before the A1 part 2 result is
  committed, and a rerun with real visits needs a git clone of the repository.
- The local runner `tests/run_notebook_local.py` checks a `--table gold_park_month` file before Spark starts, in the
  same way, for any notebook, and prints "no-peek check: `<n>` visitors values from 202401 in the gold_park_month
  stand-in equal the committed real values". It parses the file for this check exactly as for its view, and refuses
  a file whose header names a column twice, so the check and the view read the same column. It refuses the run
  ("REFUSED: `<reason>`. Nothing was run.", exit code 1) when a value is equal or the files cannot be compared, and
  either `--test-flags` is given or the flags file that 08 will read (`--flags`, else 08's default) is not
  `docs/week8/a1_part2_score.json` of the repository, committed at HEAD of the git repository whose root is the
  repository and unchanged since. So until the A1 part 2 result is committed, the runner refuses a `gold_park_month`
  stand-in that holds a committed real visit from 2024-01.
- 08 itself runs no git command: on Databricks it reads the score file of its Git folder and accepts only the score
  file of a guarded score (above).

## Secondary checks

The same 08 run computes 6 secondary checks. They are printed under "SECONDARY CHECKS, not used for pass/fail", each as
"parks where R is lower: k of 5" with per-park values. Each uses the primary rules unless stated.

| Check | What changes |
| --- | --- |
| N1 all matches | x = `news_all_per_million` of the month before: every full-name match, without the URL rule |
| N2 wider closure window | training also without 2020-03 to 2021-06 (92 training months, 90 term months per park), A2's S2 window |
| N3 other error measures | on the primary forecasts: mean absolute log error and RMSE of the log error, both in log points (100 x ln), and MAE in visitors, each with the 2-decimal tie rule |
| N4 by year | MAPE over 2024-01 to 2024-12 (12 months) and over 2025-01 to 2025-09 (9 months) |
| N5 sign | a park counts only if R is lower and delta > 0 |
| N6 reservations and news | y = a(k) + delta_otb * d_otb + delta * d by least squares, with A2's reservation term d_otb (`camping_otb`, A2's `otb_complete` and A2's scale of 1,000 bookings) and the news term d, fitted together on the 98 training months and checked against lstsq. Compared with B, and with A2's reservation-only R. SE from RSS / (n - 14) |

- Every summary row also has `months_r_lower`: the number of scored months in which R's error is lower, with both
  errors rounded to 2 decimals.
- In N6, `error_b` is the forecast it is compared with (B, or A2's reservation-only R) and `error_r` is reservations
  plus news. `delta_otb` and its SE are the reservation coefficient.
- Reported with no test: per park delta, SE(delta), the term months and the effect per 100 matches per million; the
  URL rule's pass rate per year and park (news_url / news_all), from the news counts only; and actual visits, the
  lagged news, B and R for every month 2015-01 to 2025-09 (`news_forecast_monthly`).

`news_forecast.run()` prints its settings first:
`TRAIN = 201501..202312`, `CLOSED = 202003..202012`, `HELD_OUT = 202401..202509`, `SIGNAL = news_url_per_million`,
`LAG = 1`, `SCALE = 100`, `MIN_YEARS_PER_MONTH = 2`, `METRIC = MAPE`, `TIE_DECIMALS = 2`, `PARKS_NEEDED = 3`,
`PRECISION_BAR = 0.9`; for the secondary checks N1 `SIGNAL = news_all_per_million`, N2 `WIDE_CLOSED = 202003..202106`
and N6 `camping_otb` with A2's `SCALE = 1000`; and "no coverage threshold, no spike exclusions, no refit".

## Run discipline

1. `docs/week8/a1_part2_spec.md`, this document, `src/gdelt_url_rule.py`, `src/gdelt_sample.py`,
   `src/gdelt_label_page.py`, `src/news_forecast.py`, `databricks/notebooks/07_gdelt_news.py`,
   `databricks/notebooks/08_news_test.py`, `tests/run_notebook_local.py` and `tests/check_gdelt_gold.py` are
   committed together and pushed (commit 1) before 07 runs on Databricks. `src/a2_forecast.py` does not change.
   Before commit 1, the local forecast files named in "Data seen before this commit" are deleted, and a search of
   the scratchpad for `news_forecast_monthly.csv` and `news_forecast_summary.csv` finds none.
2. 07 runs with Run all after `00_setup` and `03_gold`. It does not depend on the labels. It prints the gates, the
   totals and the months with an ok file in less than 95% of their slots (month level), never a park-month value. It
   prints the sha256 of `src/gdelt_url_rule.py`. Its two CSV files are downloaded and checked with
   `tests/check_gdelt_gold.py` (Pipeline, step 2).
3. 08 runs once, with Run all, after `05_reservations` and 07, and after the A1 part 2 result (its commit 3b, with
   `a1_part2_score.json`) is pushed and pulled into the Git folder. 08 prints the sha256 of `src/news_forecast.py`,
   `src/a2_forecast.py` and `a1_part2_score.json`, which must equal the committed files.
4. If an assertion stops 07 or 08 before the verdict, nothing is scored. The cause and the fix are reported, and the
   notebook runs again.
5. The rules do not change after commit 1. A code fix that Databricks needs to run is made before the verdict, does
   not touch the signal, the model, the windows or the pass rule, and is reported as a deviation with its commit. A
   change after the verdict is reported as a deviation next to the original result, never in its place.
6. The code is deterministic, so a rerun gives the same numbers. `src/news_forecast.py` reruns the test from
   `docs/week8/news_inputs/news_inputs_park_month.csv` and the score file, in a git clone of the repository (the
   no-peek check of the CLI needs the score file committed at HEAD).
7. The three CSV files of 08 and the two of 07 are committed under `docs/week8/news_inputs/` with
   `docs/week8/news_test_result.md`. `gdelt_sample.py draw` and `score` refuse a working tree with an untracked file,
   so until that commit the downloaded files are kept outside the repository.
8. Before this commit the code ran only on test data: the committed exports, real visits through 2023-12, synthetic
   visits for every month from 2024-01 (the visits of the same calendar month of 2023 times exp(N(0, 0.10)), rounded;
   numpy `default_rng` seed 20240101 in the module tests and 20261117 in the notebook integration run and the later 08
   and CLI tests), and fake precision flags files. The training-only fit below used real visits through 2023-12 only.

## Pipeline

| Step | Where | Code | Output |
| --- | --- | --- | --- |
| 1. Tables | Databricks, Run all after `00_setup` and `03_gold` | `databricks/notebooks/07_gdelt_news.py`, which imports `src/gdelt_url_rule.py` | `bronze_gdelt_matches`, `bronze_gdelt_daily`, `bronze_gdelt_audit`, `silver_gdelt_matches`, `silver_gdelt_files`, `silver_gdelt_days`, `gold_month_gkg`, `gold_park_month_news`; `gold_month_gkg.csv`, `gold_park_month_news.csv` |
| 2. Check | laptop | `tests/check_gdelt_gold.py`, the command below | both files compared with a recomputation from `data/landing` |
| 3. Test | Databricks, Run all once | `databricks/notebooks/08_news_test.py`, which imports `src/news_forecast.py` and `src/a2_forecast.py` | `news_forecast_summary` (55 rows), `news_forecast_monthly` (645 rows); `news_inputs_park_month.csv`, `news_forecast_summary.csv`, `news_forecast_monthly.csv` |

The check of step 2, from the repository root, with `<folder>` the folder outside the repository that holds the two
downloaded files (the command in the last cell of 07 and in the docstring of `tests/check_gdelt_gold.py`):

```
uv run --no-project --python 3.12 --with pandas==2.2.3 --with numpy==2.1.3 --with duckdb==2.0.0 python -I tests/check_gdelt_gold.py --month-gkg <folder>/gold_month_gkg.csv --park-month-news <folder>/gold_park_month_news.csv
```

It ends with "RESULT: both files match the recomputation" and exit code 0. The files stay outside the repository
until the news result is committed (Run discipline 7).

- The Bronze tables hold every CSV value as text, with `pull_date`, `_source_file` and `_ingested_at`. They are read
  with `header`, `escape='"'` and `multiLine`, as `docs/gdelt_scan.md` sets them, and with FAILFAST and
  `enforceSchema` false as checks.
- `silver_gdelt_matches` has one row per (record_id, url, park_code), from the newest pull of its shard, with
  `file_ts`, `file_day` and `month_key` (UTC), `url_path`, `url_rule` (0 or 1) and `tone` as DOUBLE; an empty field
  is NULL. `silver_gdelt_files` is the audit and `silver_gdelt_days` the daily counts, typed with TRY_CAST.
- `gold_month_gkg` has one row per month of `gold_park_month` (132): `month_key`, `month_start`, `files_listed`,
  `files_ok`, `slots` = 96 x the days of the month, `coverage` = files_ok / slots, and `records`. 2015-01 has 0 in
  every count, never NULL. It is month level and never summed over parks.
- `gold_park_month_news` has one row per key of `gold_park_month` (660): `park_code`, `month_key`, `month_start`,
  `news_all`, `news_url`, `news_all_per_million`, `news_url_per_million`. The four news columns are NULL in 2015-01
  only; elsewhere a park-month without a match has 0.
- The COMMENT of each Silver and Gold table states its grain and that a month is the UTC month of the GDELT file.
- `news_inputs_park_month.csv` holds the 660 input rows: `park_code`, `month_key`, `month_start`, `visitors`, the four
  news columns, `otb_complete` and `camping_otb`. All are aggregates.
- Anyone can rerun the test from 08's input file and the committed score file, in a git clone of the repository:
  `uv run python src/news_forecast.py --park-month docs/week8/news_inputs/news_inputs_park_month.csv --precision-flags docs/week8/a1_part2_score.json --out <folder>`.

## Expected checkpoints

These numbers come from local runs on the committed exports (`pull_date=2026-11-13`), with PySpark 4.3.0 for 07.
Any difference in the Databricks run is reported and traced, never tuned away.

**Pre-flight.** Each shard keeps `pull_date=2026-11-13`. Check (d): `gdelt_matches` 4 files with one header of 8
columns, `gdelt_daily` 4 files with 8 columns, `gdelt_audit` 4 files with 13 columns.

**Silver**
- `silver_gdelt_matches`: 636,731 rows and as many distinct keys; 184,220 with `url_rule` = 1. 56,920 rows belong to
  records that match 2 or more parks, and 1,010 are extra rows of a (url, park) seen again. `source` is empty in 8
  rows and `themes` in 31,464.
- `silver_gdelt_files`: 403,694 files: 403,565 ok, 128 missing on the server, 1 MD5 mismatch, 0 errors. 202,541 files
  have matches.
- `silver_gdelt_days`: 4,282 days, 2015-02-18 to 2026-11-08, without a gap; 735,225,688 records in ok files.

**Gold, 2015-01 to 2025-12**
- `gold_month_gkg`: 132 rows; 373,743 files listed, 373,615 ok, 703,098,249 records.
- `gold_park_month_news`: 660 rows; news_all 612,919, news_url 174,604; 20 NULL cells, all in 201501.

| Park | news_all | news_url |
| --- | ---: | ---: |
| GRCA | 56,664 | 23,085 |
| GRSM | 58,840 | 9,647 |
| YELL | 270,675 | 90,045 |
| YOSE | 179,127 | 43,088 |
| ZION | 47,613 | 8,739 |

Months with coverage below 0.95: 201501 0.0%, 201502 35.5%, 201510 94.3%, 201712 90.4%, 201801 92.4%, 201802 93.9%,
201804 94.4%, 202010 56.8%, 202011 59.4%, 202108 81.1%, 202303 94.8%, 202506 45.5%.

The local CSV files: `gold_month_gkg.csv` 132 rows, 7,120 bytes, sha256
`e4147632e63b490087d4a0a5daf929bc766de88abfe9ff0fc1c1b19fce048b7c`; `gold_park_month_news.csv` 660 rows, 44,414
bytes, sha256 `f1e39aadb5dcbbcb71c95aa32e32983187287e4d5629f49459f7b7912a4a8211`. `check_gdelt_gold.py` finds both
equal to its recomputation, with a largest share difference of 0. It checks the Databricks files the same way:
integers and text exactly, shares within 1e-9.

**Training-only fit** of the primary model, with visits through 2023-12, fitted on the training months only and never
forecast:

| Park | delta | SE(delta) | Per 100 matches per million | Term months | Calendar months with the term |
| --- | ---: | ---: | ---: | ---: | ---: |
| GRCA | 0.0415 | 0.0433 | +4.24% | 96 | 12 |
| GRSM | -0.1138 | 0.1269 | -10.76% | 96 | 12 |
| YELL | -0.0266 | 0.0135 | -2.63% | 96 | 12 |
| YOSE | -0.0140 | 0.0240 | -1.39% | 96 | 12 |
| ZION | -0.0756 | 0.0778 | -7.28% | 96 | 12 |

- delta equals the lstsq coefficient in every park; the largest difference is 6.7e-15 with pandas 2.2.3 and numpy
  2.1.3 (3.2e-15 with pandas 3.0.6 and numpy 2.5.4).
- delta is above 0 only in GRCA. No delta is more than twice its SE; YELL's ratio, 1.97, is the largest.

Training-only coefficients of the secondary models (delta, with SE):

| Park | N1 all matches | N2 wider closure (92 / 90 months) | N6 delta_otb | N6 news delta |
| --- | ---: | ---: | ---: | ---: |
| GRCA | 0.0167 (0.0282) | 0.0317 (0.0426) | 0.0302 (0.0352) | 0.0397 (0.0434) |
| GRSM | -0.0199 (0.0154) | -0.0658 (0.1249) | 0.0555 (0.0109) | -0.0798 (0.1118) |
| YELL | -0.0139 (0.0084) | -0.0268 (0.0137) | -0.1851 (0.1413) | -0.0206 (0.0142) |
| YOSE | -0.0108 (0.0088) | -0.0152 (0.0241) | 0.0356 (0.0255) | -0.0160 (0.0239) |
| ZION | -0.0259 (0.0201) | -0.0687 (0.0729) | 0.1445 (0.0347) | -0.0549 (0.0714) |

N6 used the camping counts of `docs/week8/a2_inputs/a2_inputs_park_month.csv`. On the same training months, A2's
reservation-only delta equals the committed `a2_forecast_summary.csv` in every park.

**B.** B's 645 forecasts equal A2's `forecast_b`, so 08's MAPE_B equals A2's: GRCA 16.39, GRSM 12.39, YELL 16.97,
YOSE 8.19, ZION 12.40 (`a2_part2_result.md`).

**Output shapes.** `news_forecast_summary`: 55 rows (primary 5, N1 5, N2 5, N3 15, N4 10, N5 5, N6 10).
`news_forecast_monthly`: 645 rows (train 490, closed 50, held_out 105). `news_inputs_park_month.csv`: 660 rows.
`src/a2_forecast.py` sha256: `fa6960adfe4533890b158c2804c1a135617af3027abf7dd80d30f1dddd2fb47e`.

## Known limits

- **Supply-side and syndication noise.** The count follows what outlets publish and reprint, not only interest in the
  park. Each reprint of a wire story is its own row: in A1's test week one syndicated column was 10 of 100 matches and
  one AP story 7. The denominator moves too: GKG records per year fell from 105.3 million (2016) to 42.3 million
  (2025).
- **Rule drift.** R0's pass rate moves with URL formats and the source mix: YOSE between 11.3% (2024) and 34.5%
  (2022), GRCA between 26.8% (2020) and 52.8% (2019). So `news_url` partly tracks URL style. N1 uses all matches, and
  08 prints the yearly pass rate.
- **Spike months.** One event can fill a park-month. The TV series *Yellowstone* in titles is 26% of YELL's R0 passes
  in 2024-11 and 28% in 2024-12, both held-out months. With the lag, their news is x of held-out months 2024-12 and
  2025-01. In news that enters training months: the Skywalk or Grand Canyon West is 48% of GRCA's passes in 2019-09,
  and the Yellowstone River is 43% of YELL's in 2023-06. These shares come from URL patterns, not labels. All months
  are scored as they are.
- **The outage month.** 2025-06 has an ok file in 45.5% of its slots. Its share is the news of held-out month
  2025-07.
- **Small counts.** ZION has 9 of 131 months with fewer than 10 R0 passes, 2 of them in 2024-01..2025-09. One article
  more or less moves its share.
- **Precision of the signal.** R0's precision comes from 200 labeled rows, with a 95% interval of about +-5.8 points
  at 0.90, and its recall within name matches is not measured (`a1_part2_spec.md`).
- **A small test.** 5 parks, 21 months each, and parameters fixed at one origin, the end of 2023. N4 and
  `months_r_lower` show how stable each result is.

## Data seen before this commit

- **Visits and A2's errors are public.** NPS visits for every month 2015-01 to 2025-12 are in the repository
  (`gold_park_month`, `docs/week8/a2_inputs/a2_inputs_park_month.csv`). A2's monthly forecasts and percentage errors
  for every held-out month are committed in `a2_forecast_monthly.csv`, and `a2_part2_result.md` and the README discuss
  them. B of this test is A2's B, so B's held-out MAPE is already known (Expected checkpoints). A2's reservation-only
  R, which N6 compares with, is known too.
- **News summaries were printed.** The Claude agents that designed and reviewed these rules printed, from the GDELT
  exports only:
  - per park and year 2015-2025, R0 passes per million GKG records, all matches per million and the pass rate
    (design output `gdelt_design/critique/verify2.txt`, section b);
  - R0 passes per park in the held-out months 2024-01..2025-09 and per period (`gdelt_design/critique/verify.txt`
    section 5, `verify2.txt` section c), and R0 passes and pass rates per park and year 2015-2026
    (`gdelt_design/rule/rule_report.txt` section 2);
  - the share of a park-month's R0 passes taken by single-event URL patterns in the busiest months, including held-out
    2024-11 and 2024-12 (`gdelt_design/rule/month_impact.txt`);
  - per park the smallest monthly R0 count, the months with fewer than 10 passes in training and held-out months, the
    2025-06 counts, and the UTC month-edge shifts (`gdelt_design/critique/verify3.txt` section 2,
    `gdelt_design/critique/verify.txt` section 4);
  - per park the smallest, median and largest monthly share of all matches (`gdelt_design/lake/share_scale_output.txt`);
  - all matches per park and year 2015-2026, and per park the smallest, median and largest monthly count, the
    months at the edges and in the outage, and the 5 lowest months (`gdelt_design/profile/profile_results.txt`
    sections 8 and 9, `gdelt_design/code/inspect_exports.txt`).
  A design reader also wrote the all-match count of every park-month 2015-02..2025-12 to a file
  (`gdelt_design/lake/matches_park_month_raw.csv`), and the local builds wrote `gold_park_month_news.csv` with all 660
  park-month values. These files are in the Claude session scratchpad, not in the repository.
- **No rule uses them.** Each rule follows the proposal's wording, the README plan, A2's spec or the structure of the
  GDELT files: the lag from A2's origin, the share from the outage, NULL only where there is no file, A2's model with
  only x swapped. The choices not to set a coverage threshold and not to exclude spike months were made with the
  spike shares known; they are the choice to change nothing. The training-only fit above was computed after the rules
  were fixed.
- **R's held-out forecasts were written.** R's forecast of a held-out month depends only on the training visits
  (real, through 2023-12), the fitted delta and the news. So the local test runs wrote the real B and R forecasts of
  every held-out month 2024-01..2025-09 to 9 `news_forecast_monthly.csv` files in the Claude session scratchpad, next
  to synthetic visits: in `gdelt_build/work/integrate` (`run08/exports`, `run08f/exports`, `cli08`),
  `gdelt_build/work/news` (`out_08_local`, `out_b_cli`, `out_b_cli_pd3`, `out_b_run`) and `gdelt_build/work/fix_news`
  (`cli_t`, `h1/exports`). All 9 hold the same held-out forecasts. No one compared them with real visits. They were
  deleted before this commit, with the 9 `news_forecast_summary.csv` files next to them, and a search of the
  scratchpad for `news_forecast_monthly.csv` and `news_forecast_summary.csv` then found none. The training-only deltas
  in Expected checkpoints give the same forecasts.
- **Tests used synthetic visits.** Every test that forecast a held-out month replaced the visits from 2024-01 on with
  synthetic values. A check on 2026-11-16, before the forecast files above were deleted, over every test file under
  `gdelt_build/work` with a visitors column and rows from 2024-01 found 20 files
  (`gdelt_build/work/spec2/synth_recheck.py`, output `synth_recheck.txt`). 18 are synthetic in every such row: 7 from
  seed 20240101 and 11 from seed 20261117. The other two belong to the refusal tests of the no-peek guards: a
  copy of the committed `docs/week8/a2_inputs/a2_inputs_park_month.csv` that the guards compare with, and an input
  with 119 synthetic values and one text value. The refusal tests that gave real visits from 2024-01 as input were
  all refused before anything was fitted. The MAPE values and verdicts printed by the other tests describe synthetic
  data and are not results.
- When this document was committed, no one had computed a held-out error of a news model with real visits.
