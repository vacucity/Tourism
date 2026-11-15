"""A2 part 2: does a seasonal baseline forecast monthly park visits better with camping reservations on the books?

This is the pre-registered test of docs/week8/a2_part2_spec.md. Each park is fitted on its own, on log monthly
recreation visits y = ln(visitors):
- Baseline B: a(k), the mean of y over the training months of calendar month k. Forecast exp(a(k)).
- With reservations R: B + delta * d. d is the number of camping reservations on the books at 00:00 UTC on day 1 of
  the month minus the usual number for that calendar month, in thousands. d is used only in months whose booking
  count is complete (otb_complete = 1) and in calendar months with at least 2 complete training months; elsewhere
  d = 0. delta is fitted once by least squares. Forecast exp(a(k)) * exp(delta * d).
- Training months: 2015-01..2023-12 without the 2020 closures (2020-03..2020-12), 98 per park, the same for both
  models. The parameters are fixed there and never refit.
- Scoring: MAPE over the 21 held-out months 2024-01..2025-09. A park counts when round(MAPE_R, 2) < round(MAPE_B, 2).
  PASS needs the row-count gate, which databricks/notebooks/06_forecast_test.py checks before it calls run(), and at
  least 3 of the 5 parks. Otherwise the result is descriptive, not a forecast.
Eight secondary checks (S1-S8) come from the same run. They are reported and never used for pass/fail.

All arithmetic is done in numpy with closed-form least squares, so Databricks serverless (pandas 2.x) and the
repository environment (pandas 3.0) give the same numbers. Both inputs are aggregates:
- park_month: gold_park_month joined with gold_park_month_reservations (park_code, month_key, visitors,
  otb_complete, camping_otb, camping_stays, camping_paid0_stays, all_otb).
- camping_fac: the family = 'camping' rows of gold_facility_month_reservations (park_code, facility_id,
  facility_name, month_key, stays, otb), for check S1.

06_forecast_test.py calls run(). To rerun the test from a2_inputs_park_month.csv (exported by 06) and
gold_facility_month_camping.csv (exported by 05), from the repository root:
  uv run python src/a2_forecast.py --park-month <park-month csv> --camping-facility-month <campground csv>
Add --out <folder> to also write a2_forecast_summary.csv and a2_forecast_monthly.csv.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

PARKS = ["GRCA", "GRSM", "YELL", "YOSE", "ZION"]
# Months are keys year * 100 + month (202507), like month_key in gold_park_month
TRAIN = (201501, 202312)
CLOSED = (202003, 202012)       # the 2020 closures, left out of training
HELD_OUT = (202401, 202509)
SCALE = 1000                    # d is in thousands of reservations
MIN_YEARS_PER_MONTH = 2         # complete training months a calendar month needs to carry the reservation term
METRIC = "MAPE"
TIE_DECIMALS = 2                # values equal after rounding count as not lower
PARKS_NEEDED = 3
TRAIN_MONTHS = 98               # per park
HELD_OUT_MONTHS = 21            # per park
LSTSQ_TOLERANCE = 1e-9

# Secondary checks
FIXED_FIRST_YEAR = 2016         # S1: campgrounds with a camping stay in every training year from here
WIDE_CLOSED = (202003, 202106)  # S2: also leaves out the ZION shuttle-ticket period (2020-07..2021-06)
# S3: drop training months with at least 50 camping stays starting in the month, of which at least 30% have
# total_paid = 0. A closure leaves its stays in the files, most of them refunded to 0.
FLAG_MIN_STAYS = 50
FLAG_PAID0_SHARE = 0.30
POST_FROM = 202101              # S6: the post-2020 level is 1 from this month on

CHECKS = {
    "primary": "camping reservations on the books (the pass/fail test)",
    "S1": "fixed campgrounds: on the books summed over the campgrounds with stays in every training year",
    "S2": "wider closure window: training also without 2020-03..2021-06",
    "S3": "other closure months excluded: training also without months with >= 50 camping stays, >= 30% unpaid",
    "S4": "all types: on the books summed over every reservation family",
    "S5": "other error measures on the primary forecasts",
    "S6": "stronger baseline: month means plus a post-2020 level, with and without the same reservation term",
    "S7": "by year: held-out 2024 and 2025 separately",
    "S8": "sign and stability: R lower and delta > 0; held-out months where R's error is lower",
}


def ape(forecast: np.ndarray, actual: np.ndarray) -> np.ndarray:
    """Absolute percent error per month."""
    return 100 * np.abs(forecast - actual) / actual


def abs_log_error(forecast: np.ndarray, actual: np.ndarray) -> np.ndarray:
    """Absolute log error per month, in log points (100 x natural log), so 2 decimals mean the same as for MAPE."""
    return 100 * np.abs(np.log(forecast) - np.log(actual))


def abs_error(forecast: np.ndarray, actual: np.ndarray) -> np.ndarray:
    """Absolute error per month, in visitors."""
    return np.abs(forecast - actual)


def root_mean_square(errors: np.ndarray) -> float:
    return np.sqrt(np.mean(errors ** 2))


# measure: (error per month, summary over months)
MAPE = "MAPE (%)"
MEASURES = {
    MAPE: (ape, np.mean),
    "mean abs log error (log points)": (abs_log_error, np.mean),
    "MAE (visitors)": (abs_error, np.mean),
    "RMSE of log error (log points)": (abs_log_error, root_mean_square),
}


def month_range(first: int, last: int) -> list[int]:
    """Month keys from first to last, both included."""
    keys = []
    year, month = divmod(first, 100)
    while year * 100 + month <= last:
        keys.append(year * 100 + month)
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return keys


def training_months(closed: tuple, dropped=()) -> list[int]:
    """TRAIN without the closed window and without the dropped months."""
    return [m for m in month_range(*TRAIN) if not closed[0] <= m <= closed[1] and m not in dropped]


def lower(error_b: float, error_r: float) -> bool:
    """The tie rule: R is lower only if it is lower after rounding to TIE_DECIMALS (Python round)."""
    return round(float(error_r), TIE_DECIMALS) < round(float(error_b), TIE_DECIMALS)


def print_settings() -> None:
    print("SETTINGS (docs/week8/a2_part2_spec.md)")
    print(f"  TRAIN = {TRAIN[0]}..{TRAIN[1]}, CLOSED = {CLOSED[0]}..{CLOSED[1]}, "
          f"HELD_OUT = {HELD_OUT[0]}..{HELD_OUT[1]}")
    print(f"  SCALE = {SCALE}, MIN_YEARS_PER_MONTH = {MIN_YEARS_PER_MONTH}, METRIC = {METRIC}, "
          f"TIE_DECIMALS = {TIE_DECIMALS}, PARKS_NEEDED = {PARKS_NEEDED}")
    print(f"  per park: {TRAIN_MONTHS} training months, {HELD_OUT_MONTHS} held-out months; parks: {', '.join(PARKS)}")
    print(f"  secondary: FIXED_FIRST_YEAR = {FIXED_FIRST_YEAR}, WIDE_CLOSED = {WIDE_CLOSED[0]}..{WIDE_CLOSED[1]}, "
          f"FLAG_MIN_STAYS = {FLAG_MIN_STAYS}, FLAG_PAID0_SHARE = {FLAG_PAID0_SHARE}, POST_FROM = {POST_FROM}")


# ---------------------------------------------------------------------------------------------------------------
# Inputs

PARK_MONTH_COLUMNS = ["park_code", "month_key", "visitors", "otb_complete",
                      "camping_otb", "camping_stays", "camping_paid0_stays", "all_otb"]
FACILITY_COLUMNS = ["park_code", "facility_id", "facility_name", "month_key", "stays", "otb"]


def prepare(park_month: pd.DataFrame) -> pd.DataFrame:
    """A typed copy with one row per park x month, sorted. NULL counts (months after the newest file) become NaN."""
    missing = [c for c in PARK_MONTH_COLUMNS if c not in park_month.columns]
    assert not missing, f"park_month lacks columns {missing}"
    pm = pd.DataFrame({
        "park_code": park_month["park_code"].astype(str).to_numpy(),
        "month_key": park_month["month_key"].astype("int64").to_numpy(),
        "visitors": park_month["visitors"].astype("float64").to_numpy(),
        "otb_complete": park_month["otb_complete"].astype("int64").to_numpy(),
        **{c: park_month[c].astype("float64").to_numpy() for c in PARK_MONTH_COLUMNS[4:]},
    })
    assert sorted(set(pm["park_code"])) == PARKS, f"parks must be {PARKS}, got {sorted(set(pm['park_code']))}"
    assert not pm.duplicated(["park_code", "month_key"]).any(), "park_month has a park x month twice"
    return pm.sort_values(["park_code", "month_key"], ignore_index=True)


def fixed_campgrounds(camping_fac: pd.DataFrame) -> dict:
    """S1: per park, the campgrounds with at least 1 camping stay starting in every calendar year from
    max(FIXED_FIRST_YEAR, the park's first camping stay year + 1) through the last training year, except 2020."""
    missing = [c for c in FACILITY_COLUMNS if c not in camping_fac.columns]
    assert not missing, f"camping_fac lacks columns {missing}"
    fac = pd.DataFrame({
        "park_code": camping_fac["park_code"].astype(str).to_numpy(),
        "facility_id": camping_fac["facility_id"].astype(str).to_numpy(),
        "facility_name": camping_fac["facility_name"].astype(str).to_numpy(),
        "month_key": camping_fac["month_key"].astype("int64").to_numpy(),
        "stays": camping_fac["stays"].astype("int64").to_numpy(),
        "otb": camping_fac["otb"].astype("int64").to_numpy(),
    })
    fac["year"] = fac["month_key"] // 100
    out = {}
    for park in PARKS:
        f = fac[fac["park_code"] == park]
        assert len(f), f"{park}: no camping rows in the campground x month input"
        first = max(FIXED_FIRST_YEAR, int(f["year"].min()) + 1)
        years = [y for y in range(first, TRAIN[1] // 100 + 1) if y != CLOSED[0] // 100]
        in_years = f[f["year"].isin(years)]
        stays = in_years.groupby(["facility_id", "year"])["stays"].sum()
        ids = sorted(fid for fid in in_years["facility_id"].unique()
                     if all(stays.get((fid, y), 0) > 0 for y in years))
        share = in_years.loc[in_years["facility_id"].isin(ids), "stays"].sum() / in_years["stays"].sum()
        names = [f.loc[f["facility_id"] == fid, "facility_name"].iloc[0] for fid in ids]
        otb = f[f["facility_id"].isin(ids)].groupby("month_key")["otb"].sum()
        out[park] = {"years": years, "ids": ids, "names": names, "share": float(share), "otb": otb}
    return out


def closure_months(pm: pd.DataFrame) -> dict:
    """S3: training months with at least FLAG_MIN_STAYS camping stays starting in the month, of which at least
    FLAG_PAID0_SHARE have total_paid = 0. Reservations only, past months only."""
    keys = training_months(CLOSED)
    out = {}
    for park in PARKS:
        g = pm[(pm["park_code"] == park) & pm["month_key"].isin(keys)]
        stays = g["camping_stays"].to_numpy()
        paid0 = g["camping_paid0_stays"].to_numpy()
        flag = (stays >= FLAG_MIN_STAYS) & (paid0 >= FLAG_PAID0_SHARE * stays)
        out[park] = g["month_key"].to_numpy()[flag].tolist()
    return out


# ---------------------------------------------------------------------------------------------------------------
# Models

def term(k: np.ndarray, complete: np.ndarray, x: np.ndarray, xbar: np.ndarray) -> np.ndarray:
    """d: bookings minus the usual bookings for the calendar month, in thousands. 0 in incomplete months and in
    calendar months that do not carry the term (xbar is NaN there)."""
    usual = xbar[k - 1]
    use = complete & ~np.isnan(usual)
    return np.where(use, (x - usual) / SCALE, 0.0)


def fit(train: pd.DataFrame, x: str, park: str) -> dict:
    """B and R for one park from its training months: month means, the usual bookings and delta."""
    k = train["month_key"].to_numpy() % 100
    visitors = train["visitors"].to_numpy()
    assert set(k.tolist()) == set(range(1, 13)), f"{park}: a calendar month is missing from the training months"
    assert (visitors > 0).all(), f"{park}: visitors must be > 0 in every training month"
    y = np.log(visitors)
    a = np.array([y[k == m].mean() for m in range(1, 13)])  # a[m - 1] belongs to calendar month m

    complete = train["otb_complete"].to_numpy() == 1
    xs = train[x].to_numpy()
    assert not np.isnan(xs[complete]).any(), f"{park}: {x} is NULL in a complete training month"
    years = np.array([np.sum(complete & (k == m)) for m in range(1, 13)])
    xbar = np.array([xs[complete & (k == m)].mean() if years[m - 1] >= MIN_YEARS_PER_MONTH else np.nan
                     for m in range(1, 13)])
    d = term(k, complete, xs, xbar)

    # d sums to 0 within each calendar month, so R's month terms equal B's and delta is the regression of
    # B's residuals on d
    e = y - a[k - 1]
    sdd = np.sum(d * d)
    delta = np.sum(e * d) / sdd if sdd > 0 else 0.0
    rss = np.sum((e - delta * d) ** 2)
    se = np.sqrt(rss / (len(y) - 13) / sdd) if sdd > 0 else np.nan
    lstsq_diff = 0.0
    if sdd > 0:
        dummies = [(k == m).astype(float) for m in range(1, 13)]
        coef = np.linalg.lstsq(np.column_stack(dummies + [d]), y, rcond=None)[0]
        lstsq_diff = abs(coef[12] - delta)
        assert lstsq_diff <= LSTSQ_TOLERANCE, f"{park}: delta {delta} differs from lstsq {coef[12]}"
    return {"a": a, "xbar": xbar, "delta": float(delta), "se": float(se), "d": d, "lstsq_diff": float(lstsq_diff),
            "n_train": len(y), "n_term": int(complete.sum()),
            "term_calendar_months": int(np.sum(years >= MIN_YEARS_PER_MONTH))}


def forecast(f: dict, rows: pd.DataFrame, x: str) -> tuple:
    """B and R forecasts for any months, from the fixed training parameters. Returns (B, R, d)."""
    k = rows["month_key"].to_numpy() % 100
    d = term(k, rows["otb_complete"].to_numpy() == 1, rows[x].to_numpy(), f["xbar"])
    base = np.exp(f["a"][k - 1])
    return base, base * np.exp(f["delta"] * d), d


def fit_post(train: pd.DataFrame, d: np.ndarray, with_term: bool, park: str) -> dict:
    """S6: y = a(k) + gamma * POST (+ delta * d), by least squares after removing calendar-month means
    (Frisch-Waugh), checked against lstsq. POST = 1 from POST_FROM."""
    k = train["month_key"].to_numpy() % 100
    y = np.log(train["visitors"].to_numpy())
    post = (train["month_key"].to_numpy() >= POST_FROM).astype(float)

    def demean(z):
        return z - np.array([z[k == m].mean() for m in range(1, 13)])[k - 1]

    yt, pt, dt = demean(y), demean(post), demean(d)
    spp, spy = np.sum(pt * pt), np.sum(pt * yt)
    sdd = np.sum(dt * dt) if with_term else 0.0
    if sdd > 0:
        spd, sdy = np.sum(pt * dt), np.sum(dt * yt)
        det = spp * sdd - spd ** 2
        gamma = (sdd * spy - spd * sdy) / det
        delta = (spp * sdy - spd * spy) / det
    else:
        gamma, delta = spy / spp, 0.0
    resid = y - gamma * post - delta * d
    a = np.array([resid[k == m].mean() for m in range(1, 13)])
    columns = [(k == m).astype(float) for m in range(1, 13)] + [post] + ([d] if sdd > 0 else [])
    coef = np.linalg.lstsq(np.column_stack(columns), y, rcond=None)[0]
    assert abs(coef[12] - gamma) <= LSTSQ_TOLERANCE, f"{park}: S6 gamma {gamma} differs from lstsq {coef[12]}"
    se = np.nan
    if sdd > 0:
        assert abs(coef[13] - delta) <= LSTSQ_TOLERANCE, f"{park}: S6 delta {delta} differs from lstsq {coef[13]}"
        rss = np.sum((resid - a[k - 1]) ** 2)
        se = np.sqrt(rss / (len(y) - 14) * spp / det)
    return {"a": a, "gamma": float(gamma), "delta": float(delta), "se": float(se)}


def evaluate(pm: pd.DataFrame, x: str, closed: tuple, dropped: dict | None = None) -> dict:
    """Fit B and R per park on the training months and forecast the held-out months."""
    held_keys = month_range(*HELD_OUT)
    out = {}
    for park in PARKS:
        keys = training_months(closed, (dropped or {}).get(park, ()))
        g = pm[pm["park_code"] == park]
        train = g[g["month_key"].isin(keys)]
        assert len(train) == len(keys), f"{park}: {len(train)} of {len(keys)} training months are in the input"
        f = fit(train, x, park)
        held = g[g["month_key"].isin(held_keys)]
        assert len(held) == HELD_OUT_MONTHS, f"{park}: {len(held)} held-out months, expected {HELD_OUT_MONTHS}"
        assert (held["otb_complete"] == 1).all(), f"{park}: a held-out month has otb_complete = 0"
        assert (held["visitors"] > 0).all(), f"{park}: visitors must be > 0 in every held-out month"
        assert held[x].notna().all(), f"{park}: {x} is NULL in a held-out month"
        b, r, d = forecast(f, held, x)
        out[park] = {"fit": f, "train": train, "keys": held["month_key"].to_numpy(),
                     "actual": held["visitors"].to_numpy(), "b": b, "r": r, "d": d}
    return out


# ---------------------------------------------------------------------------------------------------------------
# Scoring

def score(check: str, label: str, park: str, f: dict, actual, b, r, measure=MAPE, delta=None, se=None) -> dict:
    """One summary row: the error of B and R over the given months and whether R is lower."""
    per_month, summarize = MEASURES[measure]
    eb_month, er_month = per_month(b, actual), per_month(r, actual)
    error_b, error_r = float(summarize(eb_month)), float(summarize(er_month))
    return {"check": check, "measure": label, "park_code": park,
            "n_train": f["n_train"], "n_term": f["n_term"],
            "delta": f["delta"] if delta is None else delta, "delta_se": f["se"] if se is None else se,
            "n_months": len(actual), "error_b": error_b, "error_r": error_r, "r_lower": lower(error_b, error_r),
            "months_r_lower": int(sum(lower(eb, er) for eb, er in zip(eb_month, er_month)))}


def verdict(summary: pd.DataFrame) -> dict:
    """The pre-registered rule on the primary rows (the row-count gate is checked before run)."""
    n = int(summary.loc[summary["check"] == "primary", "r_lower"].sum())
    passed = n >= PARKS_NEEDED
    if passed:
        sentence = (f"camping reservations on the books at the start of the month lower held-out error in {n} of "
                    f"{len(PARKS)} parks; reservations are usable leading signals")
    else:
        sentence = (f"reservations lower held-out error in {n} of {len(PARKS)} parks; "
                    "the result is descriptive, not a forecast")
    return {"parks_lower": n, "parks_needed": PARKS_NEEDED, "result": "PASS" if passed else "DESCRIPTIVE",
            "sentence": sentence}


def monthly_table(pm: pd.DataFrame, primary: dict) -> pd.DataFrame:
    """Actual visits, B and R for every park and month from TRAIN[0] to HELD_OUT[1], all from the fixed training
    parameters. period is train, closed (left out of the fit) or held_out. Percent errors are filled for held-out
    months only."""
    keys = month_range(TRAIN[0], HELD_OUT[1])
    train, held = set(training_months(CLOSED)), set(month_range(*HELD_OUT))
    parts = []
    for park in PARKS:
        g = pm[(pm["park_code"] == park) & pm["month_key"].isin(keys)]
        f = primary[park]["fit"]
        b, r, d = forecast(f, g, "camping_otb")
        mk = g["month_key"].to_numpy()
        visitors = g["visitors"].to_numpy()
        otb, stays = g["camping_otb"].to_numpy(), g["camping_stays"].to_numpy()
        usual = f["xbar"][mk % 100 - 1]
        uses_term = (g["otb_complete"].to_numpy() == 1) & ~np.isnan(usual)
        is_held = np.isin(mk, list(held))
        ape_b, ape_r = np.full(len(g), np.nan), np.full(len(g), np.nan)
        ape_b[is_held], ape_r[is_held] = ape(b[is_held], visitors[is_held]), ape(r[is_held], visitors[is_held])
        parts.append(pd.DataFrame({
            "park_code": park,
            "month_key": mk,
            "period": ["held_out" if m in held else "train" if m in train else "closed" for m in mk],
            "visitors": visitors.astype("int64"),
            "otb_complete": g["otb_complete"].to_numpy(),
            "camping_otb": otb.astype("int64"),
            "camping_stays": stays.astype("int64"),
            "share_on_books": np.divide(otb, stays, out=np.full(len(g), np.nan), where=stays > 0),
            "usual_otb": np.where(uses_term, usual, np.nan),
            "d": d,
            "forecast_b": b,
            "forecast_r": r,
            "ape_b": ape_b,
            "ape_r": ape_r,
        }))
    monthly = pd.concat(parts, ignore_index=True)
    assert len(monthly) == len(PARKS) * len(keys), "monthly table: a park x month is missing"
    return monthly


# ---------------------------------------------------------------------------------------------------------------
# Printing (plain f-strings, so pandas 2 and 3 print the same text)

def print_training(pm: pd.DataFrame, primary: dict) -> None:
    print("\nTRAINING FACTS (fitted on the training months only, then fixed)")
    print("park  train  term months  calendar months  complete from     delta   SE(delta)  per 1,000 bookings")
    for park in PARKS:
        f = primary[park]["fit"]
        first = int(pm.loc[(pm["park_code"] == park) & (pm["otb_complete"] == 1), "month_key"].min())
        print(f"{park}  {f['n_train']:5d}  {f['n_term']:11d}  {f['term_calendar_months']:15d}  {first:13d}  "
              f"{f['delta']:8.4f}  {f['se']:10.4f}  {100 * np.expm1(f['delta']):+17.2f}%")
    diff = max(primary[p]["fit"]["lstsq_diff"] for p in PARKS)
    print(f"delta equals the lstsq coefficient in every park (largest difference {diff:.1e}, tolerance "
          f"{LSTSQ_TOLERANCE:.0e})")
    print("Share of camping stays on the books by day 1 (descriptive): complete training months | held-out months")
    for park in PARKS:
        g = pm[(pm["park_code"] == park) & (pm["otb_complete"] == 1)]
        t = g[g["month_key"].isin(training_months(CLOSED))]
        h = g[g["month_key"].between(*HELD_OUT)]
        print(f"  {park}  {t['camping_otb'].sum() / t['camping_stays'].sum():.3f} | "
              f"{h['camping_otb'].sum() / h['camping_stays'].sum():.3f}")


def print_rows(rows: pd.DataFrame) -> None:
    for row in rows.itertuples(index=False):
        print(f"  {row.park_code}  B {row.error_b:10.2f}  R {row.error_r:10.2f}  "
              f"{'R lower' if row.r_lower else '-      '}  delta {row.delta:8.4f}  "
              f"months R lower {row.months_r_lower:2d}/{row.n_months}  train {row.n_train}  term {row.n_term}")


def print_primary(summary: pd.DataFrame, result: dict) -> None:
    rows = summary[summary["check"] == "primary"]
    print(f"\nPRIMARY TEST: {METRIC} over the {HELD_OUT_MONTHS} held-out months {HELD_OUT[0]}..{HELD_OUT[1]} "
          f"(R lower = round(MAPE_R, {TIE_DECIMALS}) < round(MAPE_B, {TIE_DECIMALS}))")
    print_rows(rows)
    print(f"\nVERDICT: {result['result']}: R lower in {result['parks_lower']} of {len(PARKS)} parks, "
          f"{result['parks_needed']} needed")
    print(f"  {result['sentence']}")


def print_secondary(summary: pd.DataFrame, counts: pd.DataFrame, fixed: dict, flagged: dict) -> None:
    print("\nSECONDARY CHECKS, not used for pass/fail")
    print("S1 fixed campgrounds (share of the camping stays of those years):")
    for park in PARKS:
        s = fixed[park]
        print(f"  {park}  years {' '.join(str(y) for y in s['years'])}: campgrounds {len(s['ids'])}, "
              f"{100 * s['share']:.1f}%: {', '.join(s['names'])}")
    print(f"S3 training months dropped (>= {FLAG_MIN_STAYS} camping stays, >= {FLAG_PAID0_SHARE:.0%} total_paid = 0):")
    for park in PARKS:
        print(f"  {park}  {' '.join(str(m) for m in flagged[park]) or 'none'}")
    last = None
    for row in counts.itertuples(index=False):
        if row.check != last:
            print(f"\n{row.check} {CHECKS[row.check]}")
            last = row.check
        rows = summary[(summary["check"] == row.check) & (summary["measure"] == row.measure)]
        print(f"  {row.measure}: parks where R is lower: {row.parks_r_lower} of {len(PARKS)}")
        print_rows(rows)


# ---------------------------------------------------------------------------------------------------------------

def run(park_month: pd.DataFrame, camping_fac: pd.DataFrame) -> dict:
    """The whole test, once: settings, training facts, primary table, verdict, secondary checks.
    Returns {summary, monthly, verdict, secondary}:
    - summary: one row per check x measure x park (primary and S1-S8);
    - monthly: actual visits, B and R per park and month 2015-01..2025-09;
    - verdict: the pass/fail result of the primary rows;
    - secondary: per secondary check and measure, the number of parks where R is lower."""
    print_settings()
    held_keys = month_range(*HELD_OUT)
    assert not set(training_months(CLOSED)) & set(held_keys), "a held-out month is inside the training months"
    assert len(training_months(CLOSED)) == TRAIN_MONTHS, f"TRAIN without CLOSED must have {TRAIN_MONTHS} months"
    assert len(held_keys) == HELD_OUT_MONTHS, f"HELD_OUT must have {HELD_OUT_MONTHS} months"

    pm = prepare(park_month)
    fixed = fixed_campgrounds(camping_fac)
    pm["fixed_otb"] = np.nan  # NULL like camping_otb after the newest file; 0 in a month without stays
    for park in PARKS:
        rows = (pm["park_code"] == park) & pm["camping_otb"].notna()
        pm.loc[rows, "fixed_otb"] = pm.loc[rows, "month_key"].map(fixed[park]["otb"]).fillna(0).to_numpy()
    flagged = closure_months(pm)

    primary = evaluate(pm, "camping_otb", CLOSED)
    variants = {
        "S1": evaluate(pm, "fixed_otb", CLOSED),
        "S2": evaluate(pm, "camping_otb", WIDE_CLOSED),
        "S3": evaluate(pm, "camping_otb", CLOSED, flagged),
        "S4": evaluate(pm, "all_otb", CLOSED),
    }

    rows = []
    for park in PARKS:
        p = primary[park]
        rows.append(score("primary", MAPE, park, p["fit"], p["actual"], p["b"], p["r"]))
    for check, ev in variants.items():
        for park in PARKS:
            v = ev[park]
            rows.append(score(check, MAPE, park, v["fit"], v["actual"], v["b"], v["r"]))
    for measure in list(MEASURES)[1:]:
        for park in PARKS:
            p = primary[park]
            rows.append(score("S5", measure, park, p["fit"], p["actual"], p["b"], p["r"], measure))
    for park in PARKS:
        p = primary[park]
        k_held = p["keys"] % 100
        b6 = fit_post(p["train"], p["fit"]["d"], False, park)
        r6 = fit_post(p["train"], p["fit"]["d"], True, park)
        # every held-out month is after POST_FROM, so POST = 1
        fb6 = np.exp(b6["a"][k_held - 1] + b6["gamma"])
        fr6 = np.exp(r6["a"][k_held - 1] + r6["gamma"] + r6["delta"] * p["d"])
        rows.append(score("S6", "MAPE (%), B+POST vs R+POST", park, p["fit"], p["actual"], fb6, fr6,
                          delta=r6["delta"], se=r6["se"]))
        rows.append(score("S6", "MAPE (%), B+POST vs primary R", park, p["fit"], p["actual"], fb6, p["r"]))
    for label, first, last in [("2024-01..2024-12", 202401, 202412), ("2025-01..2025-09", 202501, 202509)]:
        for park in PARKS:
            p = primary[park]
            m = (p["keys"] >= first) & (p["keys"] <= last)
            rows.append(score("S7", f"MAPE (%) {label}", park, p["fit"], p["actual"][m], p["b"][m], p["r"][m]))
    for park in PARKS:
        p = primary[park]
        row = score("S8", "MAPE (%), R lower and delta > 0", park, p["fit"], p["actual"], p["b"], p["r"])
        row["r_lower"] = row["r_lower"] and row["delta"] > 0
        rows.append(row)
    summary = pd.DataFrame(rows)
    assert not summary.duplicated(["check", "measure", "park_code"]).any()

    counts = []
    for check, measure in dict.fromkeys(zip(summary["check"], summary["measure"])):
        if check != "primary":
            n = int(summary.loc[(summary["check"] == check) & (summary["measure"] == measure), "r_lower"].sum())
            counts.append({"check": check, "measure": measure, "parks_r_lower": n, "parks": len(PARKS)})
    counts = pd.DataFrame(counts)

    result = verdict(summary)
    print_training(pm, primary)
    print_primary(summary, result)
    print_secondary(summary, counts, fixed, flagged)
    return {"summary": summary, "monthly": monthly_table(pm, primary), "verdict": result, "secondary": counts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--park-month", required=True, help="a2_inputs_park_month.csv (exported by 06_forecast_test)")
    ap.add_argument("--camping-facility-month", required=True,
                    help="gold_facility_month_camping.csv (exported by 05_reservations)")
    ap.add_argument("--out", help="folder for a2_forecast_summary.csv and a2_forecast_monthly.csv")
    args = ap.parse_args()

    print("pandas", pd.__version__, "| numpy", np.__version__)
    park_month = pd.read_csv(args.park_month)
    camping_fac = pd.read_csv(args.camping_facility_month, dtype={"facility_id": str})
    result = run(park_month, camping_fac)
    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        for name in ["summary", "monthly"]:
            path = out / f"a2_forecast_{name}.csv"
            result[name].to_csv(path, index=False, lineterminator="\n")
            print(f"{len(result[name])} rows: {path}")


if __name__ == "__main__":
    main()
