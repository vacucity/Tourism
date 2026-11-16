"""News test: does a seasonal baseline forecast monthly park visits better with news coverage of the month before?

This is the pre-registered news forecast test. It is A2's test (src/a2_forecast.py, docs/week8/a2_part2_spec.md)
with only the signal swapped, and it reuses a2_forecast's functions for the fit, the forecasts and the scoring.
Each park is fitted on its own, on log monthly recreation visits y = ln(visitors):
- Baseline B: a(k), the mean of y over the training months of calendar month k. Forecast exp(a(k)).
- With news R: B + delta * d. x(m) is news_url_per_million of UTC month m - 1: the park's news matches that pass the
  URL rule R0, per million GKG records, in the GDELT files of that month. Every such file has file_ts before
  00:00 UTC on day 1 of m, A2's forecast origin, so R uses no information from after the origin.
  d = (x - xbar(k)) / SCALE, where xbar(k) is the mean of x over the
  training months of calendar month k whose lagged value is not NULL (the term months, from 2015-03), in calendar
  months with at least 2 term months; elsewhere d = 0. delta is fitted once by least squares and checked against
  numpy.linalg.lstsq. Forecast exp(a(k)) * exp(delta * d). SCALE sets only the unit of delta, never a forecast.
- Training months: 2015-01..2023-12 without the 2020 closures (2020-03..2020-12), 98 per park, the same for both
  models; 96 of them are term months. The parameters are fixed there and never refit. No coverage threshold and no
  spike exclusions: the per-million share is the only treatment of GDELT outages.
- Scoring: MAPE over the 21 held-out months 2024-01..2025-09. A park counts when round(MAPE_R, 2) < round(MAPE_B, 2).
  PASS needs the gate that databricks/notebooks/08_news_test.py checks before it calls run(), and at least 3 of the
  5 parks. Otherwise the result is descriptive, not a forecast.
- Precision bar (A1 part 2): the result is marked, never changed. A park whose precision sample has at most 32 of
  its 40 rows about the park is marked below the bar. If the pooled precision P is below 0.90, the whole news
  result is marked below the bar. The flags are required. They come from docs/week8/a1_part2_score.json, written
  by a guarded src/gdelt_sample.py score ("git_guard": true, with its "commits"): its "P", its "pass" and each
  park's "below_bar" under "parks".
- Tests only (--test-flags here, TEST_FLAGS in 08): read_flags(path, score_file=False) also reads the score file
  of a test score (git_guard false) and the short form {"P": 0.93, "pass": true, "below_bar": {"GRCA": false,
  "GRSM": false, ...}}. The input must then have synthetic visits from 2024-01: require_synthetic_visits() stops
  when a visitors value from 2024-01 equals the committed real one in docs/week8/a2_inputs/a2_inputs_park_month.csv.
  It compares for equality only and prints only the count of equal values, never a value.
- No peek in the CLI without test flags: when a visitors value from 2024-01 equals the committed real one (or the
  values cannot be compared), require_committed_score_file() stops unless --precision-flags is this repository's
  docs/week8/a1_part2_score.json, committed at HEAD and unchanged. So the CLI computes no held-out error with real
  visits before the A1 part 2 result is committed, and a rerun with real visits needs a git clone of the repository.
Six secondary checks (N1-N6) come from the same run. They are reported and never used for pass/fail.

Input: one row per park x month, gold_park_month joined with gold_park_month_news and gold_park_month_reservations
(park_code, month_key, month_start, visitors, news_all, news_url, news_all_per_million, news_url_per_million,
otb_complete, camping_otb). All are aggregates. The lag is taken here, by month key, not in the input.

08_news_test.py calls run(). To rerun the test from news_inputs_park_month.csv (exported by 08 and committed under
docs/week8/news_inputs/ with docs/week8/news_test_result.md), from the repository root:
  uv run python src/news_forecast.py --park-month docs/week8/news_inputs/news_inputs_park_month.csv \\
      --precision-flags docs/week8/a1_part2_score.json
Add --out <folder> to also write news_forecast_summary.csv and news_forecast_monthly.csv.
"""
import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# a2_forecast.py sits next to this file. `python -I` leaves the script's folder off sys.path, so add it.
SRC = Path(__file__).resolve().parent
if str(SRC) not in sys.path:
    sys.path.append(str(SRC))
import a2_forecast  # noqa: E402

PARKS = ["GRCA", "GRSM", "YELL", "YOSE", "ZION"]
# Months are keys year * 100 + month (202507), like month_key in gold_park_month. These are A2's settings.
TRAIN = (201501, 202312)
CLOSED = (202003, 202012)       # the 2020 closures, left out of training
HELD_OUT = (202401, 202509)
MIN_YEARS_PER_MONTH = 2         # term months a calendar month needs to carry the news term
METRIC = "MAPE"
TIE_DECIMALS = 2                # values equal after rounding count as not lower
PARKS_NEEDED = 3
TRAIN_MONTHS = 98               # per park
HELD_OUT_MONTHS = 21            # per park
LSTSQ_TOLERANCE = 1e-9
WIDE_CLOSED = (202003, 202106)  # N2, A2's S2 window
# The news test copies A2's settings; this stops if src/a2_forecast.py ever differs
assert (PARKS, TRAIN, CLOSED, HELD_OUT, MIN_YEARS_PER_MONTH, METRIC, TIE_DECIMALS, PARKS_NEEDED, TRAIN_MONTHS,
        HELD_OUT_MONTHS, LSTSQ_TOLERANCE, WIDE_CLOSED) == (
    a2_forecast.PARKS, a2_forecast.TRAIN, a2_forecast.CLOSED, a2_forecast.HELD_OUT, a2_forecast.MIN_YEARS_PER_MONTH,
    a2_forecast.METRIC, a2_forecast.TIE_DECIMALS, a2_forecast.PARKS_NEEDED, a2_forecast.TRAIN_MONTHS,
    a2_forecast.HELD_OUT_MONTHS, a2_forecast.LSTSQ_TOLERANCE, a2_forecast.WIDE_CLOSED), "A2's settings changed"

# The news settings
SIGNAL = "news_url_per_million"      # primary: matches that pass the URL rule R0, per million GKG records
SIGNAL_ALL = "news_all_per_million"  # N1: all full-name matches, per million GKG records
LAG = 1                              # x(m) = the signal of UTC month m - LAG
SCALE = 100                          # d is in units of 100 matches per million GKG records (1 per 10,000 records)
TERM_MONTHS = 96                     # per park: training months from 2015-03 (the lag of 2015-01 and 2015-02 is NULL)
PRECISION_BAR = 0.90                 # A1's bar for the pooled precision of the URL rule
RESERVATIONS = "camping_otb"         # N6: A2's signal, with A2's otb_complete and A2's SCALE (1,000 bookings)
# No peek: from this month on the input's visits must differ from the committed real ones, with test flags always,
# without them until the score file of A1 part 2 is committed
FIRST_SYNTHETIC = HELD_OUT[0]
REAL_VISITS = SRC.parent / "docs" / "week8" / "a2_inputs" / "a2_inputs_park_month.csv"
SCORE_FILE = "docs/week8/a1_part2_score.json"  # relative to the repository root, SRC.parent
# a2_forecast.term() divides by a2_forecast.SCALE (1,000 bookings). The news signal is handed to it multiplied by
# UNITS, so its d is (x - xbar(k)) / SCALE.
UNITS = a2_forecast.SCALE / SCALE

CHECKS = {
    "primary": "news_url_per_million of the month before (the pass/fail test)",
    "N1": "all matches: news_all_per_million of the month before, without the URL rule",
    "N2": "wider closure window: training also without 2020-03..2021-06",
    "N3": "other error measures on the primary forecasts",
    "N4": "by year: held-out 2024 and 2025 separately",
    "N5": "sign: R lower and delta > 0",
    "N6": "reservations and news together: A2's camping_otb term plus the news term, fitted together "
          "(the B column is the forecast compared with, the R column is reservations + news)",
}


def lag_column(column: str) -> str:
    """Name of the lagged copy of a signal column: the value of month m - LAG, on the row of month m."""
    return f"{column}_lag{LAG}"


def previous_month(keys: np.ndarray, months: int = LAG) -> np.ndarray:
    """Month keys `months` months earlier: 201503 -> 201502, 201501 -> 201412."""
    index = (keys // 100) * 12 + keys % 100 - 1 - months
    return (index // 12) * 100 + index % 12 + 1


def print_settings() -> None:
    print("SETTINGS (A2's model and scoring, docs/week8/a2_part2_spec.md, with the news signal)")
    print(f"  TRAIN = {TRAIN[0]}..{TRAIN[1]}, CLOSED = {CLOSED[0]}..{CLOSED[1]}, "
          f"HELD_OUT = {HELD_OUT[0]}..{HELD_OUT[1]}")
    print(f"  SIGNAL = {SIGNAL}, LAG = {LAG} month (x(m) = the signal of UTC month m - {LAG}), SCALE = {SCALE}, "
          f"MIN_YEARS_PER_MONTH = {MIN_YEARS_PER_MONTH}")
    print(f"  METRIC = {METRIC}, TIE_DECIMALS = {TIE_DECIMALS}, PARKS_NEEDED = {PARKS_NEEDED}, "
          f"PRECISION_BAR = {PRECISION_BAR}")
    print(f"  per park: {TRAIN_MONTHS} training months ({TERM_MONTHS} term months), {HELD_OUT_MONTHS} held-out "
          f"months; parks: {', '.join(PARKS)}")
    print(f"  secondary: N1 SIGNAL = {SIGNAL_ALL}, N2 WIDE_CLOSED = {WIDE_CLOSED[0]}..{WIDE_CLOSED[1]}, "
          f"N6 adds {RESERVATIONS} (A2's SCALE = {a2_forecast.SCALE})")
    print("  no coverage threshold, no spike exclusions, no refit")


# ---------------------------------------------------------------------------------------------------------------
# Inputs

PARK_MONTH_COLUMNS = ["park_code", "month_key", "month_start", "visitors", "news_all", "news_url",
                      "news_all_per_million", "news_url_per_million", "otb_complete", "camping_otb"]
FLOAT_COLUMNS = ["visitors", "news_all", "news_url", "news_all_per_million", "news_url_per_million", "camping_otb"]


def prepare(park_month: pd.DataFrame) -> pd.DataFrame:
    """A typed copy with one row per park x month, sorted, plus the lagged signals. NULL becomes NaN.
    Every park must have every month from TRAIN[0] to HELD_OUT[1], so every lag inside the test is in the input."""
    missing = [c for c in PARK_MONTH_COLUMNS if c not in park_month.columns]
    assert not missing, f"park_month lacks columns {missing}"
    pm = pd.DataFrame({
        "park_code": park_month["park_code"].astype(str).to_numpy(),
        "month_key": park_month["month_key"].astype("int64").to_numpy(),
        "month_start": pd.to_datetime(park_month["month_start"]).to_numpy(),
        "otb_complete": park_month["otb_complete"].astype("int64").to_numpy(),
        **{c: park_month[c].astype("float64").to_numpy() for c in FLOAT_COLUMNS},
    })
    assert sorted(set(pm["park_code"])) == PARKS, f"parks must be {PARKS}, got {sorted(set(pm['park_code']))}"
    assert not pm.duplicated(["park_code", "month_key"]).any(), "park_month has a park x month twice"
    first_day = pd.to_datetime((pm["month_key"] * 100 + 1).astype(str), format="%Y%m%d")
    assert (pm["month_start"] == first_day).all(), "month_start is not day 1 of month_key in some rows"
    needed = set(a2_forecast.month_range(TRAIN[0], HELD_OUT[1]))
    for park in PARKS:
        lacking = needed - set(pm.loc[pm["park_code"] == park, "month_key"])
        assert not lacking, f"{park}: months missing from the input: {sorted(lacking)}"
    pm = pm.sort_values(["park_code", "month_key"], ignore_index=True)

    # x of month m is the signal of month m - LAG, looked up by key: the row order never matters, and a month
    # before the first input month (2014-12) is NULL
    pm["news_month_key"] = previous_month(pm["month_key"].to_numpy())
    earlier = pd.MultiIndex.from_arrays([pm["park_code"], pm["news_month_key"]])
    by_key = pm.set_index(["park_code", "month_key"])
    for column in [SIGNAL, SIGNAL_ALL]:
        pm[lag_column(column)] = by_key[column].reindex(earlier).to_numpy()
    return pm


def read_flags(path, score_file: bool = True) -> dict:
    """The precision flags of A1 part 2: {"P": pooled precision, "pass": P >= 0.90, "below_bar": {park: bool}}.
    score_file=True (08 and the CLI): the file must be the score file of a guarded `gdelt_sample.py score`, with
    "git_guard": true, its "commits" and the per-park flags under "parks". score_file=False is for tests only
    (TEST_FLAGS in 08, --test-flags in the CLI): it also reads a test score and the short form.
    Other keys in the file are ignored."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    assert isinstance(raw, dict), f"{path}: the flags file must hold one JSON object"
    # The no-peek checks raise explicitly, not with assert, so that python -O cannot switch them off
    if score_file:
        if raw.get("git_guard") is not True:
            raise AssertionError(
                f"{path} is not the score file of a guarded score (git_guard is not true); a test score or the short "
                "test form is read only with test flags")
        if not isinstance(raw.get("commits"), dict):
            raise AssertionError(f"{path}: the score file lacks the commits of the protocol")
        if not (isinstance(raw.get("parks"), dict) and "below_bar" not in raw):
            raise AssertionError(f"{path}: the per-park flags of a score file are parks.<park>.below_bar")
    return check_flags(raw)


def check_flags(flags: dict) -> dict:
    """Validate the flags and return a clean copy. In the score file (a1_part2_score.json) the per-park flags are
    flags["parks"][park]["below_bar"]; they are read from there when there is no "below_bar" key."""
    if "below_bar" not in flags and isinstance(flags.get("parks"), dict):
        flags = {**flags, "below_bar": {park: v.get("below_bar") if isinstance(v, dict) else None
                                        for park, v in flags["parks"].items()}}
    for key in ["P", "pass", "below_bar"]:
        assert key in flags, f"precision flags lack '{key}'"
    p, passed, below = flags["P"], flags["pass"], flags["below_bar"]
    assert isinstance(p, (int, float)) and not isinstance(p, bool) and 0 <= p <= 1, f"P must be in [0, 1]: {p!r}"
    assert isinstance(passed, bool), f"pass must be true or false: {passed!r}"
    # score compares P with the bar exactly (fractions); a float within 1e-6 of the bar is not checked here
    assert abs(p - PRECISION_BAR) <= 1e-6 or passed == (p >= PRECISION_BAR), f"pass = {passed} contradicts P = {p}"
    assert isinstance(below, dict) and sorted(below) == PARKS, f"below_bar must name the parks {PARKS}: {below!r}"
    assert all(isinstance(v, bool) for v in below.values()), f"below_bar values must be true or false: {below!r}"
    return {"P": float(p), "pass": passed, "below_bar": {park: below[park] for park in PARKS}}


def count_real_visits(park_month: pd.DataFrame, real_csv=REAL_VISITS) -> int | None:
    """How many visitors values from FIRST_SYNTHETIC on in park_month equal the committed real value of the same park
    and month in real_csv. Equality only: nothing is printed here. Any problem (a missing file or column, a value
    that is not a number, no real value to compare with) gives None, "cannot compare", without its error message,
    which could quote a visit."""
    try:
        real = {}
        with Path(real_csv).open(encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                if int(r["month_key"]) >= FIRST_SYNTHETIC and r["visitors"] != "":
                    real[(r["park_code"], int(r["month_key"]))] = float(r["visitors"])
        equal = 0
        for park, key, visitors in zip(park_month["park_code"], park_month["month_key"], park_month["visitors"]):
            if int(key) >= FIRST_SYNTHETIC and not pd.isna(visitors):
                equal += real.get((str(park), int(key))) == float(visitors)
        return equal if real else None
    except Exception:  # noqa: BLE001 - see the docstring
        return None


def print_real_visits(equal: int | None) -> None:
    """The one line the no-peek checks print: a count, never a value."""
    if equal is None:
        print(f"no-peek check: the visitors from {FIRST_SYNTHETIC} could not be compared with {REAL_VISITS}")
    else:
        print(f"no-peek check: {equal} visitors values from {FIRST_SYNTHETIC} equal the committed real values")


def require_synthetic_visits(park_month: pd.DataFrame) -> None:
    """With test flags, stop unless every visitors value from FIRST_SYNTHETIC on differs from the committed real one
    (REAL_VISITS): no held-out news error is computed with real visits in a test. Prints only the count of equal
    values. Called before anything is fitted."""
    equal = count_real_visits(park_month)
    print_real_visits(equal)
    if equal != 0:  # explicit, not assert: python -O must not switch it off
        raise AssertionError(f"test flags need synthetic visits from {FIRST_SYNTHETIC} in every row of the input")


def committed_score_file(path) -> bool:
    """True when path is SCORE_FILE of the git repository whose root is SRC.parent, committed at HEAD and unchanged
    since (the blob of the working file = the blob at HEAD). Read-only git commands; False without git or a
    repository."""
    root = SRC.parent

    def same(a, b) -> bool:
        return os.path.normcase(str(Path(a).resolve())) == os.path.normcase(str(Path(b).resolve()))

    def git(*args: str) -> str | None:
        r = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True)
        return r.stdout.strip() if r.returncode == 0 else None

    if not same(path, root / SCORE_FILE):
        return False
    try:
        top = git("rev-parse", "--show-toplevel")
        if top is None or not same(top, root):
            return False  # SRC.parent is not the root of a git repository
        at_head = git("rev-parse", "--verify", "--quiet", f"HEAD:{SCORE_FILE}")
        working = git("hash-object", "--", SCORE_FILE)
        return at_head is not None and at_head == working
    except OSError:  # git is not installed
        return False


def require_committed_score_file(park_month: pd.DataFrame, flags_path) -> None:
    """Without test flags (the CLI): when the input may hold real visits from FIRST_SYNTHETIC on (a value equal to the
    committed real one, or no comparison), stop unless flags_path is the committed score file of A1 part 2
    (committed_score_file). Before that result is committed, no held-out news error is computed with real visits.
    Prints only the count of equal values. Called before anything is fitted."""
    equal = count_real_visits(park_month)
    print_real_visits(equal)
    if equal == 0:
        return
    if not committed_score_file(flags_path):  # explicit, not assert: python -O must not switch it off
        raise AssertionError(
            f"the input may hold real visits from {FIRST_SYNTHETIC}, so --precision-flags must be {SCORE_FILE} of the "
            f"git repository {SRC.parent}, committed at HEAD and unchanged: no held-out news error is computed with "
            "real visits before the A1 part 2 result is committed")
    print(f"no-peek check: {SCORE_FILE} is committed at HEAD and unchanged, so real visits may be used")


def news_rows(rows: pd.DataFrame, x: str) -> pd.DataFrame:
    """The columns that a2_forecast.fit() and a2_forecast.forecast() read, for the news term.
    - otb_complete: A2's flag for "the term is used in this month"; here 1 where the lagged signal is not NULL.
    - signal: the lagged signal times UNITS, so that A2's d = (x - xbar(k)) / SCALE."""
    lagged = rows[x].to_numpy()
    return pd.DataFrame({
        "month_key": rows["month_key"].to_numpy(),
        "visitors": rows["visitors"].to_numpy(),
        "otb_complete": (~np.isnan(lagged)).astype("int64"),
        "signal": lagged * UNITS,
    })


# ---------------------------------------------------------------------------------------------------------------
# Models: B and R are a2_forecast.fit() and a2_forecast.forecast(); N6 adds a second term

def fit_news(train: pd.DataFrame, x: str, park: str) -> dict:
    """B and R for one park from its training months (a2_forecast.fit with the news term)."""
    return a2_forecast.fit(news_rows(train, x), "signal", park)


def forecast_news(f: dict, rows: pd.DataFrame, x: str) -> tuple:
    """B and R forecasts for any months, from the fixed training parameters. Returns (B, R, d)."""
    return a2_forecast.forecast(f, news_rows(rows, x), "signal")


def training_rows(pm: pd.DataFrame, park: str, closed: tuple) -> pd.DataFrame:
    keys = a2_forecast.training_months(closed)
    train = pm[(pm["park_code"] == park) & pm["month_key"].isin(keys)]
    assert len(train) == len(keys), f"{park}: {len(train)} of {len(keys)} training months are in the input"
    return train


def evaluate(pm: pd.DataFrame, x: str, closed: tuple) -> dict:
    """Fit B and R per park on the training months and forecast the held-out months."""
    held_keys = a2_forecast.month_range(*HELD_OUT)
    out = {}
    for park in PARKS:
        train = training_rows(pm, park, closed)
        f = fit_news(train, x, park)
        held = pm[(pm["park_code"] == park) & pm["month_key"].isin(held_keys)]
        assert len(held) == HELD_OUT_MONTHS, f"{park}: {len(held)} held-out months, expected {HELD_OUT_MONTHS}"
        assert (held["visitors"] > 0).all(), f"{park}: visitors must be > 0 in every held-out month"
        assert held[x].notna().all(), f"{park}: {x} is NULL in a held-out month"
        b, r, d = forecast_news(f, held, x)
        out[park] = {"fit": f, "train": train, "held": held, "keys": held["month_key"].to_numpy(),
                     "actual": held["visitors"].to_numpy(), "b": b, "r": r, "d": d}
    return out


def fit_both(news: dict, otb: dict, train: pd.DataFrame, park: str) -> dict:
    """N6: y = a(k) + delta_otb * d_otb + delta * d by least squares, with A2's reservation term d_otb and the news
    term d. Both terms sum to 0 within each calendar month over the training months, so the month terms stay B's
    a(k) and the two coefficients are the regression of B's residuals on (d_otb, d) (Frisch-Waugh), checked
    against lstsq."""
    assert np.array_equal(news["a"], otb["a"]), f"{park}: B differs between the news and the reservation fits"
    k = train["month_key"].to_numpy() % 100
    y = np.log(train["visitors"].to_numpy())
    e = y - news["a"][k - 1]
    z = np.column_stack([otb["d"], news["d"]])
    zz = z.T @ z
    coef = np.linalg.solve(zz, z.T @ e)
    rss = np.sum((e - z @ coef) ** 2)
    cov = rss / (len(y) - 14) * np.linalg.inv(zz)
    dummies = [(k == m).astype(float) for m in range(1, 13)]
    check = np.linalg.lstsq(np.column_stack(dummies + [otb["d"], news["d"]]), y, rcond=None)[0]
    lstsq_diff = float(max(abs(check[12] - coef[0]), abs(check[13] - coef[1])))
    assert lstsq_diff <= LSTSQ_TOLERANCE, f"{park}: N6 coefficients {coef} differ from lstsq {check[12:]}"
    return {"a": news["a"], "delta_otb": float(coef[0]), "delta": float(coef[1]),
            "se_otb": float(np.sqrt(cov[0, 0])), "se": float(np.sqrt(cov[1, 1])), "lstsq_diff": lstsq_diff}


def evaluate_both(pm: pd.DataFrame, primary: dict) -> dict:
    """N6: A2's reservation-only R (a2_forecast.fit on camping_otb), and reservations + news fitted together."""
    out = {}
    for park in PARKS:
        p = primary[park]
        train, held = p["train"], p["held"]
        otb = a2_forecast.fit(train, RESERVATIONS, park)
        assert (held["otb_complete"] == 1).all(), f"{park}: a held-out month has otb_complete = 0"
        assert held[RESERVATIONS].notna().all(), f"{park}: {RESERVATIONS} is NULL in a held-out month"
        _, r_otb, d_otb = a2_forecast.forecast(otb, held, RESERVATIONS)
        both = fit_both(p["fit"], otb, train, park)
        k = p["keys"] % 100
        r_both = np.exp(both["a"][k - 1] + both["delta_otb"] * d_otb + both["delta"] * p["d"])
        out[park] = {"otb": otb, "both": both, "r_otb": r_otb, "r_both": r_both}
    return out


def training_table(fits: dict) -> pd.DataFrame:
    """Training facts per park from the fitted parameters: no held-out month enters them."""
    return pd.DataFrame([{
        "park_code": park, "n_train": f["n_train"], "n_term": f["n_term"],
        "term_calendar_months": f["term_calendar_months"], "delta": f["delta"], "delta_se": f["se"],
        "effect_per_scale_pct": 100 * np.expm1(f["delta"]), "lstsq_diff": f["lstsq_diff"],
    } for park, f in fits.items()])


def training_facts(park_month: pd.DataFrame) -> pd.DataFrame:
    """The primary model fitted on the training months only, never forecast: delta, SE(delta) and term months per
    park. Needs visits for the training months only; held-out visits may be NULL."""
    pm = prepare(park_month)
    return training_table({park: fit_news(training_rows(pm, park, CLOSED), lag_column(SIGNAL), park)
                           for park in PARKS})


# ---------------------------------------------------------------------------------------------------------------
# Scoring

def verdict(summary: pd.DataFrame, flags: dict) -> dict:
    """The pre-registered rule on the primary rows (the gate is checked before run), then the precision marks."""
    primary = summary[summary["check"] == "primary"]
    n = int(primary["r_lower"].sum())
    passed = n >= PARKS_NEEDED
    if passed:
        sentence = (f"news coverage of the month before lowers held-out error in {n} of {len(PARKS)} parks; "
                    "news coverage is a usable leading signal")
    else:
        sentence = (f"news coverage lowers held-out error in {n} of {len(PARKS)} parks; "
                    "the result is descriptive, not a forecast")

    # The marks follow the flag "pass" (check_flags has checked that it agrees with P)
    lower_parks = primary.loc[primary["r_lower"], "park_code"].tolist()
    below = [park for park in PARKS if flags["below_bar"][park]]
    marked = not flags["pass"]
    if marked:
        precision = (f"MARKED BELOW THE PRECISION BAR: the URL rule's pooled precision P = {flags['P']:.4f} is "
                     f"below {PRECISION_BAR:.2f}, so the whole news result is marked below the bar")
    else:
        precision = f"pooled precision P = {flags['P']:.4f} meets the {PRECISION_BAR:.2f} bar"
    if below:
        lower_below = [park for park in below if park in lower_parks]
        precision += (f"; parks marked below the bar: {', '.join(below)}; R is lower in "
                      f"{len(lower_below)} of them{': ' + ', '.join(lower_below) if lower_below else ''}")
    else:
        precision += "; no park is marked below the bar"
    return {"parks_lower": n, "parks_needed": PARKS_NEEDED, "result": "PASS" if passed else "DESCRIPTIVE",
            "sentence": sentence, "parks_r_lower": lower_parks,
            "precision_p": flags["P"], "precision_pass": flags["pass"], "parks_below_bar": below,
            "marked_below_bar": marked, "precision_sentence": precision}


def below_bar_column(parks: pd.Series, flags: dict) -> np.ndarray:
    """Per summary row: True if the park is marked below the precision bar, or every row if pooled P < 0.90."""
    return np.array([flags["below_bar"][park] or not flags["pass"] for park in parks], dtype=bool)


def monthly_table(pm: pd.DataFrame, primary: dict) -> pd.DataFrame:
    """Actual visits, the lagged signal, B and R for every park and month from TRAIN[0] to HELD_OUT[1], all from the
    fixed training parameters. period is train, closed (left out of the fit) or held_out. Percent errors are filled
    for held-out months only."""
    keys = a2_forecast.month_range(TRAIN[0], HELD_OUT[1])
    train, held = set(a2_forecast.training_months(CLOSED)), set(a2_forecast.month_range(*HELD_OUT))
    x = lag_column(SIGNAL)
    parts = []
    for park in PARKS:
        g = pm[(pm["park_code"] == park) & pm["month_key"].isin(keys)]
        f = primary[park]["fit"]
        b, r, d = forecast_news(f, g, x)
        mk = g["month_key"].to_numpy()
        visitors = g["visitors"].to_numpy()
        lagged = g[x].to_numpy()
        usual = f["xbar"][mk % 100 - 1] / UNITS
        uses_term = ~np.isnan(lagged) & ~np.isnan(usual)
        is_held = np.isin(mk, list(held))
        ape_b, ape_r = np.full(len(g), np.nan), np.full(len(g), np.nan)
        ape_b[is_held] = a2_forecast.ape(b[is_held], visitors[is_held])
        ape_r[is_held] = a2_forecast.ape(r[is_held], visitors[is_held])
        parts.append(pd.DataFrame({
            "park_code": park,
            "month_key": mk,
            "period": ["held_out" if m in held else "train" if m in train else "closed" for m in mk],
            "visitors": visitors.astype("int64"),
            "news_month_key": g["news_month_key"].to_numpy(),
            x: lagged,
            "usual_news": np.where(uses_term, usual, np.nan),
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

def print_signal(pm: pd.DataFrame) -> None:
    """Descriptive, from the news counts only: the share of full-name matches that pass the URL rule, per year."""
    print("\nURL RULE PASS RATE per year (news_url / news_all, descriptive; R0 drifts with URL formats)")
    years = sorted(set(pm["month_key"] // 100))
    print("park  " + "  ".join(f"{y:>5}" for y in years))
    for park in PARKS:
        g = pm[pm["park_code"] == park]
        rates = []
        for y in years:
            in_year = g[g["month_key"] // 100 == y]
            total = in_year["news_all"].sum()
            rates.append(f"{in_year['news_url'].sum() / total:5.3f}" if total > 0 else "    -")
        print(f"{park}  " + "  ".join(rates))


def print_training(facts: pd.DataFrame) -> None:
    print("\nTRAINING FACTS (fitted on the training months only, then fixed)")
    print("park  train  term months  calendar months     delta   SE(delta)   effect")
    for row in facts.itertuples(index=False):
        print(f"{row.park_code}  {row.n_train:5d}  {row.n_term:11d}  {row.term_calendar_months:15d}  "
              f"{row.delta:8.4f}  {row.delta_se:10.4f}  {row.effect_per_scale_pct:+7.2f}%")
    print(f"effect: 100 x (exp(delta) - 1), how much R moves B per {SCALE} URL-rule matches per million GKG records "
          "above the usual level of the calendar month")
    print(f"delta equals the lstsq coefficient in every park (largest difference {facts['lstsq_diff'].max():.1e}, "
          f"tolerance {LSTSQ_TOLERANCE:.0e})")


def print_primary(summary: pd.DataFrame, result: dict) -> None:
    rows = summary[summary["check"] == "primary"]
    print(f"\nPRIMARY TEST: {METRIC} over the {HELD_OUT_MONTHS} held-out months {HELD_OUT[0]}..{HELD_OUT[1]} "
          f"(R lower = round(MAPE_R, {TIE_DECIMALS}) < round(MAPE_B, {TIE_DECIMALS}))")
    a2_forecast.print_rows(rows)
    print(f"\nVERDICT: {result['result']}: R lower in {result['parks_lower']} of {len(PARKS)} parks, "
          f"{result['parks_needed']} needed")
    print(f"  {result['sentence']}")
    print(f"PRECISION: {result['precision_sentence']}")


def print_secondary(summary: pd.DataFrame, counts: pd.DataFrame) -> None:
    print("\nSECONDARY CHECKS, not used for pass/fail")
    last = None
    for row in counts.itertuples(index=False):
        if row.check != last:
            print(f"\n{row.check} {CHECKS[row.check]}")
            last = row.check
        rows = summary[(summary["check"] == row.check) & (summary["measure"] == row.measure)]
        print(f"  {row.measure}: parks where R is lower: {row.parks_r_lower} of {len(PARKS)}")
        a2_forecast.print_rows(rows)
    # N6's two coefficients are the same in both of its comparisons
    first = counts.loc[counts["check"] == "N6", "measure"].iloc[0]
    print("  N6 coefficients, fitted on the training months:")
    for row in summary[(summary["check"] == "N6") & (summary["measure"] == first)].itertuples(index=False):
        print(f"  {row.park_code}  delta_otb {row.delta_otb:8.4f} (SE {row.delta_otb_se:.4f})  "
              f"news delta {row.delta:8.4f} (SE {row.delta_se:.4f})")


# ---------------------------------------------------------------------------------------------------------------

def run(park_month: pd.DataFrame, flags: dict) -> dict:
    """The whole test, once: settings, signal facts, training facts, primary table, verdict, secondary checks.
    flags are the A1 part 2 precision flags (from read_flags, or a dict in check_flags format); they are required.
    Returns {summary, monthly, verdict, secondary, training}:
    - summary: one row per check x measure x park (primary and N1-N6);
    - monthly: actual visits, the lagged signal, B and R per park and month 2015-01..2025-09;
    - verdict: the pass/fail result of the primary rows and the precision marks;
    - secondary: per secondary check and measure, the number of parks where R is lower;
    - training: the training facts of the primary model per park."""
    print_settings()
    held_keys = a2_forecast.month_range(*HELD_OUT)
    assert not set(a2_forecast.training_months(CLOSED)) & set(held_keys), "a held-out month is a training month"
    assert len(a2_forecast.training_months(CLOSED)) == TRAIN_MONTHS, f"TRAIN without CLOSED must have {TRAIN_MONTHS}"
    assert len(held_keys) == HELD_OUT_MONTHS, f"HELD_OUT must have {HELD_OUT_MONTHS} months"
    flags = check_flags(flags)

    pm = prepare(park_month)
    x, x_all = lag_column(SIGNAL), lag_column(SIGNAL_ALL)
    primary = evaluate(pm, x, CLOSED)
    for park in PARKS:
        n_term = primary[park]["fit"]["n_term"]
        assert n_term == TERM_MONTHS, f"{park}: {n_term} term months, expected {TERM_MONTHS} (NULL news after 2015-01?)"
    variants = {"N1": evaluate(pm, x_all, CLOSED), "N2": evaluate(pm, x, WIDE_CLOSED)}
    both = evaluate_both(pm, primary)

    score = a2_forecast.score
    rows = []
    for park in PARKS:
        p = primary[park]
        rows.append(score("primary", a2_forecast.MAPE, park, p["fit"], p["actual"], p["b"], p["r"]))
    for check, ev in variants.items():
        for park in PARKS:
            v = ev[park]
            rows.append(score(check, a2_forecast.MAPE, park, v["fit"], v["actual"], v["b"], v["r"]))
    for measure in list(a2_forecast.MEASURES)[1:]:
        for park in PARKS:
            p = primary[park]
            rows.append(score("N3", measure, park, p["fit"], p["actual"], p["b"], p["r"], measure))
    for label, first, last in [("2024-01..2024-12", 202401, 202412), ("2025-01..2025-09", 202501, 202509)]:
        for park in PARKS:
            p = primary[park]
            m = (p["keys"] >= first) & (p["keys"] <= last)
            rows.append(score("N4", f"MAPE (%) {label}", park, p["fit"], p["actual"][m], p["b"][m], p["r"][m]))
    for park in PARKS:
        p = primary[park]
        row = score("N5", "MAPE (%), R lower and delta > 0", park, p["fit"], p["actual"], p["b"], p["r"])
        row["r_lower"] = row["r_lower"] and row["delta"] > 0
        rows.append(row)
    for label, base in [("MAPE (%), B vs reservations + news", "b"),
                        ("MAPE (%), reservation-only R vs reservations + news", "r_otb")]:
        for park in PARKS:
            p, n6 = primary[park], both[park]
            forecast_base = p["b"] if base == "b" else n6["r_otb"]
            row = score("N6", label, park, p["fit"], p["actual"], forecast_base, n6["r_both"],
                        delta=n6["both"]["delta"], se=n6["both"]["se"])
            row["delta_otb"], row["delta_otb_se"] = n6["both"]["delta_otb"], n6["both"]["se_otb"]
            rows.append(row)
    summary = pd.DataFrame(rows)
    summary["delta_otb"] = summary["delta_otb"].astype("float64")      # NaN outside N6
    summary["delta_otb_se"] = summary["delta_otb_se"].astype("float64")
    summary["below_precision_bar"] = below_bar_column(summary["park_code"], flags)
    assert not summary.duplicated(["check", "measure", "park_code"]).any()

    counts = []
    for check, measure in dict.fromkeys(zip(summary["check"], summary["measure"])):
        if check != "primary":
            n = int(summary.loc[(summary["check"] == check) & (summary["measure"] == measure), "r_lower"].sum())
            counts.append({"check": check, "measure": measure, "parks_r_lower": n, "parks": len(PARKS)})
    counts = pd.DataFrame(counts)

    result = verdict(summary, flags)
    facts = training_table({park: primary[park]["fit"] for park in PARKS})
    print_signal(pm)
    print_training(facts)
    print_primary(summary, result)
    print_secondary(summary, counts)
    return {"summary": summary, "monthly": monthly_table(pm, primary), "verdict": result, "secondary": counts,
            "training": facts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--park-month", required=True, help="news_inputs_park_month.csv (exported by 08_news_test)")
    ap.add_argument("--precision-flags", required=True,
                    help="docs/week8/a1_part2_score.json (written by a guarded src/gdelt_sample.py score)")
    ap.add_argument("--test-flags", action="store_true",
                    help="tests only: also read a test score or the short form of the flags; the visits from "
                         f"{FIRST_SYNTHETIC} in --park-month must then all differ from the committed real ones")
    ap.add_argument("--out", help="folder for news_forecast_summary.csv and news_forecast_monthly.csv")
    args = ap.parse_args()

    # round_trip: the doubles read back are exactly the ones 08 wrote, so the rerun gives the same files
    park_month = pd.read_csv(args.park_month, float_precision="round_trip")
    # The flags and the no-peek check come first: a refused run prints nothing else
    try:
        flags = read_flags(args.precision_flags, score_file=not args.test_flags)
        if args.test_flags:
            require_synthetic_visits(park_month)
        else:
            require_committed_score_file(park_month, args.precision_flags)
    except AssertionError as e:
        sys.exit(f"REFUSED: {e}. Nothing was scored.")
    print("pandas", pd.__version__, "| numpy", np.__version__)
    if args.test_flags:
        print(f"TEST FLAGS from {args.precision_flags}: the precision marks of this run are not a result")
    result = run(park_month, flags)
    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        for name in ["summary", "monthly"]:
            path = out / f"news_forecast_{name}.csv"
            result[name].to_csv(path, index=False, lineterminator="\n")
            print(f"{len(result[name])} rows: {path}")


if __name__ == "__main__":
    main()
