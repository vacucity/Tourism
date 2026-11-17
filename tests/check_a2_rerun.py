"""Check an offline rerun of the A2 part 2 test against the outputs of 06 committed in docs/week8/a2_inputs.

src/a2_forecast.py --out <folder> writes a2_forecast_summary.csv (65 rows) and a2_forecast_monthly.csv (645 rows).
06_forecast_test wrote the same two files on Databricks, and they are committed in docs/week8/a2_inputs. This script
compares each rerun file with the committed file of the same name:
- The columns are the ones listed in TABLES, in the same order, in both files.
- The rows are the same keys in the same order: (check, measure, park_code) in the summary, (park_code, month_key)
  in the monthly table.
- Text columns (r_lower too, written as True or False) are equal. Integer columns are whole numbers without ".0"
  and equal.
- Float columns agree within 1e-9, relative or absolute (math.isclose with rel_tol = abs_tol = 1e-9). The laptop and
  Databricks give one different value: S2 ZION error_r, 11.454510446089563 against 11.45451044608956, a difference
  of 1.8e-15 (docs/week8/a2_part2_result.md, Reproduction).
- An empty field (NULL, or NaN as pandas writes it) is in the same cells.
Both files are read with Python's csv module, as text, so the check needs only the standard library.

It then states the A2 verdict from the primary rows of the committed summary and of the rerun, with the rule of
a2_part2_spec.md: a park counts when round(MAPE_R, 2) < round(MAPE_B, 2), and PASS needs 3 of the 5 parks. r_lower
must follow that rule in every primary row. The committed verdict must be the one in a2_part2_result.md (PASS, 3 of
5 parks: GRCA, YOSE, ZION), and the rerun's verdict must be the same.

From the repository root, after the rerun of README.md (How to run) with --out <folder>:
  uv run python tests/check_a2_rerun.py --summary <folder>/a2_forecast_summary.csv --monthly <folder>/a2_forecast_monthly.csv
--committed changes the folder of the committed files. Exit code 0 when both files match and the verdicts agree,
1 otherwise.
"""
import argparse
import csv
import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # repository root
COMMITTED = ROOT / "docs" / "week8" / "a2_inputs"
TOLERANCE = 1e-9                            # relative or absolute, for the float columns
INTEGER = re.compile(r"-?[0-9]+")           # a whole number as pandas writes an int64 column: no ".0"
PARKS = ["GRCA", "GRSM", "YELL", "YOSE", "ZION"]
TIE_DECIMALS = 2                            # a2_part2_spec.md: values equal after rounding count as not lower
PARKS_NEEDED = 3
# The verdict of docs/week8/a2_part2_result.md
RESULT_DOC = {"result": "PASS", "parks": ["GRCA", "YOSE", "ZION"]}

# Columns of the two CSV files, in order; every column is in exactly one of texts, integers and floats
TABLES = {
    "a2_forecast_summary.csv": {
        "columns": ["check", "measure", "park_code", "n_train", "n_term", "delta", "delta_se", "n_months",
                    "error_b", "error_r", "r_lower", "months_r_lower"],
        "keys": ["check", "measure", "park_code"],
        "texts": ["check", "measure", "park_code", "r_lower"],
        "integers": ["n_train", "n_term", "n_months", "months_r_lower"],
        "floats": ["delta", "delta_se", "error_b", "error_r"],
    },
    "a2_forecast_monthly.csv": {
        "columns": ["park_code", "month_key", "period", "visitors", "otb_complete", "camping_otb", "camping_stays",
                    "share_on_books", "usual_otb", "d", "forecast_b", "forecast_r", "ape_b", "ape_r"],
        "keys": ["park_code", "month_key"],
        "texts": ["park_code", "period"],
        "integers": ["month_key", "visitors", "otb_complete", "camping_otb", "camping_stays"],
        "floats": ["share_on_books", "usual_otb", "d", "forecast_b", "forecast_r", "ape_b", "ape_r"],
    },
}
for _name, _spec in TABLES.items():
    assert sorted(_spec["texts"] + _spec["integers"] + _spec["floats"]) == sorted(_spec["columns"]), _name


def read_table(path: Path) -> tuple[list[str], list[dict]]:
    """The header and the rows of one CSV file, every value as text (an empty field stays '')."""
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        rows = []
        for i, row in enumerate(reader, 2):
            assert len(row) == len(header), f"{path}: line {i} has {len(row)} fields, the header {len(header)}"
            rows.append(dict(zip(header, row)))
    return header, rows


def compare(name: str, committed: Path, rerun: Path) -> list[str]:
    """Differences between the committed file and the rerun file, as messages; [] when they match."""
    spec = TABLES[name]
    want_header, want = read_table(committed)
    got_header, got = read_table(rerun)
    if want_header != spec["columns"]:
        return [f"{committed}: columns {want_header}, expected {spec['columns']}"]
    if got_header != spec["columns"]:
        return [f"{rerun}: columns {got_header}, expected {spec['columns']}"]
    if len(got) != len(want):
        return [f"{name}: {len(got)} rows in the rerun, {len(want)} committed"]
    for i, (g, w) in enumerate(zip(got, want), 1):
        g_key, w_key = [g[c] for c in spec["keys"]], [w[c] for c in spec["keys"]]
        if g_key != w_key:
            # The rows differ from here on, so the cells are not compared
            return [f"{name} row {i}: key {spec['keys']} = {g_key} in the rerun, {w_key} committed"]

    problems = []
    for c in spec["texts"]:
        for g, w in zip(got, want):
            if g[c] != w[c]:
                problems.append(f"{name} {[w[k] for k in spec['keys']]} {c}: rerun {g[c]!r}, committed {w[c]!r}")
    for c in spec["integers"]:
        for g, w in zip(got, want):
            if g[c] == "" and w[c] == "":
                continue
            if not (INTEGER.fullmatch(g[c]) and INTEGER.fullmatch(w[c])) or int(g[c]) != int(w[c]):
                problems.append(f"{name} {[w[k] for k in spec['keys']]} {c}: rerun {g[c]!r}, committed {w[c]!r}")
    for c in spec["floats"]:
        worst, not_equal_text = 0.0, 0
        for g, w in zip(got, want):
            if g[c] == "" and w[c] == "":
                continue
            where = f"{name} {[w[k] for k in spec['keys']]} {c}"
            try:
                a, b = float(g[c]), float(w[c])
            except ValueError:
                problems.append(f"{where}: rerun {g[c]!r}, committed {w[c]!r}")
                continue
            if not (math.isfinite(a) and math.isfinite(b)):
                problems.append(f"{where}: rerun {g[c]!r}, committed {w[c]!r}")
                continue
            not_equal_text += g[c] != w[c]
            diff = abs(a - b)
            worst = max(worst, diff / max(1.0, abs(b)))
            if not math.isclose(a, b, rel_tol=TOLERANCE, abs_tol=TOLERANCE):
                problems.append(f"{where}: rerun {g[c]}, committed {w[c]}, difference {diff:.3g}")
        print(f"  {name} {c}: {not_equal_text} cells not equal as text, largest difference {worst:.3g} "
              "(relative where |committed| > 1)")
    return problems


def verdict(rows: list[dict], label: str) -> tuple[dict | None, list[str]]:
    """The pre-registered verdict from the primary rows of a summary file, and any problem with those rows."""
    primary = [r for r in rows if r["check"] == "primary"]
    if sorted(r["park_code"] for r in primary) != PARKS:
        return None, [f"{label}: the primary rows are for {sorted(r['park_code'] for r in primary)}, expected {PARKS}"]
    problems = []
    for r in primary:
        try:
            lower = round(float(r["error_r"]), TIE_DECIMALS) < round(float(r["error_b"]), TIE_DECIMALS)
        except ValueError:
            problems.append(f"{label} primary {r['park_code']}: error_b {r['error_b']!r}, error_r {r['error_r']!r}")
            continue
        if r["r_lower"] != str(lower):
            problems.append(f"{label} primary {r['park_code']}: r_lower {r['r_lower']!r}, but round(error_r, "
                            f"{TIE_DECIMALS}) < round(error_b, {TIE_DECIMALS}) is {lower}")
    parks = sorted(r["park_code"] for r in primary if r["r_lower"] == "True")
    result = "PASS" if len(parks) >= PARKS_NEEDED else "DESCRIPTIVE"
    print(f"{label + ':':<19} VERDICT: {result}: R lower in {len(parks)} of {len(PARKS)} parks "
          f"({', '.join(parks) or 'none'}), {PARKS_NEEDED} needed")
    return {"result": result, "parks": parks}, problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--summary", required=True, help="a2_forecast_summary.csv written by the rerun")
    ap.add_argument("--monthly", required=True, help="a2_forecast_monthly.csv written by the rerun")
    ap.add_argument("--committed", default=str(COMMITTED),
                    help="the folder of the committed files (default: docs/week8/a2_inputs)")
    args = ap.parse_args()
    committed = Path(args.committed)

    problems = []
    for name, rerun in [("a2_forecast_summary.csv", Path(args.summary)),
                        ("a2_forecast_monthly.csv", Path(args.monthly))]:
        print(f"comparing {rerun} with {committed / name}")
        found = compare(name, committed / name, rerun)
        problems += found
        print(f"  {name}: {'MATCH' if not found else f'{len(found)} differences'}")

    want_header, want_rows = read_table(committed / "a2_forecast_summary.csv")
    got_header, got_rows = read_table(Path(args.summary))
    if want_header != TABLES["a2_forecast_summary.csv"]["columns"] or \
            got_header != TABLES["a2_forecast_summary.csv"]["columns"]:
        problems.append("the verdict is not stated: a summary file does not have the expected columns")
    else:
        want_verdict, found = verdict(want_rows, "committed summary")
        problems += found
        got_verdict, found = verdict(got_rows, "rerun summary")
        problems += found
        if want_verdict is not None and want_verdict != RESULT_DOC:
            problems.append(f"the committed summary gives {want_verdict}, a2_part2_result.md {RESULT_DOC}")
        if got_verdict is not None and got_verdict != want_verdict:
            problems.append(f"the rerun gives {got_verdict}, the committed summary {want_verdict}")

    for p in problems[:20]:
        print("  ", p)
    if len(problems) > 20:
        print(f"   ... and {len(problems) - 20} more")
    print("RESULT:", "both files match the committed outputs and the verdict is the same" if not problems
          else f"{len(problems)} differences")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
