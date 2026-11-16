"""Check the two GDELT Gold tables against a recomputation from the committed exports, without Spark.

databricks/notebooks/07_gdelt_news.py builds gold_month_gkg (132 months) and gold_park_month_news (660 park-months)
and exports them as gold_month_gkg.csv and gold_park_month_news.csv. This script recomputes both from data/landing
and compares them with a given pair of CSV files: downloaded from the Databricks exports Volume, or written by a
local run (tests/run_notebook_local.py).
- The exports are read with Python's csv module, the module that wrote them. Each shard comes from its newest
  pull_date= folder, as in Silver.
- url_rule is gdelt_url_rule.passes(), the Python side of rule R0 (src/gdelt_url_rule.py).
- DuckDB only joins and groups: days by month, match rows by park x month. The shares are divided in pandas.
The definitions are those of 07: months 2015-01..2025-12 in UTC, slots = 96 x days of the month, coverage =
files_ok / slots, per million = 1e6 x count / records, and the four news columns NULL where files_ok = 0.
Integer columns must be equal, with NULL (an empty field) in the same cells. The shares (coverage and the two
per-million columns) must agree within 1e-9. Text columns (park_code, month_start) must be equal.

From the repository root, with the two downloaded files in a folder outside the repository (keep them there until
the news result is committed: gdelt_sample.py draw and score refuse an untracked file), the same command as in the
last cell of 07:
  uv run --no-project --python 3.12 --with pandas==2.2.3 --with numpy==2.1.3 --with duckdb==2.0.0 python -I tests/check_gdelt_gold.py --month-gkg <folder>/gold_month_gkg.csv --park-month-news <folder>/gold_park_month_news.csv
--month-gkg and --park-month-news take any path; --landing and --src change the inputs.
Exit code 0 when both files match, 1 otherwise.
"""
import argparse
import calendar
import csv
import gzip
import hashlib
import math
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]  # repository root
PARKS = ["GRCA", "GRSM", "YELL", "YOSE", "ZION"]
FIRST_YEAR, LAST_YEAR = 2015, 2025          # the 132 months of gold_park_month
SLOTS_PER_DAY = 96                          # one GKG file every 15 minutes
TOLERANCE = 1e-9                            # for the shares
DATASETS = {"matches": "gdelt_matches", "daily": "gdelt_daily", "audit": "gdelt_audit"}
FILE_NAME = re.compile(r"(matches|daily|audit)_(s[0-9]+)[.]csv([.]gz)?")
INTEGER = re.compile(r"-?[0-9]+")           # a whole number as 07 writes it: no ".0"

# Columns of the two CSV files, in order
TABLES = {
    "gold_month_gkg": {
        "keys": ["month_key"],
        "columns": ["month_key", "month_start", "files_listed", "files_ok", "slots", "coverage", "records"],
        "integers": ["month_key", "files_listed", "files_ok", "slots", "records"],
        "shares": ["coverage"],
        "texts": ["month_start"],
    },
    "gold_park_month_news": {
        "keys": ["park_code", "month_key"],
        "columns": ["park_code", "month_key", "month_start", "news_all", "news_url",
                    "news_all_per_million", "news_url_per_million"],
        "integers": ["month_key", "news_all", "news_url"],
        "shares": ["news_all_per_million", "news_url_per_million"],
        "texts": ["park_code", "month_start"],
    },
}


def newest_files(landing: Path) -> dict[str, list[Path]]:
    """For each dataset, the file of each shard from the newest pull_date= folder that holds the shard."""
    files, kept = {}, {}  # dataset -> [file per shard]; dataset -> {shard: pull folder}
    for dataset, folder in DATASETS.items():
        newest = {}  # shard -> (pull folder name, file)
        for f in sorted((landing / folder).glob("pull_date=*/*")):
            m = FILE_NAME.fullmatch(f.name)
            assert m and m.group(1) == dataset, f"unexpected file {f}"
            shard, pull = m.group(2), f.parent.name
            assert not (shard in newest and newest[shard][0] == pull), f"two files for {shard} in {f.parent}"
            if shard not in newest or pull > newest[shard][0]:  # pull_date= names sort by date
                newest[shard] = (pull, f)
        assert newest, f"no export files under {landing / folder}"
        kept[dataset] = {shard: pull for shard, (pull, _) in newest.items()}
        files[dataset] = [f for _, (_, f) in sorted(newest.items())]
        print(f"{folder}: " + ", ".join(f"{shard} {pull}" for shard, pull in sorted(kept[dataset].items())))
    assert kept["matches"] == kept["daily"] == kept["audit"], f"the datasets keep different pulls per shard: {kept}"
    return files


def read_rows(path: Path, columns: list[str]):
    """Yield the given columns of every row of one export file, as text."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        index = [header.index(c) for c in columns]
        for row in reader:
            assert len(row) == len(header), f"{path}: a row with {len(row)} fields, expected {len(header)}"
            yield [row[i] for i in index]


def recompute(landing: Path, rule) -> dict[str, pd.DataFrame]:
    """gold_month_gkg and gold_park_month_news from the exports, with the definitions of 07."""
    files = newest_files(landing)

    # Match rows: park, UTC month of the file, and R0 from the Python rule
    parks, month_keys, url_rule = [], [], []
    for path in files["matches"]:
        for file_ts, park_code, url in read_rows(path, ["file_ts", "park_code", "url"]):
            assert park_code in PARKS, f"{path}: unknown park_code {park_code!r}"
            parks.append(park_code)
            month_keys.append(int(file_ts[:6]))
            url_rule.append(int(rule.passes(park_code, url)))
    matches = pd.DataFrame({"park_code": parks, "month_key": month_keys, "url_rule": url_rule})
    print(f"match rows: {len(matches):,}, url_rule = 1: {int(matches['url_rule'].sum()):,}")

    days = pd.DataFrame([row for path in files["daily"]
                         for row in read_rows(path, ["day", "files_listed", "files_ok", "records"])],
                        columns=["day", "files_listed", "files_ok", "records"])
    days["month_key"] = days["day"].str[:4].astype(int) * 100 + days["day"].str[5:7].astype(int)
    for c in ["files_listed", "files_ok", "records"]:
        days[c] = days[c].astype("int64")
    audit = pd.DataFrame([row for path in files["audit"] for row in read_rows(path, ["file_ts", "status", "rows_read"])],
                         columns=["file_ts", "status", "rows_read"])
    audit["month_key"] = audit["file_ts"].str[:6].astype(int)
    audit["rows_read"] = audit["rows_read"].astype("int64")
    print(f"days: {len(days):,}, audited files: {len(audit):,}")

    months = pd.DataFrame([(year * 100 + month, f"{year}-{month:02d}-01",
                            SLOTS_PER_DAY * calendar.monthrange(year, month)[1])
                           for year in range(FIRST_YEAR, LAST_YEAR + 1) for month in range(1, 13)],
                          columns=["month_key", "month_start", "slots"])
    keys = pd.DataFrame([(park, key) for park in PARKS for key in months["month_key"]],
                        columns=["park_code", "month_key"])

    con = duckdb.connect()
    con.execute("SET memory_limit = '300MB'")
    con.execute("SET threads = 2")
    for name, df in {"matches": matches, "days": days, "audit": audit, "months": months, "park_months": keys}.items():
        con.register(name, df)

    # Cross-check of the two denominators: per month, the daily file = the audit's ok files and their rows_read
    differ = con.execute("""
        WITH d AS (SELECT month_key, CAST(SUM(files_ok) AS BIGINT) AS ok, CAST(SUM(records) AS BIGINT) AS records
                   FROM days GROUP BY month_key),
             a AS (SELECT month_key, CAST(COUNT(*) FILTER (WHERE status = 'ok') AS BIGINT) AS ok,
                          CAST(SUM(CASE WHEN status = 'ok' THEN rows_read ELSE 0 END) AS BIGINT) AS records
                   FROM audit GROUP BY month_key)
        SELECT COUNT(*) FROM d FULL OUTER JOIN a USING (month_key)
        WHERE d.ok IS DISTINCT FROM a.ok OR d.records IS DISTINCT FROM a.records""").fetchone()[0]
    assert differ == 0, f"{differ} months where the daily file and the audit differ"
    print("per month, the daily file's files_ok and records = the audit's ok files and their rows_read")

    gkg = con.execute("""
        SELECT m.month_key, m.month_start,
               COALESCE(d.files_listed, 0) AS files_listed, COALESCE(d.files_ok, 0) AS files_ok,
               m.slots, COALESCE(d.records, 0) AS records
        FROM months AS m
        LEFT JOIN (SELECT month_key, CAST(SUM(files_listed) AS BIGINT) AS files_listed,
                          CAST(SUM(files_ok) AS BIGINT) AS files_ok, CAST(SUM(records) AS BIGINT) AS records
                   FROM days GROUP BY month_key) AS d USING (month_key)
        ORDER BY m.month_key""").df()
    gkg["coverage"] = gkg["files_ok"] / gkg["slots"]
    gkg = gkg[TABLES["gold_month_gkg"]["columns"]]

    news = con.execute("""
        SELECT k.park_code, k.month_key, c.news_all, c.news_url
        FROM park_months AS k
        LEFT JOIN (SELECT park_code, month_key, CAST(COUNT(*) AS BIGINT) AS news_all,
                          CAST(SUM(url_rule) AS BIGINT) AS news_url
                   FROM matches GROUP BY park_code, month_key) AS c USING (park_code, month_key)
        ORDER BY k.park_code, k.month_key""").df()
    news = news.merge(gkg[["month_key", "month_start", "files_ok", "records"]], on="month_key", how="left")
    has_files = news["files_ok"] > 0
    for c in ["news_all", "news_url"]:
        news[c] = news[c].fillna(0).astype("Int64").where(has_files, pd.NA)   # 0 when no match, NULL without files
    for c in ["news_all", "news_url"]:
        # the formula of 07: (1e6 * count) / records, NULL where the month has no ok file
        news[f"{c}_per_million"] = (1e6 * news[c].astype("float64") / news["records"]).where(has_files)
    news = news[TABLES["gold_park_month_news"]["columns"]]
    return {"gold_month_gkg": gkg, "gold_park_month_news": news}


def compare(name: str, expected: pd.DataFrame, path: Path) -> list[str]:
    """Differences between the recomputed table and one CSV file, as messages; [] when they match."""
    spec = TABLES[name]
    got = pd.read_csv(path, dtype=str, keep_default_na=False)
    if list(got.columns) != spec["columns"]:
        return [f"{path.name}: columns {list(got.columns)}, expected {spec['columns']}"]
    if len(got) != len(expected):
        return [f"{path.name}: {len(got)} rows, expected {len(expected)}"]
    got["_key"] = got[spec["keys"]].astype(str).agg("|".join, axis=1)
    want = expected.copy()
    want["_key"] = want[spec["keys"]].astype(str).agg("|".join, axis=1)
    if got["_key"].duplicated().any() or set(got["_key"]) != set(want["_key"]):
        return [f"{path.name}: the keys {spec['keys']} differ from the recomputed table or repeat"]
    got = got.set_index("_key").loc[want["_key"]]
    want = want.set_index("_key")

    problems = []
    for c in spec["integers"]:
        for key, text, value in zip(want.index, got[c], want[c]):
            if text == "" and pd.isna(value):
                continue
            if text == "" or pd.isna(value) or not INTEGER.fullmatch(text) or int(text) != int(value):
                problems.append(f"{path.name} {key} {c}: file {text!r}, recomputed {value}")
    for c in spec["shares"]:
        worst = 0.0
        for key, text, value in zip(want.index, got[c], want[c]):
            if text == "" and pd.isna(value):
                continue
            if text == "" or pd.isna(value) or not math.isfinite(float(text)):
                problems.append(f"{path.name} {key} {c}: file {text!r}, recomputed {value}")
                continue
            diff = abs(float(text) - float(value))
            worst = max(worst, diff)
            if diff > TOLERANCE:
                problems.append(f"{path.name} {key} {c}: file {text}, recomputed {value!r}, difference {diff:.3g}")
        print(f"  {name}.{c}: largest difference {worst:.3g}")
    for c in spec["texts"]:
        for key, text, value in zip(want.index, got[c], want[c]):
            if text != str(value):
                problems.append(f"{path.name} {key} {c}: file {text!r}, recomputed {value!r}")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--month-gkg", required=True, help="gold_month_gkg.csv written by 07")
    ap.add_argument("--park-month-news", required=True, help="gold_park_month_news.csv written by 07")
    ap.add_argument("--landing", default=str(ROOT / "data" / "landing"), help="the landing folder (default: data/landing)")
    ap.add_argument("--src", default=str(ROOT / "src"), help="the folder with gdelt_url_rule.py (default: src)")
    args = ap.parse_args()

    module = Path(args.src) / "gdelt_url_rule.py"
    sys.path.insert(0, str(Path(args.src)))
    import gdelt_url_rule  # noqa: E402

    print("gdelt_url_rule.py sha256", hashlib.sha256(module.read_bytes()).hexdigest())
    csv.field_size_limit(2**31 - 1)  # a themes field may be longer than the default limit of 131,072 characters
    tables = recompute(Path(args.landing), gdelt_url_rule)

    gkg, news = tables["gold_month_gkg"], tables["gold_park_month_news"]
    null_cells = int(news[["news_all", "news_url", "news_all_per_million", "news_url_per_million"]].isna().sum().sum())
    print(f"recomputed: gold_month_gkg {len(gkg)} rows, {int(gkg['records'].sum()):,} records, "
          f"{int(gkg['files_ok'].sum()):,} files ok; gold_park_month_news {len(news)} rows, "
          f"news_all {int(news['news_all'].sum()):,}, news_url {int(news['news_url'].sum()):,}, {null_cells} NULL cells")

    problems = []
    for name, path in [("gold_month_gkg", Path(args.month_gkg)), ("gold_park_month_news", Path(args.park_month_news))]:
        print(f"comparing {path}")
        found = compare(name, tables[name], path)
        problems += found
        print(f"  {name}: {'MATCH' if not found else f'{len(found)} differences'}")
    for p in problems[:20]:
        print("  ", p)
    if len(problems) > 20:
        print(f"   ... and {len(problems) - 20} more")
    print("RESULT:", "both files match the recomputation" if not problems else f"{len(problems)} differences")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
