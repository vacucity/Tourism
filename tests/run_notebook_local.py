"""Run a Databricks notebook (.py source format) cell by cell on local PySpark, for tests and CI.

Written for databricks/notebooks/07_gdelt_news.py and 08_news_test.py. The notebook's own code runs; only what local
Spark cannot do is replaced:
- %md cells are skipped. %sql cells are split into statements and run with spark.sql(). Python cells run with exec(),
  in order, in one namespace, as on Databricks.
- Local Spark has no Unity Catalog and no Delta, so every table becomes a temporary view of the session:
  - DataFrameWriter.saveAsTable(name)                       -> DataFrame.createOrReplaceTempView(name)
  - CREATE OR REPLACE TABLE <name> COMMENT '<text>' AS <query> -> CREATE OR REPLACE TEMP VIEW <name> AS <query>
  - USE ..., CREATE SCHEMA ... and CREATE VOLUME ...         -> skipped
  - SELECT ... and WITH ... queries                          -> run unchanged
  Every statement, from a %sql cell or from spark.sql() in a Python cell, must have one of these forms. Any other
  statement stops the run, so nothing else is rewritten without notice.
- Stand-ins: dbutils.fs.ls() lists a local folder; display() shows nothing and is counted.
- On Windows only: Hadoop cannot list a local folder without its native library (hadoop.dll, not installed), so
  DataFrameReader.csv(<folder>) reads the files of the folder instead, with basePath = the folder. Spark reads the
  same files and still finds the pull_date= partition column. On Linux (CI) the folder is read as it is.
- LANDING, EXPORTS and TEST_FLAGS are defined before the first cell: the repository's data/landing, <out>/exports
  and True with --test-flags (else False). With --flags, FLAGS (the precision flags that 08 reads) is defined too.
- gold_park_month (03_gold builds it on Databricks) is a view of its 660 keys and month_start, read from
  docs/week8/a2_inputs/a2_inputs_park_month.csv with usecols, so the visitors column is never loaded. The keys must
  be the 5 parks x 2015-01..2025-12 grid that this script builds. That is all 07 reads from it.
- --table NAME=CSV (repeatable) makes a view of a Gold table that the notebook reads but does not build, with the
  types of STAND_INS. For 08: gold_park_month (it then replaces the keys view), gold_park_month_news and
  gold_park_month_reservations. The view holds exactly the rows of the file, so a test can leave a row out. A file
  whose header names a column twice is refused.
- No peek: no held-out news error is computed with real visits before the A1 part 2 result is committed. Before
  Spark starts, every visitors value from 2024-01 in a --table gold_park_month file is compared with the committed
  real value in docs/week8/a2_inputs/a2_inputs_park_month.csv, for equality only: only the count of equal values
  is printed, never a value or an error. The file is parsed for this check exactly as for its view, so both read
  the same column. The run is refused when a value is equal (or the files cannot be compared) and either
  --test-flags is given or the flags file that 08 will read is not docs/week8/a1_part2_score.json of this
  repository, committed at HEAD of the git repository whose root is this repository and unchanged since. So tests
  give 08 synthetic visits from 2024-01.
- --test-flags (tests only) lets 08 read the short test form of the flags or the score file of a test score
  (git_guard false); without it 08 accepts only the score file of a guarded score. 08 then checks the visits of
  its inputs again with news_forecast.require_synthetic_visits().

A view is computed again by each query that reads it, so the CSV files are parsed many times. The numbers are the
same as with tables; only the run is slower.

From the repository root, with Java 17 or 21 (JAVA_HOME):
  uv run --no-project --python 3.12 --with pyspark==4.3.0 --with pandas==2.2.3 --with numpy==2.1.3 \\
      python -I tests/run_notebook_local.py --out <scratch folder>
  uv run ... python -I tests/run_notebook_local.py --notebook databricks/notebooks/08_news_test.py \\
      --table gold_park_month=<csv with synthetic visits from 2024-01> --table gold_park_month_news=<csv> \\
      --table gold_park_month_reservations=<csv> --flags <test flags json> --test-flags --out <scratch folder>
The exports and Spark's scratch files go under --out. Exit code 0 when every cell ran, 1 otherwise (also when the
run is refused).
"""
import argparse
import csv
import os
import re
import subprocess
import sys
import time
import traceback
from collections import Counter, namedtuple
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]  # repository root
PARKS = ["GRCA", "GRSM", "YELL", "YOSE", "ZION"]
FIRST_YEAR, LAST_YEAR = 2015, 2025         # gold_park_month: 132 months, 2015-01..2025-12
# No peek: the committed real visits, the first month whose test visits must be synthetic (A2's first held-out
# month), and the A1 part 2 score file; once that file is committed at HEAD, unchanged, and is the flags file 08
# reads, real visits may enter 08
REAL_VISITS = ROOT / "docs" / "week8" / "a2_inputs" / "a2_inputs_park_month.csv"
FIRST_SYNTHETIC = 202401
SCORE_FILE = "docs/week8/a1_part2_score.json"

NOTEBOOK_HEADER = "# Databricks notebook source"
CELL_SEPARATOR = re.compile(r"^# COMMAND -{10}[ \t]*$", re.M)
NAME = r"[A-Za-z_][A-Za-z0-9_]*"  # an unqualified table name; a temporary view cannot have a catalog or schema
USE = re.compile(r"USE\s", re.I)
CREATE_SCHEMA_OR_VOLUME = re.compile(r"CREATE\s+(SCHEMA|VOLUME)\s", re.I)
# The COMMENT text is one literal without quotes or backslashes, so the pattern cannot end inside it
CREATE_TABLE_AS = re.compile(rf"CREATE\s+OR\s+REPLACE\s+TABLE\s+({NAME})\s+COMMENT\s+'[^'\\]*'\s+AS\s", re.I)
QUERY = re.compile(r"(SELECT|WITH)\s", re.I)

# --table stand-ins: the columns that the notebooks read, with the types of the Databricks tables
STAND_INS = {
    "gold_park_month": "park_code STRING, month_key INT, month_start DATE, visitors BIGINT",
    "gold_park_month_news": "park_code STRING, month_key INT, month_start DATE, news_all BIGINT, news_url BIGINT, "
                            "news_all_per_million DOUBLE, news_url_per_million DOUBLE",
    "gold_park_month_reservations": "park_code STRING, month_key INT, month_start DATE, otb_complete INT, "
                                    "camping_otb BIGINT",
}


# ---------------------------------------------------------------------------------------------------------------
# Reading the notebook

def read_cells(path: Path) -> list[tuple[str, str]]:
    """(kind, source) for every cell, in order: kind is 'md', 'sql' or 'python'."""
    text = path.read_text(encoding="utf-8")
    assert text.startswith(NOTEBOOK_HEADER), f"{path}: not a Databricks .py notebook (no {NOTEBOOK_HEADER!r} line)"
    cells = []
    for raw in CELL_SEPARATOR.split(text[len(NOTEBOOK_HEADER):]):
        lines = raw.strip("\n").splitlines()
        if not any(line.strip() for line in lines):
            continue
        magic = [line.startswith("# MAGIC") for line in lines if line.strip()]
        if not any(magic):
            cells.append(("python", raw))
            continue
        assert all(magic), f"{path}: a cell mixes '# MAGIC' lines with Python:\n{raw[:300]}"
        body = "\n".join(re.sub(r"^# MAGIC ?", "", line) for line in lines).strip()
        kind = body.split()[0]  # the magic command on the first line: %md or %sql
        if kind == "%md":
            cells.append(("md", body))
        elif kind == "%sql":
            cells.append(("sql", body[len("%sql"):]))
        else:
            raise AssertionError(f"{path}: magic command {kind} is not supported here")
    return cells


def strip_leading_comments(statement: str) -> str:
    """The statement without its leading blank lines and -- comment lines."""
    lines = statement.strip().splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    return "\n".join(lines).strip()


def split_statements(sql: str) -> list[str]:
    """Split the text of a %sql cell at each semicolon that is outside quotes and -- comments."""
    statements, current, quote, i = [], [], None, 0
    while i < len(sql):
        ch = sql[i]
        if quote:                                  # inside '...', "..." or `...`
            current.append(ch)
            if ch == "\\" and i + 1 < len(sql):    # Spark SQL escapes with a backslash inside a literal
                current.append(sql[i + 1])
                i += 1
            elif ch == quote:
                quote = None
        elif ch in "'\"`":
            quote = ch
            current.append(ch)
        elif sql.startswith("--", i):              # a comment runs to the end of the line
            end = sql.find("\n", i)
            end = len(sql) if end < 0 else end
            current.append(sql[i:end])
            i = end - 1
        elif ch == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    statements.append("".join(current))
    return [s for s in statements if strip_leading_comments(s)]


# ---------------------------------------------------------------------------------------------------------------
# What runs in place of Databricks

class Rewriter:
    """Maps each SQL statement to the form local Spark runs, and counts the forms it saw."""

    def __init__(self) -> None:
        self.forms = Counter()

    def rewrite(self, statement: str) -> str | None:
        """The statement to run, or None to skip it. An unknown form raises AssertionError."""
        text = strip_leading_comments(statement)
        if USE.match(text):
            self.forms["USE: skipped"] += 1
            return None
        if CREATE_SCHEMA_OR_VOLUME.match(text):
            self.forms["CREATE SCHEMA/VOLUME: skipped"] += 1
            return None
        m = CREATE_TABLE_AS.match(text)
        if m:
            self.forms["CREATE OR REPLACE TABLE ... COMMENT ... AS -> CREATE OR REPLACE TEMP VIEW ... AS"] += 1
            return f"CREATE OR REPLACE TEMP VIEW {m.group(1)} AS\n{text[m.end():]}"
        if QUERY.match(text):
            self.forms["SELECT/WITH query: unchanged"] += 1
            return text
        raise AssertionError(f"unknown statement form, not run: {text[:200]!r}")


class LocalSpark:
    """The notebook's `spark`: every sql() statement goes through the Rewriter; everything else is the session."""

    def __init__(self, session, rewriter: Rewriter) -> None:
        self._session = session
        self._rewriter = rewriter

    def sql(self, statement: str, *args, **kwargs):
        text = self._rewriter.rewrite(statement)
        if text is None:
            return self._session.createDataFrame([], "skipped STRING")
        return self._session.sql(text, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._session, name)


FileInfo = namedtuple("FileInfo", "path name size modificationTime")  # the fields of dbutils.fs.ls() entries


class LocalFs:
    @staticmethod
    def ls(path: str) -> list[FileInfo]:
        """Like dbutils.fs.ls on a local folder: folder names end with '/'; a missing folder raises."""
        folder = Path(path)
        if not folder.is_dir():
            raise FileNotFoundError(f"dbutils.fs.ls stand-in: no folder {path}")
        entries = []
        for p in sorted(folder.iterdir()):
            slash = "/" if p.is_dir() else ""
            entries.append(FileInfo(p.as_posix() + slash, p.name + slash, 0 if slash else p.stat().st_size,
                                    int(p.stat().st_mtime * 1000)))
        return entries


class LocalDbutils:
    fs = LocalFs()


def gold_park_month_stand_in(session, keys_csv: Path) -> None:
    """gold_park_month as a temporary view of its 660 keys and month_start; the visitors column is never loaded."""
    keys = pd.read_csv(keys_csv, usecols=["park_code", "month_key", "month_start"], dtype=str)
    got = sorted(keys[["park_code", "month_key", "month_start"]].itertuples(index=False, name=None))
    grid = sorted((park, f"{year}{month:02d}", f"{year}-{month:02d}-01")
                  for park in PARKS for year in range(FIRST_YEAR, LAST_YEAR + 1) for month in range(1, 13))
    assert got == grid, f"{keys_csv}: the keys are not the 5 parks x {FIRST_YEAR}-01..{LAST_YEAR}-12 grid"
    rows = [(park, int(key), date.fromisoformat(start)) for park, key, start in got]
    session.createDataFrame(rows, "park_code STRING, month_key INT, month_start DATE") \
        .createOrReplaceTempView("gold_park_month")
    print(f"gold_park_month stand-in: {len(rows)} keys (park_code, month_key, month_start) from {keys_csv}")


def stand_in_text(path: Path, columns: list[str]) -> pd.DataFrame:
    """The given columns of a stand-in CSV file, as text (an empty field stays ''). The view and the no-peek check
    both read a file with this function, so they read the same column. A header that names a column twice is
    refused: pandas would rename the second copy, and a check by name could then read another column."""
    with path.open(encoding="utf-8-sig", newline="") as f:  # -sig: the names pandas sees, without a BOM
        header = next(csv.reader(f), [])
    twice = sorted({c for c in header if header.count(c) > 1})
    assert not twice, f"{path}: the header names these columns more than once: {twice}"
    return pd.read_csv(path, usecols=columns, dtype=str, keep_default_na=False)


def table_stand_in(session, name: str, path: Path) -> None:
    """A view of one Gold table from a CSV file, with the columns and types of STAND_INS[name].

    The file is read as text; an empty field is NULL. Whole numbers must be written without '.0', and a DOUBLE is
    parsed with float(), which gives back exactly the double that pandas wrote."""
    assert name in STAND_INS, f"--table {name}: no stand-in schema; known tables: {sorted(STAND_INS)}"
    fields = [field.split() for field in STAND_INS[name].split(", ")]
    text = stand_in_text(path, [c for c, _ in fields])
    parse = {"STRING": str, "INT": int, "BIGINT": int, "DOUBLE": float, "DATE": date.fromisoformat}
    rows = [tuple(None if value == "" else parse[kind](value) for value, (_, kind) in zip(row, fields))
            for row in text[[c for c, _ in fields]].itertuples(index=False, name=None)]
    session.createDataFrame(rows, STAND_INS[name]).createOrReplaceTempView(name)
    print(f"{name} stand-in: {len(rows)} rows ({', '.join(c for c, _ in fields)}) from {path}")


# ---------------------------------------------------------------------------------------------------------------
# No peek: real visits from 2024-01 never reach 08 before the A1 part 2 result is committed

def equal_real_visits(stand_in: Path) -> int | None:
    """How many visitors values from 2024-01 in a gold_park_month stand-in equal the committed real value of the
    same park and month. The stand-in is parsed as its view is (stand_in_text). Equality only: nothing is printed
    here. Any problem (a missing file or column, a column named twice, a field that is not a number) gives None,
    "cannot compare", without its error message, which could quote a visit."""
    try:
        real = {}
        with REAL_VISITS.open(encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                if int(r["month_key"]) >= FIRST_SYNTHETIC and r["visitors"] != "":
                    real[(r["park_code"], int(r["month_key"]))] = float(r["visitors"])
        equal = 0
        text = stand_in_text(stand_in, ["park_code", "month_key", "visitors"])
        for park, key, visitors in text[["park_code", "month_key", "visitors"]].itertuples(index=False, name=None):
            if int(key) >= FIRST_SYNTHETIC and visitors != "" and real.get((park, int(key))) == float(visitors):
                equal += 1
        return equal if real else None
    except Exception:  # noqa: BLE001 - see the docstring
        return None


def same_path(a, b) -> bool:
    return os.path.normcase(str(Path(a).resolve())) == os.path.normcase(str(Path(b).resolve()))


def score_file_committed(flags: Path) -> bool:
    """True when flags, the file that 08 will read, is SCORE_FILE of the git repository whose root is ROOT, committed
    at HEAD and unchanged since (the blob of the working file = the blob at HEAD). Read-only git commands."""
    if not same_path(flags, ROOT / SCORE_FILE):
        return False
    try:
        def git(*args: str) -> str | None:
            r = subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True, text=True)
            return r.stdout.strip() if r.returncode == 0 else None

        top = git("rev-parse", "--show-toplevel")
        if top is None or not same_path(top, ROOT):
            return False  # ROOT is not the root of a git repository
        at_head = git("rev-parse", "--verify", "--quiet", f"HEAD:{SCORE_FILE}")
        return at_head is not None and at_head == git("hash-object", "--", SCORE_FILE)
    except OSError:  # git is not installed
        return False


def no_peek_refusal(tables: dict, test_flags: bool, flags: Path) -> str | None:
    """Why the run is refused, or None when it may start. Only a --table gold_park_month file has visits. flags is
    the precision flags file that 08 will read (--flags, else 08's default docs/week8/a1_part2_score.json)."""
    if "gold_park_month" not in tables:
        return None
    equal = equal_real_visits(Path(tables["gold_park_month"]))
    if equal is None:
        print(f"no-peek check: the visitors from {FIRST_SYNTHETIC} of the gold_park_month stand-in could not be "
              f"compared with {REAL_VISITS}")
    else:
        print(f"no-peek check: {equal} visitors values from {FIRST_SYNTHETIC} in the gold_park_month stand-in equal "
              "the committed real values")
    if equal == 0:
        return None
    if test_flags:
        return (f"--test-flags needs synthetic visits from {FIRST_SYNTHETIC} in every row of the gold_park_month "
                "stand-in")
    if score_file_committed(flags):
        print(f"no-peek check: 08 reads {SCORE_FILE}, committed at HEAD in {ROOT} and unchanged, so real visits may "
              "be used")
        return None
    return (f"the gold_park_month stand-in may hold real visits from {FIRST_SYNTHETIC}, and the flags file that 08 "
            f"reads is not {SCORE_FILE} (the A1 part 2 result) committed at HEAD of a git repository at {ROOT} and "
            "unchanged: no held-out news error is computed with real visits before that result is committed")


# ---------------------------------------------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--notebook", default=str(ROOT / "databricks" / "notebooks" / "07_gdelt_news.py"))
    ap.add_argument("--landing", default=str(ROOT / "data" / "landing"), help="the landing folder (default: data/landing)")
    ap.add_argument("--keys", default=str(ROOT / "docs" / "week8" / "a2_inputs" / "a2_inputs_park_month.csv"),
                    help="CSV with the 660 keys of gold_park_month (park_code, month_key, month_start)")
    ap.add_argument("--table", action="append", default=[], metavar="NAME=CSV",
                    help=f"a view of a Gold table from a CSV file (repeatable); NAME is one of {sorted(STAND_INS)}")
    ap.add_argument("--flags", help="defines FLAGS, the precision flags file that 08 reads")
    ap.add_argument("--test-flags", action="store_true",
                    help="tests only: defines TEST_FLAGS = True, so 08 also accepts the short test form of the flags "
                         "or a test score file; needs synthetic visits from 2024-01 in gold_park_month")
    ap.add_argument("--out", required=True, help="folder for the exports and Spark's scratch files")
    ap.add_argument("--master", default="local[2]")
    ap.add_argument("--driver-memory", default="1g")
    args = ap.parse_args()
    tables = dict(item.split("=", 1) for item in args.table)
    assert len(tables) == len(args.table), f"--table names a table twice: {args.table}"
    # Before Spark starts, so a refused run costs nothing. 08 reads FLAGS, else <its repository>/SCORE_FILE.
    flags = Path(args.flags) if args.flags else Path(args.notebook).resolve().parents[2] / SCORE_FILE
    refusal = no_peek_refusal(tables, args.test_flags, flags)
    if refusal:
        print(f"REFUSED: {refusal}. Nothing was run.")
        return 1

    notebook = Path(args.notebook).resolve()
    out = Path(args.out).resolve()
    exports, scratch = out / "exports", out / "spark_tmp"
    # PySpark writes temp files as <scratch>\spark-<uuid>\pyspark-<uuid>\tmpXXXXXXXX, 100 characters more than the
    # scratch folder. Without long-path support Windows refuses a path of 260 characters or more (seen in a test run
    # as a FileNotFoundError inside createDataFrame), so stop at once with a clear message instead.
    assert os.name != "nt" or len(str(scratch)) <= 155, (
        f"--out is too deep for Windows paths: {scratch} has {len(str(scratch))} characters, at most 155 work")
    exports.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    cells = read_cells(notebook)
    print(f"{notebook}: {len(cells)} cells ({dict(Counter(kind for kind, _ in cells))})")

    # The Python workers use this interpreter (with pandas); Spark's scratch files stay under --out
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_DIRS"] = str(scratch)
    from pyspark.sql import SparkSession
    from pyspark.sql.readwriter import DataFrameWriter

    session = (SparkSession.builder.master(args.master)
               .appName("run_notebook_local")
               .config("spark.driver.memory", args.driver_memory)
               .config("spark.sql.shuffle.partitions", "4")  # 200 by default: slow for a few small groups on a laptop
               .config("spark.ui.enabled", "false")
               .config("spark.sql.warehouse.dir", (scratch / "warehouse").as_posix())
               .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={scratch.as_posix()}")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    print("spark", session.version, "| pandas", pd.__version__, "| master", args.master,
          "| driver memory", args.driver_memory)

    rewriter = Rewriter()
    display_calls = []

    def save_as_temp_view(writer, name, *_args, **_kwargs):
        """Stand-in for DataFrameWriter.saveAsTable: the DataFrame becomes a temporary view."""
        assert re.fullmatch(NAME, name), f"saveAsTable({name!r}): only unqualified names can become a view"
        writer._df.createOrReplaceTempView(name)
        rewriter.forms["saveAsTable -> createOrReplaceTempView"] += 1

    def display(obj, *_args, **_kwargs):
        """Stand-in for Databricks display(): shows nothing, only counts the call."""
        display_calls.append(type(obj).__name__)
        print(f"[display() stand-in: a {type(obj).__name__}, not shown]")

    DataFrameWriter.saveAsTable = save_as_temp_view
    if os.name == "nt":
        from pyspark.sql.readwriter import DataFrameReader
        read_csv = DataFrameReader.csv

        def csv_of_folder_files(reader, path, *args, **kwargs):
            """Windows stand-in for csv(<folder>): the files of the folder, as Spark would list them."""
            if isinstance(path, str) and Path(path).is_dir():
                folder = Path(path)
                # Spark skips files and folders whose names start with _ or . when it lists a folder
                files = [p.as_posix() for p in sorted(folder.rglob("*"))
                         if p.is_file() and not any(part.startswith(("_", ".")) for part in p.relative_to(folder).parts)]
                rewriter.forms["DataFrameReader.csv(folder) -> csv(files of the folder), Windows only"] += 1
                return read_csv(reader.option("basePath", folder.as_posix()), files, *args, **kwargs)
            return read_csv(reader, path, *args, **kwargs)

        DataFrameReader.csv = csv_of_folder_files
    if "gold_park_month" not in tables:
        gold_park_month_stand_in(session, Path(args.keys))
    for name, path in tables.items():
        table_stand_in(session, name, Path(path))

    namespace = {
        "__name__": "__main__",
        "spark": LocalSpark(session, rewriter),
        "dbutils": LocalDbutils(),
        "display": display,
        "LANDING": Path(args.landing).resolve().as_posix(),
        "EXPORTS": exports.as_posix(),
        "TEST_FLAGS": args.test_flags,
    }
    if args.flags:
        namespace["FLAGS"] = Path(args.flags).resolve().as_posix()
    os.chdir(notebook.parent)  # the notebook finds the repository as Path.cwd().parents[1], as on Databricks

    status = 0
    started = time.time()
    try:
        for i, (kind, source) in enumerate(cells, 1):
            if kind == "md":
                continue
            print(f"\n===== cell {i} ({kind}) =====", flush=True)
            t0 = time.time()
            try:
                if kind == "sql":
                    for statement in split_statements(source):
                        namespace["spark"].sql(statement)
                else:
                    exec(compile(source, f"{notebook.name}, cell {i}", "exec"), namespace)
            except Exception:  # noqa: BLE001 - report the failing cell and stop, as Run all does
                traceback.print_exc(file=sys.stdout)
                print(f"\nFAILED in cell {i} ({kind}); the cells after it were not run")
                status = 1
                break
            print(f"----- cell {i}: {time.time() - t0:.1f} s", flush=True)
    finally:
        print("\n===== summary =====")
        print(f"{'all cells ran' if status == 0 else 'stopped by an error'} in {time.time() - started:.0f} s")
        for form, n in sorted(rewriter.forms.items()):
            print(f"  {n:>3}  {form}")
        print(f"  display() calls: {len(display_calls)}")
        for f in sorted(exports.glob("*.csv")):
            rows = sum(1 for _ in f.open(encoding="utf-8")) - 1
            print(f"  export {f.name}: {rows} rows, {f.stat().st_size:,} bytes")
        session.stop()
    return status


if __name__ == "__main__":
    sys.exit(main())
