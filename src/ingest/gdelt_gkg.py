"""Scan the GDELT 2.0 GKG archive: keep records that name one of the five parks, audit every file.

The English master file list is the manifest. Each 15-minute GKG file is downloaded, checked against
its MD5, unzipped in memory, filtered, and discarded; only matches and per-file audit rows are kept
in a local SQLite database, so a scan can stop (Ctrl+C) and resume at any time.

Run from any folder (paths are relative to the repository root):
  uv run python src/ingest/gdelt_gkg.py scan   --start 2025-01-06 --end 2025-01-12
  uv run python src/ingest/gdelt_gkg.py status
  uv run python src/ingest/gdelt_gkg.py export --start 2025-01-06 --end 2025-01-12 --shard 2025w02
  uv run python src/ingest/gdelt_gkg.py sample --n 100
"""
import argparse
import csv
import ctypes
import gzip
import hashlib
import io
import random
import sqlite3
import sys
import threading
import time
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from parks import PARKS

ROOT = Path(__file__).resolve().parents[2]  # repository root
WORK = ROOT / "data" / "work" / "gdelt"
MANIFEST = WORK / "masterfilelist.txt"
DB_PATH = WORK / "gdelt.sqlite"
MASTER_URL = "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
HEADERS = {"User-Agent": "nps-tourism-lakehouse/0.1 (university course project)"}

N_COLS = 27  # GKG 2.1 has 27 tab-separated fields
COL_ID, COL_SOURCE, COL_URL, COL_THEMES, COL_TONE = 0, 3, 4, 7, 15
MATCH_FIELDS = {9: "V1Locations", 10: "V2EnhancedLocations", 13: "V1Organizations", 23: "V2AllNames"}
# Full park names only, matched case-insensitively: short names such as "Zion" match churches,
# places and people. Plain lowercase search is about 25x faster than a case-insensitive regex.
PARK_NAMES = {code: info["name"].lower() for code, info in PARKS.items()}
PARK_NEEDLES = [name.encode() for name in PARK_NAMES.values()]

SCHEMA = """
CREATE TABLE IF NOT EXISTS file_audit (
    file_ts            TEXT PRIMARY KEY,  -- YYYYMMDDHHMMSS (UTC) from the file name
    url                TEXT,
    expected_bytes     INTEGER,
    downloaded_bytes   INTEGER,
    decompressed_bytes INTEGER,
    md5_ok             INTEGER,
    rows_read          INTEGER,           -- well-formed records (27 fields)
    malformed_rows     INTEGER,           -- lines that do not split into 27 fields
    matched_rows       INTEGER,
    status             TEXT,              -- ok | md5_mismatch | missing_on_server | error
    error              TEXT,
    seconds            REAL,
    processed_at       TEXT
);
CREATE TABLE IF NOT EXISTS matches (
    record_id      TEXT,
    file_ts        TEXT,
    park_code      TEXT,
    matched_fields TEXT,
    source         TEXT,
    url            TEXT,
    tone           REAL,
    themes         TEXT,
    PRIMARY KEY (record_id, url, park_code)  -- some early files reuse record ids
);
CREATE INDEX IF NOT EXISTS matches_file ON matches (file_ts);
"""


def connect() -> sqlite3.Connection:
    WORK.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")  # status, export and sample can read while a scan writes
    db.executescript(SCHEMA)
    return db


def load_manifest(refresh: bool) -> list[tuple[str, int, str, str]]:
    """(file_ts, size, md5, url) for every GKG file in the English master list."""
    stale = not MANIFEST.exists() or time.time() - MANIFEST.stat().st_mtime > 24 * 3600
    if refresh or stale:
        WORK.mkdir(parents=True, exist_ok=True)
        print("Downloading the master file list (about 130 MB) ...")
        with requests.get(MASTER_URL, headers=HEADERS, stream=True, timeout=(15, 300)) as r:
            r.raise_for_status()
            tmp = MANIFEST.with_suffix(".part")
            with tmp.open("wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
            tmp.replace(MANIFEST)
    entries = []
    for line in MANIFEST.open(encoding="utf-8", errors="replace"):
        parts = line.split()
        if len(parts) == 3 and parts[2].endswith(".gkg.csv.zip"):
            name = parts[2].rsplit("/", 1)[1]
            url = parts[2].replace("http://", "https://", 1)
            entries.append((name[:14], int(parts[0]), parts[1], url))
    return entries


_local = threading.local()


def session() -> requests.Session:
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
        _local.s.headers.update(HEADERS)
    return _local.s


def fetch(url: str, attempts: int = 4) -> bytes | None:
    """File bytes, or None if the server says the file does not exist."""
    for i in range(attempts):
        try:
            r = session().get(url, timeout=(15, 180))
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.content
        except requests.RequestException:
            if i == attempts - 1:
                raise
            time.sleep(5 * 2 ** i)


def process(file_ts: str, size: int, md5: str, url: str) -> dict:
    """Download, verify, filter one GKG file. Never raises: errors become an audit status."""
    t0 = time.time()
    res = {"file_ts": file_ts, "url": url, "expected_bytes": size, "downloaded_bytes": 0,
           "decompressed_bytes": 0, "md5_ok": 0, "rows_read": 0, "malformed_rows": 0,
           "matched_rows": 0, "status": "error", "error": None, "matches": []}
    try:
        data = fetch(url)
        if data is None:
            res["status"] = "missing_on_server"
            return res
        res["downloaded_bytes"] = len(data)
        res["md5_ok"] = int(hashlib.md5(data).hexdigest() == md5)
        if not res["md5_ok"]:
            res["status"] = "md5_mismatch"
            return res
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = zf.namelist()
            if len(names) != 1:
                raise ValueError(f"expected 1 file in the zip, found {len(names)}: {names[:3]}")
            raw = zf.read(names[0])
        res["decompressed_bytes"] = len(raw)

        for line in raw.split(b"\n"):
            if not line.strip():
                continue
            if line.count(b"\t") == N_COLS - 1:
                res["rows_read"] += 1
            else:
                res["malformed_rows"] += 1

        # Find every occurrence of a park name, then take the whole line around it
        low = raw.lower()
        line_starts = set()
        for needle in PARK_NEEDLES:
            i = low.find(needle)
            while i != -1:
                line_starts.add(low.rfind(b"\n", 0, i) + 1)
                i = low.find(needle, i + len(needle))
        candidates = []
        for start in sorted(line_starts):
            end = raw.find(b"\n", start)
            line = raw[start:] if end == -1 else raw[start:end]
            if line.count(b"\t") == N_COLS - 1:
                candidates.append(line)

        seen = set()
        for line in candidates:
            cols = line.decode("utf-8", "replace").split("\t")
            for code, name in PARK_NAMES.items():
                fields = [label for idx, label in MATCH_FIELDS.items() if name in cols[idx].lower()]
                key = (cols[COL_ID], cols[COL_URL], code)
                if not fields or key in seen:
                    continue
                seen.add(key)
                try:
                    tone = float(cols[COL_TONE].split(",")[0])
                except ValueError:
                    tone = None
                res["matches"].append((cols[COL_ID], file_ts, code, ",".join(fields),
                                       cols[COL_SOURCE], cols[COL_URL], tone, cols[COL_THEMES]))
        res["matched_rows"] = len(res["matches"])
        res["status"] = "ok"
    except Exception as e:  # recorded in the audit table, retried on the next scan
        res["error"] = f"{type(e).__name__}: {e}"[:500]
    finally:
        res["seconds"] = round(time.time() - t0, 2)
    return res


def save(db: sqlite3.Connection, res: dict) -> None:
    """Replace this file's audit row and matches in one transaction, so reruns stay idempotent."""
    with db:
        db.execute("DELETE FROM matches WHERE file_ts = ?", (res["file_ts"],))
        db.executemany("INSERT OR REPLACE INTO matches VALUES (?,?,?,?,?,?,?,?)", res["matches"])
        db.execute("INSERT OR REPLACE INTO file_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            res["file_ts"], res["url"], res["expected_bytes"], res["downloaded_bytes"],
            res["decompressed_bytes"], res["md5_ok"], res["rows_read"], res["malformed_rows"],
            res["matched_rows"], res["status"], res["error"], res["seconds"],
            datetime.now().isoformat(timespec="seconds")))


def day_range(start: str, end: str) -> tuple[str, str]:
    """Inclusive dates -> half-open file_ts bounds."""
    lo = date.fromisoformat(start)
    hi = date.fromisoformat(end) + timedelta(days=1)
    return lo.strftime("%Y%m%d000000"), hi.strftime("%Y%m%d000000")


def scan(args: argparse.Namespace) -> None:
    lo, hi = day_range(args.start, args.end)
    entries = [e for e in load_manifest(args.refresh_manifest) if lo <= e[0] < hi]
    db = connect()
    # Only finished files are skipped; errors, MD5 mismatches and 404s are asked again on every run
    done = {r[0] for r in db.execute("SELECT file_ts FROM file_audit WHERE status = 'ok'")}
    todo = [e for e in entries if e[0] not in done]
    n_done = len(entries) - len(todo)
    if args.limit:
        todo = todo[:args.limit]
    total_bytes = sum(e[1] for e in todo)
    print(f"{args.start} to {args.end}: {len(entries)} files listed, {n_done} already done, "
          f"{len(todo)} to process ({total_bytes / 1e9:.2f} GB) with {args.workers} workers")
    if not todo:
        status(args)
        return
    if sys.platform == "win32":  # keep Windows from sleeping while the scan runs
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)

    t0 = last = time.time()
    n = got = done_bytes = rows = errors_in_a_row = 0
    pool = ThreadPoolExecutor(args.workers)
    try:
        # No list of futures is kept, so each result is freed as soon as it is saved
        for fut in as_completed([pool.submit(process, *e) for e in todo]):
            res = fut.result()
            save(db, res)
            n += 1
            got += res["downloaded_bytes"]
            done_bytes += res["expected_bytes"]
            rows += res["rows_read"]
            if res["status"] != "ok":
                print(f"  {res['file_ts']}: {res['status']} {res['error'] or ''}")
            errors_in_a_row = errors_in_a_row + 1 if res["status"] == "error" else 0
            if errors_in_a_row >= 50:
                raise SystemExit("50 files in a row failed: check the network, then run scan again to resume")
            if time.time() - last > 30 or n == len(todo):
                last = time.time()
                el = last - t0
                eta = (total_bytes - done_bytes) / (done_bytes / el) if done_bytes else 0
                print(f"  {n}/{len(todo)} files | {got / 1e9:.2f} GB | {got / el / 1e6:.1f} MB/s | "
                      f"{rows / el:,.0f} rows/s | elapsed {timedelta(seconds=int(el))} | "
                      f"ETA {timedelta(seconds=int(eta))}")
    finally:
        # Ctrl+C or an error: drop the queued files instead of downloading all of them first
        pool.shutdown(wait=True, cancel_futures=True)
        if sys.platform == "win32":
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
    status(args)


def status(args: argparse.Namespace) -> None:
    db = connect()
    print("Files by status:", dict(db.execute("SELECT status, COUNT(*) FROM file_audit GROUP BY status")))
    q = db.execute("""SELECT MIN(file_ts), MAX(file_ts), SUM(downloaded_bytes), SUM(decompressed_bytes),
                             SUM(rows_read), SUM(malformed_rows), SUM(matched_rows)
                      FROM file_audit WHERE status = 'ok'""").fetchone()
    if q[0] is None:
        print("No files processed yet.")
        return
    print(f"Covered {q[0]} to {q[1]}")
    print(f"Downloaded {q[2] / 1e9:.2f} GB, decompressed and scanned {q[3] / 1e9:.2f} GB, "
          f"{q[4]:,} records ({q[5]:,} malformed lines), {q[6]:,} park matches")
    print("Matches by park:", dict(db.execute("SELECT park_code, COUNT(*) FROM matches GROUP BY park_code")))


def write_csv(path: Path, header: list[str], rows) -> None:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "wt", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def export(args: argparse.Namespace) -> None:
    """Write one shard's matches, daily coverage and audit rows as landing files for Databricks."""
    lo, hi = day_range(args.start, args.end)
    listed = Counter(e[0][:8] for e in load_manifest(False) if lo <= e[0] < hi)
    db = connect()
    by_status = dict(db.execute("SELECT status, COUNT(*) FROM file_audit WHERE file_ts >= ? AND file_ts < ? "
                                "GROUP BY status", (lo, hi)))
    not_scanned = sum(listed.values()) - sum(by_status.values())
    print(f"{sum(listed.values())} files listed, by status {by_status}, {not_scanned} not scanned yet")
    incomplete = not_scanned or by_status.get("error") or by_status.get("md5_mismatch")
    if incomplete and not args.allow_incomplete:
        raise SystemExit("Shard incomplete: run scan again for this range, or pass --allow-incomplete")

    pull = f"pull_date={date.today().isoformat()}"
    out = Path(args.out)
    targets = {name: out / name / pull for name in ("gdelt_matches", "gdelt_daily", "gdelt_audit")}
    for p in targets.values():
        p.mkdir(parents=True, exist_ok=True)

    cur = db.execute("SELECT * FROM matches WHERE file_ts >= ? AND file_ts < ? "
                     "ORDER BY file_ts, record_id, park_code", (lo, hi))
    write_csv(targets["gdelt_matches"] / f"matches_{args.shard}.csv.gz", [c[0] for c in cur.description], cur)

    audited = {r[0]: r[1:] for r in db.execute("""
        SELECT substr(file_ts, 1, 8), SUM(status = 'ok'), SUM(status = 'missing_on_server'),
               SUM(status = 'md5_mismatch'), SUM(status = 'error'),
               SUM(CASE WHEN status = 'ok' THEN rows_read ELSE 0 END)
        FROM file_audit WHERE file_ts >= ? AND file_ts < ? GROUP BY 1""", (lo, hi))}
    daily = []
    day, last_day = date.fromisoformat(args.start), date.fromisoformat(args.end)
    while day <= last_day:  # every calendar day, so days the master list skips (outages) show as 0 listed
        key = day.strftime("%Y%m%d")
        ok, missing, bad_md5, error, records = audited.get(key, (0, 0, 0, 0, 0))
        n_listed = listed.get(key, 0)
        daily.append((day.isoformat(), n_listed, ok, missing, bad_md5, error,
                      n_listed - ok - missing - bad_md5 - error, records))
        day += timedelta(days=1)
    write_csv(targets["gdelt_daily"] / f"daily_{args.shard}.csv",
              ["day", "files_listed", "files_ok", "files_missing_on_server", "files_md5_mismatch",
               "files_error", "files_not_scanned", "records"], daily)

    cur = db.execute("SELECT * FROM file_audit WHERE file_ts >= ? AND file_ts < ? ORDER BY file_ts", (lo, hi))
    write_csv(targets["gdelt_audit"] / f"audit_{args.shard}.csv.gz", [c[0] for c in cur.description], cur)

    for name, p in targets.items():
        for f in sorted(p.glob(f"*_{args.shard}.csv*")):
            mb = f.stat().st_size / 1e6
            print(f"{name}: {f} ({mb:.1f} MB)")
            if mb > 95:
                raise SystemExit(f"{f.name} is over GitHub's 100 MB limit: upload it to the Volume instead")


def sample(args: argparse.Namespace) -> None:
    """Random matches for the hand check of precision; fill in the is_about_park column."""
    db = connect()
    rows = db.execute("SELECT record_id, park_code, matched_fields, source, url FROM matches "
                      "ORDER BY record_id, url, park_code").fetchall()  # fixed order, so the seed decides
    random.Random(args.seed).shuffle(rows)
    path = WORK / "precision_sample.csv"
    if path.exists():
        raise SystemExit(f"{path} already exists and may hold hand labels; rename it first")
    write_csv(path, ["record_id", "park_code", "matched_fields", "source", "url", "is_about_park"],
              (r + ("",) for r in rows[:args.n]))
    print(f"{min(args.n, len(rows))} of {len(rows)} matches written to {path}")


def main() -> None:
    global DB_PATH
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DB_PATH), help="SQLite file; use a second file for the rerun check")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("scan", help="process GKG files between two dates (inclusive, UTC)")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--limit", type=int, default=0, help="process at most N files (smoke test)")
    p.add_argument("--refresh-manifest", action="store_true")
    sub.add_parser("status", help="summarize the local scan database")
    p = sub.add_parser("export", help="write one shard's landing files for Databricks")
    p.add_argument("--start", required=True, help="first day of this machine's shard, YYYY-MM-DD")
    p.add_argument("--end", required=True, help="last day of this machine's shard, YYYY-MM-DD")
    p.add_argument("--shard", required=True, help="label for this shard; use the same label for every export")
    p.add_argument("--out", default=str(ROOT / "data" / "landing"))
    p.add_argument("--allow-incomplete", action="store_true", help="export even if files are unscanned or failed")
    p = sub.add_parser("sample", help="random matches for the precision hand check")
    p.add_argument("--n", type=int, default=100)
    p.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    DB_PATH = Path(args.db)
    {"scan": scan, "status": status, "export": export, "sample": sample}[args.cmd](args)


if __name__ == "__main__":
    main()
