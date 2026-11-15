"""Extract the five-park reservations from MySQL to Parquet for the Databricks landing volume (A2 part 2).

This is the only lakehouse code that reads MySQL. It reads the tables that src/load_mysql.py fills
(reservation, booking_order, facility, product) and writes one row per reservation, with 16 typed columns, to
  data/work/landing/ridb_reservations/pull_date=YYYY-MM-DD/
data/work is git-ignored because the rows are private under the RIDB terms. Upload the folder's files by hand
to /Volumes/workspace/nps_tourism/landing/ridb_reservations/pull_date=YYYY-MM-DD/.

All queries run in one read-only REPEATABLE READ transaction, so they all see the snapshot of the first one.
A pull continues from the newest earlier pull folder, whose _manifest.json holds the watermarks:
- new rows: reservation keys above the previous maximum, one file per block of 1,000,000 keys. Keys grow in
  load order. Order time cannot be the watermark: a later fiscal-year file adds orders older than ones
  already loaded.
- changed rows: keys at or below the previous maximum whose reservation or order updated_at is later than the
  previous maximum (a loader rerun that changes a value moves updated_at), in changed.parquet.
A run with nothing new or changed writes nothing. The run stops and says why when a load is running or did not
finish, when the database was rebuilt, or when a product's use type or a facility's park or name changed
(they are copied into every row, so use --full). Files go to a .part folder that is renamed after
_manifest.json is written: an interrupted run leaves only the .part folder, and the next run starts it again.

Needs MYSQL_URL in .env at the repository root (see src/load_mysql.py); --database replaces the database it
names. Run from any folder after the load. A second run prints "no new or changed reservations":
  uv run python src/extract_reservations.py
"""
import argparse
import hashlib
import json
import os
import shutil
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import create_engine, make_url, text

ROOT = Path(__file__).resolve().parents[1]  # repository root
OUT_DIR = ROOT / "data" / "work" / "landing" / "ridb_reservations"
KEY_BLOCK = 1_000_000  # reservation keys per file of new rows
BATCH = 50_000  # rows per fetch, per pyarrow batch and per Parquet row group
TS = "%Y-%m-%d %H:%M:%S.%f"  # watermarks are kept as text, always with 6 decimals

# Spark would read an unsigned 64-bit key as a decimal, so reservation_key is int64. number_of_people is
# SMALLINT UNSIGNED, which can exceed int16. TIMESTAMP and DATETIME values arrive in UTC (see main).
SCHEMA = pa.schema([
    ("reservation_key", pa.int64()),
    ("fiscal_year", pa.int32()),
    ("park_code", pa.string()),
    ("facility_id", pa.string()),
    ("facility_name", pa.string()),
    ("product_id", pa.string()),
    ("use_type", pa.string()),
    ("inventory_type", pa.string()),
    ("start_date", pa.date32()),
    ("end_date", pa.date32()),
    ("number_of_people", pa.int32()),
    ("total_paid", pa.decimal128(10, 2)),
    ("ordered_at", pa.timestamp("us", tz="UTC")),
    ("ordered_date_utc", pa.date32()),  # the on-the-books rule compares it with day 1 of the stay month
    ("updated_at", pa.timestamp("us", tz="UTC")),
    ("order_updated_at", pa.timestamp("us", tz="UTC")),
])

# ordered_at is stored in UTC, so DATE() gives the UTC date
COLUMNS = """r.reservation_key, r.fiscal_year, f.park_code, r.facility_id, f.facility_name, r.product_id, p.use_type,
       r.inventory_type, r.start_date, r.end_date, r.number_of_people, r.total_paid,
       o.ordered_at, DATE(o.ordered_at) AS ordered_date_utc, r.updated_at, o.updated_at AS order_updated_at"""
# The foreign keys fk_reservation_order, fk_reservation_product and fk_product_facility make the inner joins safe
FROM = """FROM reservation r
JOIN booking_order o ON o.order_number = r.order_number
JOIN facility f ON f.facility_id = r.facility_id
JOIN product p ON p.facility_id = r.facility_id AND p.product_id = r.product_id"""
# STRAIGHT_JOIN keeps reservation first, read in key order. When a block covers most of the table, MySQL would
# otherwise start from facility, reach the rows in random order and sort the whole block before sending a row.
NEW_ROWS = (f"SELECT STRAIGHT_JOIN {COLUMNS}\n{FROM}\n"
            "WHERE r.reservation_key > :lo AND r.reservation_key <= :hi\nORDER BY r.reservation_key")
# Each half uses its own index: ix_reservation_updated_at, and ix_order_updated_at with ix_reservation_order.
# A changed order brings all its lines; UNION keeps one copy of a row that both halves find.
CHANGED_ROWS = (f"(SELECT {COLUMNS}\n{FROM}\nWHERE r.reservation_key <= :prev_key AND r.updated_at > :prev_r_ts)\n"
                "UNION\n"
                f"(SELECT {COLUMNS}\n{FROM}\nWHERE r.reservation_key <= :prev_key AND o.updated_at > :prev_o_ts)\n"
                "ORDER BY reservation_key")


def ts(value: datetime | None) -> str | None:
    return value.strftime(TS) if value else None


def key_blocks(lo: int, hi: int) -> list[tuple[int, int]]:
    """The key range (lo, hi] cut at multiples of KEY_BLOCK: (0, 8953544] -> (0, 1000000], ..., (8000000, 8953544]."""
    edges = [lo, *range((lo // KEY_BLOCK + 1) * KEY_BLOCK, hi, KEY_BLOCK), hi]
    return [(a, b) for a, b in zip(edges, edges[1:]) if a < b]


def previous_pull(out_root: Path) -> tuple[Path, dict] | None:
    """The newest pull_date=YYYY-MM-DD folder that holds a _manifest.json (the names sort by date)."""
    pulls = sorted(p for p in out_root.glob("pull_date=*") if (p / "_manifest.json").is_file())
    if not pulls:
        return None
    return pulls[-1], json.loads((pulls[-1] / "_manifest.json").read_text(encoding="utf-8"))


def snapshot(conn) -> tuple[dict, list]:
    """Watermarks and row counts of the snapshot, and every product with the lowest reservation key that uses it."""
    max_key, rows, max_r_ts = conn.execute(text(
        "SELECT MAX(reservation_key), COUNT(*), MAX(updated_at) FROM reservation")).one()
    max_o_ts, max_ordered = conn.execute(text("SELECT MAX(updated_at), MAX(ordered_at) FROM booking_order")).one()
    # Count per facility first (the index on fiscal_year, facility_id answers it), then per park
    by_fy_park = conn.execute(text(
        "SELECT c.fiscal_year, f.park_code, SUM(c.n) FROM (SELECT fiscal_year, facility_id, COUNT(*) AS n "
        "FROM reservation GROUP BY fiscal_year, facility_id) c JOIN facility f ON f.facility_id = c.facility_id "
        "GROUP BY c.fiscal_year, f.park_code ORDER BY c.fiscal_year, f.park_code")).all()
    products = conn.execute(text(
        "SELECT p.facility_id, p.product_id, p.use_type, f.park_code, f.facility_name, u.first_key "
        "FROM product p JOIN facility f ON f.facility_id = p.facility_id "
        "LEFT JOIN (SELECT facility_id, product_id, MIN(reservation_key) AS first_key FROM reservation "
        "GROUP BY facility_id, product_id) u ON u.facility_id = p.facility_id AND u.product_id = p.product_id")).all()
    snap = {"max_reservation_key": int(max_key or 0), "reservation_rows": int(rows),
            "max_r_updated_at": ts(max_r_ts), "max_o_updated_at": ts(max_o_ts), "max_ordered_at": ts(max_ordered),
            "rows_by_fy_park": [{"fiscal_year": int(fy), "park_code": park, "rows": int(n)}
                                for fy, park, n in by_fy_park]}
    return snap, products


def fingerprints(products: list, max_key: int) -> tuple[str, str]:
    """sha256 of the sorted product rows (facility_id, product_id, use_type) and facility rows (facility_id,
    park_code, facility_name) that reservations with keys up to max_key use. A later load that adds products
    or facilities leaves the fingerprint of the earlier keys as it was; a changed use type, park or name
    changes it."""
    used = [p for p in products if p.first_key is not None and p.first_key <= max_key]
    product_rows = sorted([p.facility_id, p.product_id, p.use_type] for p in used)
    facility_rows = sorted({(p.facility_id, p.park_code, p.facility_name) for p in used})
    return tuple(hashlib.sha256(json.dumps(rows, ensure_ascii=False).encode("utf-8")).hexdigest()
                 for rows in (product_rows, facility_rows))


def to_batch(rows: list) -> pa.RecordBatch:
    """One fetched partition -> a RecordBatch of SCHEMA. Each column is converted to its declared type directly,
    so a value of another type (text in a number column, say) or out of range stops the run."""
    return pa.RecordBatch.from_arrays(
        [pa.array(values, type=field.type) for values, field in zip(zip(*rows), SCHEMA)], schema=SCHEMA)


def write_rows(conn, sql: str, params: dict, path: Path, kind: str, tally: Counter) -> dict | None:
    """Stream one query into one Parquet file, one row group per batch, and add its rows per (fiscal_year,
    park_code) to tally. Returns the file's manifest entry, or None (and no file) when there are no rows."""
    t0 = time.time()
    writer, rows, min_key, max_key = None, 0, None, None
    # stream_results uses PyMySQL's unbuffered cursor: memory holds one batch at a time
    with conn.execute(text(sql), params, execution_options={"stream_results": True, "yield_per": BATCH}) as result:
        if list(result.keys()) != SCHEMA.names:
            raise SystemExit(f"The query returns {list(result.keys())}, the schema declares {SCHEMA.names}")
        for chunk in result.partitions(BATCH):
            if writer is None:
                writer = pq.ParquetWriter(path, SCHEMA, compression="zstd")
                min_key = chunk[0][0]
            writer.write_batch(to_batch(chunk))
            rows += len(chunk)
            max_key = chunk[-1][0]
            tally.update((row[1], row[2]) for row in chunk)
    if writer is None:
        print(f"  {path.name}: no rows, no file ({time.time() - t0:.0f} s)")
        return None
    writer.close()
    secs = time.time() - t0
    print(f"  {path.name}: {rows:,} rows in {secs:.0f} s ({rows / max(secs, 0.001):,.0f} rows/s)")
    return {"name": path.name, "kind": kind, "rows": rows, "min_key": min_key, "max_key": max_key,
            "bytes": path.stat().st_size}


def print_counts(in_mysql: dict, written: Counter, full: bool) -> None:
    """Rows per (fiscal_year, park_code): in MySQL at the snapshot, and new rows written by this pull."""
    label = "parquet_rows" if full else "new_rows"
    print(f"{'fiscal_year':>11}  {'park_code':<9}  {'mysql_rows':>12}  {label:>12}")
    for (fy, park), n in in_mysql.items():
        print(f"{fy:>11}  {park:<9}  {n:>12,}  {written.get((fy, park), 0):>12,}")
    print(f"{'total':>11}  {'':<9}  {sum(in_mysql.values()):>12,}  {sum(written.values()):>12,}")


def publish(part: Path, final: Path) -> None:
    """Rename the finished .part folder to its pull_date= name. Windows refuses while another program (a virus
    scanner, the search indexer, Explorer) has a file in the folder open, so this tries again for 30 seconds."""
    for _ in range(30):
        try:
            part.rename(final)
            return
        except PermissionError:
            time.sleep(1)
    raise SystemExit(f"Could not rename {part} to {final.name}: another program has a file in it open. Its files "
                     f"and _manifest.json are complete: close that program, then rename the folder by hand.")


def run_extract(engine, out_root: Path, pull_date: date, full: bool) -> dict | None:
    """Write the folder pull_date=YYYY-MM-DD under out_root and return its manifest, or None when nothing is new
    or changed. full=True ignores earlier pulls and writes every reservation."""
    out_root = Path(out_root)
    day = pull_date.isoformat()
    final, part = out_root / f"pull_date={day}", out_root / f".pull_date={day}.part"
    found = previous_pull(out_root)
    if found and found[1]["pull_date"] > day:
        raise SystemExit(f"{found[0]} is dated after today ({day}): check the computer's date")
    prev = None if full or not found else found[1]
    database = engine.url.database
    if prev and prev["database"] != database:
        raise SystemExit(f"{found[0].name} was pulled from the database {prev['database']} and this run reads "
                         f"{database}: pass --database {prev['database']}, or --full to start over")

    t0 = time.time()
    with engine.connect() as conn:  # closing the connection rolls back whatever is still open
        tx = conn.begin()
        # The first SELECT fixes the snapshot. load_mysql.py sets completed_at to NULL when a load starts and
        # sets it after the last replay transaction, so a NULL means a load is running or did not finish.
        if conn.execute(text("SELECT COUNT(*) FROM source_file WHERE completed_at IS NULL")).scalar():
            raise SystemExit("source_file has completed_at NULL: a load is running or did not finish. Let it "
                             "finish, or run src/load_mysql.py again, then run this again.")
        snap, products = snapshot(conn)
        snap["product_sha256"], snap["facility_sha256"] = fingerprints(products, snap["max_reservation_key"])
        p = prev["snapshot"] if prev else None
        prev_key = p["max_reservation_key"] if p else 0
        print(f"Snapshot: {snap['reservation_rows']:,} reservations, keys up to {snap['max_reservation_key']:,}, "
              f"updated_at up to {snap['max_r_updated_at']} (orders {snap['max_o_updated_at']}); "
              f"read in {time.time() - t0:.0f} s")
        print(f"Previous pull: {found[0].name}, keys up to {prev_key:,}" if p else
              "Full pull: every reservation")
        if p and snap["max_reservation_key"] < prev_key:
            raise SystemExit(f"The highest reservation key is {snap['max_reservation_key']:,}, below {prev_key:,} in "
                             f"{found[0].name}: the database was rebuilt or rows were deleted. Use --full.")
        new_rows = conn.execute(text("SELECT COUNT(*) FROM reservation WHERE reservation_key > :lo "
                                     "AND reservation_key <= :hi"),
                                {"lo": prev_key, "hi": snap["max_reservation_key"]}).scalar()
        if p:
            # The keys up to the previous maximum must hold as many rows as they did then
            old_rows = snap["reservation_rows"] - new_rows
            if old_rows != p["reservation_rows"]:
                raise SystemExit(f"{old_rows:,} reservations have keys up to {prev_key:,}, and {found[0].name} had "
                                 f"{p['reservation_rows']:,}: the database was rebuilt. Use --full.")
            old_product, old_facility = fingerprints(products, prev_key)
            if old_product != p["product_sha256"]:
                raise SystemExit(f"A product's use type changed since {found[0].name}. use_type is copied into "
                                 f"every row, so the earlier rows are out of date: use --full.")
            if old_facility != p["facility_sha256"]:
                raise SystemExit(f"A facility's park or name changed since {found[0].name}. Both are copied into "
                                 f"every row, so the earlier rows are out of date: use --full.")
        changed = p is not None and (snap["max_r_updated_at"] > p["max_r_updated_at"]
                                     or snap["max_o_updated_at"] > p["max_o_updated_at"])
        if not new_rows and not changed:
            print("no new or changed reservations")
            return None
        if final.exists():
            raise SystemExit(f"{final} exists and there are rows to write: delete it to redo today's pull")
        for old in out_root.glob(".pull_date=*.part"):
            shutil.rmtree(old)  # left by an interrupted run
            print(f"Removed {old.name}, left by an interrupted run")
        part.mkdir(parents=True)

        files, tally = [], Counter()
        for lo, hi in key_blocks(prev_key, snap["max_reservation_key"]):
            entry = write_rows(conn, NEW_ROWS, {"lo": lo, "hi": hi},
                               part / f"keys_{lo + 1:09d}-{hi:09d}.parquet", "new", tally)
            if entry:
                files.append(entry)
        if p:
            # '>' and not '>=': no load ran at the previous snapshot (completed_at check), so every later change
            # has a later updated_at, and an unchanged database gives no rows
            entry = write_rows(conn, CHANGED_ROWS, {"prev_key": prev_key, "prev_r_ts": p["max_r_updated_at"],
                                                    "prev_o_ts": p["max_o_updated_at"]},
                               part / "changed.parquet", "changed", Counter())
            if entry:
                files.append(entry)
        tx.rollback()  # read only: nothing to commit

    # Checks before the folder is published
    rows_new = sum(f["rows"] for f in files if f["kind"] == "new")
    rows_changed = sum(f["rows"] for f in files if f["kind"] == "changed")
    if rows_new != new_rows:
        raise SystemExit(f"Wrote {rows_new:,} new rows, but MySQL holds {new_rows:,} keys in "
                         f"({prev_key:,}, {snap['max_reservation_key']:,}]")
    in_mysql = {(c["fiscal_year"], c["park_code"]): c["rows"] for c in snap["rows_by_fy_park"]}
    print_counts(in_mysql, tally, p is None)
    if p is None and (dict(tally) != in_mysql or rows_new != snap["reservation_rows"]):
        raise SystemExit("The rows written per (fiscal_year, park_code) differ from MySQL's counts (table above)")
    if not files:
        shutil.rmtree(part)
        print("no new or changed reservations")
        return None

    manifest = {
        "source": "src/extract_reservations.py from the MySQL tables reservation, booking_order, facility, product",
        "database": database,
        "server": f"{engine.url.host}:{engine.url.port or 3306}",
        "pull_date": day,
        "mode": "incremental" if p else "full",
        "written_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "previous": {"pull_date": prev["pull_date"], "max_reservation_key": prev_key,
                     "max_r_updated_at": p["max_r_updated_at"], "max_o_updated_at": p["max_o_updated_at"],
                     "reservation_rows": p["reservation_rows"]} if p else None,
        "snapshot": snap,
        "files": files,
        "rows_new": rows_new,
        "rows_changed": rows_changed,
    }
    (part / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    publish(part, final)
    print(f"{final.name}: {rows_new:,} new rows in {sum(f['kind'] == 'new' for f in files)} files, "
          f"{rows_changed:,} changed rows, {sum(f['bytes'] for f in files) / 1e6:.1f} MB -> {final}")
    print(f"Upload its files to /Volumes/workspace/nps_tourism/landing/ridb_reservations/{final.name}/")
    if p is None and found:
        print(f"This full pull holds every reservation: delete the earlier pull folders (newest {found[0].name}) "
              f"here and on the volume")
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--database", default="nps_tourism_full", help="replaces the database named in MYSQL_URL")
    ap.add_argument("--full", action="store_true", help="ignore earlier pulls and write every reservation")
    ap.add_argument("--out", default=str(OUT_DIR), help="folder for the pull_date= folders")
    args = ap.parse_args()

    load_dotenv(ROOT / ".env")
    if not os.environ.get("MYSQL_URL"):
        raise SystemExit(f"Set MYSQL_URL in {ROOT / '.env'}")
    url = make_url(os.environ["MYSQL_URL"]).set(database=args.database)
    print(url.render_as_string(hide_password=True))  # the server, port and database this run reads
    # REPEATABLE READ: the first SELECT fixes one snapshot for every query. The session is read only, and its
    # time zone is UTC, so TIMESTAMP updated_at comes back in UTC like the DATETIME ordered_at.
    engine = create_engine(url, pool_pre_ping=True, isolation_level="REPEATABLE READ",
                           connect_args={"init_command": "SET time_zone = '+00:00', transaction_read_only = ON"})
    t0 = time.time()
    run_extract(engine, Path(args.out), date.today(), args.full)
    print(f"Total time {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()
