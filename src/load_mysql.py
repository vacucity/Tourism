"""Replay Recreation.gov reservations for the five parks into the MySQL OLTP schema (mysql/01_schema.sql).

Each fiscal-year zip is streamed in chunks and only NPS rows for the five parks are kept. The two file
layouts (FY2015-FY2018 and FY2019+) are normalized to one, then each file's bookings are replayed in
order-date order (one file after another, oldest fiscal year first): one transaction per ~2,000
reservations, never splitting an order. Every write is an upsert on the table's primary key (reservation:
on its UNIQUE reservation_id), so a rerun adds nothing. Rows that miss a required value or would break a
CHECK go to a rejects CSV instead.

Needs MYSQL_URL in .env at the repository root, for example
  MYSQL_URL=mysql+pymysql://nps_app:PASSWORD@localhost:3306/nps_tourism
Use letters and digits in the password, or write @ as %40 and % as %25: an unescaped @ moves the rest of
the password into the host name, which connection errors print.
Run from any folder, after src/ingest/ridb_download.py:
  uv run python src/load_mysql.py --fiscal-years 2017 2024
"""
import argparse
import os
import sys
import time
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]  # repository root
sys.path.insert(0, str(ROOT / "src" / "ingest"))  # parks.py is the single source of park names
from parks import PARKS  # noqa: E402

ZIP_DIR = ROOT / "data" / "work" / "ridb"
SCHEMA_SQL = ROOT / "mysql" / "01_schema.sql"

# First fiscal year of the layout with inventorytype, discount and equipmentdescription. FY2015-FY2018
# files hold the park name in RegionDescription, FY2019+ files in ParentLocation.
NEW_LAYOUT_FROM = 2019
PARK_OF = {info["name"]: code for code, info in PARKS.items()}
# FY2017 files this Yellowstone permit program under its own name; FY2024 lists the same
# facility (250849) under "Yellowstone National Park"
YELL_PROGRAM = "Yellowstone Non-Commercially Guided Snowmobile Access Program"
PARK_OF[YELL_PROGRAM] = "YELL"

# Full state names (FY2019+) to USPS codes (FY2015-FY2018), for the states the five parks touch
STATE_CODES = {"Arizona": "AZ", "California": "CA", "Idaho": "ID", "Montana": "MT",
               "North Carolina": "NC", "Tennessee": "TN", "Utah": "UT", "Wyoming": "WY"}

# FY2015-FY2018 equipment count columns -> equipment_type labels. Ten are spelled as FY2024's
# equipmentdescription spells them. FY2024 has no 'Van' (it has 'Caravan/Camper Van') and none of the other eleven.
EQUIPMENT_COLUMNS = {
    "tent": "Tent", "popup": "Pop up", "trailer": "Trailer", "rvmotorhome": "RV/Motorhome",
    "boat": "Boat", "horsetrailer": "Horse Trailer", "car": "Car", "fifthwheel": "Fifth Wheel",
    "van": "Van", "canoekayak": "Canoe/Kayak", "boattrailer": "Boat Trailer", "motorcycle": "Motorcycle",
    "truck": "Truck", "bus": "Bus", "bicycle": "Bicycle", "snowmobile": "Snowmobile",
    "offroadlallterrainvehicle": "Off Road/All Terrain Vehicle", "powerboat": "Power Boat",
    "pickupcamper": "Pickup Camper", "largetentover9x12": "Large Tent Over 9X12",
    "smalltent": "Small Tent", "marinaboat": "Marina Boat",
}
WANTED = {
    "historicalreservationid", "ordernumber", "agency", "regiondescription", "parentlocation", "park",
    "sitetype", "usetype", "productid", "entitytype", "inventorytype", "facilityid", "facilityzip",
    "facilitystate", "facilitylatitude", "facilitylongitude", "customerzip", "customerstate",
    "customercountry", "tax", "usefee", "tranfee", "attrfee", "totalbeforetax", "discount", "totalpaid",
    "startdate", "enddate", "orderdate", "numberofpeople", "equipmentdescription", "equipmentlength",
} | set(EQUIPMENT_COLUMNS)

MONEY = ["tax", "use_fee", "tran_fee", "attr_fee", "total_before_tax", "discount", "total_paid"]
ORDER_COLS = ["order_number", "ordered_at", "customer_zip", "customer_state", "customer_country"]
RES_COLS = ["reservation_id", "order_number", "order_line", "facility_id", "product_id", "fiscal_year",
            "inventory_type", "site_type", "start_date", "end_date", "number_of_people"] + MONEY
EQUIP_COLS = ["reservation_id", "equipment_type", "quantity", "length_ft"]
FAC_COLS = ["facility_id", "park_code", "facility_name", "facility_zip", "state_code", "latitude",
            "longitude", "updated_fy"]
PROD_COLS = ["facility_id", "product_id", "use_type"]


def blank_to_na(s: pd.Series) -> pd.Series:
    return s.mask(s.str.strip() == "")


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(blank_to_na(s))


def utc(s: pd.Series) -> pd.Series:
    """'2016-10-02', '2018-06-21 11:00:00' or '2024-08-04T02:13:37.018512Z' -> naive timestamps (UTC when the source gives a zone)."""
    return pd.to_datetime(blank_to_na(s), utc=True, format="ISO8601").dt.tz_convert(None)


def state_code(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    if len(value) == 2:
        return value.upper()
    if value not in STATE_CODES:
        raise SystemExit(f"Unknown facility state {value!r}: add it to STATE_CODES")
    return STATE_CODES[value]


def normalize(raw: pd.DataFrame, year: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """One chunk of kept rows in either layout -> (reservations, facilities, equipment) in one layout."""
    new_layout = year >= NEW_LAYOUT_FROM
    r = pd.DataFrame(index=raw.index)
    r["reservation_id"] = raw["historicalreservationid"].str.strip()
    r["src_row"] = raw.index  # row number in the CSV (read_csv chunks number rows across the whole file)
    if new_layout:
        # "0610465289-3" is line 3 of order 0610465289: the lines share order time and customer ZIP
        parts = raw["ordernumber"].str.rsplit("-", n=1, expand=True)
        r["order_number"] = parts[0]
        r["order_line"] = pd.to_numeric(parts[1]).astype("Int32")
    else:
        r["order_number"] = raw["ordernumber"]
        r["order_line"] = pd.Series(pd.NA, index=raw.index, dtype="Int32")
    r["park_code"] = raw["park_code"]
    r["facility_id"] = raw["facilityid"]
    r["product_id"] = raw["productid"]
    r["use_type"] = raw["usetype"].mask(raw["usetype"].isin(["", "None"]))  # product attribute
    r["inventory_type"] = raw["inventorytype" if new_layout else "entitytype"]
    r["site_type"] = blank_to_na(raw["sitetype"])
    # Dates are kept as the UTC date: the few FY2024 rows with a time (07:00-20:00 UTC) are local
    # midnight or noon, so the UTC date is the local date
    r["start_date"] = utc(raw["startdate"]).dt.normalize()
    r["end_date"] = utc(raw["enddate"]).dt.normalize()
    r["ordered_at"] = utc(raw["orderdate"])
    r["number_of_people"] = num(raw["numberofpeople"]).astype("Int32")
    for col, src in [("tax", "tax"), ("use_fee", "usefee"), ("tran_fee", "tranfee"), ("attr_fee", "attrfee"),
                     ("total_before_tax", "totalbeforetax"), ("total_paid", "totalpaid")]:
        r[col] = num(raw[src]).round(2)
    # FY2019+ files store the discount as a negative number; it is kept as a positive amount
    # (0 - x rather than -x, so a zero discount is 0.0 and not -0.0)
    r["discount"] = (0 - num(raw["discount"])).round(2) if new_layout else np.nan
    zip_ = raw["customerzip"].str.strip()
    r["customer_zip"] = zip_.mask((zip_ == "") | (zip_.str.len() > 10))  # longer ones are free text
    r["zip_too_long"] = zip_.str.len() > 10
    if "customerstate" in raw.columns:
        r["customer_state"] = raw["customerstate"].mask(raw["customerstate"].isin(["", "UNKN"]))
        r["customer_country"] = raw["customercountry"].mask(raw["customercountry"].isin(["", "UNKN"]))
    else:  # the FY2024 file has no customer state or country
        r["customer_state"] = r["customer_country"] = None

    fac = pd.DataFrame({
        "facility_id": raw["facilityid"],
        "park_code": raw["park_code"],
        "facility_name": raw["park"].str.strip(),  # the "park" column holds the facility name
        "facility_zip": blank_to_na(raw["facilityzip"]),
        "state_code": raw["facilitystate"].map({v: state_code(v) for v in raw["facilitystate"].unique()}),
        "latitude": num(raw["facilitylatitude"]).round(6),
        "longitude": num(raw["facilitylongitude"]).round(6),
    }).drop_duplicates()

    if new_layout:
        desc = raw["equipmentdescription"].str.strip().str.rstrip("`")
        length = num(raw["equipmentlength"])
        has = desc != ""
        equip = pd.DataFrame({"reservation_id": r.loc[has, "reservation_id"],
                              "src_row": r.loc[has, "src_row"], "equipment_type": desc[has],
                              "quantity": pd.Series(pd.NA, index=desc[has].index, dtype="Int32"),
                              "length_ft": length[has].where(length[has] > 0).round(2)})
    else:
        pieces = []
        for col, label in EQUIPMENT_COLUMNS.items():
            q = num(raw[col])
            has = q.notna() & (q > 0)
            pieces.append(pd.DataFrame({"reservation_id": r.loc[has, "reservation_id"],
                                        "src_row": r.loc[has, "src_row"], "equipment_type": label,
                                        "quantity": q[has].astype("Int32"), "length_ft": np.nan}))
        equip = pd.concat(pieces)
    return r, fac, equip


def read_year(year: int, zip_path: Path) -> dict:
    """Stream one zip, keep the five parks, normalize, and count every row that is not loaded."""
    t0 = time.time()
    with zipfile.ZipFile(zip_path) as zf:
        (member,) = zf.infolist()  # each RIDB zip holds a single CSV
        chunks = pd.read_csv(zf.open(member), dtype=str, keep_default_na=False, chunksize=250_000,
                             usecols=lambda c: c.lower() in WANTED, encoding_errors="replace")
        rows_read = program_rows = 0
        program_facilities = set()
        res_parts, fac_parts, eq_parts, rejects = [], [], [], []
        source, dropped = pd.Series(0, index=list(PARKS)), pd.Series(0, index=list(PARKS))
        for chunk in chunks:
            chunk.columns = chunk.columns.str.lower()
            rows_read += len(chunk)
            location = chunk["parentlocation" if year >= NEW_LAYOUT_FROM else "regiondescription"]
            raw = chunk.assign(park_code=location.map(PARK_OF)).dropna(subset=["park_code"])
            is_program = location[raw.index] == YELL_PROGRAM
            program_rows += int(is_program.sum())
            program_facilities.update(raw.loc[is_program, "facilityid"])
            source = source.add(raw["park_code"].value_counts(), fill_value=0)
            # Rows of any agency other than NPS, and DONATION rows, are not park reservations
            inventory = raw["inventorytype" if year >= NEW_LAYOUT_FROM else "entitytype"]
            drop = (raw["agency"] != "NPS") | (inventory == "DONATION")
            dropped = dropped.add(raw.loc[drop, "park_code"].value_counts(), fill_value=0)
            raw = raw[~drop]
            if raw.empty:
                continue
            r, fac, equip = normalize(raw, year)
            money = r[MONEY]
            bad = pd.Series("", index=r.index)
            bad = bad.mask(r["ordered_at"].isna(), "missing order date")
            bad = bad.mask((r["facility_id"] == "") | (r["product_id"] == ""), "missing facility or product id")
            bad = bad.mask(r["inventory_type"] == "", "missing inventory type")
            bad = bad.mask(r[["tax", "total_before_tax", "total_paid"]].isna().any(axis=1), "missing amount")
            # total_before_tax is not checked: 5 FY2024 backcountry permits have it negative and total_paid positive
            bad = bad.mask((money.drop(columns="total_before_tax") < 0).any(axis=1), "negative amount")
            bad = bad.mask(r["end_date"] < r["start_date"], "end date before start date")
            if (bad != "").any():
                rejects.append(raw.loc[bad != ""].assign(reject_reason=bad[bad != ""]))
            res_parts.append(r[bad == ""])
            fac_parts.append(fac)
            eq_parts.append(equip)
    res = pd.concat(res_parts, ignore_index=True)
    fac = pd.concat(fac_parts).drop_duplicates()
    equip = pd.concat(eq_parts, ignore_index=True)
    rej = pd.concat(rejects) if rejects else pd.DataFrame(columns=["park_code", "reject_reason"])
    rejects_path = zip_path.parent / f"rejects_FY{year}.csv"
    if len(rej):
        rej.to_csv(rejects_path, index=False)
    elif rejects_path.exists():
        rejects_path.unlink()  # from an earlier run of different code

    # Replay order: by order time, keeping each order's lines together. A repeated reservation id (FY2017: 70 ids
    # listed under two products, 1 listed twice under one product with two customer ZIPs) keeps the row with the
    # lowest product id, or the first row in the file on a tie.
    replay_order = ["ordered_at", "order_number", "order_line", "reservation_id", "product_id"]
    res = res.sort_values(replay_order, kind="stable", ignore_index=True)
    dup = res["reservation_id"].duplicated()
    duplicates = res.loc[dup, "park_code"].value_counts()
    res = res[~dup].reset_index(drop=True)

    # An order's time and customer location must be the same on all its lines; if not, the first wins
    conflicts = int(res.drop_duplicates(ORDER_COLS).duplicated("order_number").sum())
    if conflicts:
        res[ORDER_COLS[1:]] = res.groupby("order_number")[ORDER_COLS[1:]].transform("first")
        res = res.sort_values(replay_order, kind="stable", ignore_index=True)

    # facility_id -> park and attributes, (facility_id, product_id) -> use_type: must hold within a file
    fac = fac[fac["facility_id"].isin(res["facility_id"])]
    if fac["facility_id"].duplicated().any():
        raise SystemExit(f"FY{year}: facilities with conflicting attributes:\n"
                         f"{fac[fac['facility_id'].duplicated(keep=False)].to_string()}")
    prod = res[PROD_COLS].drop_duplicates()
    if prod.duplicated(["facility_id", "product_id"]).any():
        raise SystemExit(f"FY{year}: products with more than one use_type")
    fac["updated_fy"] = year
    res["fiscal_year"] = year

    equip = equip[equip["src_row"].isin(res["src_row"])]  # equipment of the kept row only
    equip = equip.drop_duplicates(["reservation_id", "equipment_type"])
    position = pd.Series(np.arange(len(res)), index=res["reservation_id"])
    equip = equip.assign(pos=equip["reservation_id"].map(position)).dropna(subset=["pos"])
    equip = equip.sort_values("pos", kind="stable", ignore_index=True)  # same order as the replay

    counts = pd.DataFrame({"source_rows": source, "dropped_rows": dropped,
                           "rejected_rows": rej["park_code"].value_counts(),
                           "duplicate_rows": duplicates}).reindex(list(PARKS)).fillna(0).astype(int)
    counts["expected"] = res["park_code"].value_counts().reindex(list(PARKS)).fillna(0).astype(int)
    # Every source row is accounted for: loaded, dropped, rejected or a repeated id
    accounted = counts["source_rows"] - counts["dropped_rows"] - counts["rejected_rows"] - counts["duplicate_rows"]
    if not counts["expected"].equals(accounted):
        raise SystemExit(f"FY{year}: row accounting does not add up:\n{counts}")
    print(f"FY{year}: read {rows_read:,} rows from {member.filename} in {time.time() - t0:.0f} s; "
          f"kept {len(res):,} reservations, {res['order_number'].nunique():,} orders, {len(fac)} facilities, "
          f"{len(prod):,} products, {len(equip):,} equipment rows")
    print(f"  rejected {len(rej)} rows {rej['reject_reason'].value_counts().to_dict()} -> "
          f"{rejects_path.name if len(rej) else 'no rejects file'}; repeated reservation ids {int(dup.sum())}; "
          f"orders with conflicting time/location {conflicts}; customer ZIPs over 10 characters set to NULL "
          f"{int(res['zip_too_long'].sum())}; rows named '{YELL_PROGRAM}' {program_rows} "
          f"(facilities {sorted(program_facilities)})")
    return {"year": year, "res": res, "fac": fac, "prod": prod, "equip": equip, "counts": counts,
            "file": {"fiscal_year": year, "zip_name": zip_path.name, "csv_name": member.filename,
                     "csv_bytes": member.file_size, "rows_read": rows_read,
                     "completed_at": None}}  # reset at the start of every load, set when the replay finishes


def records(frame: pd.DataFrame) -> list[tuple]:
    """Rows as tuples of plain Python values that pymysql can send; NaN, NaT and NA become NULL."""
    out = frame.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            fmt = "%Y-%m-%d %H:%M:%S.%f" if col == "ordered_at" else "%Y-%m-%d"
            out[col] = out[col].dt.strftime(fmt)
    out = out.astype(object)
    return list(out.where(out.notna(), None).itertuples(index=False, name=None))


def upsert(conn, table: str, cols: list[str], rows: list[tuple], keys: list[str]) -> None:
    """One multi-row INSERT ... ON DUPLICATE KEY UPDATE: new keys are added, existing keys take the new values."""
    if not rows:
        return
    one = "(" + ", ".join(["%s"] * len(cols)) + ")"
    updates = ", ".join(f"{c} = new.{c}" for c in cols if c not in keys)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES {', '.join([one] * len(rows))} "
           f"AS new ON DUPLICATE KEY UPDATE {updates}")
    conn.exec_driver_sql(sql, tuple(v for row in rows for v in row))


def apply_schema(engine) -> None:
    statements = [s for s in SCHEMA_SQL.read_text(encoding="utf-8").split(";")
                  if any(line.strip() and not line.strip().startswith("--") for line in s.splitlines())]
    with engine.begin() as conn:
        for stmt in statements:
            conn.exec_driver_sql(stmt.replace("%", "%%"))  # pymysql formats the text with %, so a % must be doubled
        in_db = dict(conn.exec_driver_sql("SELECT park_code, park_name FROM park").fetchall())
    if in_db != {code: info["name"] for code, info in PARKS.items()}:
        raise SystemExit(f"park table does not match parks.py: {in_db}")
    print(f"Schema applied ({len(statements)} statements); park table matches parks.py")


def table_counts(conn) -> dict:
    tables = ["park", "source_file", "source_file_park", "facility", "product", "booking_order",
              "reservation", "reservation_equipment"]
    return {t: conn.exec_driver_sql(f"SELECT COUNT(*) FROM {t}").scalar() for t in tables}


def load_year(engine, data: dict, batch: int) -> None:
    year, res, equip = data["year"], data["res"], data["equip"]
    t0 = time.time()
    with engine.connect() as conn:
        # Reference data first, in one transaction: the file record, facilities, products, source counts
        with conn.begin():
            upsert(conn, "source_file", list(data["file"]), [tuple(data["file"].values())], ["fiscal_year"])
            stored = dict(conn.exec_driver_sql("SELECT facility_id, park_code FROM facility").fetchall())
            moved = {f: p for f, p in zip(data["fac"]["facility_id"], data["fac"]["park_code"])
                     if f in stored and stored[f] != p}
            if moved:
                raise SystemExit(f"FY{year}: facilities assigned to a different park than before: {moved}")
            # A facility's attributes come from the newest file: an older file never overwrites them
            newer = {f for (f,) in conn.exec_driver_sql("SELECT facility_id FROM facility WHERE updated_fy > %s",
                                                        (year,)).fetchall()}
            fac = data["fac"][~data["fac"]["facility_id"].isin(newer)]
            upsert(conn, "facility", FAC_COLS, records(fac[FAC_COLS]), ["facility_id"])
            upsert(conn, "product", PROD_COLS, records(data["prod"]), ["facility_id", "product_id"])
            counts = data["counts"].drop(columns="expected").reset_index(names="park_code")
            counts.insert(0, "fiscal_year", year)
            upsert(conn, "source_file_park", list(counts.columns), records(counts), ["fiscal_year", "park_code"])

        # Cut the replay into transactions of about `batch` reservations, at order boundaries
        n = len(res)
        order_starts = np.flatnonzero(res["order_number"].ne(res["order_number"].shift()).to_numpy())
        cuts = [0]
        while cuts[-1] + batch < n:
            i = np.searchsorted(order_starts, cuts[-1] + batch)
            if i == len(order_starts):
                break
            cuts.append(int(order_starts[i]))
        cuts.append(n)
        eq_pos = equip["pos"].to_numpy()

        last = time.time()
        for b, (lo, hi) in enumerate(zip(cuts[:-1], cuts[1:])):
            part = res.iloc[lo:hi]
            e_lo, e_hi = np.searchsorted(eq_pos, lo), np.searchsorted(eq_pos, hi)
            with conn.begin():  # one transaction: the orders, their reservations, their equipment
                upsert(conn, "booking_order", ORDER_COLS,
                       records(part.drop_duplicates("order_number")[ORDER_COLS]), ["order_number"])
                upsert(conn, "reservation", RES_COLS, records(part[RES_COLS]), ["reservation_id"])
                upsert(conn, "reservation_equipment", EQUIP_COLS, records(equip.iloc[e_lo:e_hi][EQUIP_COLS]),
                       ["reservation_id", "equipment_type"])
            if time.time() - last > 30 or hi == n:
                last = time.time()
                print(f"  FY{year}: {hi:,}/{n:,} reservations replayed in {b + 1}/{len(cuts) - 1} transactions "
                      f"({hi / (last - t0):,.0f}/s), up to order time {part['ordered_at'].iloc[-1]}")
        with conn.begin():  # the file counts as loaded only now
            conn.exec_driver_sql("UPDATE source_file SET completed_at = CURRENT_TIMESTAMP WHERE fiscal_year = %s",
                                 (year,))


def reconcile(engine, data: dict) -> None:
    """Source rows kept per park versus rows now in MySQL for this fiscal year."""
    with engine.connect() as conn:
        # Count per facility first (answered by the index on fiscal_year, facility_id), then per park
        in_db = {park: int(n) for park, n in conn.exec_driver_sql(
            "SELECT f.park_code, SUM(c.n) FROM (SELECT facility_id, COUNT(*) AS n FROM reservation "
            "WHERE fiscal_year = %s GROUP BY facility_id) c JOIN facility f ON f.facility_id = c.facility_id "
            "GROUP BY f.park_code", (data["year"],)).fetchall()}
    table = data["counts"].assign(in_mysql=[in_db.get(code, 0) for code in PARKS])
    table["diff"] = table["in_mysql"] - table["expected"]
    table.loc["total"] = table.sum()
    print(f"FY{data['year']} reconciliation (expected = source - dropped - rejected - duplicate):")
    print(table.to_string())


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fiscal-years", type=int, nargs="+", required=True, help="e.g. 2017 2024")
    ap.add_argument("--batch", type=int, default=2000, help="reservations per transaction")
    ap.add_argument("--zip-dir", default=str(ZIP_DIR), help="folder with reservations{YEAR}.zip")
    args = ap.parse_args()

    load_dotenv(ROOT / ".env")
    if not os.environ.get("MYSQL_URL"):
        raise SystemExit(f"Set MYSQL_URL in {ROOT / '.env'}")
    engine = create_engine(os.environ["MYSQL_URL"], pool_pre_ping=True)
    t_all = time.time()
    apply_schema(engine)
    with engine.connect() as conn:
        before = table_counts(conn)
        run_start = conn.exec_driver_sql("SELECT NOW(6)").scalar()

    for year in sorted(set(args.fiscal_years)):  # oldest first, so the newest facility attributes win
        zip_path = Path(args.zip_dir) / f"reservations{year}.zip"
        if not zip_path.exists():
            raise SystemExit(f"{zip_path} not found: run src/ingest/ridb_download.py --fiscal-years {year}")
        data = read_year(year, zip_path)
        t0 = time.time()
        load_year(engine, data, args.batch)
        print(f"FY{year}: replay took {time.time() - t0:.0f} s")
        reconcile(engine, data)
        del data

    with engine.connect() as conn:
        after = table_counts(conn)
        changed = {t: conn.exec_driver_sql(f"SELECT COUNT(*) FROM {t} WHERE updated_at >= %s", (run_start,)).scalar()
                   for t in ["booking_order", "reservation"]}
    print("Rows per table (before -> after):", {t: f"{before[t]:,} -> {after[t]:,}" for t in after})
    print("Rows inserted or changed by this run:", changed)
    print(f"Total time {time.time() - t_all:.0f} s")


if __name__ == "__main__":
    main()
