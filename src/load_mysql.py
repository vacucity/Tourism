"""Replay Recreation.gov reservations for the five parks into the MySQL OLTP schema (mysql/01_schema.sql).

Each fiscal-year zip is streamed in chunks and only NPS rows for the five parks are kept. The two file
layouts are told apart by their columns (entitytype: FY2015-FY2018, inventorytype: FY2019+) and normalized
to one, then each file's bookings are replayed in order-time order (one file after another, oldest fiscal
year first): one transaction per ~2,000 reservations, never splitting an order. Every write is an upsert on
the table's primary key (reservation: on its UNIQUE reservation_id), so a rerun adds nothing. Rows that miss
a required value, hold text that does not parse, or would break a CHECK go to a rejects CSV instead.

A reservation is stored once, from the first file that lists it. Load the fiscal years oldest first into
an empty database: the loader stops if asked to load a fiscal year older than one already loaded (reruns
of loaded years are allowed in any order).

Needs MYSQL_URL in .env at the repository root, for example
  MYSQL_URL=mysql+pymysql://nps_app:PASSWORD@localhost:3306/nps_tourism
Use letters and digits in the password, or write @ as %40 and % as %25: an unescaped @ moves the rest of
the password into the host name, which connection errors print.
Run from any folder, after src/ingest/ridb_download.py:
  uv run python src/load_mysql.py --fiscal-years 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025
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

PARK_OF = {info["name"]: code for code, info in PARKS.items()}
# FY2015-FY2018 files list this Yellowstone permit program under its own name; FY2019+ files list the
# same facility (250849) under "Yellowstone National Park"
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
# Columns every file has, then the columns of each layout
COMMON = {"historicalreservationid", "ordernumber", "agency", "park", "sitetype", "usetype", "productid",
          "facilityid", "facilityzip", "facilitystate", "facilitylatitude", "facilitylongitude", "customerzip",
          "tax", "usefee", "tranfee", "attrfee", "totalbeforetax", "totalpaid", "startdate", "enddate",
          "orderdate", "numberofpeople"}
OLD_LAYOUT = {"entitytype", "regiondescription"} | set(EQUIPMENT_COLUMNS)
NEW_LAYOUT = {"inventorytype", "parentlocation", "discount", "equipmentdescription", "equipmentlength"}
WANTED = COMMON | OLD_LAYOUT | NEW_LAYOUT | {"customerstate", "customercountry"}  # state, country: FY2015-FY2020 only

# Order times. FY2015-FY2017 give a date only (stored at 00:00). FY2018 gives Eastern local time: on the days
# campgrounds release, orders peak at 10:00 in FY2018 and at 14:00-15:00 UTC in FY2019+, the Recreation.gov
# release time of 10:00 Eastern. FY2019-FY2021 times have no zone marker but are UTC: the same order in the
# FY2021 and FY2022 (Z) files differs by under 1 ms. FY2022+ times end in Z.
OLD_SYSTEM_ZONE = "America/New_York"
# Old-system order numbers ('2-36409369': one digit, a dash, 6 to 8 digits). FY2019 and FY2020 files hold
# 4,458 rows with such numbers, 4,454 of them bookings of the FY2015-FY2018 files under new reservation and
# product ids.
OLD_ORDER_FORM = r"^\d-\d{6,8}$"
MAX_LENGTH_FT = 999_999.99  # largest value of reservation_equipment.length_ft DECIMAL(8,2)

MONEY = ["tax", "use_fee", "tran_fee", "attr_fee", "total_before_tax", "discount", "total_paid"]
ORDER_COLS = ["order_number", "ordered_at", "customer_zip", "customer_state", "customer_country"]
RES_COLS = ["reservation_id", "order_number", "order_line", "facility_id", "product_id", "fiscal_year",
            "inventory_type", "site_type", "start_date", "end_date", "number_of_people"] + MONEY
EQUIP_COLS = ["reservation_id", "equipment_type", "quantity", "length_ft"]
FAC_COLS = ["facility_id", "park_code", "facility_name", "facility_zip", "state_code", "latitude",
            "longitude", "updated_fy"]
FAC_ATTRS = ["facility_name", "facility_zip", "state_code", "latitude", "longitude"]
PROD_COLS = ["facility_id", "product_id", "use_type"]
COUNT_COLS = ["source_rows", "dropped_rows", "rejected_rows", "duplicate_rows", "other_file_rows"]


def blank_to_na(s: pd.Series) -> pd.Series:
    return s.mask(s.str.strip() == "")


def num(s: pd.Series) -> pd.Series:
    """Text -> number; text that is not a number becomes NaN (read_year rejects the row)."""
    return pd.to_numeric(blank_to_na(s), errors="coerce")


def utc(s: pd.Series) -> pd.Series:
    """'2016-10-02', '2018-10-05 14:25:51.283' or '2024-08-04T02:13:37.018512Z' -> naive timestamps (UTC when
    the source gives a zone, the time as written otherwise). Text that is not a date becomes NaT."""
    return pd.to_datetime(blank_to_na(s), utc=True, format="ISO8601", errors="coerce").dt.tz_convert(None)


def eastern_to_utc(s: pd.Series, t: pd.Series) -> pd.Series:
    """Old-layout order times: values with a time of day (FY2018) are Eastern local time and become UTC.
    Date-only values (FY2015-FY2017) stay at 00:00. In the repeated hour of 2017-11-05 a time is read as
    daylight time (EDT)."""
    has_time = (s.str.len() > 10) & t.notna()
    if not has_time.any():
        return t
    local = t[has_time].dt.tz_localize(OLD_SYSTEM_ZONE, ambiguous=np.ones(int(has_time.sum()), dtype=bool),
                                       nonexistent="shift_forward")
    t = t.copy()
    t[has_time] = local.dt.tz_convert(None)
    return t


def state_code(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    if len(value) == 2:
        return value.upper()
    if value not in STATE_CODES:
        raise SystemExit(f"Unknown facility state {value!r}: add it to STATE_CODES")
    return STATE_CODES[value]


def detect_layout(columns: list[str], year: int) -> dict:
    """The file layout from the columns it has: FY2015-FY2018 files have entitytype and hold the park name
    in RegionDescription, FY2019+ files have inventorytype and hold it in ParentLocation."""
    cols = set(columns)
    if "inventorytype" in cols:
        layout = {"new": True, "inventory": "inventorytype", "location": "parentlocation", "need": NEW_LAYOUT}
    elif "entitytype" in cols:
        layout = {"new": False, "inventory": "entitytype", "location": "regiondescription", "need": OLD_LAYOUT}
    else:
        raise SystemExit(f"FY{year}: neither entitytype nor inventorytype in the header: {columns}")
    missing = sorted((COMMON | layout["need"]) - cols)
    if missing:
        raise SystemExit(f"FY{year}: columns missing for the {'new' if layout['new'] else 'old'} layout: {missing}")
    return layout


def normalize(raw: pd.DataFrame, layout: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """One chunk of kept rows in either layout -> (reservations, facilities, equipment) in one layout.
    reservations also carries `unparseable`: the row holds text that does not parse as a number or date."""
    new_layout = layout["new"]
    r = pd.DataFrame(index=raw.index)
    r["reservation_id"] = raw["historicalreservationid"].str.strip()
    r["src_row"] = raw.index  # row number in the CSV (read_csv chunks number rows across the whole file)
    unparseable = pd.Series(False, index=raw.index)

    def parsed(src: str, value: pd.Series) -> pd.Series:
        nonlocal unparseable
        unparseable |= (raw[src].str.strip() != "") & value.isna()
        return value

    if new_layout:
        # "0610465289-3" is line 3 of order 0610465289: the lines share order time and customer ZIP. An
        # old-system number such as "2-36409369" is a whole order number: it keeps order_line NULL.
        parts = raw["ordernumber"].str.extract(r"^(\d{2,})-(\d{1,4})$")
        r["order_number"] = parts[0].where(parts[0].notna(), raw["ordernumber"])
        r["order_line"] = pd.to_numeric(parts[1]).astype("Int32")
    else:
        r["order_number"] = raw["ordernumber"]
        r["order_line"] = pd.Series(pd.NA, index=raw.index, dtype="Int32")
    r["park_code"] = raw["park_code"]
    r["facility_id"] = raw["facilityid"]
    r["product_id"] = raw["productid"]
    r["use_type"] = raw["usetype"].mask(raw["usetype"].isin(["", "None"]))  # product attribute
    r["inventory_type"] = raw[layout["inventory"]]
    r["site_type"] = blank_to_na(raw["sitetype"])
    # Dates are kept as the UTC date: the few FY2024 rows with a time (07:00-20:00 UTC) are local
    # midnight or noon, so the UTC date is the local date. FY2018-FY2021 dates have no zone: the date as written.
    r["start_date"] = parsed("startdate", utc(raw["startdate"])).dt.normalize()
    r["end_date"] = parsed("enddate", utc(raw["enddate"])).dt.normalize()
    ordered = parsed("orderdate", utc(raw["orderdate"]))
    r["ordered_at"] = ordered if new_layout else eastern_to_utc(raw["orderdate"], ordered)
    r["number_of_people"] = parsed("numberofpeople", num(raw["numberofpeople"])).astype("Int32")
    for col, src in [("tax", "tax"), ("use_fee", "usefee"), ("tran_fee", "tranfee"), ("attr_fee", "attrfee"),
                     ("total_before_tax", "totalbeforetax"), ("total_paid", "totalpaid")]:
        r[col] = parsed(src, num(raw[src])).round(2)
    # FY2019+ files store the discount as a negative number; it is kept as a positive amount
    # (0 - x rather than -x, so a zero discount is 0.0 and not -0.0)
    r["discount"] = (0 - parsed("discount", num(raw["discount"]))).round(2) if new_layout else np.nan
    zip_ = raw["customerzip"].str.strip()
    r["customer_zip"] = zip_.mask((zip_ == "") | (zip_.str.len() > 10))  # longer ones are free text
    r["zip_too_long"] = zip_.str.len() > 10
    if "customerstate" in raw.columns:
        r["customer_state"] = raw["customerstate"].mask(raw["customerstate"].isin(["", "UNKN"]))
        r["customer_country"] = raw["customercountry"].mask(raw["customercountry"].isin(["", "UNKN"]))
    else:  # FY2021+ files have no customer state or country
        r["customer_state"] = r["customer_country"] = None

    lat = num(raw["facilitylatitude"]).round(6)
    lon = num(raw["facilitylongitude"]).round(6)
    # 300015, 300016 (FY2020-FY2021) and 4675323 (FY2022-FY2025) give a negative latitude and a positive
    # longitude; every five-park facility lies north of the equator and west of Greenwich
    coords_ok = (lat > 0) & (lon < 0)
    fac = pd.DataFrame({
        "facility_id": raw["facilityid"],
        "park_code": raw["park_code"],
        "facility_name": raw["park"].str.strip(),  # the "park" column holds the facility name
        "facility_zip": blank_to_na(raw["facilityzip"]),
        "state_code": raw["facilitystate"].map({v: state_code(v) for v in raw["facilitystate"].unique()}),
        "latitude": lat.where(coords_ok),
        "longitude": lon.where(coords_ok),
        "ordered_at": r["ordered_at"], "src_row": r["src_row"],  # which attribute set is the newest
    })

    if new_layout:
        desc = raw["equipmentdescription"].str.strip().str.rstrip("`")
        length = parsed("equipmentlength", num(raw["equipmentlength"]))
        has = desc != ""
        length_ft = length[has].where(length[has] > 0).round(2)
        equip = pd.DataFrame({"reservation_id": r.loc[has, "reservation_id"],
                              "src_row": r.loc[has, "src_row"], "equipment_type": desc[has],
                              "quantity": pd.Series(pd.NA, index=desc[has].index, dtype="Int32"),
                              # a length that does not fit DECIMAL(8,2) (12 FY2020-FY2023 rows) is not a length
                              "length_ft": length_ft.mask(length_ft > MAX_LENGTH_FT),
                              "length_too_big": length_ft > MAX_LENGTH_FT})
    else:
        pieces = []
        for col, label in EQUIPMENT_COLUMNS.items():
            q = parsed(col, num(raw[col]))
            has = q.notna() & (q > 0)
            pieces.append(pd.DataFrame({"reservation_id": r.loc[has, "reservation_id"],
                                        "src_row": r.loc[has, "src_row"], "equipment_type": label,
                                        "quantity": q[has].astype("Int32"), "length_ft": np.nan,
                                        "length_too_big": False}))
        equip = pd.concat(pieces)
    r["unparseable"] = unparseable
    return r, fac, equip


def newest_attributes(fac: pd.DataFrame) -> pd.DataFrame:
    """One row per (facility, attribute set): the latest order time and CSV row that shows it."""
    return (fac.sort_values(["ordered_at", "src_row"], kind="stable", na_position="first")
               .drop_duplicates(["facility_id", "park_code"] + FAC_ATTRS, keep="last"))


def csv_member(zf: zipfile.ZipFile) -> zipfile.ZipInfo:
    """The single CSV of a RIDB zip (FY2018 also holds a README.TXT)."""
    members = [m for m in zf.infolist() if m.filename.lower().endswith(".csv")]
    if len(members) != 1:
        raise SystemExit(f"{zf.filename}: expected one CSV, found {[m.filename for m in zf.infolist()]}")
    return members[0]


def read_year(year: int, zip_path: Path) -> dict:
    """Stream one zip, keep the five parks, normalize, and count every row that is not loaded."""
    t0 = time.time()
    with zipfile.ZipFile(zip_path) as zf:
        member = csv_member(zf)
        header = pd.read_csv(zf.open(member), nrows=0, encoding_errors="replace").columns
        header = [c.strip().lower() for c in header]
        layout = detect_layout(header, year)
        chunks = pd.read_csv(zf.open(member), dtype=str, keep_default_na=False, chunksize=50_000,
                             usecols=lambda c: c.strip().lower() in WANTED, encoding_errors="replace")
        rows_read = program_rows = 0
        program_facilities = set()
        res_parts, fac_parts, eq_parts, rejects = [], [], [], []
        source, dropped = pd.Series(0, index=list(PARKS)), pd.Series(0, index=list(PARKS))
        for chunk in chunks:
            chunk.columns = chunk.columns.str.strip().str.lower()
            rows_read += len(chunk)
            location = chunk[layout["location"]]
            raw = chunk.assign(park_code=location.map(PARK_OF)).dropna(subset=["park_code"])
            is_program = location[raw.index] == YELL_PROGRAM
            program_rows += int(is_program.sum())
            program_facilities.update(raw.loc[is_program, "facilityid"])
            source = source.add(raw["park_code"].value_counts(), fill_value=0)
            # Rows of any agency other than NPS, and DONATION rows, are not park reservations
            inventory = raw[layout["inventory"]]
            drop = (raw["agency"] != "NPS") | (inventory == "DONATION")
            dropped = dropped.add(raw.loc[drop, "park_code"].value_counts(), fill_value=0)
            raw = raw[~drop]
            if raw.empty:
                continue
            r, fac, equip = normalize(raw, layout)
            money = r[MONEY]
            bad = pd.Series("", index=r.index)
            bad = bad.mask(r["ordered_at"].isna(), "missing order date")
            bad = bad.mask((r["facility_id"] == "") | (r["product_id"] == ""), "missing facility or product id")
            bad = bad.mask(r["inventory_type"] == "", "missing inventory type")
            bad = bad.mask(r[["tax", "total_before_tax", "total_paid"]].isna().any(axis=1), "missing amount")
            # total_before_tax is not checked: 5 FY2024 backcountry permits have it negative and total_paid positive
            bad = bad.mask((money.drop(columns="total_before_tax") < 0).any(axis=1), "negative amount")
            bad = bad.mask(r["end_date"] < r["start_date"], "end date before start date")
            # FY2021: 27 rows give the order time 0001-01-01 (DATETIME starts at year 1000)
            bad = bad.mask(r["ordered_at"] < pd.Timestamp("2000-01-01"), "order date before 2000")
            # FY2022 and FY2023: 4 rows whose customer ZIP holds a stray quote, which shifts later columns
            bad = bad.mask(r["unparseable"], "unparseable value")
            if (bad != "").any():
                rejects.append(raw.loc[bad != ""].assign(reject_reason=bad[bad != ""]))
            res_parts.append(r[bad == ""].drop(columns="unparseable"))
            fac_parts.append(newest_attributes(fac[bad == ""]))
            eq_parts.append(equip)
    res = pd.concat(res_parts, ignore_index=True)
    fac = newest_attributes(pd.concat(fac_parts))
    equip = pd.concat(eq_parts, ignore_index=True)
    rej = pd.concat(rejects) if rejects else pd.DataFrame(columns=["park_code", "reject_reason"])
    rejects_path = zip_path.parent / f"rejects_FY{year}.csv"
    if len(rej):
        rej.to_csv(rejects_path, index=False)
    elif rejects_path.exists():
        rejects_path.unlink()  # from an earlier run of different code

    # Replay order: by order time, keeping each order's lines together. A repeated reservation id keeps the
    # first row in this order: the earliest order time (FY2018: the booking before a site change), then the
    # lowest product id as text (FY2015: the Entrance row of an Entrance + LOTTERY pair, FY2017: 70 ids listed
    # under two products, FY2025: 59 ids), then the first row in the file (FY2025: the other 622 of the 681
    # ids listed with two start dates).
    replay_order = ["ordered_at", "order_number", "order_line", "reservation_id", "product_id"]
    res = res.sort_values(replay_order, kind="stable", ignore_index=True)
    # An id '<id>__<its facility id>' whose '<id>' is also in the file repeats that reservation (FY2019: 29
    # Yellowstone snowmobile permits on the same order line). QUEUE_LOTTERY ids carry the same suffix alone.
    base = res["reservation_id"].str.split("__", n=1).str[0]
    repeats_base = (res["reservation_id"] == base + "__" + res["facility_id"]) & base.isin(res["reservation_id"])
    dup = res["reservation_id"].where(~repeats_base, base).duplicated()
    duplicates = res.loc[dup, "park_code"].value_counts()
    res = res[~dup].reset_index(drop=True)

    # An order's time and customer location must be the same on all its lines; if not, the first wins
    conflicts = int(res.drop_duplicates(ORDER_COLS).duplicated("order_number").sum())
    if conflicts:
        res[ORDER_COLS[1:]] = res.groupby("order_number")[ORDER_COLS[1:]].transform("first")
        res = res.sort_values(replay_order, kind="stable", ignore_index=True)

    # facility_id -> park must hold within a file. Other facility attributes take the values of the row with
    # the latest order time (FY2025 changes the address or coordinates of 19 facilities during the year).
    fac = fac[fac["facility_id"].isin(res["facility_id"])]
    parks_per_facility = fac.groupby("facility_id")["park_code"].nunique()
    if (parks_per_facility > 1).any():
        raise SystemExit(f"FY{year}: facilities listed under two parks:\n"
                         f"{fac[fac['facility_id'].isin(parks_per_facility[parks_per_facility > 1].index)]}")
    changed_facilities = sorted(fac.loc[fac["facility_id"].duplicated(), "facility_id"].unique())
    fac = fac.drop_duplicates("facility_id", keep="last").drop(columns=["ordered_at", "src_row"])
    fac["updated_fy"] = year
    res["fiscal_year"] = year

    equip = equip[equip["src_row"].isin(res["src_row"])]  # equipment of the kept row only
    equip = equip.drop_duplicates(["reservation_id", "equipment_type"])

    counts = pd.DataFrame({"source_rows": source, "dropped_rows": dropped,
                           "rejected_rows": rej["park_code"].value_counts(),
                           "duplicate_rows": duplicates}).reindex(list(PARKS)).fillna(0).astype(int)
    counts["other_file_rows"] = 0  # set by cross_file_rules
    print(f"FY{year}: read {rows_read:,} rows from {member.filename} in {time.time() - t0:.0f} s "
          f"({'new' if layout['new'] else 'old'} layout, {len(header)} columns); "
          f"kept {len(res):,} reservations, {res['order_number'].nunique():,} orders, {len(fac)} facilities, "
          f"{len(equip):,} equipment rows")
    print(f"  rejected {len(rej)} rows {rej['reject_reason'].value_counts().to_dict()} -> "
          f"{rejects_path.name if len(rej) else 'no rejects file'}; repeated reservation ids {int(dup.sum())}; "
          f"orders with conflicting time/location {conflicts}; customer ZIPs over 10 characters set to NULL "
          f"{int(res['zip_too_long'].sum())}; equipment lengths over {MAX_LENGTH_FT:,} set to NULL "
          f"{int(equip['length_too_big'].sum())}; facilities whose attributes change within the file "
          f"{changed_facilities}; rows named '{YELL_PROGRAM}' {program_rows} "
          f"(facilities {sorted(program_facilities)})")
    return {"year": year, "new_layout": layout["new"], "res": res, "fac": fac, "equip": equip, "counts": counts,
            "file": {"fiscal_year": year, "zip_name": zip_path.name, "csv_name": member.filename,
                     "csv_bytes": member.file_size, "rows_read": rows_read,
                     "completed_at": None}}  # reset at the start of every load, set when the replay finishes


def lookup(conn, sql: str, keys, params: tuple = (), batch: int = 5000) -> list[tuple]:
    """Run `sql` (with one IN ({keys}) list) for the keys in batches; return all rows."""
    keys = list(keys)
    out = []
    for i in range(0, len(keys), batch):
        part = keys[i:i + batch]
        out += conn.exec_driver_sql(sql.format(keys=", ".join(["%s"] * len(part))), tuple(part) + params).fetchall()
    return out


def cross_file_rules(engine, data: dict) -> None:
    """A reservation is stored once, from the first file that lists it (fiscal years load oldest first):
    - a row whose reservation id is already stored from another fiscal-year file is not loaded (FY2018 lists
      435 FY2016/FY2017 bookings again with a later order time, FY2020-FY2022 4 bookings moved to the next year);
    - an FY2019+ row with an old-system order number that a reservation from another file already uses is
      not loaded (4,433 FY2019 and 13 FY2020 rows repeat FY2015-FY2018 bookings under new reservation ids);
    - an order that already holds reservations from an earlier file keeps that file's order time and
      customer values; this file's lines of the order take them over before the replay is sorted.
    Skipped rows are counted in source_file_park.other_file_rows."""
    year, res = data["year"], data["res"]
    t0 = time.time()
    with engine.connect() as conn:
        stored = dict(lookup(conn, "SELECT reservation_id, fiscal_year FROM reservation "
                                   "WHERE reservation_id IN ({keys}) AND fiscal_year <> %s",
                             res["reservation_id"], (year,)))
        newer = {k: v for k, v in stored.items() if v > year}
        if newer:
            raise SystemExit(f"FY{year}: {len(newer)} reservation ids are stored from a newer fiscal year "
                             f"({sorted(set(newer.values()))}): load fiscal years oldest first into an empty database")
        # old-system order numbers are the norm in the old layout: the rule is for new-layout files only
        old_form = (res.loc[res["order_number"].str.match(OLD_ORDER_FORM), "order_number"].unique()
                    if data["new_layout"] else [])
        migrated = {o for (o,) in lookup(conn, "SELECT DISTINCT order_number FROM reservation "
                                               "WHERE order_number IN ({keys}) AND fiscal_year <> %s",
                                         old_form, (year,))}
        skip = res["reservation_id"].isin(stored.keys()) | res["order_number"].isin(migrated)
        data["counts"]["other_file_rows"] = (res.loc[skip, "park_code"].value_counts()
                                             .reindex(list(PARKS)).fillna(0).astype(int))
        res = res[~skip].reset_index(drop=True)
        earlier = lookup(conn, "SELECT o.order_number, o.ordered_at, o.customer_zip, o.customer_state, "
                               "o.customer_country FROM booking_order o WHERE o.order_number IN ({keys}) "
                               "AND EXISTS (SELECT 1 FROM reservation r WHERE r.order_number = o.order_number "
                               "AND r.fiscal_year < %s)", res["order_number"].unique(), (year,))
    if earlier:
        old = pd.DataFrame(earlier, columns=ORDER_COLS).set_index("order_number")
        hit = res["order_number"].isin(old.index)
        for col in ORDER_COLS[1:]:
            values = res.loc[hit, "order_number"].map(old[col])
            if col == "ordered_at":
                values = pd.to_datetime(values)
            res.loc[hit, col] = values.where(values.notna(), None) if col != "ordered_at" else values
        res = res.sort_values(["ordered_at", "order_number", "order_line", "reservation_id", "product_id"],
                              kind="stable", ignore_index=True)
    data["res"] = res
    print(f"  cross-file: {int(skip.sum())} rows already stored from another file "
          f"({int(res.shape[0])} left; ids {len(stored)}, old-system orders {len(migrated)}); "
          f"{len(earlier)} orders keep the values of an earlier file; {time.time() - t0:.0f} s")


def finish(data: dict) -> None:
    """Reference rows and counts of the reservations that will be replayed; equipment in replay order."""
    res, equip, counts, year = data["res"], data["equip"], data["counts"], data["year"]
    data["fac"] = data["fac"][data["fac"]["facility_id"].isin(res["facility_id"])]
    prod = res[PROD_COLS].drop_duplicates()
    if prod.duplicated(["facility_id", "product_id"]).any():
        raise SystemExit(f"FY{year}: products with more than one use_type")
    data["prod"] = prod
    position = pd.Series(np.arange(len(res)), index=res["reservation_id"])
    equip = equip.assign(pos=equip["reservation_id"].map(position)).dropna(subset=["pos"])
    data["equip"] = equip.sort_values("pos", kind="stable", ignore_index=True)  # same order as the replay
    counts["expected"] = res["park_code"].value_counts().reindex(list(PARKS)).fillna(0).astype(int)
    # Every source row is accounted for: loaded, dropped, rejected, a repeated id, or stored from another file
    accounted = counts["source_rows"] - counts[COUNT_COLS[1:]].sum(axis=1)
    if not counts["expected"].equals(accounted):
        raise SystemExit(f"FY{year}: row accounting does not add up:\n{counts}")
    print(f"  replaying {len(res):,} reservations, {res['order_number'].nunique():,} orders, "
          f"{len(data['fac'])} facilities, {len(prod):,} products, {len(data['equip']):,} equipment rows")


def records(frame: pd.DataFrame) -> list[tuple]:
    """Rows as tuples of plain Python values that pymysql can send; NaN, NaT and NA become NULL."""
    out = frame.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            fmt = "%Y-%m-%d %H:%M:%S.%f" if col == "ordered_at" else "%Y-%m-%d"
            out[col] = out[col].dt.strftime(fmt)
    out = out.astype(object)
    return list(out.where(out.notna(), None).itertuples(index=False, name=None))


def upsert(conn, table: str, cols: list[str], rows: list[tuple], keys: list[str], keep: tuple = ()) -> None:
    """One multi-row INSERT ... ON DUPLICATE KEY UPDATE: new keys are added, existing keys take the new values
    (a column named in `keep` keeps its stored value where the new value is NULL)."""
    if not rows:
        return
    one = "(" + ", ".join(["%s"] * len(cols)) + ")"
    updates = ", ".join(f"{c} = COALESCE(new.{c}, {table}.{c})" if c in keep else f"{c} = new.{c}"
                        for c in cols if c not in keys)
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
            # A product's use type is the same in every file that gives one. FY2018 gives none for 6 products
            # that FY2015-FY2017 list as Day (306809, 375920-375923, 455771): an empty value keeps the stored one
            upsert(conn, "product", PROD_COLS, records(data["prod"]), ["facility_id", "product_id"],
                   keep=("use_type",))
            counts = data["counts"][COUNT_COLS].reset_index(names="park_code")
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
            if lo == hi:  # a file whose rows are all stored from other files
                continue
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
    print(f"FY{data['year']} reconciliation (expected = source - dropped - rejected - duplicate - other file):")
    print(table.to_string())


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fiscal-years", type=int, nargs="+", required=True, help="e.g. 2015 2016 ... 2025")
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
        loaded = {y for (y,) in conn.exec_driver_sql("SELECT fiscal_year FROM source_file").fetchall()}

    years = sorted(set(args.fiscal_years))  # oldest first, so the newest facility attributes win
    for year in years:
        zip_path = Path(args.zip_dir) / f"reservations{year}.zip"
        if not zip_path.exists():
            raise SystemExit(f"{zip_path} not found: run src/ingest/ridb_download.py --fiscal-years {year}")
    first_new = [y for y in years if y not in loaded]
    if first_new and loaded and min(first_new) < max(loaded):
        raise SystemExit(f"FY{min(first_new)} is not loaded yet but FY{max(loaded)} is: a reservation is stored "
                         f"from the first file that lists it, so fiscal years load oldest first. "
                         f"Load into an empty database, oldest year first.")

    for year in years:
        data = read_year(year, Path(args.zip_dir) / f"reservations{year}.zip")
        cross_file_rules(engine, data)
        finish(data)
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
