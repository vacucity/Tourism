"""Download Recreation.gov historical reservation files (one zip per fiscal year) from RIDB.

A fiscal year runs October to September. The FY2015-FY2018 files hold the reservations ordered in the fiscal
year (FY2017: Oct 2016 .. Sep 2017), the FY2019+ files those that start in it (FY2024: Oct 2023 .. Sep 2024).
Each zip holds one CSV for every agency and facility (FY2018 also holds a README.TXT). FY2015-FY2025 are
3.5 GB of zips; FY2026 is not published yet (HTTP 403).
A file already on disk with the size the server reports is skipped, so reruns download nothing.

Run from any folder (paths are relative to the repository root):
  uv run python src/ingest/ridb_download.py --fiscal-years 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025
"""
import argparse
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]  # repository root
OUT_DIR = ROOT / "data" / "work" / "ridb"
URL = "https://ridb.recreation.gov/downloads/reservations{year}.zip"
# Name the project honestly, as gdelt_gkg.py does
HEADERS = {"User-Agent": "nps-tourism-lakehouse/0.1 (university course project)"}


def download(year: int, out_dir: Path) -> Path | None:
    """Stream one fiscal year's zip to disk; skip it if the local copy is already complete."""
    url = URL.format(year=year)
    path = out_dir / f"reservations{year}.zip"
    head = requests.head(url, headers=HEADERS, timeout=60)
    if head.status_code in (403, 404):  # RIDB answers 403 for a year it has not published
        print(f"FY{year} is not published at {url} (HTTP {head.status_code}), skipped")
        return None
    head.raise_for_status()
    expected = int(head.headers["Content-Length"])
    if path.exists() and path.stat().st_size == expected:
        print(f"FY{year}: {path.name} already complete ({expected / 1e6:.0f} MB), skipped")
        return path

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")  # renamed only when complete, so a broken download is never used
    t0 = last = time.time()
    got = 0
    with requests.get(url, headers=HEADERS, stream=True, timeout=(15, 300)) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
                got += len(chunk)
                if time.time() - last > 15:
                    last = time.time()
                    print(f"  FY{year}: {got / 1e6:.0f} / {expected / 1e6:.0f} MB "
                          f"({got / (last - t0) / 1e6:.1f} MB/s)")
    if got != expected:
        raise SystemExit(f"FY{year}: got {got} bytes, expected {expected}; run again")
    tmp.replace(path)
    print(f"FY{year}: {path.name} {got / 1e6:.0f} MB in {time.time() - t0:.0f} s")
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fiscal-years", type=int, nargs="+", required=True, help="e.g. 2017 2024")
    ap.add_argument("--out", default=str(OUT_DIR), help="folder for the zips")
    args = ap.parse_args()
    missing = [year for year in args.fiscal_years if download(year, Path(args.out)) is None]
    if missing:
        raise SystemExit(f"Not published: {missing}")


if __name__ == "__main__":
    main()
