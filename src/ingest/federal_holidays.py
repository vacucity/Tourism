"""Scrape the OPM federal holiday tables and land them as one raw CSV."""
import re
from collections import Counter
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from parks import START_YEAR

URL = "https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/"
# Some sites block the default python-requests User-Agent, so look like a browser.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
    )
}
OUT_DIR = Path("data/landing/federal_holidays") / f"pull_date={date.today().isoformat()}"


def fetch_page() -> str:
    response = requests.get(URL, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return response.text


def parse_tables(html: str) -> list[dict]:
    """One row per holiday, text kept exactly as shown on the page."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for table in soup.find_all("table"):
        caption = table.find("caption")
        if caption is None:
            continue
        match = re.fullmatch(r"(\d{4}) Holiday Schedule", caption.get_text(strip=True))
        if match is None:
            continue
        year = int(match.group(1))
        if year < START_YEAR:
            continue
        for tr in table.find_all("tr"):
            cells = tr.find_all("td")  # the header row uses <th>, so it is skipped
            if len(cells) != 2:
                continue
            rows.append({
                "schedule_year": year,
                "date_text": cells[0].get_text(" ", strip=True),  # keeps footnote stars
                "holiday_name": cells[1].get_text(" ", strip=True),
            })
    return rows


def main() -> None:
    rows = parse_tables(fetch_page())
    if not rows:
        raise RuntimeError("No holiday tables found; the OPM page layout may have changed")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUT_DIR / "opm_holidays.csv"
    pd.DataFrame(rows).to_csv(out_file, index=False)

    per_year = Counter(r["schedule_year"] for r in rows)
    for year in sorted(per_year):
        print(year, per_year[year], "holidays")
    print(len(rows), "rows saved to", out_file)


if __name__ == "__main__":
    main()