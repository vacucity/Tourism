"""Pull monthly NPS visitation for every park and land the raw JSON untouched."""
import json
import time
from datetime import date
from pathlib import Path

import requests

from parks import PARKS, START_YEAR

URL = "https://irmaservices.nps.gov/v3/rest/stats/visitation"
# One folder per pull: same-day reruns overwrite, a new day keeps history.
OUT_DIR = Path("data/landing/nps_visitation") / f"pull_date={date.today().isoformat()}"


def fetch_park(park_code: str) -> bytes:
    """Raw API response for one park, from START_YEAR-01 to the latest published month."""
    params = {
        "unitCodes": park_code,
        "startMonth": 1,
        "startYear": START_YEAR,
        # Ask up to December of this year; the API only returns months that exist.
        "endMonth": 12,
        "endYear": date.today().year,
    }
    response = requests.get(
        URL,
        params=params,
        headers={"Accept": "application/json"},  # without it the API returns XML
        timeout=60,
    )
    response.raise_for_status()
    return response.content


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for code in PARKS:
        raw = fetch_park(code)
        records = json.loads(raw)  # only to check the data
        if not records:
            raise RuntimeError(f"No visitation data returned for {code}")
        (OUT_DIR / f"{code}.json").write_bytes(raw)
        latest = max(r["Year"] * 100 + r["Month"] for r in records)
        print(code, len(records), "records, latest", latest)
        time.sleep(0.5)  # be polite to a public API
    print("Saved to", OUT_DIR)


if __name__ == "__main__":
    main()