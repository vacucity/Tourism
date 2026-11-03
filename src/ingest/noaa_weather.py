"""Pull monthly weather (NOAA GSOM) for every park station and land the raw JSON untouched."""
import json
import time
from datetime import date
from pathlib import Path

import requests

from parks import PARKS, START_YEAR

URL = "https://www.ncei.noaa.gov/access/services/data/v1"
# One folder per pull: same-day reruns overwrite, a new day keeps history.
OUT_DIR = Path("data/landing/noaa_weather") / f"pull_date={date.today().isoformat()}"


def fetch_station(station_id: str) -> bytes:
    """Raw monthly summaries for one station, from START_YEAR-01 up to today."""
    params = {
        "dataset": "global-summary-of-the-month",
        "stations": station_id,
        "startDate": f"{START_YEAR}-01-01",
        "endDate": date.today().isoformat(),
        "dataTypes": "TAVG,TMAX,TMIN,PRCP,SNOW",
        "units": "standard",  # degrees F and inches
        "format": "json",
    }
    response = requests.get(URL, params=params, timeout=120)
    response.raise_for_status()
    return response.content


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    total = 0
    for code, info in PARKS.items():
        for station in info["noaa_stations"]:
            raw = fetch_station(station)
            records = json.loads(raw)  # only to check the data
            if not records:
                raise RuntimeError(f"No weather data returned for {code} station {station}")
            (OUT_DIR / f"{station}.json").write_bytes(raw)
            months = sorted(r["DATE"] for r in records)
            print(code, station, len(records), "months,", months[0], "to", months[-1])
            total += len(records)
            time.sleep(0.5)  # be polite to a public API
    print(total, "station-months saved to", OUT_DIR)


if __name__ == "__main__":
    main()