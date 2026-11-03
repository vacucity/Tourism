"""Pull Google Trends interest for each park and land the raw values as one CSV."""
import time
from datetime import date
from pathlib import Path

import pandas as pd
from pytrends.exceptions import TooManyRequestsError
from pytrends.request import TrendReq

from parks import PARKS, START_YEAR

OUT_DIR = Path("data/landing/google_trends") / f"pull_date={date.today().isoformat()}"
# Google rescales 0-100 inside every request, so each pull re-downloads the full history.
# End on Dec 31 of last year: NPS publishes whole years, and it avoids a partial month.
TIMEFRAME = f"{START_YEAR}-01-01 {date.today().year - 1}-12-31"


def fetch_park(pytrends: TrendReq, topic: str) -> pd.Series:
    """Monthly 0-100 interest for one park, scaled to that park's own peak month."""
    for attempt in range(3):
        try:
            pytrends.build_payload([topic], timeframe=TIMEFRAME, geo="US")
            df = pytrends.interest_over_time()
        except TooManyRequestsError:
            print("  Google returned 429 (too many requests), waiting 60 s ...")
            time.sleep(60)
            continue
        if df.empty:
            raise RuntimeError(f"Google Trends returned no data for {topic}")
        return df[topic]
    raise RuntimeError(f"Still rate limited after 3 tries for {topic}; try again later")


def main() -> None:
    # Do not pass retries=... to TrendReq: it breaks with urllib3 v2.
    pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 30))
    columns = {}
    for code, info in PARKS.items():
        series = fetch_park(pytrends, info["trends_topic"])
        columns[code] = series
        print(code, info["trends_topic"], len(series), "months, median", int(series.median()))
        time.sleep(10)  # space requests out to avoid 429

    wide = pd.DataFrame(columns)
    wide.index.name = "date"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUT_DIR / "interest_over_time.csv"
    wide.to_csv(out_file)
    print(len(wide), "months,", wide.index.min().date(), "to", wide.index.max().date())
    print("Saved to", out_file)


if __name__ == "__main__":
    main()