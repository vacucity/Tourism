"""Park metadata: the single source of truth for every ingest script."""

# Every run pulls from here up to the latest month each source has published.
START_YEAR = 2015

PARKS = {
    "YELL": {
        "name": "Yellowstone National Park",
        "latitude": 44.60,
        "longitude": -110.50,
        # [primary, backup]: Yellowstone Park Mammoth, Yellowstone Lake
        "noaa_stations": ["USC00489905", "USW00094173"],
        # Google Trends topic id: covers every way people search for the park
        "trends_topic": "/m/08874",
    },
    "YOSE": {
        "name": "Yosemite National Park",
        "latitude": 37.84,
        "longitude": -119.54,
        # Yosemite Village 12 W, Yosemite Park Headquarters
        "noaa_stations": ["USW00053150", "USC00049855"],
        "trends_topic": "/m/0c_y8",
    },
    "GRCA": {
        "name": "Grand Canyon National Park",
        "latitude": 36.10,
        "longitude": -112.11,
        # Grand Canyon NP Airport, Grand Canyon Visitor Center
        "noaa_stations": ["USW00003195", "USC00023582"],
        "trends_topic": "/m/0cnrr",
    },
    "ZION": {
        "name": "Zion National Park",
        "latitude": 37.30,
        "longitude": -113.03,
        # Zion National Park, La Verkin
        "noaa_stations": ["USC00429717", "USC00424968"],
        "trends_topic": "/m/01626x",
    },
    "GRSM": {
        "name": "Great Smoky Mountains National Park",
        "latitude": 35.61,
        "longitude": -83.49,
        # Gatlinburg 2 SW, Newfound Gap
        "noaa_stations": ["USC00403420", "USC00406500"],
        "trends_topic": "/m/01_yf9",
    },
}


if __name__ == "__main__":
    # Self-check: run `uv run python src/ingest/parks.py`
    for code, info in PARKS.items():
        primary, backup = info["noaa_stations"]
        print(f"{code}  {info['name']:<36}  primary={primary}  backup={backup}")
    print(f"{len(PARKS)} parks, from {START_YEAR}")