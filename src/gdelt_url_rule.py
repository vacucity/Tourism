"""URL rule R0 for the GDELT park matches: the park's own name must appear in the article URL path.

R0 is frozen in docs/week8/a1_part2_spec.md. It is the refined rule of docs/week8/a1_gdelt_test.md, made precise:
1. Lower-case the URL.
2. path = the text after scheme://host and before the first ? or #. It is not percent-decoded. The host, the
   query and the fragment are not part of it.
3. A match row (record_id, url, park_code) passes when the pattern of ITS OWN park_code occurs anywhere in the
   path: a substring search, with no word boundaries. Nothing else is included or excluded.

The patterns contain no backslash and no quote, so the same strings work in Python (re), in Spark SQL (RLIKE, Java
regular expressions) and in DuckDB. Every user of the rule takes them from this file:
- src/gdelt_sample.py uses passes() to build the frame of the A1 part 2 precision sample;
- databricks/notebooks/07_gdelt_news.py uses spark_sql_path() and spark_sql_case() for the Silver columns
  url_path and url_rule, and checks url_rule against passes() with a UDF.

Self-tests, from the repository root (standard library only, about half a minute):
  uv run python src/gdelt_url_rule.py
They check hand-made URLs, the A1 labels (28 kept, 27 of them about the park, 34 about the park in all) and the
counts on all exported matches (184,220 of 636,731 pass).
"""
import argparse
import csv
import gzip
import re
from collections import Counter
from pathlib import Path

# Group 1 is the path: everything after scheme://host up to the first ? or #. Applied to the lower-cased URL.
PATH_REGEX = "^[a-z][a-z0-9+.-]*://[^/?#]*([^?#]*)"

# One pattern per park, searched anywhere in the path. Grand Canyon allows no separator, one character that is not
# a letter or digit (grand-canyon, grand_canyon, grand+canyon), or an encoded space (grand%20canyon).
PATTERNS = {
    "YELL": "yellowstone",
    "YOSE": "yosemite",
    "GRCA": "grand([^a-z0-9]|%20)?canyon",
    "ZION": "zion",
    "GRSM": "smoky|smokies",
}

# The same text must be valid inside a single-quoted SQL string literal, with no escaping
for _text in [PATH_REGEX, *PATTERNS.values()]:
    assert "\\" not in _text and "'" not in _text, _text

_PATH = re.compile(PATH_REGEX)
_PARK = {park: re.compile(pattern) for park, pattern in PATTERNS.items()}


def url_path(url: str) -> str:
    """The lower-cased path of an absolute URL; '' when the URL has no scheme://host (as Spark's regexp_extract)."""
    m = _PATH.match(url.lower())
    return m.group(1) if m else ""


def passes(park_code: str, url: str) -> bool:
    """R0: True when the pattern of this row's own park occurs in the URL path."""
    if park_code not in _PARK:
        raise ValueError(f"unknown park_code {park_code!r}; expected one of {sorted(_PARK)}")
    return _PARK[park_code].search(url_path(url)) is not None


def spark_sql_path(url_col: str) -> str:
    """Spark SQL expression for the lower-cased URL path of column url_col (the Silver column url_path)."""
    return f"regexp_extract(lower({url_col}), '{PATH_REGEX}', 1)"


def spark_sql_case(url_col: str, park_col: str) -> str:
    """Spark SQL expression for R0 as an integer, 1 = pass and 0 = fail, built from the same strings as passes().

    RLIKE finds the pattern anywhere in the string, like re.search. A park_code outside PATTERNS gives NULL, so a
    NULL gate catches it instead of a silent 0.
    """
    path = spark_sql_path(url_col)
    whens = "\n".join(f"  WHEN '{park}' THEN CASE WHEN {path} RLIKE '{pattern}' THEN 1 ELSE 0 END"
                      for park, pattern in PATTERNS.items())
    return f"CASE {park_col}\n{whens}\nEND"


# ---------------------------------------------------------------------------------------------------------------
# Reading the exported matches (used by the self-test below and by src/gdelt_sample.py)

def export_match_files(repo_root: Path) -> list[Path]:
    """The matches file of each shard from its newest pull_date= folder, as Silver keeps them.

    data/landing/gdelt_matches/pull_date=YYYY-MM-DD/matches_<shard>.csv.gz; pull_date= names sort by date.
    """
    newest = {}  # shard -> (pull folder name, file)
    for f in sorted((Path(repo_root) / "data" / "landing" / "gdelt_matches").glob("pull_date=*/matches_*.csv.gz")):
        shard = f.name[len("matches_"):-len(".csv.gz")]
        pull = f.parent.name
        if shard not in newest or pull > newest[shard][0]:
            newest[shard] = (pull, f)
    assert newest, f"no matches_*.csv.gz under {repo_root}/data/landing/gdelt_matches/pull_date=*"
    return [newest[shard][1] for shard in sorted(newest)]


def read_export_matches(path: Path):
    """Yield (record_id, file_ts, park_code, url) for every row of one exported matches file.

    The exports are written by Python's csv module (gdelt_gkg.py write_csv): header, doubled quotes, CRLF.
    """
    csv.field_size_limit(2**31 - 1)  # a themes field may be longer than the default 131,072 characters
    with gzip.open(path, "rt", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        i_id, i_ts, i_park, i_url = (header.index(c) for c in ("record_id", "file_ts", "park_code", "url"))
        for row in reader:
            assert len(row) == len(header), f"{path}: a row with {len(row)} fields, expected {len(header)}"
            yield row[i_id], row[i_ts], row[i_park], row[i_url]


# ---------------------------------------------------------------------------------------------------------------
# Self-tests

# (park_code, url, expected) for each point of the rule
CASES = [
    # the path is searched, after lower-casing
    ("YELL", "https://www.example.com/news/Yellowstone-Bison-Calf/", True),
    ("YELL", "https://gizmodo.com/yellowstones-standing-dead-trees-are-a-wildfire-disaster-2000547968", True),
    # host only, query only, fragment only: not part of the path
    ("YELL", "https://www.westyellowstonenews.com/news/some-local-story/article_1.html", False),
    ("GRCA", "https://www.grandcanyonnews.com/news/2023/aug/01/local-story/", False),
    ("YELL", "https://example.com/a?title=yellowstone", False),
    ("YELL", "https://example.com/a#yellowstone", False),
    ("YOSE", "https://example.com/story/123?ref=yosemite&x=1", False),
    # a URL with no scheme://host has an empty path
    ("YELL", "www.example.com/yellowstone-trip", False),
    # no percent-decoding: %79 is 'y', but the encoded form does not match
    ("YELL", "https://example.com/%79ellowstone-trip", False),
    # another park's name passes only for that park (own park)
    ("YOSE", "https://eturbonews.com/favorites-include-niagara-falls-grand-canyon-sahara-mount-everest/", False),
    ("GRCA", "https://eturbonews.com/favorites-include-niagara-falls-grand-canyon-sahara-mount-everest/", True),
    # Grand Canyon separators: none, one non-alphanumeric character, or %20; not two characters or a letter
    ("GRCA", "https://example.com/grand-canyon-trip", True),
    ("GRCA", "https://example.com/grand_canyon/trip", True),
    ("GRCA", "https://example.com/grand+canyon", True),
    ("GRCA", "https://example.com/grandcanyon/trip", True),
    ("GRCA", "https://example.com/grand%20canyon-trip", True),
    ("GRCA", "https://example.com/grand.canyon", True),
    ("GRCA", "https://example.com/grand--canyon", False),
    ("GRCA", "https://example.com/grand-the-canyon", False),
    ("GRCA", "https://example.com/grandscanyon", False),
    # Great Smoky Mountains: smoky or smokies anywhere in the path
    ("GRSM", "https://www.local3news.com/local-news/take-a-virtual-field-trip-to-the-smokies/article_1.html", True),
    ("GRSM", "https://example.com/great-smoky-mountains-national-park", True),
    ("GRSM", "https://example.com/smoke-in-the-mountains", False),
    # substring search, no word boundaries and no exclusions
    ("ZION", "https://example.com/zion-williamson-scores-30", True),
    ("ZION", "https://example.com/horizons", False),
    ("YOSE", "https://example.com/apple-os-x-yosemite-review", True),
    # the two A1 rows the design names (gdelt_precision_sample.csv lines 6 and 92, both labeled 0)
    ("YOSE", "https://eturbonews.com/favorites-include-niagara-falls-grand-canyon-sahara-mount-everest-the-dead-sea-"
             "ha-long-bay/", False),
    ("YELL", "https://screenrant.com/yellowstone-show-lowest-rated-episodes-imdb-harsh-reality-explainer/", True),
]

# Frozen check values (docs/week8/a1_part2_spec.md)
EXPECTED_ROWS = 636_731
EXPECTED_PASS = {"YELL": 94_952, "YOSE": 44_899, "GRCA": 24_668, "ZION": 9_639, "GRSM": 10_062}  # 184,220 in all
EXPECTED_A1 = {"rows": 100, "kept": 28, "kept_relevant": 27, "relevant": 34}


def a1_check(repo_root: Path) -> dict:
    """R0 on the 100 A1 labels: rows kept, kept rows about the park, rows about the park in all."""
    path = Path(repo_root) / "docs" / "week8" / "gdelt_precision_sample.csv"
    with path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    kept = [r for r in rows if passes(r["park_code"], r["url"])]
    return {"rows": len(rows), "kept": len(kept),
            "kept_relevant": sum(r["is_about_park"] == "1" for r in kept),
            "relevant": sum(r["is_about_park"] == "1" for r in rows)}


def export_check(repo_root: Path) -> tuple[int, Counter]:
    """All exported match rows, and the rows that pass per park."""
    n, passing = 0, Counter()
    for path in export_match_files(repo_root):
        for _record_id, _file_ts, park_code, url in read_export_matches(path):
            n += 1
            passing[park_code] += passes(park_code, url)
    return n, passing


def self_test(repo_root: Path) -> None:
    for park, url, expected in CASES:
        assert passes(park, url) == expected, (park, url, expected)
    print(f"{len(CASES)} hand-made cases passed")

    sql = spark_sql_case("url", "park_code")
    assert sql.count(f"'{PATH_REGEX}'") == len(PATTERNS)
    for park, pattern in PATTERNS.items():
        assert f"WHEN '{park}' THEN" in sql and f"RLIKE '{pattern}'" in sql, park
    print("spark_sql_case() holds the same strings as passes():")
    print(sql)

    a1 = a1_check(repo_root)
    print(f"A1 labels: {a1['rows']} rows, {a1['kept']} kept, {a1['kept_relevant']} kept and about the park, "
          f"{a1['relevant']} about the park in all")
    assert a1 == EXPECTED_A1, (a1, EXPECTED_A1)

    files = export_match_files(repo_root)
    print("export files:", ", ".join(f"{f.parent.name}/{f.name}" for f in files))
    n, passing = export_check(repo_root)
    print(f"exported matches: {n:,} rows, {sum(passing.values()):,} pass: "
          + ", ".join(f"{park} {passing[park]:,}" for park in PATTERNS))
    assert n == EXPECTED_ROWS, n
    assert dict(passing) == EXPECTED_PASS, dict(passing)
    print("all self-tests passed")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[1]),
                    help="repository root that holds data/landing and docs/week8 (default: the parent of src/)")
    self_test(Path(ap.parse_args().repo_root))
