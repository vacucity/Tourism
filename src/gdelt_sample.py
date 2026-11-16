"""A1 part 2: a fresh precision sample of the GDELT matches that pass URL rule R0, drawn once and scored once.

This is the pre-registered check of docs/week8/a1_part2_spec.md. The rule is src/gdelt_url_rule.py.
- Unit: one match row (record_id, url, park_code) of the committed exports, the row that Gold counts.
- Frame: the rows that pass R0 with file_ts from 2015-02-18 to 2025-12-31 (UTC), without the A1 test week
  (20250106000000 <= file_ts < 20250113000000) and without any row whose url is in gdelt_precision_sample.csv.
- Strata: park x period of file_ts. Period A = 2015-02..2023-12, 30 rows per park. Period B = 2024-01..2025-12,
  10 rows per park. 200 rows in all.
- Draw: the frame is sorted by (record_id, url, park_code), and one random.Random(20261116) does all the drawing.
  The strata come in the fixed order GRCA A, GRCA B, GRSM A, ..., ZION B. Each stratum is shuffled and its first
  n rows are taken, as in gdelt_gkg.py sample. Then the 200 rows are shuffled once more for the page order, and
  sample_id = 1..200 follows that order.
- Labels come from the article content, on the page that src/gdelt_label_page.py builds. Each row is labeled for
  its own park_code. The only note kind that score recognises is url_only (see note_kind()).
- Score: P = sum over strata of W_h * x_h / n_h, with W_h = N_h / N from the exact frame counts and x_h the rows
  labeled 1. PASS when P >= 0.90, compared as exact fractions. The 95% interval is
  P +- 1.96 * sqrt(sum W_h^2 * p_h * (1 - p_h) / n_h) with p_h = (x_h + 1) / (n_h + 2), clipped to [0, 1].
  Per park: x / 40 with a Wilson interval. A park with x <= 32 of 40 is below the bar in the news forecast test.

From the repository root:
  uv run python src/gdelt_sample.py check
      The rule check values, the frame per stratum, the weights and the chance of a PASS at true precision
      0.85-0.93. Draws nothing and writes nothing.
  uv run python src/gdelt_sample.py draw --seen-dir <folder or file> [--seen-dir <folder or file> ...]
      Once, right after commit 1 is pushed: writes docs/week8/a1_part2_sample.csv and its sidecar
      a1_part2_sample.json. Each --seen-dir is a folder or a file of earlier outputs (design and build outputs,
      session transcripts); the sidecar records how many sampled rows have a url printed there.
  uv run python src/gdelt_sample.py check-labels [--labels <file>]
      Before commit 3a: reads the labels file (default docs/week8/a1_part2_labels.csv) against the committed sample
      as score will read it, and refuses it with the reason if score would. No git checks; writes nothing; prints
      no count of labels.
  uv run python src/gdelt_sample.py score
      Once, right after commit 3a is pushed: reads docs/week8/a1_part2_labels.csv and writes a1_part2_score.json,
      a1_part2_score.txt and a copy of Claude's drafts, a1_part2_claude_drafts.csv (columns sample_id, draft),
      the files of commit 3b.

Git guards: draw refuses unless the working tree is clean, the spec, this script and the rule module at HEAD
equal the working files, HEAD is pushed to its upstream and at least one --seen-dir is given. score refuses
unless the tree is clean; the sample, the sidecar and the labels are committed at HEAD and HEAD is pushed; the
sidecar comes from a guarded draw with seed 20261116; the labels were committed after the sample, in a commit
that changes only the labels file; and the sample, the sidecar and the labels are unchanged since their commits.
It reports (does not refuse) whether the spec or the code changed since the draw.
For tests only, --no-git-guard skips the guards. It must come with --out-dir outside the repository (draw writes
there, score reads and writes there), and draw then needs an explicit positive --seed other than 20261116, on any
frame (random.Random(-s) draws what random.Random(s) draws). A test score refuses a sidecar from a guarded draw or
with a seed that is not a positive integer other than 20261116.

score also checks the sample itself: every input file listed in the sidecar must still have its sha256 (a shard
exported again later, into a newer pull_date= folder, is not read); it rebuilds the frame from those files,
compares its sha256 with the sidecar, draws again with the sidecar's seed and refuses unless that gives exactly
the committed sample.
"""
import argparse
import csv
import hashlib
import json
import math
import os
import random
import re
import string
import subprocess
import sys
import time
from collections import Counter
from fractions import Fraction
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))  # src/, also under python -I
import gdelt_url_rule as rule  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]  # repository root
SPEC = "docs/week8/a1_part2_spec.md"
GUARDED = [SPEC, "src/gdelt_sample.py", "src/gdelt_url_rule.py"]  # must be committed before the draw
A1_SAMPLE = "docs/week8/gdelt_precision_sample.csv"
OUT_DIR = "docs/week8"
SAMPLE_NAME = "a1_part2_sample.csv"
SIDECAR_NAME = "a1_part2_sample.json"
LABELS_NAME = "a1_part2_labels.csv"
SCORE_JSON = "a1_part2_score.json"
SCORE_TXT = "a1_part2_score.txt"
DRAFTS = "data/work/gdelt/a1_part2_claude_drafts.csv"  # Claude's drafts from the content, for agreement only
DRAFTS_COPY = "a1_part2_claude_drafts.csv"  # score writes the copy next to the score files (commit 3b)

SEED = 20261116
PARKS = ["GRCA", "GRSM", "YELL", "YOSE", "ZION"]  # the fixed stratum order: parks alphabetically, A before B
PERIODS = {"A": ("201502", "202312"), "B": ("202401", "202512")}  # months (YYYYMM) of file_ts, both included
N_PER_STRATUM = {"A": 30, "B": 10}
STRATA = [(park, period) for park in PARKS for period in PERIODS]
WINDOW = ("20150218000000", "20260101000000")     # file_ts, half-open: 2015-02-18 to 2025-12-31
TEST_WEEK = ("20250106000000", "20250113000000")  # the A1 test week, half-open: 2025-01-06 to 2025-01-12
BAR = Fraction(9, 10)
PARK_BAR = 32             # a park with x <= 32 of its 40 rows is below the bar
Z = 1.96
TRUE_PRECISIONS = [0.85, 0.88, 0.90, 0.93]

SAMPLE_COLUMNS = ["sample_id", "record_id", "file_ts", "park_code", "url"]
LABEL_COLUMNS = ["sample_id", "record_id", "url", "park_code", "label", "note"]
DRAFT_COLUMNS = ["sample_id", "draft"]  # the drafts are committed later, so they may hold no article text

# Frozen frame (docs/week8/a1_part2_spec.md): rows per stratum and the sha256 of the sorted frame keys, measured
# by `check` on the exports of pull_date=2026-11-13. 174,519 rows in all. draw refuses to run on any other frame.
EXPECTED_FRAME = {
    "GRCA A": 18_895, "GRCA B": 4_189,
    "GRSM A": 8_680, "GRSM B": 964,
    "YELL A": 78_290, "YELL B": 11_677,
    "YOSE A": 40_740, "YOSE B": 2_346,
    "ZION A": 7_903, "ZION B": 835,
}
EXPECTED_FRAME_SHA256 = "d231bfd61b17313ca43102bba3c244950584ef6273de39b460b7fc1e6de7f23e"


def stop(message: str) -> None:
    raise SystemExit(f"REFUSED: {message}")


def shown(value: str) -> str:
    """A field value for a REFUSED message: quoted when short, else only its length, so a url that has slipped into
    a field is never printed."""
    return repr(value) if len(value) <= 12 else f"a text of {len(value)} characters"


def months_label(period: str) -> str:
    first, last = PERIODS[period]
    return f"{first[:4]}-{first[4:]}..{last[:4]}-{last[4:]}"


def period_of(file_ts: str) -> str | None:
    for period, (first, last) in PERIODS.items():
        if first <= file_ts[:6] <= last:
            return period
    return None


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def inside(path: Path, root: Path) -> bool:
    """True when path is root or below it (Windows paths compare without case)."""
    p, r = os.path.normcase(str(Path(path).resolve())), os.path.normcase(str(Path(root).resolve()))
    return p == r or p.startswith(r.rstrip(os.sep) + os.sep)


# ---------------------------------------------------------------------------------------------------------------
# The frame

def build_frame(repo: Path, files: list[Path] | None = None) -> dict:
    """Read every exported match once: the rule check values, the removed rows, and the frame per stratum.

    files: the matches files to read. None (check and draw) = the newest pull of each shard; score passes the
    files that the draw read, from the sidecar. Each stratum is a list of (record_id, url, park_code, file_ts)
    in key order.
    """
    with (repo / A1_SAMPLE).open(encoding="utf-8", newline="") as f:
        a1_urls = {r["url"] for r in csv.DictReader(f)}
    paths = rule.export_match_files(repo) if files is None else files
    files_read, passing, removed = [], Counter(), Counter()
    window, window_pass, frame_fail = 0, 0, Counter()
    strata = {h: [] for h in STRATA}
    for path in paths:
        rows = 0
        for record_id, file_ts, park, url in rule.read_export_matches(path):
            rows += 1
            assert len(file_ts) == 14 and file_ts.isdigit(), f"{path.name}: file_ts {file_ts!r}"
            ok = rule.passes(park, url)
            passing[park] += ok
            if not WINDOW[0] <= file_ts < WINDOW[1]:
                continue
            window += 1
            window_pass += ok
            if TEST_WEEK[0] <= file_ts < TEST_WEEK[1]:
                removed["test week", ok] += 1
            elif url in a1_urls:
                removed["A1 url", ok] += 1
            elif ok:
                strata[(park, period_of(file_ts))].append((record_id, url, park, file_ts))
            else:
                frame_fail[park] += 1
        files_read.append({"path": path.relative_to(repo).as_posix(), "sha256": sha256_file(path), "rows": rows})
    for rows in strata.values():
        rows.sort()  # by (record_id, url, park_code); the key is unique, so file_ts never decides

    keys = sorted(r[:3] for rows in strata.values() for r in rows)
    assert len(set(keys)) == len(keys), "a (record_id, url, park_code) key is in the frame twice"
    h = hashlib.sha256()
    for record_id, url, park in keys:
        h.update(f"{record_id}\t{url}\t{park}\n".encode("utf-8"))
    return {"files": files_read, "rows": sum(f["rows"] for f in files_read), "passing": passing,
            "a1": rule.a1_check(repo), "a1_sample_sha256": sha256_file(repo / A1_SAMPLE),
            "window": window, "window_pass": window_pass, "removed": removed, "frame_fail": frame_fail,
            "strata": strata, "sizes": {h_: len(rows) for h_, rows in strata.items()},
            "frame_rows": len(keys), "frame_sha256": h.hexdigest()}


def frozen_differences(frame: dict) -> list[str]:
    """Where the frame differs from the frozen check values (an empty list when it matches all of them)."""
    out = []
    if frame["rows"] != rule.EXPECTED_ROWS:
        out.append(f"exported rows {frame['rows']:,} != {rule.EXPECTED_ROWS:,}")
    if dict(frame["passing"]) != rule.EXPECTED_PASS:
        out.append(f"R0 passes {dict(frame['passing'])} != {rule.EXPECTED_PASS}")
    if frame["a1"] != rule.EXPECTED_A1:
        out.append(f"A1 check {frame['a1']} != {rule.EXPECTED_A1}")
    if EXPECTED_FRAME is None or EXPECTED_FRAME_SHA256 is None:
        out.append("the frame is not frozen yet (EXPECTED_FRAME is None)")
    else:
        sizes = {f"{park} {period}": n for (park, period), n in frame["sizes"].items()}
        if sizes != EXPECTED_FRAME:
            out.append(f"frame sizes {sizes} != {EXPECTED_FRAME}")
        if frame["frame_sha256"] != EXPECTED_FRAME_SHA256:
            out.append(f"frame sha256 {frame['frame_sha256']} != {EXPECTED_FRAME_SHA256}")
    return out


def weights(sizes: dict) -> dict:
    """W_h = N_h / N as exact fractions."""
    total = sum(sizes.values())
    return {h: Fraction(n, total) for h, n in sizes.items()}


def draw_sample(strata: dict, seed: int) -> list[dict]:
    """The sample in page order. Deterministic: the same frame and seed always give the same rows."""
    rng = random.Random(seed)
    picked = []
    for park, period in STRATA:
        rows = list(strata[(park, period)])  # key order
        n = N_PER_STRATUM[period]
        assert len(rows) >= n, f"{park} {period}: {len(rows)} frame rows, fewer than {n}"
        rng.shuffle(rows)
        picked.extend(rows[:n])
    rng.shuffle(picked)  # page order, so neighbouring rows do not share a stratum
    return [{"sample_id": i, "record_id": r[0], "file_ts": r[3], "park_code": r[2], "url": r[1]}
            for i, r in enumerate(picked, start=1)]


# ---------------------------------------------------------------------------------------------------------------
# Statistics

def wilson(x: int, n: int) -> tuple[float, float]:
    """Wilson 95% interval for x of n."""
    p = x / n
    d = 1 + Z * Z / n
    centre = (p + Z * Z / (2 * n)) / d
    half = Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n)) / d
    return max(0.0, centre - half), min(1.0, centre + half)


def estimate(w: dict, x: dict, n: dict) -> Fraction | None:
    """P = sum W_h * x_h / n_h, exact. None when a stratum has no rows."""
    if any(n[h] == 0 for h in w):
        return None
    return sum((w[h] * Fraction(x[h], n[h]) for h in w), Fraction(0))


def interval(p: Fraction, w: dict, x: dict, n: dict) -> tuple[float, float]:
    """Stratified normal 95% interval around P. The variance uses p_h = (x_h + 1) / (n_h + 2), so a stratum
    with every row labeled 1 still adds variance. Clipped to [0, 1]."""
    var = 0.0
    for h in w:
        ph = (x[h] + 1) / (n[h] + 2)
        var += float(w[h]) ** 2 * ph * (1 - ph) / n[h]
    se = math.sqrt(var)
    return max(0.0, float(p) - Z * se), min(1.0, float(p) + Z * se)


def pass_chance_exact(sizes: dict, t: float) -> float:
    """Chance that P >= 0.90 when every stratum has true precision t and x_h ~ Binomial(n_h, t), independent.

    With L = lcm of the n_h, N * L * P = sum_h x_h * (N_h * L / n_h) is an integer. Its distribution is built
    stratum by stratum in an array, and P >= 9/10 exactly when that integer is >= ceil(9 * N * L / 10).
    """
    n = {h: N_PER_STRATUM[h[1]] for h in sizes}
    lcm = math.lcm(*n.values())
    steps = {h: sizes[h] * lcm // n[h] for h in sizes}
    g = math.gcd(*steps.values())  # a common factor only shortens the array
    top = sum(steps[h] * n[h] for h in sizes) // g
    need = -(-(BAR.numerator * sum(sizes.values()) * lcm) // BAR.denominator)  # ceil(BAR * N * L)
    need = -(-need // g)
    dist = np.zeros(top + 1)
    dist[0] = 1.0
    hi = 0
    for h in sizes:
        new = np.zeros_like(dist)
        step = steps[h] // g
        for k in range(n[h] + 1):
            prob = math.comb(n[h], k) * t ** k * (1 - t) ** (n[h] - k)
            new[k * step:k * step + hi + 1] += prob * dist[:hi + 1]
        dist = new
        hi += n[h] * step
    return float(dist[need:].sum())


def pass_chance_normal(sizes: dict, t: float) -> float:
    """The same chance from the normal approximation, for comparison."""
    w = weights(sizes)
    se = math.sqrt(sum(float(w[h]) ** 2 * t * (1 - t) / N_PER_STRATUM[h[1]] for h in sizes))
    return 1 - 0.5 * (1 + math.erf((float(BAR) - t) / se / math.sqrt(2)))


def design_lines(sizes: dict) -> tuple[list[str], dict]:
    """The operating characteristic of the pass rule, as printed lines and as numbers."""
    w = weights(sizes)
    se90 = math.sqrt(sum(float(w[h]) ** 2 * 0.09 / N_PER_STRATUM[h[1]] for h in sizes))
    chances = {f"{t:.2f}": {"exact": pass_chance_exact(sizes, t), "normal": pass_chance_normal(sizes, t)}
               for t in TRUE_PRECISIONS}
    lines = [f"  standard error of P at true precision 0.90 in every stratum: {100 * se90:.2f} points "
             f"(95% +- {100 * Z * se90:.1f} points)",
             "  chance of PASS (P >= 0.90) when every stratum has true precision t:",
             "     t    exact (binomial)   normal approximation"]
    lines += [f"  {t}    {c['exact']:.4f}             {c['normal']:.4f}" for t, c in chances.items()]
    return lines, {"se_at_0.90": se90, "pass_chance": chances}


# ---------------------------------------------------------------------------------------------------------------
# Git guards (read-only git commands)

def git(repo: Path, *args: str) -> str:
    r = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
    if r.returncode != 0:
        stop(f"git {' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout.strip()


def is_ancestor(repo: Path, older: str, newer: str) -> bool:
    r = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", older, newer], capture_output=True)
    return r.returncode == 0


def require_clean(repo: Path) -> None:
    status = git(repo, "status", "--porcelain", "--untracked-files=all")
    if status:
        stop(f"the working tree is not clean; commit or remove these first:\n{status}")


def require_committed(repo: Path, rel: str) -> str:
    """The file's blob at HEAD, which must equal the working file."""
    if not (repo / rel).is_file():
        stop(f"{rel} does not exist")
    r = subprocess.run(["git", "-C", str(repo), "rev-parse", f"HEAD:{rel}"], capture_output=True, text=True)
    if r.returncode != 0:
        stop(f"{rel} is not committed at HEAD")
    work = git(repo, "hash-object", "--", rel)
    if r.stdout.strip() != work:
        stop(f"{rel} differs from its blob at HEAD")
    return work


def require_pushed(repo: Path) -> None:
    """HEAD must be on its upstream branch: the pushed commits, not local commit times, show the order.

    Both commands are read-only. @{u} is the remote-tracking branch as of the last push or fetch.
    """
    r = subprocess.run(["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        stop("the branch has no upstream; push it first (git push -u origin <branch>)")
    if not is_ancestor(repo, "HEAD", "@{u}"):
        stop(f"HEAD is not pushed to {r.stdout.strip()}; push first")


def added_in(repo: Path, rel: str) -> str:
    """The first commit that added the file."""
    commits = git(repo, "log", "--diff-filter=A", "--format=%H", "--", rel).splitlines()
    if not commits:
        stop(f"no commit adds {rel}")
    return commits[-1]


def setup(args: argparse.Namespace) -> tuple[Path, Path, bool]:
    """(repository root, folder for the sample and the score, git guard on)."""
    repo = Path(args.repo_root).resolve()
    if args.no_git_guard or args.out_dir:
        if not (args.no_git_guard and args.out_dir):
            stop("--no-git-guard and --out-dir are for tests and go together")
        out = Path(args.out_dir).resolve()
        if inside(out, repo):
            stop(f"--out-dir must be outside the repository {repo}")
        return repo, out, False
    return repo, repo / OUT_DIR, True


# ---------------------------------------------------------------------------------------------------------------
# check

def frame_lines(frame: dict) -> list[str]:
    p, rm = frame["passing"], frame["removed"]
    w = weights(frame["sizes"])
    lines = [
        "R0 CHECK VALUES (src/gdelt_url_rule.py)",
        f"  exported matches: {frame['rows']:,} rows in {len(frame['files'])} files (newest pull per shard)",
        f"  pass R0: {sum(p.values()):,}: " + ", ".join(f"{park} {p[park]:,}" for park in PARKS),
        f"  A1 labels (gdelt_precision_sample.csv): {frame['a1']['rows']} rows, {frame['a1']['kept']} kept, "
        f"{frame['a1']['kept_relevant']} kept and about the park, {frame['a1']['relevant']} about the park in all",
        "FRAME",
        f"  file_ts 2015-02-18..2025-12-31: {frame['window']:,} rows, {frame['window_pass']:,} pass R0",
        f"  removed, the A1 test week: {rm['test week', True]:,} passing and {rm['test week', False]:,} failing rows",
        f"  removed, a url of the A1 sample outside that week: {rm['A1 url', True]:,} passing and "
        f"{rm['A1 url', False]:,} failing rows",
        f"  frame: {frame['frame_rows']:,} passing rows (failing rows left outside: "
        f"{sum(frame['frame_fail'].values()):,})",
        "  park  period  months              N_h       W_h   n_h",
    ]
    for park, period in STRATA:
        n_h = frame["sizes"][(park, period)]
        lines.append(f"  {park}  {period}       {months_label(period)}  {n_h:>7,}  {float(w[(park, period)]):.6f}"
                     f"  {N_PER_STRATUM[period]:>4}")
    per_park = {park: sum(frame["sizes"][(park, period)] for period in PERIODS) for park in PARKS}
    lines.append("  per park: " + ", ".join(f"{park} {n:,} ({n / frame['frame_rows']:.4f})"
                                            for park, n in per_park.items()))
    lines.append(f"  frame sha256 (sorted 'record_id<TAB>url<TAB>park_code<LF>'): {frame['frame_sha256']}")
    return lines


def cmd_check(args: argparse.Namespace) -> None:
    repo = Path(args.repo_root).resolve()
    frame = build_frame(repo)
    for line in frame_lines(frame):
        print(line)
    print("DESIGN")
    for line in design_lines(frame["sizes"])[0]:
        print(line)
    print("FROZEN VALUES")
    diffs = frozen_differences(frame)
    for d in diffs:
        print("  DIFFERENT:", d)
    print("  all equal" if not diffs else f"  {len(diffs)} difference(s): draw would refuse")
    if diffs:
        sys.exit(1)


# ---------------------------------------------------------------------------------------------------------------
# draw: the count of sampled urls printed in earlier outputs

# A url is printed in a text when the text holds the whole url and what follows does not make it a longer url.
# It ends at the end of the text or at a character that cannot continue a url (a space, a quote, a bracket, a
# backslash, a non-ASCII byte, ...). A run of the punctuation . , : ; ! ) ' after it ends it too, unless a url
# character follows the run: 'see <url>.' and '(<url>).' are prints, '<url>.html' and '<url>):8080/x' are not.
URL_CONTINUES = frozenset((string.ascii_letters + string.digits + "/_-%=&+?#~").encode("ascii"))
URL_TRAILING = frozenset(b".,:;!)'")
SEEN_SKIP = "a1_part2_sample"  # a test sample and its sidecar are not earlier outputs
# Spark's scratch files of a local run (shuffle and block files, the warehouse) hold data rows that no one read:
# a file below a folder named spark_tmp, warehouse or blockmgr-..., or with one of these suffixes, is left out
SPARK_SCRATCH_FOLDERS = ("spark_tmp", "warehouse")
SPARK_SCRATCH_PREFIX = "blockmgr-"
SPARK_SCRATCH_SUFFIXES = (".data", ".index", ".parquet", ".crc")
SEEN_TOP = 5  # per --seen-dir, the draw prints the files that hold the most sampled rows


def url_ends(text: bytes, j: int) -> bool:
    """True when a url that ends just before byte j of text is not the start of a longer url (see above)."""
    k = j
    while k < len(text) and text[k] in URL_TRAILING:
        k += 1
    return k == len(text) or text[k] not in URL_CONTINUES


def printed_in(data: bytes, by_prefix: dict, k: int, anchors: list[bytes]) -> set:
    """The urls (UTF-8 bytes) that are printed in data.

    Every occurrence of a url starts with its first 4 bytes (an anchor, in practice only b'http'), so the scan
    visits each anchor in data once and looks up the k bytes from there in by_prefix (url[:k] -> urls, k = the
    length of the shortest url): exact, with one dictionary lookup per anchor instead of one search per url.
    """
    found = set()
    for anchor in anchors:
        i = data.find(anchor)
        while i != -1:
            for url in by_prefix.get(data[i:i + k], ()):
                if data.startswith(url, i) and url_ends(data, i + len(url)):
                    found.add(url)
            i = data.find(anchor, i + 1)
    return found


def spark_scratch(rel: Path) -> bool:
    """True for a Spark scratch file, from its path below the --seen-dir."""
    return (any(part in SPARK_SCRATCH_FOLDERS or part.startswith(SPARK_SCRATCH_PREFIX) for part in rel.parts)
            or rel.suffix.lower() in SPARK_SCRATCH_SUFFIXES)


def seen_files(path: Path) -> tuple[list[Path], int]:
    """The files of one --seen-dir and the number of Spark scratch files left out.

    A file path is read itself; a folder gives every file below it. Test samples (SEEN_SKIP) are left out, and in a
    folder the Spark scratch files too.
    """
    if path.is_file():
        return ([] if path.name.startswith(SEEN_SKIP) else [path]), 0
    files, scratch = [], 0
    for f in sorted(f for f in path.rglob("*") if f.is_file()):
        if f.name.startswith(SEEN_SKIP):
            continue
        if spark_scratch(f.relative_to(path)):
            scratch += 1
        else:
            files.append(f)
    return files, scratch


def check_seen_paths(paths: list[str]) -> None:
    """Refuse, before the frame is read, a --seen-dir that does not exist or holds no file to read."""
    for p in paths:
        if not Path(p).exists():
            stop(f"--seen-dir {p} does not exist")
        if not seen_files(Path(p))[0]:
            stop(f"--seen-dir {p} holds no file to read")


def seen_urls(urls: list[str], paths: list[str]) -> dict:
    """How many sampled rows have a url printed in the earlier outputs: every file of every --seen-dir.

    One file is in memory at a time. The files are searched as UTF-8 bytes; a file that starts with a UTF-16
    byte order mark is decoded first. The sidecar gets, per --seen-dir, its name (not its full path), the counts of
    files and bytes, and each file that holds a printed url: its path below the --seen-dir and how many sampled rows
    have a url printed in it. It never gets a url.
    """
    rows_per_url = Counter(u.encode("utf-8") for u in urls)  # a url in the sample for two parks is two rows
    k = min(len(u) for u in rows_per_url)
    assert k >= 4, "a sampled url is shorter than 4 bytes"
    by_prefix = {}
    for u in rows_per_url:
        by_prefix.setdefault(u[:k], []).append(u)
    anchors = sorted({u[:4] for u in rows_per_url})
    found, per_path = set(), []
    for p in paths:
        path = Path(p)
        files, scratch = seen_files(path)
        n_files, n_bytes, unreadable, hits = 0, 0, 0, []
        for f in files:
            try:
                data = f.read_bytes()
            except OSError:  # removed or locked since the listing
                unreadable += 1
                continue
            if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
                data = data.decode("utf-16", "replace").encode("utf-8")
            in_file = printed_in(data, by_prefix, k, anchors)
            if in_file:
                found |= in_file
                rel = f.name if f == path else f.relative_to(path).as_posix()
                hits.append({"file": rel, "sample_rows": sum(rows_per_url[u] for u in in_file)})
            n_files += 1
            n_bytes += len(data)
            del data
        if n_files == 0:
            stop(f"--seen-dir {p}: no file could be read")
        hits.sort(key=lambda h: (-h["sample_rows"], h["file"]))
        per_path.append({"name": path.name, "kind": "file" if path.is_file() else "folder",
                         "files_read": n_files, "bytes_read": n_bytes, "files_unreadable": unreadable,
                         "spark_scratch_files_left_out": scratch, "files_with_printed_url": hits})
    return {"paths": per_path, "files_read": sum(x["files_read"] for x in per_path),
            "sample_rows_with_printed_url": sum(rows_per_url[u] for u in found)}


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------------------------------------------
# draw

def cmd_draw(args: argparse.Namespace) -> None:
    repo, out, guard = setup(args)
    seen_paths = args.seen_dir or []
    if guard:
        if args.seed is not None and args.seed != SEED:
            stop(f"the pre-registered seed is {SEED}; --seed is for tests with --no-git-guard")
        if not seen_paths:
            stop("the draw needs --seen-dir for each folder or file of earlier outputs (repeatable): the design "
                 "and build folders and the session transcripts")
        require_clean(repo)
        blobs = {rel: require_committed(repo, rel) for rel in GUARDED}
        require_pushed(repo)
        head = git(repo, "rev-parse", "HEAD")
        seed = SEED
    else:
        if args.seed is None:
            stop("a test draw (--no-git-guard) needs an explicit --seed")
        # On any frame, not only the frozen one. Only a positive seed: random.Random(-s) is random.Random(s)
        if args.seed <= 0 or args.seed == SEED:
            stop(f"a test draw (--no-git-guard) needs a positive seed other than {SEED}, the pre-registered draw, "
                 "on any frame")
        blobs, head, seed = {}, None, args.seed
    check_seen_paths(seen_paths)
    sample_path, sidecar_path = out / SAMPLE_NAME, out / SIDECAR_NAME
    for p in (sample_path, sidecar_path):
        if p.exists():
            stop(f"{p} already exists; the sample is drawn once and never overwritten")

    frame = build_frame(repo)
    for line in frame_lines(frame):
        print(line)
    diffs = frozen_differences(frame)
    if diffs and guard:
        stop("the frame differs from the frozen values:\n  " + "\n  ".join(diffs))
    for d in diffs:
        print("  TEST MODE, frame differs from the frozen values:", d)

    rows = draw_sample(frame["strata"], seed)
    seen, seconds = None, None
    if seen_paths:
        start = time.perf_counter()
        seen = seen_urls([r["url"] for r in rows], seen_paths)
        seconds = time.perf_counter() - start
    out.mkdir(parents=True, exist_ok=True)
    write_csv(sample_path, SAMPLE_COLUMNS, rows)
    sidecar = {
        "spec": SPEC,
        "git_guard": guard,
        "head": head,
        "seed": seed,
        "frame_rows": frame["frame_rows"],
        "frame_sha256": frame["frame_sha256"],
        "frame_key_format": "sha256 over the frame rows sorted by key, one 'record_id<TAB>url<TAB>park_code<LF>' each",
        "strata": [{"park_code": park, "period": period, "months": months_label(period),
                    "frame_rows": frame["sizes"][(park, period)], "sample_rows": N_PER_STRATUM[period]}
                   for park, period in STRATA],
        "inputs": frame["files"] + [{"path": A1_SAMPLE, "sha256": frame["a1_sample_sha256"]}],
        "blobs_at_head": blobs,
        "seen_urls": seen,
        "sample": {"path": f"{OUT_DIR}/{SAMPLE_NAME}", "rows": len(rows), "sha256": sha256_file(sample_path)},
    }
    sidecar_path.write_text(json.dumps(sidecar, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(f"seed {seed}: {len(rows)} rows written to {sample_path}")
    print(f"sidecar: {sidecar_path}")
    if seen is not None:
        print(f"sampled rows whose url was printed in earlier outputs: {seen['sample_rows_with_printed_url']} "
              f"({seen['files_read']} files searched in {seconds:.1f} s)")
        for x in seen["paths"]:
            print(f"  {x['kind']} {x['name']}: {x['files_read']} files, {x['bytes_read'] / 1e6:.1f} MB"
                  + (f", {x['files_unreadable']} could not be read" if x["files_unreadable"] else "")
                  + (f", {x['spark_scratch_files_left_out']} Spark scratch files left out"
                     if x["spark_scratch_files_left_out"] else "")
                  + f"; {len(x['files_with_printed_url'])} files hold a printed url")
            # Paths below the --seen-dir and counts only, never a url
            for h in x["files_with_printed_url"][:SEEN_TOP]:
                print(f"    {h['sample_rows']:>4} sampled rows  {h['file']}")


# ---------------------------------------------------------------------------------------------------------------
# score

def read_sample(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == SAMPLE_COLUMNS, f"{path}: columns {reader.fieldnames}, expected {SAMPLE_COLUMNS}"
        rows = [dict(r, sample_id=int(r["sample_id"])) for r in reader]
    assert [r["sample_id"] for r in rows] == list(range(1, len(rows) + 1)), f"{path}: sample_id is not 1..n"
    return rows


def note_kind(note: str) -> str:
    """'url_only' when the note, normalised, starts with url_only; '' for every other note (free text or empty).

    Normalised = stripped, lower-cased, and each run of spaces or dashes replaced by '_', so 'URL only' and
    'url-only' count. url_only is the only note kind: the label then comes from the title words in the URL.
    """
    low = re.sub(r"[\s-]+", "_", note.strip().lower())
    return "url_only" if low.startswith("url_only") else ""


def read_labels(path: Path, sample: list[dict]) -> dict:
    """sample_id -> (label 0/1, note). Refuses empty or unknown labels and rows that do not match the sample."""
    if not path.is_file():
        stop(f"{path} does not exist")
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != LABEL_COLUMNS:
            # shown(): a file without a header has a data row there, which holds a url
            stop(f"{path}: columns {[shown(c) for c in reader.fieldnames or []]}, expected {LABEL_COLUMNS}")
        rows = list(reader)
    by_id = {r["sample_id"]: r for r in sample}
    labels = {}
    for i, r in enumerate(rows, start=1):
        if None in r or None in r.values():
            stop(f"{path}: data row {i} does not have exactly the columns {LABEL_COLUMNS}")
        if not re.fullmatch(r"[0-9]+", r["sample_id"]):
            stop(f"{path}: data row {i} has sample_id {shown(r['sample_id'])}")
        sid = int(r["sample_id"])
        if sid not in by_id or sid in labels:
            stop(f"{path}: sample_id {sid} is not in the sample or appears twice")
        s = by_id[sid]
        if (r["record_id"], r["url"], r["park_code"]) != (s["record_id"], s["url"], s["park_code"]):
            stop(f"{path}: sample_id {sid} does not match the sample row")
        if r["label"] not in ("0", "1"):
            stop(f"{path}: sample_id {sid} has label {shown(r['label'])}; every row needs 1 or 0")
        labels[sid] = (int(r["label"]), r["note"])
    if len(labels) != len(sample):
        stop(f"{path}: {len(labels)} labels for {len(sample)} sample rows")
    return labels


def read_drafts(path: Path, sample: list[dict]) -> tuple[dict, list[dict]]:
    """sample_id -> draft ('1', '0' or '?'), and the rows for the committed copy.

    The file must have exactly the columns sample_id, draft: score copies it into the result commit, so it may
    hold no headline, lead or reason from the article.
    """
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != DRAFT_COLUMNS:
            stop(f"{path}: columns {[shown(c) for c in reader.fieldnames or []]}, expected {DRAFT_COLUMNS}; the "
                 "drafts file may hold no article text")
        rows = list(reader)
    ids = {r["sample_id"] for r in sample}
    drafts, copy = {}, []
    for i, r in enumerate(rows, start=1):
        if None in r or None in r.values():
            stop(f"{path}: data row {i} does not have exactly the columns {DRAFT_COLUMNS}")
        if not re.fullmatch(r"[0-9]+", r["sample_id"].strip()):
            stop(f"{path}: data row {i} has sample_id {shown(r['sample_id'])}")
        sid, draft = int(r["sample_id"]), r["draft"].strip()
        if sid not in ids or sid in drafts:
            stop(f"{path}: sample_id {sid} is not in the sample or appears twice")
        if draft not in ("1", "0", "?"):
            stop(f"{path}: sample_id {sid} has draft {shown(draft)}; drafts must be 1, 0 or ?")
        drafts[sid] = draft
        copy.append({"sample_id": sid, "draft": draft})
    return drafts, copy


def agreement(labels: dict, drafts: dict) -> dict:
    """Claude's content drafts against the labels: counts, share agreeing and Cohen's kappa on the 1/0 drafts."""
    pairs = [(drafts[sid], labels[sid][0]) for sid in labels if sid in drafts]
    firm = [(int(d), lab) for d, lab in pairs if d in ("0", "1")]
    agree = sum(d == lab for d, lab in firm)
    m = len(firm)
    kappa = None
    if m:
        po = agree / m
        d1 = sum(d for d, _ in firm) / m
        l1 = sum(lab for _, lab in firm) / m
        pe = d1 * l1 + (1 - d1) * (1 - l1)
        kappa = (po - pe) / (1 - pe) if pe < 1 else None
    table = Counter(f"draft {d} / label {lab}" for d, lab in pairs)
    return {"rows_with_draft": len(pairs), "firm_drafts": m, "agree": agree,
            "agree_share": agree / m if m else None, "kappa": kappa, "question_marks": sum(d == "?" for d, _ in pairs),
            "table": dict(sorted(table.items()))}


def commit_facts(repo: Path, sidecar: dict, rels: dict) -> dict:
    """The commits of the protocol, with the refusals for a wrong order or a file changed after its commit."""
    head = git(repo, "rev-parse", "HEAD")
    spec_commit = sidecar["head"]
    sample_commit = added_in(repo, rels["sample"])
    if added_in(repo, rels["sidecar"]) != sample_commit:
        stop(f"{rels['sidecar']} is not added in the sample commit {sample_commit}")
    labels_commit = added_in(repo, rels["labels"])
    if not (is_ancestor(repo, spec_commit, sample_commit) and spec_commit != sample_commit):
        stop(f"the sample commit {sample_commit} does not follow the draw's HEAD {spec_commit}")
    if not (is_ancestor(repo, sample_commit, labels_commit) and sample_commit != labels_commit):
        stop(f"the labels commit {labels_commit} does not follow the sample commit {sample_commit}")
    # No file of the protocol changes after its commit: the labels scored are the labels of commit 3a
    for key, first in (("sample", sample_commit), ("sidecar", sample_commit), ("labels", labels_commit)):
        if git(repo, "rev-parse", f"{first}:{rels[key]}") != git(repo, "rev-parse", f"HEAD:{rels[key]}"):
            stop(f"{rels[key]} changed after its commit {first}")
    changed = git(repo, "show", "--name-only", "--format=", labels_commit).splitlines()
    if changed != [rels["labels"]]:
        stop(f"the labels commit {labels_commit} changes {', '.join(changed)}; it must change only {rels['labels']}")
    # Reported, not refused: the spec and the code that score now should be the ones the draw ran with
    edited = sorted(rel for rel, blob in sidecar["blobs_at_head"].items()
                    if subprocess.run(["git", "-C", str(repo), "rev-parse", f"HEAD:{rel}"],
                                      capture_output=True, text=True).stdout.strip() != blob)
    return {"spec_and_code": spec_commit, "sample": sample_commit, "labels": labels_commit, "head": head,
            "labels_commit_files": changed, "labels_commit_only_labels": changed == [rels["labels"]],
            "changed_since_draw": edited}


def draw_inputs(repo: Path, sidecar: dict) -> list[Path]:
    """The matches files that the draw read, each with the sha256 the sidecar recorded (the A1 sample too).

    A shard exported again after the draw goes into a newer pull_date= folder and is not read here; a changed or
    missing input file is refused.
    """
    inputs = sidecar["inputs"]
    for item in inputs:
        path = repo / item["path"]
        if not path.is_file():
            stop(f"{item['path']}, an input of the draw, does not exist")
        if sha256_file(path) != item["sha256"]:
            stop(f"{item['path']} differs from the file the draw read (sha256 in the sidecar)")
    exports = [repo / item["path"] for item in inputs if item["path"] != A1_SAMPLE]
    if not exports or len(exports) != len(inputs) - 1:
        stop(f"the sidecar must list the exported matches files and {A1_SAMPLE} as inputs")
    return exports


def score_numbers(frame: dict, sample: list[dict], labels: dict) -> dict:
    """Every number of the result, from the frame sizes, the sample and the labels."""
    w = weights(frame["sizes"])
    n, x, u, xu = (Counter() for _ in range(4))
    for r in sample:
        h = (r["park_code"], period_of(r["file_ts"]))
        label, note = labels[r["sample_id"]]
        url_only = note_kind(note) == "url_only"
        n[h] += 1
        x[h] += label
        u[h] += url_only
        xu[h] += label if url_only else 0
    assert all(n[h] == N_PER_STRATUM[h[1]] for h in STRATA), f"rows per stratum {dict(n)}"

    p = estimate(w, x, n)
    lo, hi = interval(p, w, x, n)
    p_url0 = estimate(w, {h: x[h] - xu[h] for h in STRATA}, n)
    p_urlx = estimate(w, {h: x[h] - xu[h] for h in STRATA}, {h: n[h] - u[h] for h in STRATA})
    parks = {}
    for park in PARKS:
        xp = sum(x[(park, period)] for period in PERIODS)
        np_ = sum(n[(park, period)] for period in PERIODS)
        parks[park] = {"x": xp, "n": np_, "precision": xp / np_, "wilson_95": wilson(xp, np_),
                       "below_bar": xp <= PARK_BAR}
    xb = sum(x[(park, "B")] for park in PARKS)
    nb = sum(n[(park, "B")] for park in PARKS)
    return {
        "P": float(p), "P_fraction": f"{p.numerator}/{p.denominator}", "pass": p >= BAR, "bar": float(BAR),
        "interval_95": [lo, hi],
        "strata": [{"park_code": park, "period": period, "months": months_label(period),
                    "frame_rows": frame["sizes"][(park, period)], "weight": float(w[(park, period)]),
                    "n": n[(park, period)], "x": x[(park, period)], "url_only": u[(park, period)]}
                   for park, period in STRATA],
        "parks": parks, "parks_below_bar": [park for park in PARKS if parks[park]["below_bar"]],
        "period_B": {"x": xb, "n": nb, "precision": xb / nb, "wilson_95": wilson(xb, nb)},
        "url_only": {"rows": sum(u.values()), "labeled_1": sum(xu.values()),
                     "P_as_0": float(p_url0), "P_excluded": None if p_urlx is None else float(p_urlx)},
    }


def score_lines(res: dict, design: list[str]) -> list[str]:
    def pct(v):
        return "n/a" if v is None else f"{v:.4f}"

    c = res.get("commits")
    lines = ["A1 PART 2 SCORE (docs/week8/a1_part2_spec.md)"]
    if c:
        lines += [f"  commits: spec and code {c['spec_and_code']}, sample {c['sample']}, labels {c['labels']}, "
                  f"HEAD {c['head']} (pushed)",
                  "  the labels commit changes only the labels file: "
                  f"{'yes' if c['labels_commit_only_labels'] else 'no'} ({', '.join(c['labels_commit_files'])}); "
                  "the sample, the sidecar and the labels are unchanged since their commits",
                  "  the spec and the code at HEAD are the ones the draw ran with: "
                  + ("yes" if not c["changed_since_draw"] else
                     f"NO, changed since the draw: {', '.join(c['changed_since_draw'])}")]
    else:
        lines.append("  TEST MODE (--no-git-guard): no git checks")
    lines += [f"  frame: {res['frame_rows']:,} rows, sha256 {res['frame_sha256']} (= the sidecar), from the "
              f"{len(res['inputs'])} input files of the draw (sha256 unchanged)",
              f"  sample: {res['sample_rows']} rows, redrawn with seed {res['seed']}: identical to the committed file",
              "STRATA",
              "  park  period  months              N_h       W_h    n   x  url_only"]
    for s in res["strata"]:
        lines.append(f"  {s['park_code']}  {s['period']}       {s['months']}  {s['frame_rows']:>7,}  {s['weight']:.6f}"
                     f"  {s['n']:>3} {s['x']:>3}  {s['url_only']:>8}")
    verdict = (f"PASS: P = {res['P']:.4f} >= 0.90. A1's fourth condition is met for rule R0."
               if res["pass"] else
               f"NOT MET: P = {res['P']:.4f} < 0.90. A1's fourth condition stays not met, R0 is not changed, and "
               "the news result is marked below the bar.")
    lines += ["PRIMARY",
              f"  P = sum W_h x_h / n_h = {res['P']:.6f} (exactly {res['P_fraction']})",
              f"  95% interval {res['interval_95'][0]:.4f}-{res['interval_95'][1]:.4f} "
              "(stratified normal, p_h = (x_h + 1) / (n_h + 2), clipped to [0, 1])",
              f"  {verdict}",
              "  how to read a result near the bar:"] + design
    lines += ["PER PARK (x of 40, both periods; below the bar when x <= 32)"]
    for park, v in res["parks"].items():
        lines.append(f"  {park}  {v['x']:>2} / {v['n']}  {v['precision']:.3f}  Wilson {v['wilson_95'][0]:.3f}-"
                     f"{v['wilson_95'][1]:.3f}  {'BELOW THE BAR' if v['below_bar'] else 'at or above the bar'}")
    lines.append("  parks below the bar: " + (", ".join(res["parks_below_bar"]) or "none"))
    b, u = res["period_B"], res["url_only"]
    lines += ["DESCRIPTIVE",
              f"  period B (2024-01..2025-12), 50 rows: {b['x']} / {b['n']} = {b['precision']:.3f}, Wilson "
              f"{b['wilson_95'][0]:.3f}-{b['wilson_95'][1]:.3f}",
              f"  url_only rows (labeled from the title words in the URL): {u['rows']}, {u['labeled_1']} of them "
              f"labeled 1; P with them as 0 = {pct(u['P_as_0'])}, P without them = {pct(u['P_excluded'])}"]
    seen = res["seen_urls"]
    lines.append("  sampled rows whose url had been printed in earlier outputs: "
                 + ("not counted at the draw" if seen is None else
                    f"{seen['sample_rows_with_printed_url']} (" + ", ".join(
                        f"{x['kind']} {x['name']}: {x['files_read']} files, "
                        f"{len(x['files_with_printed_url'])} with a printed url" for x in seen["paths"]) + ")"))
    a = res["drafts"]
    if a is None:
        lines.append("  Claude's content drafts: no drafts file")
    else:
        lines.append(f"  Claude's content drafts (agreement only): {a['rows_with_draft']} rows, {a['question_marks']} "
                     f"'?', {a['agree']} of {a['firm_drafts']} firm drafts agree "
                     f"({pct(a['agree_share'])}), kappa {pct(a['kappa'])}; " + ", ".join(
                         f"{k}: {v}" for k, v in a["table"].items()))
        lines.append(f"  drafts copy {a['copy']['path']}: {a['copy']['rows']} rows, sha256 {a['copy']['sha256']}")
    return lines


def cmd_score(args: argparse.Namespace) -> None:
    repo, out, guard = setup(args)
    rels = {"sample": f"{OUT_DIR}/{SAMPLE_NAME}", "sidecar": f"{OUT_DIR}/{SIDECAR_NAME}",
            "labels": f"{OUT_DIR}/{LABELS_NAME}"}
    if guard:
        require_clean(repo)
        for rel in rels.values():
            require_committed(repo, rel)
        require_pushed(repo)
    sample_path, sidecar_path, labels_path = out / SAMPLE_NAME, out / SIDECAR_NAME, out / LABELS_NAME
    for p in (out / SCORE_JSON, out / SCORE_TXT, out / DRAFTS_COPY):
        if p.exists():
            stop(f"{p} already exists; the labels are scored once")
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    if guard:
        if not sidecar["git_guard"]:
            stop("the sidecar comes from a test draw (--no-git-guard)")
        if sidecar["seed"] != SEED:
            stop(f"the sidecar seed {sidecar['seed']} is not {SEED}")
    elif (sidecar.get("git_guard") or not isinstance(sidecar["seed"], int) or isinstance(sidecar["seed"], bool)
          or sidecar["seed"] <= 0 or sidecar["seed"] == SEED):
        stop(f"test mode scores only a test draw with a positive seed other than {SEED} (not a guarded draw)")
    commits = commit_facts(repo, sidecar, rels) if guard else None
    if sha256_file(sample_path) != sidecar["sample"]["sha256"]:
        stop(f"{sample_path} differs from the file the draw wrote (sha256 in the sidecar)")
    sample = read_sample(sample_path)
    labels = read_labels(labels_path, sample)
    drafts_path = Path(args.drafts) if args.drafts else repo / DRAFTS
    if args.drafts and not drafts_path.is_file():
        stop(f"--drafts {drafts_path} does not exist")
    drafts, drafts_copy = read_drafts(drafts_path, sample) if drafts_path.is_file() else (None, None)

    exports = draw_inputs(repo, sidecar)
    frame = build_frame(repo, exports)
    if frame["frame_sha256"] != sidecar["frame_sha256"]:
        stop("the frame differs from the one in the sidecar")
    if guard and frozen_differences(frame):
        stop("the frame differs from the frozen values: " + "; ".join(frozen_differences(frame)))
    redraw = draw_sample(frame["strata"], sidecar["seed"])
    if [{k: str(v) for k, v in r.items()} for r in redraw] != [{k: str(v) for k, v in r.items()} for r in sample]:
        stop("the sample is not what the seed in the sidecar draws from this frame")

    res = score_numbers(frame, sample, labels)
    design, design_numbers = design_lines(frame["sizes"])
    res.update({"frame_rows": frame["frame_rows"], "frame_sha256": frame["frame_sha256"], "seed": sidecar["seed"],
                "inputs": [f["path"] for f in frame["files"]] + [A1_SAMPLE],
                "sample_rows": len(sample), "design": design_numbers, "seen_urls": sidecar.get("seen_urls"),
                "drafts": None if drafts is None else agreement(labels, drafts), "commits": commits,
                "git_guard": guard})
    if drafts is not None:
        # The committed copy of the drafts (commit 3b), with its sha256 in the score file
        write_csv(out / DRAFTS_COPY, DRAFT_COLUMNS, drafts_copy)
        res["drafts"]["copy"] = {"path": f"{OUT_DIR}/{DRAFTS_COPY}", "rows": len(drafts_copy),
                                 "sha256": sha256_file(out / DRAFTS_COPY), "source_sha256": sha256_file(drafts_path)}
    lines = score_lines(res, design)
    for line in lines:
        print(line)
    (out / SCORE_JSON).write_text(json.dumps(res, indent=2) + "\n", encoding="utf-8", newline="\n")
    (out / SCORE_TXT).write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print(f"written: {out / SCORE_JSON}, {out / SCORE_TXT}" + ("" if drafts is None else f", {out / DRAFTS_COPY}"))


# ---------------------------------------------------------------------------------------------------------------
# check-labels

def cmd_check_labels(args: argparse.Namespace) -> None:
    """Before commit 3a: read the labels file as score will, against the committed sample.

    score refuses any change to the labels after commit 3a, so a file that score would refuse must be found before
    that commit. No git checks and nothing written. Nothing about the labels is printed but OK or the reason for a
    refusal: the labels are checked here, not scored.
    """
    repo = Path(args.repo_root).resolve()
    out = Path(args.out_dir).resolve() if args.out_dir else repo / OUT_DIR
    sample_path, sidecar_path = out / SAMPLE_NAME, out / SIDECAR_NAME
    for p in (sample_path, sidecar_path):
        if not p.is_file():
            stop(f"{p} does not exist")
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    if sha256_file(sample_path) != sidecar["sample"]["sha256"]:
        stop(f"{sample_path} differs from the file the draw wrote (sha256 in the sidecar)")
    sample = read_sample(sample_path)
    labels_path = Path(args.labels).resolve() if args.labels else out / LABELS_NAME
    read_labels(labels_path, sample)
    print(f"labels file OK: {labels_path}")
    print(f"  the six columns of the download and one row for each of the {len(sample)} sample rows, with the "
          "sample's record_id, url and park_code and a label 1 or 0; score reads it as it is, so commit it unchanged")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    parsers = {
        "check": sub.add_parser("check", help="frame per stratum, weights, check values; draws nothing"),
        "draw": sub.add_parser("draw", help="draw the sample once (after commit 1 is pushed)"),
        "check-labels": sub.add_parser("check-labels", help="before commit 3a: is the labels file what score reads? "
                                                            "No git checks; writes nothing"),
        "score": sub.add_parser("score", help="score the committed labels once (after commit 3a is pushed)"),
    }
    for name, p in parsers.items():
        p.add_argument("--repo-root", default=str(ROOT), help="repository root (default: the parent of src/)")
        if name in ("draw", "score"):
            p.add_argument("--no-git-guard", action="store_true", help="tests only; needs --out-dir")
            p.add_argument("--out-dir", help="tests only: a folder outside the repository, instead of docs/week8")
    parsers["draw"].add_argument("--seed", type=int, help=f"tests only; the draw always uses {SEED}")
    parsers["draw"].add_argument("--seen-dir", action="append",
                                 help="a folder or a file of earlier outputs; counts the sampled rows whose url is "
                                      "printed there (repeatable; the guarded draw needs at least one)")
    parsers["check-labels"].add_argument("--labels", help=f"the labels file (default: {OUT_DIR}/{LABELS_NAME})")
    parsers["check-labels"].add_argument("--out-dir", help="tests only: the folder of a test sample and its sidecar, "
                                                           "instead of docs/week8")
    parsers["score"].add_argument("--drafts", help=f"Claude's content drafts, columns sample_id, draft "
                                                   f"(default: {DRAFTS})")
    args = ap.parse_args()
    {"check": cmd_check, "draw": cmd_draw, "check-labels": cmd_check_labels, "score": cmd_score}[args.cmd](args)


if __name__ == "__main__":
    main()
