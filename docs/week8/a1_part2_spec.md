# Week 8 test A1 part 2: pre-registered precision check of the URL rule

This document fixes the rules of A1 part 2 before the sample is drawn. It is committed in one commit (commit 1) with
the code that draws, shows and scores the sample, and with the rules of the news forecast test (`news_test_spec.md`).
`src/gdelt_sample.py draw` refuses to run unless this document is committed and unchanged. When it was committed, the
pre-registered seed had never been used on the frame and no sampled row had been labeled. The result document,
`docs/week8/a1_part2_result.md`, gives this commit's hash.

A1 (`a1_gdelt_test.md`) did not meet its fourth condition: 34 of 100 matches were about the park. It proposed a URL
rule and named the next step (lines 71-77):

> "keep a match only when the park's own name appears in the article URL path (`yellowstone`, `yosemite`,
> `grand canyon`, `zion`, `smoky`/`smokies`). It keeps 28 matches, 27 of them about the park (96%), and finds 27 of
> the 34 relevant matches (79%). ... The URL rule was chosen after seeing these labels, so the next step is to label
> a fresh random sample of matches that pass it before the news signal is used in the evaluation."

| Wording | Rule in this check |
| --- | --- |
| "at least 90% of checked matches are about the park" (the proposal's A1 condition, `a1_gdelt_test.md`:11) | the weighted precision P of the rows that pass the rule is at least 0.90 |
| "the park's own name appears in the article URL path" | rule R0 below, frozen in `src/gdelt_url_rule.py` |
| "a fresh random sample of matches that pass it" | 200 random rule-passing rows from 2015-02-18 to 2025-12-31, outside the A1 test week and without the URLs of the A1 sample: 40 per park, 30 from 2015-02..2023-12 and 10 from 2024-01..2025-12 |
| "about the park" | A1's definition: 1 = the park itself is the main subject or a central part of the article (`a1_gdelt_test.md`:50-51), read from the article, not from the URL |
| "before the news signal is used in the evaluation" | the news test reads this check's score file and runs only after it is committed |

## Rule R0

R0 is the A1 rule made precise. It is defined once, in `src/gdelt_url_rule.py`, and every user takes it from there:
`src/gdelt_sample.py` builds the frame with `passes()`, and `07_gdelt_news` builds the Silver column `url_rule` with
the Spark SQL that the same module writes from the same strings.

1. Lower-case the URL (the GKG `DocumentIdentifier`, column `url` of the exports).
2. The path is the text after `scheme://host` and before the first `?` or `#`: group 1 of
   `^[a-z][a-z0-9+.-]*://[^/?#]*([^?#]*)`. It is not percent-decoded. The host, the query and the fragment are not
   part of it. A URL without `scheme://` has an empty path.
3. A match row (record_id, url, park_code) passes when the pattern of its own park occurs anywhere in the path. It is
   a substring search with no word boundaries. Nothing else is included or excluded.

```
YELL  yellowstone
YOSE  yosemite
GRCA  grand([^a-z0-9]|%20)?canyon      grand-canyon, grand_canyon, grand+canyon, grandcanyon, grand%20canyon
ZION  zion
GRSM  smoky|smokies
```

The patterns hold no backslash and no quote, so the same text works in Python, Spark SQL (RLIKE) and DuckDB. No URL
from 2015-02-18 to 2025-12-31 has a non-ASCII character, so lower-casing gives the same result in all three.

**Check values** on the exports of `pull_date=2026-11-13` (636,731 match rows):

| | YELL | YOSE | GRCA | ZION | GRSM | All |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Match rows | 280,730 | 184,569 | 59,948 | 50,708 | 60,776 | 636,731 |
| Pass R0 | 94,952 | 44,899 | 24,668 | 9,639 | 10,062 | 184,220 |
| Pass rate | 33.8% | 24.3% | 41.1% | 19.0% | 16.6% | 28.9% |

On the A1 labels (`gdelt_precision_sample.csv`, 100 rows), R0 keeps 28 rows, 27 of them about the park, and 34 rows
are about the park in all.

`uv run python src/gdelt_url_rule.py` checks the 636,731 rows, the passes per park and the three A1 values, and 28
hand-made URLs for the points of the rule. Notebook 07 checks on every run that Python `passes()` and Spark
`url_rule` agree on every row; on local PySpark 4.3.0 they agree on all 636,731 rows.

### What was fixed after seeing which data

The A1 text does not say what the path covers or how "grand canyon" is written in a URL. R0 fixes the points below
and nothing else.

- **Own park.** A match passes only on the pattern of its own park_code. This is A1's wording ("the park's own
  name"), and on the A1 labels it is the only reading that gives A1's 28 kept and 27 about the park. Accepting any of
  the five names keeps 29: it adds CSV line 6, a YOSE match whose URL names the Grand Canyon, labeled 0.
- **Host, query, decoding, separators, word boundaries.** These were fixed on 2026-11-15 after reading the URLs of
  all 636,731 matches, not labels. The 100 labels cannot decide them: each of the last five variants in the table
  below keeps the same 28 rows as R0, 27 of them about the park, and no labeled URL has a park name in its host or
  query. R0 takes the literal meaning of "URL path".

| Choice | R0 | Decided the other way | Rows that pass, against R0 |
| --- | --- | --- | ---: |
| Park name | the row's own park | any of the five patterns | +5,756 |
| Host | not part of the path | host + path | +1,614 (YELL 265, YOSE 15, GRCA 776, GRSM 558; mostly grandcanyonnews.com, smokymountainnews.com and westyellowstonenews.com) |
| Query and fragment | not part of the path | path + query | +180 |
| Percent-decoding | none | decode the path first | 0 |
| Grand Canyon separator | none, one character that is not a letter or digit, or `%20` | a dash only | -201 |
| Word boundaries | none, a substring | the name as a word, with an optional s | -176 |

- **No exclusions.** Some R0 passes are clearly wrong: Zion Williamson (9 rows), OS X Yosemite (9), Grand Canyon
  University (6), smoky weather (24), zionism (2) and others, 73 rows, 0.04% of passes. A wider set of URL heuristics
  (the *Yellowstone* TV series, the Skywalk, the Yellowstone Club and similar) would remove 1,820 rows, 0.99% of
  passes: YELL 990, YOSE 17, GRCA 774, ZION 12, GRSM 27. They are not removed. The heuristics have unknown accuracy
  (the TV-cast part also drops documentaries about the park), their TV-series part copies A1's one false positive
  (CSV line 92), and a changed rule would not be the one A1 proposed. If all 1,820 were wrong, precision would fall
  by about 1 point. The sample measures how often such rows get through.
- **Left to the labels.** Ambiguous groups among R0 passes: the Yellowstone River (2,210 rows), the Grand Canyon
  national monument (716), West Yellowstone (686), smoky not followed by mountain(s) (227), zion inside a longer word
  (13).

## The frame

The frame is built from the committed exports, the newest pull of each shard, as Silver keeps them:
`data/landing/gdelt_matches/pull_date=2026-11-13/matches_s1.csv.gz` to `matches_s4.csv.gz`.

| Step | Rows | Pass R0 |
| --- | ---: | ---: |
| file_ts from 2015-02-18 to 2025-12-31 (the months of the 660 park-months) | 612,919 | 174,604 |
| minus the A1 test week, 20250106000000 <= file_ts < 20250113000000 | -388 | -85 |
| minus any other row whose url is in `gdelt_precision_sample.csv` | -2 | 0 |
| **Frame: the rows that pass R0** | | **174,519** |

- The unit is one match row (record_id, url, park_code), the unit that Gold counts, so P is the precision of the rows
  that Gold counts in `news_url`, apart from the 85 of the A1 test week. record_id alone is not unique.
- Failing rows are not sampled. 438,010 failing rows stay outside the frame.
- Per park: YELL 89,967 (0.5155), YOSE 43,086 (0.2469), GRCA 23,084 (0.1323), GRSM 9,644 (0.0553), ZION 8,738
  (0.0501).
- Frame sha256: `d231bfd61b17313ca43102bba3c244950584ef6273de39b460b7fc1e6de7f23e`, over the frame keys sorted as in
  the draw below, one `record_id<TAB>url<TAB>park_code<LF>` line each, in UTF-8.

These values are frozen in `src/gdelt_url_rule.py` and `src/gdelt_sample.py`. `uv run python src/gdelt_sample.py
check` prints them and exits with code 1 if any differs; `draw` refuses to run on any other frame. `score` rebuilds
the frame from the match files that the draw read, listed in the sidecar with their sha256, so a later export into a
newer `pull_date=` folder does not change it.

## Strata, sizes and the draw

The strata are park x period of file_ts. Period A is 2015-02 to 2023-12 and gets 30 rows per park. Period B is
2024-01 to 2025-12, the years of the news test's held-out months, and gets 10 rows per park. 200 rows in all.

| Park | Period | Months | N_h | W_h | n_h |
| --- | --- | --- | ---: | ---: | ---: |
| GRCA | A | 2015-02..2023-12 | 18,895 | 0.108269 | 30 |
| GRCA | B | 2024-01..2025-12 | 4,189 | 0.024003 | 10 |
| GRSM | A | 2015-02..2023-12 | 8,680 | 0.049737 | 30 |
| GRSM | B | 2024-01..2025-12 | 964 | 0.005524 | 10 |
| YELL | A | 2015-02..2023-12 | 78,290 | 0.448604 | 30 |
| YELL | B | 2024-01..2025-12 | 11,677 | 0.066910 | 10 |
| YOSE | A | 2015-02..2023-12 | 40,740 | 0.233442 | 30 |
| YOSE | B | 2024-01..2025-12 | 2,346 | 0.013443 | 10 |
| ZION | A | 2015-02..2023-12 | 7,903 | 0.045284 | 30 |
| ZION | B | 2024-01..2025-12 | 835 | 0.004785 | 10 |

- 40 rows per park, so each park gets its own estimate. A simple sample of 200 would follow the frame shares and give
  ZION about 10 rows and GRSM about 11.
- Period B has its own rows because 2024-2025 is a small part of each park's frame: YOSE 5.4%, ZION 9.6%, GRSM 10.0%,
  YELL 13.0%, GRCA 18.1%. With 40 random rows per park, YOSE would get about 1.7 rows from the held-out months
  2024-01..2025-09. The split raises the standard error of P at 0.90 from 2.81 to 2.94 points.

**The draw** (`src/gdelt_sample.py draw`, seed 20261116):
1. Each stratum's rows are sorted by (record_id, url, park_code), compared as Python compares strings (by code
   point).
2. One `random.Random(20261116)` does all the drawing. The strata come in the fixed order GRCA A, GRCA B, GRSM A,
   GRSM B, YELL A, YELL B, YOSE A, YOSE B, ZION A, ZION B. Each stratum is shuffled and its first n_h rows are taken,
   as in `gdelt_gkg.py sample`.
3. The 200 rows are shuffled once more for the page order, so the page does not follow the strata. `sample_id` =
   1..200 follows that order.
4. It writes `docs/week8/a1_part2_sample.csv` (sample_id, record_id, file_ts, park_code, url) and never overwrites
   it.
5. It writes the sidecar `docs/week8/a1_part2_sample.json`: the HEAD hash, the seed, whether the git guards ran, the
   frame rows and sha256, the stratum sizes, the inputs (path, sha256 and rows of each of the four match files, and
   the sha256 of `gdelt_precision_sample.csv`), the blobs of the guarded files at HEAD, the seen-URL count and the
   sha256 of the sample.

**The seen-URL count.** The draw counts how many sampled rows have a URL that was printed in earlier outputs. It
searches every file under each `--seen-dir`, a folder or a single file. The real draw names four paths on the laptop
that ran the build. `<scratchpad>` is the scratchpad folder of the Claude Code session that designed and built this
check, and `<session>` is that session's folder under the user's Claude Code projects folder:
- `<scratchpad>\gdelt_design`, the design outputs of 2026-11-15;
- `<scratchpad>\gdelt_build`, the build and test outputs, from 2026-11-15 on;
- `<session>`, the transcripts of the session's subagents, its tool results and its workflow scripts;
- `<session>.jsonl`, the main transcript of the session.

How it counts:
- Each file is read as bytes, one file at a time; a file that starts with a UTF-16 byte order mark is decoded first.
  Files whose names start with `a1_part2_sample` are skipped, because a test sample is not an earlier output. A file
  that cannot be read is counted as unreadable, and a path from which no file can be read stops the draw.
- Spark's scratch files of local runs are left out and counted: a file below a folder named `spark_tmp`, `warehouse`
  or `blockmgr-...`, or a file ending in `.data`, `.index`, `.parquet` or `.crc`. They hold data rows that no one
  read.
- A URL counts as printed when a file holds the whole URL and what follows does not make it a longer URL: the end of
  the text, a byte other than an ASCII letter, a digit or one of `/ _ - % = & + ? # ~`, or a run of `. , : ; ! ) '`
  that is not followed by such a URL character. So `see <url>.` and `(<url>).` count, and `<url>.html` does not.
- The sidecar records, for each path, its name (the last part of the path, not the full path; for `<session>` that
  is the session id), whether it is a folder or a file, the files read, the bytes read, the files that could not be
  read and the Spark scratch files left out, and each file that
  holds a printed URL: its path below the `--seen-dir` and how many sampled rows have a URL printed in it, never the
  URL. Then the files read in all and the count of sampled rows. The draw prints the 5 files of each path that hold
  the most sampled rows.
- A test output that copies rows of the exports or of a test sample would count as printed, so such files are
  deleted before the draw (see the commit table).

## Estimator, interval and pass rule

```
N      = 174,519, the frame rows;  N_h, n_h as in the table
x_h    = sampled rows of stratum h labeled 1
W_h    = N_h / N, exact fractions (the table rounds them to 6 decimals)
P      = sum over the 10 strata of W_h * x_h / n_h
p_h    = (x_h + 1) / (n_h + 2)
95%    = P -/+ 1.96 * sqrt( sum over h of W_h^2 * p_h * (1 - p_h) / n_h ), clipped to [0, 1]
PASS   when P >= 9/10, compared as exact fractions
```

- P uses every row as labeled, including the url_only rows below.
- The interval is centred on P. Its variance uses p_h, so a stratum with every row labeled 1 still adds variance.
- The bar 0.90 is A1's.

**How to read a result near the bar.** The standard error of P at a true precision of 0.90 in every stratum is 2.94
points (95%: +-5.8 points). The chance of a PASS, when every stratum has true precision t:

| t | 0.85 | 0.88 | 0.90 | 0.93 |
| --- | ---: | ---: | ---: | ---: |
| Exact (binomial, P >= 9/10) | 0.0695 | 0.2764 | 0.5226 | 0.8796 |
| Normal approximation | 0.0764 | 0.2649 | 0.5000 | 0.8850 |

`score` writes these values into its output next to the result.

**Consequences.**
- PASS: A1's fourth condition is met for rule R0.
- NOT MET: A1's fourth condition stays not met, R0 is not changed, and the whole news result is marked below the bar.

`score` prints exactly one of these two sentences, with `<P>` to 4 decimals. The comparison uses the exact fraction,
which `score` prints before the sentence, as a fraction and to 6 decimals, so a P just below 0.90 can read 0.9000 in
a NOT MET sentence:
- "PASS: P = `<P>` >= 0.90. A1's fourth condition is met for rule R0."
- "NOT MET: P = `<P>` < 0.90. A1's fourth condition stays not met, R0 is not changed, and the news result is marked
  below the bar."

## Per-park bar

Each park has 40 rows, 30 from period A and 10 from period B. Its precision is x/40, unweighted, with a Wilson 95%
interval. Unweighted, period B has a quarter of each park's rows, against 5.4% (YOSE) to 18.1% (GRCA) of its frame.
A park with x <= 32 of 40 is marked below the bar in the news forecast test, whatever P is. At 32 of 40 the Wilson
interval is 0.652-0.895, so its upper end is below 0.90; at 33 of 40 it is 0.680-0.913.

The pooled bar alone would let a weak small park through, because YELL has 51.55% of the weight. For example, YELL
39, YOSE 37, GRCA 36, GRSM 30 and ZION 30 of 40 (29 + 10, 28 + 9, 27 + 9, 23 + 7 and 23 + 7 over periods A and B)
give P = 0.930, a PASS, with two parks at 75%. The news test counts parks one by one, so each park needs its own
mark.

## Labels

Every row is labeled from the article content, its headline and first paragraphs, not from the URL. The definition
is A1's. The reading order and the rulings below were fixed before the draw. The label page shows the same text at
the top.

**How to read a row.** The label comes from the article's headline and first paragraphs, from the first of these
sources that shows them:
1. The fetched title and lead shown in the row.
2. The **article** link, when the row shows no fetched text, when the text was fetched from a different address than
   the article link (the row then shows that address), or when the text does not settle the label.
3. The **archive copy**, `https://web.archive.org/web/<file_ts>/<url>`, when the article does not load or hides its
   text (a paywall, a login or cookie wall, a removed page).
4. If none of them shows the text: the title words in the URL. Then click **url_only**, which puts url_only at the
   start of the note.

**Rulings.**
- **1** = the park itself is the main subject or a central part of the article. **0** = anything else. Every row
  needs a 1 or a 0.
- Each row is labeled for its own park, the one named in the row. An article that is in the sample for two parks is
  labeled once for each park.
- Things that only share the name are 0: the TV series *Yellowstone*, the Yellowstone Club, Grand Canyon University,
  Zion Williamson, macOS Yosemite, the Smokies baseball team.
- Places outside the park are 0 unless the park itself is central: the Yellowstone River in Montana, Yellowstone
  County, Grand Canyon West or the Skywalk, the national monument next to the Grand Canyon, West Yellowstone or
  Gardiner town news.
- An article about national parks (NPS policy, fees, reservations, visitation figures, a ranking of national parks)
  is 1 when this park has its own item, paragraph or headline mention.
- A list of mixed destinations or landmarks is 0. A trip to a gateway town that includes the park is 1.
- The note is free text. The only note with a meaning is one that starts with url_only.

| Ruling | A1 precedent (line of `gdelt_precision_sample.csv`, label) |
| --- | --- |
| Things that only share the name | 92, the TV series, 0 |
| An article about national parks | 28, 34, 61, 71 and 81, all 1 |
| A list of mixed destinations or landmarks | 6 and 7, both 0 |
| A trip to a gateway town that includes the park | 49 and 72, both 1 |

`score` reads a note as url_only when the note, stripped, lower-cased and with each run of white space or dashes
replaced by `_`, starts with `url_only`; so "URL only" and "url-only" count. Every other note is free text. No row is
relabeled after the labels are committed.

## Labeling procedure

1. **Snippets.** After commit 2, Claude fetches each sampled article's headline and first paragraphs into
   `data/work/gdelt/a1_part2_snippets.csv` (sample_id, fetched_url, status, title, lead), one row for each
   sample_id. A URL that is in the sample for two parks has two sample_ids and gets a row for each.
   - `title` is the article's headline. `lead` is the first paragraphs of the article text, at most 3 paragraphs and
     at most 1,500 characters, copied without edits.
   - The text comes from the article. The archive copy is fetched only when the article itself gives no text.
     `fetched_url` is the address the text came from, after redirects.
   - `status` is `ok` when the row has a title or a lead. Otherwise it says why there is none: `http_<code>` (such as
     `http_404`), `paywall`, `login_wall`, `cookie_wall`, `removed`, `timeout`, `blocked` or `error`.
   - `gdelt_label_page.py` refuses any other status, `ok` on a row without text, another status on a row with text,
     and a lead of more than 1,500 characters. It does not count paragraphs.
2. **Drafts.** Claude writes its own label for each row, from the fetched content, into
   `data/work/gdelt/a1_part2_claude_drafts.csv`, before the labeling starts. The file has exactly the columns
   sample_id and draft (1, 0 or ?), and holds no article text. The labeler never sees it. The drafts are compared with
   the labels as agreement only and never enter P.
   - `gdelt_label_page.py` builds the page only when this file exists and has a draft for every sample row, in the
     format that `score` reads. It prints the file's sha256 and never puts a draft on the page.
   - `a1_part2_result.md` quotes the sha256 printed when the page was first built. `score` records the sha256 of the
     drafts file it reads (`source_sha256`); if the two differ, the drafts changed after the page was built, and the
     result reports it. The code does not check the order in any other way.
3. **Page.** `uv run python src/gdelt_label_page.py` writes `data/work/gdelt/a1_part2_label.html`. For each row it
   shows:
   - the sample_id and the park name;
   - the fetched title and lead, or "(not fetched: `<status>`)" when the row has neither ("missing" when the snippets
     file has no row for that sample_id);
   - "text fetched from: `<address>`" when the fetched address differs from the URL, and "(no address recorded)" when
     the row has fetched text but no fetched address;
   - the article link, whose text is the URL, and the archive link;
   - a choice "1 about this park" / "0 not about it", and a note box with a `url_only` button.

   At the top it shows the reading order, the rulings and how the labels are saved, in English. It shows no stratum
   label, no draft, no running precision and no count of 1s; only "labeled n / 200". The URL and the archive link,
   which holds file_ts, are shown, so a row's date can be read from them.
4. **Labeling.** The team member labels every row on the page.
   - The labels are saved in the browser under the localStorage key `gdelt_a1p2_labels_v1`, keyed by sample_id and
     tied to the sha256 of the sample file, so labels saved for another sample are never mixed in.
   - Each change re-reads what is saved and writes back only the changed field of the changed row, so two open tabs
     of the page do not overwrite each other's rows.
   - "Save progress" downloads `a1_part2_label_progress.json` at any time: the sample's sha256, the key, the time and
     each row's label and note. "Load progress" reads such a file back after one confirmation; its rows replace the
     same rows in the browser and other rows stay. It refuses, and changes nothing, a file that is not a progress file
     of the page, a file saved from a different sample, and a file with a sample_id outside the sample or a row that
     is not a label (1, 0 or empty) with a note.
   - "Clear all" deletes every label and note saved in the browser for the page, after one confirmation.
5. **Download.** "Download CSV" works only when every row has a 1 or a 0. It saves `a1_part2_labels.csv` with columns
   sample_id, record_id, url, park_code, label, note. The note holds `url_only` or a few words of the labeler's own,
   never copied article text. The file is moved to `docs/week8/` unchanged: `score` refuses a file whose header
   differs from these six columns, which opening and saving it in a spreadsheet program can cause.
6. **Check.** Before commit 3a, `uv run python src/gdelt_sample.py check-labels` reads `docs/week8/a1_part2_labels.csv`
   against the committed sample exactly as `score` will, with no git checks, and writes nothing. It prints OK or the
   reason `score` would refuse the file, and nothing about the labels themselves: no count of 1s and no precision.
   If it refuses, the file is downloaded from the page again, moved unchanged and checked again. Commit 3a is made
   only after it prints OK. If `score` still refuses the committed labels file, nothing is scored: `score` refuses
   any later change to that file, and the refusal is reported in `a1_part2_result.md`.

## What the score reports

`uv run python src/gdelt_sample.py score` writes `docs/week8/a1_part2_score.json` and `a1_part2_score.txt`, the
numbers for `a1_part2_result.md`, and `docs/week8/a1_part2_claude_drafts.csv`, the copy of Claude's drafts:
- P, exactly as a fraction and to 6 decimals, the 95% interval, and the PASS or NOT MET sentence;
- per stratum: N_h, W_h, n_h, x_h and the url_only rows;
- the standard error at 0.90 and the pass chances above;
- per park: x of 40, the precision, its Wilson interval and whether it is below the bar; the list of parks below the
  bar;
- the precision of the 50 period-B rows, with a Wilson interval, as a description;
- url_only rows: how many, how many labeled 1, P with them set to 0 and P with them left out;
- how many sampled rows have a URL that was printed in earlier outputs (the count made at the draw), with the name,
  the files read and the number of files with a printed URL of each path (the sidecar lists those files);
- Claude's drafts: the rows with a draft, the `?` drafts, how many firm drafts agree with the labels, Cohen's kappa
  and the draft x label table; and the rows and sha256 of the copy;
- the frame rows and sha256, rebuilt from the input files of the draw, and the sample redrawn with the sidecar's seed;
- the commits of the spec and code (the draw's HEAD), the sample and the labels, and HEAD; and whether this document,
  `src/gdelt_sample.py` or `src/gdelt_url_rule.py` at HEAD differs from its blob at the draw.

The copy of the drafts has exactly the columns sample_id and draft, in the order of the drafts file. The score file
records its path, rows and sha256 and the sha256 of the drafts file it came from. Without a drafts file at
`data/work/gdelt/a1_part2_claude_drafts.csv`, `score` runs, writes no copy and reports "no drafts file".

The news test reads three values from `a1_part2_score.json`: `P`, `pass` and `parks.<park>.below_bar`. It accepts
the file only when `git_guard` is true and the file holds the commits, that is, only from a guarded `score`.

**Checking the result.** Every number of the score follows from three committed files, with the formulas above: the
sidecar gives N_h of each stratum (and `check` rebuilds the frame and its N_h from the committed exports); the sample
gives each row's stratum, from park_code and the month of file_ts; the labels give x_h and the notes. `score` itself
runs once: it refuses when a score file exists, and its test mode refuses the pre-registered sample.

## Commit order and run discipline

| Step | What happens | Guard |
| --- | --- | --- |
| Commit 1 | This document, `docs/week8/news_test_spec.md`, `src/gdelt_url_rule.py`, `src/gdelt_sample.py`, `src/gdelt_label_page.py`, `src/news_forecast.py`, `databricks/notebooks/07_gdelt_news.py`, `databricks/notebooks/08_news_test.py`, `tests/run_notebook_local.py` and `tests/check_gdelt_gold.py`. Pushed | the `gdelt_url_rule.py` self-tests and `gdelt_sample.py check` pass |
| Before the draw | no file under the four seen paths copies rows of the exports or of a test sample: test folders that hold such copies are deleted. No local Spark run is going (`<scratchpad>\gdelt_build\work\spark.lock` does not exist), and no Spark scratch is left under `<scratchpad>\gdelt_build` | checked by hand |
| Draw | the command below, once | refuses unless the seed is 20261116 (`--seed` is for tests only); at least one `--seen-dir` is given; the working tree is clean (untracked files included); this document, `gdelt_sample.py` and `gdelt_url_rule.py` at HEAD equal the working files; HEAD is pushed to its upstream branch; every `--seen-dir` exists and holds a file to read; no sample or sidecar exists; and the frame equals the frozen values |
| Commit 2 | `a1_part2_sample.csv` and `a1_part2_sample.json` only. Pushed | |
| Labels | snippets, drafts, page and labeling, as above | the page is built only with a draft for every row |
| Check | `uv run python src/gdelt_sample.py check-labels` | prints OK, or the reason `score` would refuse the file |
| Commit 3a | `docs/week8/a1_part2_labels.csv` only. Pushed | |
| Score | `uv run python src/gdelt_sample.py score`, once | refuses unless the conditions below hold |
| Commit 3b | `a1_part2_result.md`, `a1_part2_score.json`, `a1_part2_score.txt` and `a1_part2_claude_drafts.csv`, all in `docs/week8/`. Pushed | |

The draw, from the repository root:

```
uv run python src/gdelt_sample.py draw --seen-dir "<scratchpad>\gdelt_design" --seen-dir "<scratchpad>\gdelt_build" --seen-dir "<session>" --seen-dir "<session>.jsonl"
```

`score` refuses unless:
- the working tree is clean; the sample, the sidecar and the labels are committed at HEAD and equal the working
  files; HEAD is pushed to its upstream branch; and no score file, score text or drafts copy exists;
- the sidecar comes from a guarded draw (`git_guard` true) with the seed 20261116;
- the sidecar was added in the commit that added the sample; the sample commit follows the draw's HEAD, and the labels
  commit follows the sample commit;
- the sample, the sidecar and the labels are unchanged since the commit that added them;
- the labels commit changes only `docs/week8/a1_part2_labels.csv`;
- the sample file has the sha256 in the sidecar; every input file of the draw exists with the sha256 in the sidecar;
  the frame built from those files has the sidecar's sha256 and equals the frozen values; and the seed in the sidecar
  draws exactly the committed sample from it;
- the labels file has exactly the six columns of the download, every row has exactly six fields and a whole-number
  sample_id, there is one row for each sample row, with the same record_id, url and park_code, and every label is 1
  or 0;
- the drafts file, when there is one, has exactly the columns sample_id and draft, and every row has exactly these
  two fields; each sample_id is in the sample and appears once, and each draft is 1, 0 or ?. A `--drafts` path that
  is given must exist.

- `a1_part2_result.md` quotes the hashes of commits 1, 2 and 3a.
- `score` reads the drafts from `data/work/gdelt/`, which git ignores, and writes their copy for commit 3b itself.
- `score` does not check which other files commit 2 holds.
- `score` reports, but does not refuse, a change to this document, `gdelt_sample.py` or `gdelt_url_rule.py` at HEAD
  since the draw (their blobs at HEAD against the sidecar). Such a change is a deviation and is reported as one.
- The draw and the score each run once. A change to the rules, the code or a label after the score is reported as a
  deviation next to the original result, never in its place.
- For tests, `--no-git-guard` with `--out-dir` outside the repository skips the git guards, and a test draw needs an
  explicit positive `--seed` other than 20261116, on any frame. A negative seed is refused too, because
  `random.Random(-s)` draws what `random.Random(s)` draws. A test score refuses a sidecar from a guarded draw, or one
  whose seed is not a positive integer other than 20261116. Guard tests with the seed 20261116 run only in throwaway
  git repositories whose code copies hold the frozen values of a synthetic frame.

## What stays private

These files hold article text, the headlines and first paragraphs fetched from the publishers' pages:
- `data/work/gdelt/a1_part2_snippets.csv`, the fetched text;
- `data/work/gdelt/a1_part2_label.html`, the page, which holds the same text.

The text belongs to the publishers and is under their copyright. GDELT's free-use terms cover GDELT's own data, such
as the URLs in `data/landing`, not the articles. `data/work/` is git-ignored, and `gdelt_label_page.py` refuses to
write the page anywhere in the repository outside `data/work/`. The progress file of the page holds no article text:
the sample's sha256, the storage key, the time, and each row's label and note. The committed files hold ids, URLs,
park codes, labels, notes, drafts and counts.

## Known limits

- **Precision only.** The sample holds only rows that pass R0. It does not measure the rule's recall within name
  matches: the share of full-name matches about the park that R0 keeps. On the A1 labels R0 kept 27 of 34 (79%),
  measured after the rule was chosen, on the test week only. The 7 it dropped were multi-park articles and a
  gateway-town piece. R0 keeps 16.6% (GRSM) to 41.1% (GRCA) of each park's full-name matches, and how many relevant
  articles it drops outside the test week is not measured.
- **Drift of the pass rate by year.** The share of full-name matches that pass R0 moves with URL formats and the
  source mix: YOSE between 11.3% (2024) and 34.5% (2022), GRCA between 26.8% (2020) and 52.8% (2019), all parks
  between 23.4% (2020) and 40.4% (2026). The share of matches with a readable title in the path rose from 80-81%
  (2015-2016) to 95% (2021-2024). The sample has two periods and cannot show precision year by year; period B's 50
  rows are a description.
- **Single-event spike months.** One story can fill a park-month. Measured with URL patterns, not labels: the TV
  series *Yellowstone* in titles is 26% and 28% of YELL's R0 passes in 2024-11 and 2024-12; the Skywalk or Grand
  Canyon West is 48% of GRCA's in 2019-09; the Yellowstone River is 43% of YELL's in 2023-06 (the train that fell
  into the river) and 27% in 2016-08 (the river closed after a fish kill); the national monument is 41% of GRCA's in
  2023-08 and 62% in 2017-01. A sample of 40 rows per park rarely meets a given month, so P describes the average
  row, not each month.
- **Lost pages.** A row whose text none of the sources shows is labeled from the title words in the URL
  (`url_only`), the kind of label this check replaces. P is also reported with those rows set to 0 and with them left
  out.
- **One labeler.** One team member labels each row once. Claude's drafts show agreement; they do not replace a second
  labeler.

## Data seen before this commit

- The A1 labels (100 rows of the test week) were seen, and R0 was chosen with them. The test week and every A1 URL
  are outside the frame.
- The rulings above and the "no exclusions" decision were written after seeing pattern counts across all matches and
  example URLs, but no labels of frame rows.
- 360 distinct URLs were printed in the reader and design outputs. Before the 2 rows with an A1 URL were removed, 302
  passing rows and 86 failing rows of the window outside the test week had one of them
  (`gdelt_design/critique/verify3.txt`, section 5). The draw counts how many sampled rows have a URL printed under the
  four seen paths, and the result reports the count.
- Before this commit the code ran only on test data: test draws with seed 1 on the real frame; the git guards in
  throwaway git repositories with synthetic exports (an 800-row frame), the only place the seed 20261116 was used;
  fake labels, drafts and snippets; and the page's own script under Node with a stand-in for the browser and in
  headless Edge. The seed 20261116 was never used on the real frame, and every test sample file was deleted.
- The rows of the seed-1 test draws are not the sample. URLs of those draws that agents printed in their tool output
  are in the session transcripts, which the count searches.
- When this document was committed, the pre-registered draw had not run and no sampled row had been labeled.
