# Week 8 test A1 part 2: result

The pre-registered precision check of the URL rule R0 was scored once, on 2026-11-18 at 20:22 -06:00 (2026-11-19
UTC). Its rules are in `a1_part2_spec.md`. The commits:

| Commit | Hash | Date | Holds |
| --- | --- | --- | --- |
| 1 | `784147f02be5e55eff67f6426d2a265196498acd` | 2026-11-16 09:59 -06:00 | the spec, `news_test_spec.md` and the code that draws, shows and scores the sample: the pre-registration |
| 2 | `94c22f5a84256acc92da0a3de73bcd19910726c3` | 2026-11-16 10:08 -06:00 | `a1_part2_sample.csv` and `a1_part2_sample.json`, drawn once with seed 20261116 |
| 3a | `b6c1026eab6ad422b9bf5c055530ceda510f258d` | 2026-11-18 20:21 -06:00 (2026-11-19 UTC) | `a1_part2_labels.csv` only |
| 3b | | | this document, the score output and the copy of Claude's drafts |

The check, in short (the spec has the full definitions):
- R0 keeps a GDELT match only when the pattern of its own park occurs in the path of the article URL.
- The frame is the 174,519 match rows that pass R0, from 2015-02-18 to 2025-12-31, outside the A1 test week.
- The sample is 200 random frame rows, 40 per park: 30 from period A (2015-02 to 2023-12) and 10 from period B
  (2024-01 to 2025-12).
- P is the share of sampled rows labeled 1 in each of the 10 strata, weighted by the stratum's share of the frame.
  The bar is A1's, 0.90.
- A park is below the bar when 32 or fewer of its 40 rows are about the park.

## Verdict

**NOT MET: P = 0.5022 < 0.90.** `score` printed:

> NOT MET: P = 0.5022 < 0.90. A1's fourth condition stays not met, R0 is not changed, and the news result is marked
> below the bar.

| | Value |
| --- | --- |
| P | 0.502191, exactly 1314629/2617785 |
| 95% interval | 0.4080-0.5964 |
| Bar | 0.90 |
| Parks below the bar | all five |

| Park | About the park | Precision | Wilson 95% | Below the bar |
| --- | ---: | ---: | --- | --- |
| GRCA | 22 of 40 | 0.550 | 0.398-0.693 | yes |
| GRSM | 19 of 40 | 0.475 | 0.329-0.625 | yes |
| YELL | 22 of 40 | 0.550 | 0.398-0.693 | yes |
| YOSE | 25 of 40 | 0.625 | 0.470-0.758 | yes |
| ZION | 17 of 40 | 0.425 | 0.285-0.578 | yes |

- A1's fourth condition stays not met. R0 is not changed.
- The news forecast test reads `P`, `pass` and `parks.<park>.below_bar` from `a1_part2_score.json`
  (`news_test_spec.md`, `src/news_forecast.py`). Here `pass` is false and `below_bar` is true for all five parks.
  - Its result will be marked below the bar for the pooled precision: it will print "PRECISION: MARKED BELOW THE
    PRECISION BAR: the URL rule's pooled precision P = 0.5022 is below 0.90, so the whole news result is marked below
    the bar".
  - It will be marked below the bar for each of the five parks, and every summary row will have
    `below_precision_bar` true.

## What the result rests on

### How the rows were read

**Snippets.** Claude fetched each sampled article's headline and lead into `data/work/gdelt/a1_part2_snippets.csv`.
- Rows 1-3 were fetched once, directly, with Python `requests` and an honest User-Agent. The sites' bot checks
  refused them (HTTP 429, 402 and 429), and they were not retried.
- The other 197 rows were fetched once each with Claude's WebFetch tool, with the user's explicit approval. There was
  no retry and no workaround when a site blocked.
- WebFetch quotes at most about 125 characters of a source. Each fetched lead is the opening words of the first
  paragraph, 123 characters at the longest.
- WebFetch refused every request to web.archive.org, so no archive text was fetched.
- 56 rows have fetched text (status `ok`) and 144 do not:

| Status | Rows |
| --- | ---: |
| `ok` | 56 |
| `blocked` | 56 |
| `http_404` | 25 |
| `http_403` | 21 |
| `http_429` | 16 |
| `error` | 16 |
| `removed` | 5 |
| `http_301` | 2 |
| `http_402` | 1 |
| `http_503` | 1 |
| `timeout` | 1 |
| All | 200 |

**Labels.** Labeler 1 and labeler 2 are team members. The labeling took three passes, all before commit 3a.
1. **Pass 1**, labeler 1, on a 200-row CSV with the fetched text and both links. The 56 rows with fetched text were
   labeled from it. The 144 rows without fetched text were all labeled from the URL with the note url_only, without
   opening the links. The reading order says to open the article link and then the archive copy first (steps 2 and
   3).
2. **Pass 2**, labeler 1, asked to open the links of those 144 rows. 6 rows were relabeled from the article, with
   the same labels as in pass 1. 138 stayed url_only. Labeler 1's later attempts at the 138 rows were a partial file
   and a file with the source field empty in 102 rows. Neither could be accepted, and neither was used.
3. **Pass 3**, labeler 2, on a separate 138-row page (`a1_part2_redo_138.html`). Each row showed the article link
   and the archive link and needed a source: article, archive or none. The page showed no earlier label and no
   draft.
   - 50 rows were read from the article and 2 from the archive copy.
   - 86 rows were marked none: neither link showed the text.
   - Reading-order step 4 says to label such a row from the title words in the URL. The user decided instead to keep
     all 86 rows labeled 0. They carry the note url_only.

The final labels, by where each one came from (from `data/work/gdelt/a1_part2_label_provenance.csv` and the
committed labels):

| Source of the final label | Labeler | Rows | Labeled 1 |
| --- | --- | ---: | ---: |
| Pass 1: fetched title and lead | 1 | 56 | 50 |
| Pass 2: article | 1 | 6 | 6 |
| Pass 3: article | 2 | 50 | 47 |
| Pass 3: archive copy | 2 | 2 | 2 |
| Pass 3: none, set to 0, note url_only | 2 | 86 | 0 |
| All | | 200 | 105 |

114 labels come from the article content. 86 rows were set to 0 without being read.

Per stratum:

| Park | Period | n | Pass 1 | Pass 2 | Pass 3 article | Pass 3 archive | Read | Set to 0 unread | x |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GRCA | A | 30 | 6 | 0 | 7 | 1 | 14 | 16 | 14 |
| GRCA | B | 10 | 5 | 0 | 4 | 0 | 9 | 1 | 8 |
| GRSM | A | 30 | 1 | 0 | 12 | 1 | 14 | 16 | 13 |
| GRSM | B | 10 | 6 | 0 | 1 | 0 | 7 | 3 | 6 |
| YELL | A | 30 | 9 | 1 | 6 | 0 | 16 | 14 | 13 |
| YELL | B | 10 | 5 | 2 | 3 | 0 | 10 | 0 | 9 |
| YOSE | A | 30 | 8 | 0 | 8 | 0 | 16 | 14 | 16 |
| YOSE | B | 10 | 8 | 1 | 1 | 0 | 10 | 0 | 9 |
| ZION | A | 30 | 2 | 1 | 5 | 0 | 8 | 22 | 8 |
| ZION | B | 10 | 6 | 1 | 3 | 0 | 10 | 0 | 9 |
| All | | 200 | 56 | 6 | 50 | 2 | 114 | 86 | 105 |

x is the rows labeled 1. Every row labeled 1 is a read row.

### The 86 rows set to 0 without reading

- P counts them as 0: P = 0.5022.
- `score` also gives P with them left out: 0.9004. It uses the same weights W_h and, in each stratum, x over the read
  rows. It has no interval.
- Neither number is the precision of the frame.
  - P = 0.5022 counts 86 rows that no one read as not about the park.
  - 0.9004 is over the 114 rows whose text a labeler could read. They are not a random subset of the sample, and the
    share read differs by stratum: in period A, 8 of 30 rows in ZION, 14 of 30 in GRCA and GRSM, and 16 of 30 in
    YELL and YOSE; in period B, 7 of 10 in GRSM, 9 of 10 in GRCA and 10 of 10 in YELL, YOSE and ZION.
  - The check does not measure how often the 86 unread rows are about the park.
- `score` describes the url_only rows as "labeled from the title words in the URL", the spec's wording. These 86 rows
  were not labeled from the URL. They were set to 0.

### Period B

41 of the 50 period-B rows are about the park: 0.820, Wilson 0.692-0.902. 46 of the 50 were read; the 4 others
(GRCA 1, GRSM 3) were set to 0 unread. This is a description, as the spec fixes; it is not used for the verdict.

### Claude's drafts

- Claude wrote its drafts to `data/work/gdelt/a1_part2_claude_drafts.csv` before the label page was first built.
- Their sha256 when the page was first built was `4cd7bc7d77908a5b7f5917df08b692a1f68c1d6f7dbba4a4306e55b531e61b8f`.
  `score` recorded the same sha256 for the drafts file it read (`source_sha256`) and for its copy, so the drafts did
  not change after the page was built.
- The drafts are firm (1 or 0) only for the 56 rows with fetched text, and `?` for the 144 others.
- The labelers never saw the drafts. While checking file formats, Claude saw the labelers' files, after the drafts
  were fixed.
- 53 of the 56 firm drafts agree with the labels (0.9464), Cohen's kappa 0.6978. The table is under the secondary
  numbers.

## Deviations from the spec

| Spec | What happened | Effect |
| --- | --- | --- |
| "The team member labels every row on the page" (Labeling procedure, step 4); "One labeler" (Known limits) | Two labelers. Labeler 1 gave the final label of 62 rows (passes 1 and 2). Labeler 2 read 52 rows in pass 3 and marked 86 none; those 86 were kept at 0 by the user's decision | Each row has one final label from one labeler. Agreement between the two labelers is not measured |
| Reading order: open the article link, then the archive copy, before using the URL (steps 2 and 3) | In pass 1, labeler 1 labeled all 144 rows without fetched text from the URL, without opening the links. Pass 2 read 6 of them from the article | The 138 remaining URL-only labels of labeler 1 are not used. Those rows were labeled again in pass 3 |
| "If none of them shows the text: the title words in the URL" (step 4); Known limits, "Lost pages" | The 86 rows that labeler 2 marked none were set to 0, by the user's decision, instead of being judged from the title words in the URL | P counts the 86 rows as 0. P with URL-title labels for them was not measured. The note url_only on these rows means "set to 0 unread" |
| `lead`: "the first paragraphs of the article text, at most 3 paragraphs and at most 1,500 characters" (Labeling procedure, step 1) | WebFetch quotes at most about 125 characters of a source. The leads are the opening words of the first paragraph, 123 characters at the longest | The 56 pass-1 labels and the 56 firm drafts were made from a title and a lead of at most 123 characters |
| "The archive copy is fetched only when the article itself gives no text" (step 1) | WebFetch refused every request to web.archive.org. No archive text was fetched | No row has fetched archive text. 2 rows were read from the archive copy by labeler 2 in pass 3 |
| "Claude fetches each sampled article's headline and first paragraphs" (step 1) | Rows 1-3: one direct test fetch with Python `requests`, refused (HTTP 429, 402, 429), not retried. Rows 4-200: WebFetch, with the user's explicit approval, once each, no retry and no workaround | Rows 1-3 have no fetched text. They were read in pass 3: row 1 from the archive copy, rows 2 and 3 from the article |
| The page of `src/gdelt_label_page.py` (steps 3 to 5) | Pass 1 was labeled on a 200-row CSV with the fetched text and both links. Pass 3 used a separate 138-row page with a required source field (article, archive or none), not `gdelt_label_page.py` | The committed labels file combines the three passes; it is not one download of the page. It has the six columns of the download, and `score` accepted it. Neither file showed a draft; the pass-3 page showed no earlier label |

The deviations are in how the rows were read and labeled, not in the code. `score` reports that the spec,
`src/gdelt_sample.py` and `src/gdelt_url_rule.py` at HEAD equal their blobs at the draw. Between commit 1 and commit
3a, no file of commit 1 changed; the only change under `src/`, `tests/`, `databricks/` and the two specs is the new
A2 file `tests/check_a2_rerun.py`. The labels file is unchanged since commit 3a.

## Secondary numbers from score

None of these is used for the verdict.

**Strata.**

| Park | Period | Months | N_h | W_h | n | x | url_only |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| GRCA | A | 2015-02..2023-12 | 18,895 | 0.108269 | 30 | 14 | 16 |
| GRCA | B | 2024-01..2025-12 | 4,189 | 0.024003 | 10 | 8 | 1 |
| GRSM | A | 2015-02..2023-12 | 8,680 | 0.049737 | 30 | 13 | 16 |
| GRSM | B | 2024-01..2025-12 | 964 | 0.005524 | 10 | 6 | 3 |
| YELL | A | 2015-02..2023-12 | 78,290 | 0.448604 | 30 | 13 | 14 |
| YELL | B | 2024-01..2025-12 | 11,677 | 0.066910 | 10 | 9 | 0 |
| YOSE | A | 2015-02..2023-12 | 40,740 | 0.233442 | 30 | 16 | 14 |
| YOSE | B | 2024-01..2025-12 | 2,346 | 0.013443 | 10 | 9 | 0 |
| ZION | A | 2015-02..2023-12 | 7,903 | 0.045284 | 30 | 8 | 22 |
| ZION | B | 2024-01..2025-12 | 835 | 0.004785 | 10 | 9 | 0 |

**Per park.** As in the verdict: GRCA 22, GRSM 19, YELL 22, YOSE 25 and ZION 17 of 40, all below the bar.

**Period B.** 41 of 50 = 0.820, Wilson 0.692-0.902.

**url_only rows.** 86, 0 of them labeled 1. P with them as 0 = 0.5022; P without them = 0.9004.

**Near the bar.** The standard error of P at a true precision of 0.90 in every stratum is 2.94 points (95%: +-5.8
points).

**Seen URLs.** 0 of the 200 sampled rows have a URL that was printed in earlier outputs. The draw searched 911 files:

| Seen path | Kind | Files read | Files with a printed URL |
| --- | --- | ---: | ---: |
| `gdelt_design` | folder | 49 | 0 |
| `gdelt_build` | folder | 429 | 0 |
| `06f3c0d6-4881-46e3-99b8-500c0dacff49` | folder | 432 | 0 |
| `06f3c0d6-4881-46e3-99b8-500c0dacff49.jsonl` | file | 1 | 0 |

**Claude's drafts.** 200 rows with a draft, 144 of them `?`. 53 of the 56 firm drafts agree with the labels
(0.9464), Cohen's kappa 0.6978.

| Draft | Label 0 | Label 1 |
| --- | ---: | ---: |
| 0 | 4 | 1 |
| 1 | 2 | 49 |
| ? | 89 | 55 |

The copy `docs/week8/a1_part2_claude_drafts.csv` has 200 rows and sha256
`4cd7bc7d77908a5b7f5917df08b692a1f68c1d6f7dbba4a4306e55b531e61b8f`, the sha256 of the drafts file it came from.

## Integrity checks that score passed

| Check | Result |
| --- | --- |
| Commits | spec and code `784147f`, sample `94c22f5`, labels `b6c1026`; HEAD `b6c1026`, pushed |
| The labels commit changes only the labels file | yes: `docs/week8/a1_part2_labels.csv` |
| The sample, the sidecar and the labels are unchanged since their commits | yes |
| The spec, `src/gdelt_sample.py` and `src/gdelt_url_rule.py` at HEAD equal their blobs at the draw | yes |
| Frame | 174,519 rows, sha256 `d231bfd61b17313ca43102bba3c244950584ef6273de39b460b7fc1e6de7f23e`, equal to the sidecar, rebuilt from the 5 input files of the draw (the four match files and `gdelt_precision_sample.csv`), each with its sha256 unchanged |
| Redraw | the sample redrawn with seed 20261116 is identical to the committed file |
| Guarded score | `git_guard` true |

Every number of the score follows from three committed files, with the spec's formulas: the sidecar gives N_h, the
sample gives each row's stratum, and the labels give x_h and the notes. The provenance of each label (which pass and
which source) is in the git-ignored `data/work/gdelt/a1_part2_label_provenance.csv`, not in a committed file.

## Files

Commit 3b adds, in `docs/week8/`:

| File | What it holds |
| --- | --- |
| `a1_part2_result.md` | this document |
| `a1_part2_score.json` | the score, read by the news test |
| `a1_part2_score.txt` | the score as `score` printed it |
| `a1_part2_claude_drafts.csv` | Claude's drafts: sample_id and draft, 200 rows |

These stay in the git-ignored `data/work/gdelt/`:

| File | What it holds |
| --- | --- |
| `a1_part2_snippets.csv` | the fetched titles and leads, article text |
| `a1_part2_label.html` | the label page of `gdelt_label_page.py`, which holds the same text |
| `a1_part2_to_label.csv` | a 200-row labeling CSV with the fetched text and both links, labels blank |
| `a1_part2_redo_138.html`, `a1_part2_redo_138.csv` | the pass-3 page and its 138 rows (links only, labels blank) |
| `a1_part2_claude_drafts.csv` | the drafts source that `score` read |
| `a1_part2_label_provenance.csv` | the pass and source of each final label |

The article text belongs to the publishers and stays out of git, as the spec fixes.
