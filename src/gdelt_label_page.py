"""Build the local labeling page for the A1 part 2 sample: data/work/gdelt/a1_part2_label.html.

The rules are in docs/week8/a1_part2_spec.md. Every row is labeled from the article content, its headline and first
paragraphs, for the park named in the row. Inputs:
- docs/week8/a1_part2_sample.csv, written once by `src/gdelt_sample.py draw`:
  sample_id, record_id, file_ts, park_code, url.
- data/work/gdelt/a1_part2_snippets.csv, git-ignored: the article text fetched after the draw, with columns
  sample_id, fetched_url, status, title, lead. title is the article's headline; lead is its first paragraphs, at
  most 3 and at most LEAD_MAX characters, copied without edits; fetched_url is the address the text came from.
  status is ok for a row with a title or a lead, and else one of FETCH_STATUSES (http_<code> such as http_404).
  The page refuses any other status and a lead of more than LEAD_MAX characters. A row without a title or lead is
  shown as "(not fetched: <status>)", and a sample row with no snippets row as "(not fetched: missing)".
- data/work/gdelt/a1_part2_claude_drafts.csv, git-ignored: Claude's drafts (sample_id, draft), written before the
  labeling starts. The page is built only when the file has a draft for every sample row; it never shows them. The
  build prints the file's sha256, and score records the sha256 of the drafts file it reads, so a change of the
  drafts after the page was built shows.

For each row the page shows the sample_id, the park name, the fetched title and lead, the address they were fetched
from when it is not the sampled url, the article link, an archive link https://web.archive.org/web/<file_ts>/<url>,
a 1 / 0 choice and a note (free text, with a url_only button). At the top it shows the reading order and the rulings.
It shows no stratum, no prior guess at the label and no running count of 1s: only "labeled n / 200".

The labels stay in the browser (localStorage key gdelt_a1p2_labels_v1), keyed by sample_id and tied to the sha256 of
the sample file. Each change re-reads what is saved and writes back only the changed row, so two open tabs of the
page do not overwrite each other's rows. "Save progress" downloads the saved rows as a JSON file at any time, and
"Load progress" reads such a file back, only when it was saved from the same sample. When every row has a label,
"Download CSV" saves a1_part2_labels.csv with columns sample_id, record_id, url, park_code, label, note. Move that
file to docs/week8/ unchanged, check it with `gdelt_sample.py check-labels`, and commit it on its own (commit 3a)
before `gdelt_sample.py score`.

The page holds article text, so it is written only under data/work/, which git ignores. From the repository root:
  uv run python src/gdelt_label_page.py
"""
import argparse
import csv
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # src/, also under python -I
from gdelt_sample import (DRAFTS, LABEL_COLUMNS, LABELS_NAME, OUT_DIR, SAMPLE_COLUMNS, SAMPLE_NAME,  # noqa: E402
                          inside, read_drafts, sha256_file)

ROOT = Path(__file__).resolve().parents[1]  # repository root
SNIPPETS = "data/work/gdelt/a1_part2_snippets.csv"
PAGE = "data/work/gdelt/a1_part2_label.html"
WORK = "data/work"
SNIPPET_COLUMNS = ["sample_id", "fetched_url", "status", "title", "lead"]
STORAGE_KEY = "gdelt_a1p2_labels_v1"
PROGRESS_NAME = "a1_part2_label_progress.json"  # the "Save progress" file: labels and notes, no article text
NO_ADDRESS = "(no address recorded)"            # fetched text whose snippet row has no fetched_url
# The snippets format, fixed before the draw: status ok when the row has fetched text, else why it has none
FETCH_STATUSES = ["http_<code>", "paywall", "login_wall", "cookie_wall", "removed", "timeout", "blocked", "error"]
FETCH_STATUS = re.compile(r"ok|http_[0-9]{3}|paywall|login_wall|cookie_wall|removed|timeout|blocked|error")
LEAD_MAX = 1500                                 # characters of the lead, at most 3 paragraphs
PARK_NAMES = {"YELL": "Yellowstone", "YOSE": "Yosemite", "GRCA": "Grand Canyon", "ZION": "Zion",
              "GRSM": "Great Smoky Mountains"}

# The text at the top of the page. It repeats docs/week8/a1_part2_spec.md and must say the same.
READING = [
    "The fetched title and lead shown in the row.",
    "The <b>article</b> link, when the row shows no fetched text, when the text was fetched from a different "
    "address than the article link (the row then shows that address), or when the text does not settle the label.",
    "The <b>archive copy</b>, https://web.archive.org/web/&lt;file_ts&gt;/&lt;url&gt;, when the article does not "
    "load or hides its text (a paywall, a login or cookie wall, a removed page).",
    "If none of them shows the text: the title words in the URL. Then click <b>url_only</b>, which puts url_only at "
    "the start of the note.",
]
RULINGS = [
    "<b>1</b> = the park itself is the main subject or a central part of the article. <b>0</b> = anything else. "
    "Every row needs a 1 or a 0.",
    "Each row is labeled for its own park, the one named in the row. An article that is in the sample for two parks "
    "is labeled once for each park.",
    "Things that only share the name are 0: the TV series <i>Yellowstone</i>, the Yellowstone Club, "
    "Grand Canyon University, Zion Williamson, macOS Yosemite, the Smokies baseball team.",
    "Places outside the park are 0 unless the park itself is central: the Yellowstone River in Montana, "
    "Yellowstone County, Grand Canyon West or the Skywalk, the national monument next to the Grand Canyon, "
    "West Yellowstone or Gardiner town news.",
    "An article about national parks (NPS policy, fees, reservations, visitation figures, a ranking of national "
    "parks) is 1 when this park has its own item, paragraph or headline mention.",
    "A list of mixed destinations or landmarks is 0. A trip to a gateway town that includes the park is 1.",
    "The note is free text. The only note with a meaning is one that starts with url_only.",
]
SAVING = [
    "Your choices are saved in this browser as you go. Each change is merged with what other tabs of this page "
    "saved, so two tabs do not overwrite each other's rows.",
    "<b>Save progress</b> downloads what is saved as a file, at any time. <b>Load progress</b> reads such a file "
    "back and refuses a file saved from a different sample; rows in the file replace the same rows in this browser, "
    "other rows stay.",
    "<b>Download CSV</b> works when every row has a 1 or a 0. It saves a1_part2_labels.csv: move it to docs/week8/ "
    "unchanged, without opening and saving it in a spreadsheet program.",
    "<b>Clear all</b> deletes every label and note saved in this browser, for every tab of this page.",
]

HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>GDELT labels, A1 part 2 (__N__ rows)</title>
<style>
  body { font-family: "Segoe UI", "Microsoft YaHei", sans-serif; margin: 0; background: #f4f1ea; color: #1f2a24; }
  header { position: sticky; top: 0; background: #2f5d3a; color: #fff; padding: 12px 20px; display: flex;
           gap: 18px; align-items: center; flex-wrap: wrap; z-index: 2; }
  header h1 { font-size: 18px; margin: 0; }
  header .stat { font-size: 15px; }
  header button { font-size: 15px; padding: 6px 14px; border: 0; border-radius: 6px; cursor: pointer; }
  header button:disabled { background: #9aa59d; color: #fff; cursor: not-allowed; }
  #download { background: #d9822b; color: #fff; font-weight: 600; }
  #download:disabled { background: #9aa59d; cursor: not-allowed; }
  #stale { display: none; background: #c62828; color: #fff; margin: 14px 20px; padding: 10px 16px; font-size: 14px; }
  .rules { background: #fff; margin: 14px 20px; padding: 10px 16px; border-left: 5px solid #2f5d3a; font-size: 14px; }
  .rules h2 { font-size: 15px; margin: 8px 0 2px; color: #2f5d3a; }
  .rules b { color: #2f5d3a; }
  .rules li { margin: 3px 0; }
  table { border-collapse: collapse; margin: 0 20px 40px; width: calc(100% - 40px); background: #fff; }
  th, td { border-bottom: 1px solid #ddd; padding: 8px 10px; text-align: left; vertical-align: top; font-size: 14px; }
  th { background: #e6e0d2; position: sticky; top: 52px; }
  td.n { color: #777; width: 40px; }
  td.park { font-weight: 700; width: 110px; }
  td.title .headline { font-size: 15px; font-weight: 600; }
  td.title .lead { margin-top: 4px; white-space: pre-wrap; color: #333; }
  td.title .none { color: #a0522d; font-style: italic; }
  td.title .fetched { margin-top: 4px; color: #a0522d; font-size: 13px; word-break: break-all; }
  td.title .links { margin-top: 6px; word-break: break-all; }
  td.title .links div { margin-top: 2px; }
  td.title .links a { color: #1f4e8c; text-decoration: none; }
  td.title .links a:hover { text-decoration: underline; }
  td.btns { width: 130px; white-space: nowrap; }
  td.btns label { display: block; padding: 3px 0; cursor: pointer; }
  td.note { width: 220px; }
  td.note input[type=text] { width: 200px; font-size: 13px; padding: 3px; }
  td.note button { font-size: 12px; padding: 2px 8px; margin: 4px 4px 0 0; border: 1px solid #aaa; border-radius: 5px;
                   background: #fff; cursor: pointer; }
  tr.done td { background: #f7faf7; }
</style>
</head>
<body>
<header>
  <h1>GDELT labels, A1 part 2</h1>
  <span class="stat" id="progress"></span>
  <button id="download" disabled>Download CSV</button>
  <button id="saveprogress">Save progress</button>
  <button id="load">Load progress</button>
  <input type="file" id="loadfile" accept=".json,application/json" hidden>
  <button id="reset">Clear all</button>
</header>
<div id="stale"></div>
<div class="rules">
  <h2>How to read a row</h2>
  <p>The label comes from the article's headline and first paragraphs, from the first of these sources that shows
  them:</p>
  <ol>__READING__</ol>
  <h2>Rulings</h2>
  <ul>__RULINGS__</ul>
  <h2>Saving</h2>
  <ul>__SAVING__</ul>
</div>
<table>
  <thead><tr><th>#</th><th>Park</th><th>Article</th><th>Label</th><th>Note</th></tr></thead>
  <tbody id="rows"></tbody>
</table>
<script>
const ROWS = __ROWS__;
const SAMPLE = "__FINGERPRINT__";  // sha256 of the sample file: saved labels of another sample are never mixed in
const KEY = "__KEY__";
const COLUMNS = __COLUMNS__;
const DOWNLOAD_NAME = "__DOWNLOAD__";
const PROGRESS_NAME = "__PROGRESS__";
const IDS = new Set(ROWS.map(r => String(r.sample_id)));

// What localStorage holds under KEY: { sample: SAMPLE, rows: { sample_id: { label: "1" | "0" | "", note: "..." } } }.
let labels = {};      // this tab's copy of the saved rows
let blocked = false;  // true when the browser holds rows of a different sample (or a value that cannot be read)
let foreignRows = 0;  // how many rows that other sample has
let broken = false;   // the saved value cannot be read

function readSaved() {
  let s;
  try { s = JSON.parse(localStorage.getItem(KEY) || "{}"); } catch (e) { return { broken: true }; }
  if (!s || typeof s !== "object" || (s.rows != null && typeof s.rows !== "object")) return { broken: true };
  return s;
}

function isForeign(s) {
  return !!s.broken || (Object.keys(s.rows || {}).length > 0 && s.sample !== SAMPLE);
}

function syncFromStorage() {
  // Take what is saved, including the rows other tabs wrote; never show rows of another sample
  const s = readSaved();
  blocked = isForeign(s);
  broken = !!s.broken;
  foreignRows = blocked ? Object.keys(s.rows || {}).length : 0;
  labels = blocked ? {} : (s.rows || {});
}

function writeRows(change) {
  // Re-read what is saved (another tab may have written since), apply change(rows) and write it back.
  // If another sample took the key meanwhile, nothing is written and the page is blocked.
  const s = readSaved();
  if (!isForeign(s)) {
    const rows = s.rows || {};
    change(rows);
    localStorage.setItem(KEY, JSON.stringify({ sample: SAMPLE, rows: rows }));
  }
  syncFromStorage();
  render();
}

function setRow(id, field, value) {
  // Only this row's one field changes; the other rows and the other field stay as saved
  writeRows(rows => {
    const old = rows[id] || { label: "", note: "" };
    const row = { label: old.label || "", note: old.note || "" };
    row[field] = value;
    rows[id] = row;
  });
}

function startsUrlOnly(note) {
  // as gdelt_sample.note_kind reads a note: strip, lower case, runs of spaces or dashes become "_"
  return note.trim().toLowerCase().replace(/[\s-]+/g, "_").startsWith("url_only");
}

function link(text, href) {
  // only http(s) links become clickable
  const a = document.createElement("a");
  a.textContent = text;
  if (/^https?:\/\//i.test(href)) { a.href = href; a.target = "_blank"; a.rel = "noopener noreferrer"; }
  return a;
}

function line(prefix, a) {
  const div = document.createElement("div");
  if (prefix) div.textContent = prefix;
  div.appendChild(a);
  return div;
}

function buildRows() {
  const body = document.getElementById("rows");
  for (const r of ROWS) {
    const id = String(r.sample_id);
    const tr = document.createElement("tr");
    tr.id = "row" + id;
    tr.innerHTML =
      `<td class="n"></td><td class="park"></td>` +
      `<td class="title"><div class="headline"></div><div class="lead"></div><div class="fetched"></div>` +
      `<div class="links"></div></td>` +
      `<td class="btns"><label><input type="radio" value="1"> 1 about this park</label>` +
      `<label><input type="radio" value="0"> 0 not about it</label></td>` +
      `<td class="note"><input type="text" class="note" placeholder="note"><br>` +
      `<button class="urlonly">url_only</button></td>`;
    tr.querySelector(".n").textContent = id;
    tr.querySelector(".park").textContent = r.park_name;
    const head = tr.querySelector(".headline"), lead = tr.querySelector(".lead");
    if (r.title || r.lead) {
      head.textContent = r.title;
      lead.textContent = r.lead;
    } else {
      head.textContent = "(not fetched: " + r.not_fetched + ")";
      head.className = "headline none";
    }
    if (r.fetched_from) tr.querySelector(".fetched").textContent = "text fetched from: " + r.fetched_from;
    const links = tr.querySelector(".links");
    links.appendChild(line("article: ", link(r.url, r.url)));
    links.appendChild(line("", link("archive copy", r.archive_url)));
    tr.querySelectorAll("input[type=radio]").forEach(b => {
      b.name = "label" + id;
      b.onchange = () => setRow(id, "label", b.value);
    });
    const note = tr.querySelector("input.note");
    note.oninput = () => setRow(id, "note", note.value);
    tr.querySelector("button.urlonly").onclick = () => {
      const v = note.value.trim();
      note.value = startsUrlOnly(v) ? v : ("url_only " + v).trim();
      setRow(id, "note", note.value);
    };
    body.appendChild(tr);
  }
}

function render() {
  let done = 0;
  for (const r of ROWS) {
    const id = String(r.sample_id), e = labels[id] || { label: "", note: "" };
    const tr = document.getElementById("row" + id);
    tr.querySelectorAll("input[type=radio]").forEach(b => { b.checked = b.value === e.label; b.disabled = blocked; });
    const note = tr.querySelector("input.note");
    if (note.value !== (e.note || "")) note.value = e.note || "";  // an unchanged value keeps the cursor
    note.disabled = blocked;
    tr.querySelector("button.urlonly").disabled = blocked;
    const ok = e.label === "1" || e.label === "0";
    done += ok;
    tr.className = ok ? "done" : "";
  }
  document.getElementById("progress").textContent = `labeled ${done} / ${ROWS.length}`;
  document.getElementById("download").disabled = blocked || done !== ROWS.length;
  document.getElementById("saveprogress").disabled = blocked;
  document.getElementById("load").disabled = blocked;
  const box = document.getElementById("stale");
  box.style.display = blocked ? "block" : "none";
  box.textContent = !blocked ? "" :
    (broken ? "This browser holds saved labels under this page's key that cannot be read."
            : `This browser holds saved labels of a different sample (${foreignRows} rows).`) +
    ` They are not shown and nothing can be labeled until you press "Clear all".`;
}

function offer(text, type, name) {
  // the browser saves text as a file called name
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([text], { type: type }));
  a.download = name;
  a.click();
}

function csvField(s) {
  s = String(s == null ? "" : s);
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

document.getElementById("download").onclick = () => {
  syncFromStorage();  // the rows of every tab
  render();
  if (document.getElementById("download").disabled) return;
  const lines = [COLUMNS.join(",")];
  for (const r of ROWS) {
    const e = labels[String(r.sample_id)];
    const out = { sample_id: r.sample_id, record_id: r.record_id, url: r.url, park_code: r.park_code,
                  label: e.label, note: (e.note || "").trim() };
    lines.push(COLUMNS.map(c => csvField(out[c])).join(","));
  }
  offer(lines.join("\n") + "\n", "text/csv;charset=utf-8", DOWNLOAD_NAME);
};

document.getElementById("saveprogress").onclick = () => {
  syncFromStorage();
  render();
  if (blocked) return;
  const data = { sample: SAMPLE, key: KEY, saved_at: new Date().toISOString(), rows: labels };
  offer(JSON.stringify(data, null, 1) + "\n", "application/json", PROGRESS_NAME);
};

function progressProblem(d) {
  // "" when d is a progress file of this sample, else why it is refused
  if (!d || typeof d !== "object" || !d.rows || typeof d.rows !== "object" || Array.isArray(d.rows))
    return "it is not a progress file of this page.";
  if (d.sample !== SAMPLE) return "it was saved from a different sample.";
  for (const [id, e] of Object.entries(d.rows)) {
    if (!IDS.has(id)) return `sample_id ${id} is not in this sample.`;
    if (!e || typeof e !== "object" || !["1", "0", ""].includes(e.label) || typeof e.note !== "string")
      return `the row of sample_id ${id} is not a label and a note.`;
  }
  return "";
}

document.getElementById("load").onclick = () => document.getElementById("loadfile").click();

document.getElementById("loadfile").onchange = async () => {
  const input = document.getElementById("loadfile");
  const file = input.files[0];
  input.value = "";  // the same file can be chosen again
  if (!file) return;
  let data = null;
  try { data = JSON.parse(await file.text()); } catch (e) { data = null; }
  const problem = progressProblem(data);
  if (problem) { alert(`Not loaded: ${file.name}: ${problem} Nothing was changed.`); return; }
  const n = Object.keys(data.rows).length;
  if (!confirm(`Load ${n} saved rows from ${file.name}? They replace the same rows in this browser; other rows stay.`))
    return;
  writeRows(rows => {
    for (const [id, e] of Object.entries(data.rows)) rows[id] = { label: e.label, note: e.note };
  });
};

document.getElementById("reset").onclick = () => {
  if (confirm("Clear every label and note saved in this browser, for every tab of this page? " +
              "Use Save progress first to keep a copy.")) {
    localStorage.removeItem(KEY);
    location.reload();
  }
};

// Another tab saved or cleared: show what is saved now
window.addEventListener("storage", e => {
  if (e.key === KEY || e.key === null) { syncFromStorage(); render(); }
});

syncFromStorage();
buildRows();
render();
</script>
</body>
</html>
"""


def read_csv(path: Path, columns: list[str]) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == columns, f"{path}: columns {reader.fieldnames}, expected {columns}"
        return list(reader)


def script_json(value) -> str:
    """JSON that is safe inside a <script> element: no '<', '>' or '&', and only ASCII."""
    text = json.dumps(value, ensure_ascii=True)
    return text.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")


def page_rows(sample: list[dict], snippets: list[dict]) -> list[dict]:
    """The fields the page may show, per sample row. Nothing else from the input files reaches the page."""
    by_id = {}
    for s in snippets:
        sid = int(s["sample_id"])
        assert sid not in by_id, f"snippets: sample_id {sid} twice"
        by_id[sid] = s
    ids = {int(r["sample_id"]) for r in sample}
    assert set(by_id) <= ids, f"snippets: sample_ids outside the sample: {sorted(set(by_id) - ids)[:5]}"
    rows = []
    for r in sample:
        sid = int(r["sample_id"])
        s = by_id.get(sid)
        if s is None:
            s = {"fetched_url": "", "status": "missing", "title": "", "lead": ""}
        title, lead, fetched_url = s["title"].strip(), s["lead"].strip(), s["fetched_url"].strip()
        text = bool(title or lead)
        if sid in by_id:
            # The messages name the sample_id and the rule, never the field, which could hold a url
            status = s["status"].strip()
            assert FETCH_STATUS.fullmatch(status), (
                f"snippets: sample_id {sid}: the status is not ok or one of {', '.join(FETCH_STATUSES)}")
            assert (status == "ok") == text, (
                f"snippets: sample_id {sid}: the status is ok exactly when the row has a title or a lead")
            assert len(lead) <= LEAD_MAX, f"snippets: sample_id {sid}: the lead is longer than {LEAD_MAX} characters"
        # The address is shown whenever it is not the sampled url: the labeler then reads the article link
        if fetched_url and fetched_url != r["url"]:
            fetched_from = fetched_url
        elif text and not fetched_url:
            fetched_from = NO_ADDRESS
        else:
            fetched_from = ""
        rows.append({"sample_id": sid, "record_id": r["record_id"], "url": r["url"], "park_code": r["park_code"],
                     "park_name": PARK_NAMES[r["park_code"]],
                     "archive_url": f"https://web.archive.org/web/{r['file_ts']}/{r['url']}",
                     "title": title, "lead": lead,
                     "not_fetched": "" if text else s["status"].strip(),
                     "fetched_from": fetched_from})
    return rows


def status_name(status: str) -> str:
    """A fetch status for the printed summary; anything that is not a short code (it could hold an address) is
    printed as '(other)', so no sampled url reaches the console."""
    return status if re.fullmatch(r"[A-Za-z0-9_.-]{1,40}", status) else "(other)"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=str(ROOT), help="repository root (default: the parent of src/)")
    ap.add_argument("--sample", help=f"the sample (default: {OUT_DIR}/{SAMPLE_NAME})")
    ap.add_argument("--snippets", help=f"the fetched article text (default: {SNIPPETS})")
    ap.add_argument("--drafts", help=f"Claude's drafts, written before the page is built (default: {DRAFTS})")
    ap.add_argument("--out", help=f"the page (default: {PAGE}); inside the repository only under {WORK}/")
    args = ap.parse_args()

    repo = Path(args.repo_root).resolve()
    sample_path = Path(args.sample) if args.sample else repo / OUT_DIR / SAMPLE_NAME
    snippets_path = Path(args.snippets) if args.snippets else repo / SNIPPETS
    drafts_path = Path(args.drafts) if args.drafts else repo / DRAFTS
    out = Path(args.out) if args.out else repo / PAGE
    if inside(out, repo) and not inside(out, repo / WORK):
        raise SystemExit(f"REFUSED: the page holds article text; write it under {repo / WORK} (git-ignored)")

    sample = read_csv(sample_path, SAMPLE_COLUMNS)
    # The drafts come first (docs/week8/a1_part2_spec.md, labeling procedure): a draft for every row, in the format
    # that score reads. The page never shows them; only their sha256 is printed.
    if not drafts_path.is_file():
        raise SystemExit(f"REFUSED: no drafts file at {drafts_path}; Claude writes the drafts before the page is built")
    drafts, _ = read_drafts(drafts_path, [{"sample_id": int(r["sample_id"])} for r in sample])
    if len(drafts) != len(sample):
        raise SystemExit(f"REFUSED: {drafts_path} has drafts for {len(drafts)} of the {len(sample)} sample rows")
    snippets = read_csv(snippets_path, SNIPPET_COLUMNS) if snippets_path.is_file() else []
    rows = page_rows(sample, snippets)
    fingerprint = hashlib.sha256(sample_path.read_bytes()).hexdigest()
    # The rows go in last: fetched text such as "__KEY__" in a title must reach the page unchanged
    html = (HTML.replace("__N__", str(len(rows)))
            .replace("__READING__", "".join(f"<li>{t}</li>" for t in READING))
            .replace("__RULINGS__", "".join(f"<li>{t}</li>" for t in RULINGS))
            .replace("__SAVING__", "".join(f"<li>{t}</li>" for t in SAVING))
            .replace("__FINGERPRINT__", fingerprint)
            .replace("__KEY__", STORAGE_KEY)
            .replace("__COLUMNS__", json.dumps(LABEL_COLUMNS))
            .replace("__DOWNLOAD__", LABELS_NAME)
            .replace("__PROGRESS__", PROGRESS_NAME)
            .replace("__ROWS__", script_json(rows)))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="ascii", newline="\n")
    # Counts only: the console never shows a sampled url
    shown = sum(bool(r["title"] or r["lead"]) for r in rows)
    print(f"{len(rows)} rows from {sample_path}")
    print(f"snippets: {len(snippets)} rows from {snippets_path}" if snippets else f"snippets: none at {snippets_path}")
    print(f"rows with fetched text: {shown}; fetched from another address than the url: "
          f"{sum(bool(r['fetched_from']) and r['fetched_from'] != NO_ADDRESS for r in rows)}; "
          f"text without a recorded address: {sum(r['fetched_from'] == NO_ADDRESS for r in rows)}")
    print("rows not fetched, by status:",
          dict(sorted(Counter(status_name(r["not_fetched"]) for r in rows if r["not_fetched"]).items())))
    print(f"page: {out}")
    print(f"sample sha256 (the page keeps labels of this sample only): {fingerprint}")
    print(f"drafts sha256 (not on the page; score's drafts source_sha256 must be this one): "
          f"{sha256_file(drafts_path)}, {len(drafts)} rows")


if __name__ == "__main__":
    main()
