"""Build the two-page team proposal and its architecture diagram."""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from docx import Document
from docx.enum.table import WD_CELL_VERTICAL_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Inches, Pt, RGBColor
from docx.oxml import OxmlElement
from docx.oxml.ns import qn


HERE = Path(__file__).resolve().parent
OUT = HERE / "team_project_proposal.docx"
DIAGRAM = HERE / "team_architecture.png"
BLUE = RGBColor(31, 69, 112)
DARK = RGBColor(34, 45, 58)
GRAY = RGBColor(90, 100, 110)


def font(size, bold=False):
    path = Path(r"C:\Windows\Fonts\arialbd.ttf" if bold else r"C:\Windows\Fonts\arial.ttf")
    return ImageFont.truetype(str(path), size)


def centered(draw, box, lines, fill, size=34, bold=False, gap=8):
    f = font(size, bold)
    heights = [draw.textbbox((0, 0), line, font=f)[3] for line in lines]
    total = sum(heights) + gap * (len(lines) - 1)
    y = box[1] + (box[3] - box[1] - total) / 2
    for line, h in zip(lines, heights):
        width = draw.textlength(line, font=f)
        draw.text((box[0] + (box[2] - box[0] - width) / 2, y), line, font=f, fill=fill)
        y += h + gap


def arrow(draw, x1, y1, x2, y2, color="#657586", width=8):
    draw.line((x1, y1, x2, y2), fill=color, width=width)
    if x2 > x1:
        draw.polygon([(x2, y2), (x2 - 22, y2 - 14), (x2 - 22, y2 + 14)], fill=color)
    else:
        draw.polygon([(x2, y2), (x2 - 14, y2 - 22), (x2 + 14, y2 - 22)], fill=color)


def make_diagram():
    im = Image.new("RGB", (2400, 510), "white")
    d = ImageDraw.Draw(im)
    lanes = [
        (20, 34, 660, 475, "#FBF5EF", "#B95A2B", "OLTP"),
        (695, 34, 1745, 475, "#F0F5FA", "#336DA7", "TRANSFER AND LAKEHOUSE"),
        (1780, 34, 2380, 475, "#F0F7F1", "#368260", "OLAP"),
    ]
    for x1, y1, x2, y2, fill, outline, label in lanes:
        d.rounded_rectangle((x1, y1, x2, y2), radius=26, fill=fill, outline=outline, width=5)
        centered(d, (x1, 42, x2, 106), [label], outline, size=35, bold=True)

    boxes = [
        ((55, 138, 625, 270), ["Recreation.gov", "reservation CSVs"], "#B95A2B"),
        ((55, 322, 625, 436), ["MySQL 3NF", "booking replay"], "#B95A2B"),
        ((735, 138, 1140, 278), ["Incremental", "Parquet extract"], "#336DA7"),
        ((1175, 138, 1705, 278), ["Wikimedia 5 TB+", "NPS  NOAA  OPM"], "#336DA7"),
        ((735, 322, 1705, 436), ["UC landing  >  Bronze  >  Silver  >  Gold", "audit and quality checks"], "#336DA7"),
        ((1820, 138, 2340, 278), ["Snowflake", "park by month star"], "#368260"),
        ((1820, 322, 2340, 436), ["Dashboard and", "analysis SQL"], "#368260"),
    ]
    for box, lines, outline in boxes:
        d.rounded_rectangle(box, radius=18, fill="white", outline=outline, width=4)
        centered(d, box, lines, "#1D2935", size=32, bold=False)
    arrow(d, 340, 270, 340, 313)
    arrow(d, 625, 379, 728, 379)
    arrow(d, 935, 278, 935, 313)
    arrow(d, 1440, 278, 1440, 313)
    arrow(d, 1705, 379, 1812, 209)
    arrow(d, 2080, 278, 2080, 313)
    im.save(DIAGRAM, dpi=(300, 300))


def shade(cell, fill):
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:fill"), fill)
    tc_pr.append(shd)


def margins(cell, top=80, start=100, bottom=80, end=100):
    tc = cell._tc
    tc_pr = tc.get_or_add_tcPr()
    tc_mar = OxmlElement("w:tcMar")
    for tag, value in (("top", top), ("start", start), ("bottom", bottom), ("end", end)):
        node = OxmlElement(f"w:{tag}")
        node.set(qn("w:w"), str(value))
        node.set(qn("w:type"), "dxa")
        tc_mar.append(node)
    tc_pr.append(tc_mar)


def add_run(p, text, bold=False, color=DARK, size=9.5):
    r = p.add_run(text)
    r.bold = bold
    r.font.name = "Aptos"
    r.font.size = Pt(size)
    r.font.color.rgb = color
    return r


def section(doc, title):
    p = doc.add_paragraph(style="Heading 2")
    p.paragraph_format.space_before = Pt(9)
    p.paragraph_format.space_after = Pt(3)
    add_run(p, title, bold=True, color=BLUE, size=10.5)
    return p


def body(doc, text, before=0, after=4):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(before)
    p.paragraph_format.space_after = Pt(after)
    p.paragraph_format.line_spacing = 1.08
    add_run(p, text)
    return p


def labeled(doc, label, text, after=5):
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(after)
    p.paragraph_format.line_spacing = 1.08
    add_run(p, label, bold=True, color=BLUE)
    add_run(p, text)
    return p


make_diagram()
doc = Document()
sec = doc.sections[0]
sec.page_width = Inches(8.5)
sec.page_height = Inches(11)
sec.top_margin = Inches(0.61)
sec.bottom_margin = Inches(0.55)
sec.left_margin = Inches(0.63)
sec.right_margin = Inches(0.63)
sec.header_distance = Inches(0.3)
sec.footer_distance = Inches(0.3)

normal = doc.styles["Normal"]
normal.font.name = "Aptos"
normal.font.size = Pt(9.5)
normal.font.color.rgb = DARK
normal.paragraph_format.space_after = Pt(4)
title_style = doc.styles["Title"]
title_style.font.name = "Aptos"
title_style.font.color.rgb = DARK
title_ppr = title_style._element.get_or_add_pPr()
for border in title_ppr.findall(qn("w:pBdr")):
    title_ppr.remove(border)

p = doc.add_paragraph(style="Title")
p.paragraph_format.space_after = Pt(2)
add_run(p, "National Park Demand Signals and a Cloud Lakehouse", bold=True, color=DARK, size=16)
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(7)
add_run(p, "Course project proposal  |  October 2026  |  Chujie Wang, Fan Du, You Fu, Tianyang Zhang", color=GRAY, size=8.8)

section(doc, "Problem and users")
body(doc, "We will ask whether reservation activity and Wikipedia attention help explain or anticipate monthly visits to Yellowstone, Yosemite, Grand Canyon, Zion, and Great Smoky Mountains. Park managers need staffing and shuttle plans; nearby businesses need capacity plans. We will compare each signal with a seasonality-only baseline for 2015-2025, with particular care around closures and permit caps. The outcome is NPS recreation visits at the park-by-month grain (5 parks x 132 months = 660 observations). The goal is a credible, auditable comparison, not a claim that these signals cause visits.")

section(doc, "Datasets rights and the 5 TB data rule")
table = doc.add_table(rows=1, cols=3)
table.autofit = False
col_widths = (1.55, 1.77, 3.92)
for i, w in enumerate(col_widths):
    table.columns[i].width = Inches(w)
hdr = table.rows[0].cells
for i, title in enumerate(("Dataset", "Location and role", "Rights and relevant scale")):
    hdr[i].width = Inches(col_widths[i])
    shade(hdr[i], "244D75")
    margins(hdr[i])
    hdr[i].vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
    p = hdr[i].paragraphs[0]
    p.paragraph_format.space_after = Pt(0)
    add_run(p, title, bold=True, color=RGBColor(255, 255, 255), size=8.3)

rows = [
    ("Wikimedia hourly pageviews", "Hourly dump [1]; attention signal and scale driver", "CC0. The May 2015-Oct 2026 archive is estimated at about 5.4 TB gzip (about 19 TB expanded), spread across roughly 100,000 hourly files. Week 8 will verify the exact byte total from the archive manifest."),
    ("Recreation.gov reservations", "Historical CSVs [2]; booking events replayed into MySQL", "Public download under the RIDB API Access Agreement; credit RIDB and publish aggregates only. FY2015-FY2025 files supply orders, facilities, and use dates for the five parks."),
    ("NPS visits, NOAA weather, OPM holidays", "Federal APIs and tables [3-5]; outcome and context", "Public federal sources. Local samples already contain 660 visit rows, 1,345 station-month weather rows, and 172 holiday rows. These small feeds do not establish the 5 TB rule."),
]
for ridx, row in enumerate(rows):
    cells = table.add_row().cells
    for i, value in enumerate(row):
        cells[i].width = Inches(col_widths[i])
        margins(cells[i])
        cells[i].vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
        if ridx % 2:
            shade(cells[i], "F5F7F9")
        p = cells[i].paragraphs[0]
        p.paragraph_format.space_after = Pt(0)
        p.paragraph_format.line_spacing = 1.04
        add_run(p, value, size=8.2)

body(doc, "Data rule: the Wikimedia archive itself exceeds 5 TB and is too large for one computer by storage and row count. Our worker will scan every row, retain park article counts and an all-English-Wikipedia denominator, and log input bytes, rows, and missing hours. Only the compact aggregates enter the lakehouse. The data is unrelated to Sakila and ClassicModels.", before=5, after=2)

section(doc, "Target architecture")
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(2)
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
p.add_run().add_picture(str(DIAGRAM), width=Inches(7.19))
body(doc, "A replay loader writes reservation records as idempotent transactions into a normalized MySQL OLTP schema. An order-time-and-ID watermark exports incremental Parquet files to a Databricks Unity Catalog landing volume. Wikimedia and the federal feeds enter the same landing area. Databricks builds Bronze raw tables, Silver typed park-month data, and Gold analytical tables with quality checks. Gold Parquet loads into a Snowflake star schema for OLAP queries and a dashboard. The repository currently contains the small-feed landing scripts and Bronze/Silver code; the MySQL replay, Wikimedia worker, and Snowflake load are planned.", after=0)

doc.add_page_break()
section(doc, "Two risky assumptions and Week 8 tests")
labeled(doc, "1  Full archive access and throughput. ", "The 5 TB claim and scan plan depend on complete, reachable hourly files and enough transfer bandwidth. By Week 8, Fan and Chujie will inventory the archive's file count and compressed bytes, then process 168 consecutive hours end to end. They will record bytes per second, rows read, missing hours, and replay behavior; daily park totals will be checked against Wikimedia's article API. Success means a verified archive above 5 TB, no lost files, reproducible totals, and a projected full-scan time within the remaining project schedule. Otherwise we will revise the ingestion plan and disclose the limit before claiming full historical coverage.", after=9)
labeled(doc, "2  Reservation events are usable leading signals. ", "Facility names, dates, and columns change across fiscal years, and a booked slot can reflect permit supply rather than unconstrained demand. By Week 8, You will load FY2024 plus an early fiscal year into MySQL, map facilities to the five parks, and verify primary/foreign keys and sample counts. Tianyang will form 'on the books' reservations as known at the end of the preceding month, then compare a seasonal baseline with and without reservations and pageviews on held-out 2024-2025 months, excluding 2020 closures. We will report mapping coverage, leakage checks, and error by park. If the signal does not improve out-of-sample error, the final result will be descriptive rather than a forecast.", after=5)

section(doc, "Team roles")
labeled(doc, "Chujie Wang  ", "integration lead; NPS/NOAA/OPM feeds, Databricks Silver/Gold, quality checks, and Week 8 synthesis.", after=2)
labeled(doc, "Fan Du  ", "Wikimedia archive manifest, hourly streaming filter, throughput benchmark, and ingestion audit.", after=2)
labeled(doc, "You Fu  ", "Recreation.gov schema mapping, MySQL OLTP model and replay loader, and incremental extract.", after=2)
labeled(doc, "Tianyang Zhang  ", "Snowflake OLAP star schema, dashboard, and held-out signal evaluation.", after=5)
body(doc, "All four members review the data rights, diagram, Week 8 evidence, and final presentation. Each component has one primary owner and shared review.", after=4)

section(doc, "Source notes")
sources = [
    "[1] Wikimedia Analytics pageviews readme, dumps.wikimedia.org/other/pageviews/readme.html (archive and CC0).",
    "[2] RIDB historical downloads and access terms, ridb.recreation.gov/download and ridb.recreation.gov/login.",
    "[3] NPS IRMA visitation API, irmaservices.nps.gov/v3/rest/Stats/help.",
    "[4] NOAA GSOM access, ncei.noaa.gov/access/search/data-search/global-summary-of-the-month.",
    "[5] OPM federal holidays, opm.gov/policy-data-oversight/pay-leave/federal-holidays/.",
]
for source in sources:
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(1)
    p.paragraph_format.line_spacing = 1.0
    add_run(p, source, color=GRAY, size=7.6)

footer = sec.footer.paragraphs[0]
footer.alignment = WD_ALIGN_PARAGRAPH.RIGHT
add_run(footer, "National Park Demand Signals  |  Course Project Proposal", color=GRAY, size=7.5)

doc.save(OUT)
print(OUT)
