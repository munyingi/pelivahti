"""Builds docs/Pelivahti_case_study.pdf from the warehouse produced by `pelivahti run-local`.

python docs/build_case_study.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from reportlab.graphics.charts.barcharts import HorizontalBarChart
from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    Image,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "Pelivahti_case_study.pdf"
IMG = ROOT / "docs" / "images"
REPO = "https://github.com/munyingi/pelivahti"

INK, MUTED, RULE = colors.HexColor("#1f1f1d"), colors.HexColor("#52514e"), colors.HexColor("#d9d8d4")
BLUE, ORANGE, GREEN, RED = (colors.HexColor(c) for c in ("#2a78d6", "#eb6834", "#0ca30c", "#d03b3b"))
SEV = {"critical": "#d03b3b", "high": "#ec835a", "medium": "#fab219", "low": "#8a8986"}
PAGE_W, PAGE_H = A4
MARGIN = 18 * mm
WIDTH = PAGE_W - 2 * MARGIN

H1 = ParagraphStyle("h1", fontName="Helvetica-Bold", fontSize=22, leading=27, textColor=INK, spaceAfter=4)
SUB = ParagraphStyle("sub", fontName="Helvetica", fontSize=11, leading=15, textColor=MUTED, spaceAfter=10)
H2 = ParagraphStyle(
    "h2", fontName="Helvetica-Bold", fontSize=13, leading=17, textColor=INK, spaceBefore=10, spaceAfter=5
)
BODY = ParagraphStyle(
    "body", fontName="Helvetica", fontSize=9.6, leading=13.6, textColor=INK, alignment=TA_LEFT, spaceAfter=5
)
SMALL = ParagraphStyle("small", parent=BODY, fontSize=8.4, leading=11.2, textColor=MUTED)
CELL = ParagraphStyle("cell", parent=BODY, fontSize=8.6, leading=11.4, spaceAfter=0)
CELLB = ParagraphStyle("cellb", parent=CELL, fontName="Helvetica-Bold")
KPI_V = ParagraphStyle("kv", fontName="Helvetica-Bold", fontSize=16, leading=19, textColor=INK)
KPI_L = ParagraphStyle("kl", fontName="Helvetica", fontSize=8, leading=10, textColor=MUTED)


def compact(x) -> str:
    x = float(x)
    return f"EUR {x / 1000:,.0f}k" if x >= 100_000 else f"EUR {x:,.0f}".replace(",", " ")


def eur(x) -> str:
    return f"EUR {float(x):,.0f}".replace(",", " ")


def table(rows, widths, header=True):
    data = [[Paragraph(str(c), CELLB if (header and i == 0) else CELL) for c in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=widths, hAlign="LEFT")
    style = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LINEBELOW", (0, 0), (-1, -1), 0.4, RULE),
        ("TOPPADDING", (0, 0), (-1, -1), 3.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]
    if header:
        style += [("LINEBELOW", (0, 0), (-1, 0), 0.9, INK)]
    t.setStyle(TableStyle(style))
    return t


def kpis(items):
    cells = [[Paragraph(v, KPI_V) for v, _ in items], [Paragraph(label, KPI_L) for _, label in items]]
    t = Table(cells, colWidths=[WIDTH / len(items)] * len(items), hAlign="LEFT")
    t.setStyle(
        TableStyle(
            [
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 1),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
                ("LINEABOVE", (0, 0), (-1, 0), 0.9, INK),
            ]
        )
    )
    return t


def architecture() -> Drawing:
    w, h = WIDTH, 205
    d = Drawing(w, h)

    def box(x, y, bw, bh, title, sub, fill="#f3f2ef", stroke="#8a8986"):
        d.add(
            Rect(
                x,
                y,
                bw,
                bh,
                rx=5,
                ry=5,
                fillColor=colors.HexColor(fill),
                strokeColor=colors.HexColor(stroke),
                strokeWidth=0.8,
            )
        )
        d.add(
            String(
                x + bw / 2,
                y + bh - 14,
                title,
                fontName="Helvetica-Bold",
                fontSize=8.6,
                fillColor=INK,
                textAnchor="middle",
            )
        )
        for i, line in enumerate(sub):
            d.add(
                String(
                    x + bw / 2,
                    y + bh - 26 - i * 10,
                    line,
                    fontName="Helvetica",
                    fontSize=7.2,
                    fillColor=MUTED,
                    textAnchor="middle",
                )
            )

    def arrow(x1, y1, x2, y2):
        d.add(Line(x1, y1, x2, y2, strokeColor=MUTED, strokeWidth=0.9))
        import math

        a = math.atan2(y2 - y1, x2 - x1)
        p = [
            x2,
            y2,
            x2 - 6 * math.cos(a - 0.4),
            y2 - 6 * math.sin(a - 0.4),
            x2 - 6 * math.cos(a + 0.4),
            y2 - 6 * math.sin(a + 0.4),
        ]
        d.add(Polygon(p, fillColor=MUTED, strokeColor=MUTED))

    bw, bh = 86, 52
    gap = (w - 5 * bw) / 4
    cols = [i * (bw + gap) for i in range(5)]
    box(cols[0], 125, bw, bh, "Generator", ["players, bets, results", "late, duplicate, invalid"])
    box(cols[1], 125, bw, bh, "Redpanda", ["pelivahti.events", "3 partitions by player"], "#e6f0fb", "#2a78d6")
    box(cols[2], 160, bw, bh - 8, "Stream engine", ["watermark, settlement", "RG rules"], "#e6f0fb", "#2a78d6")
    box(cols[2], 85, bw, bh - 8, "Lander", ["contracts + MERGE"], "#fdece4", "#eb6834")
    box(cols[3], 160, bw, bh - 8, "DuckDB realtime", ["ledger, alerts, offsets", "+ checkpoint (1 txn)"])
    box(cols[3], 85, bw, bh - 8, "Delta Lake bronze", ["events, DLQ, ingest log"], "#fdece4", "#eb6834")
    box(cols[4], 122, bw, bh, "dbt + DuckDB", ["marts, reports", "reconciliation tests"])
    box(cols[4], 30, bw, bh - 10, "Streamlit", ["GGR, tax, alerts, DQ"])
    box(
        cols[2],
        0,
        bw * 2 + gap,
        40,
        "Dagster schedules ingest, stream and dbt;",
        ["GitHub Actions runs it all on every push"],
        "#ffffff",
        "#d9d8d4",
    )
    arrow(cols[0] + bw, 151, cols[1], 151)
    arrow(cols[1] + bw, 160, cols[2], 182)
    arrow(cols[1] + bw, 142, cols[2], 107)
    arrow(cols[2] + bw, 182, cols[3], 182)
    arrow(cols[2] + bw, 107, cols[3], 107)
    arrow(cols[3] + bw, 182, cols[4], 160)
    arrow(cols[3] + bw, 107, cols[4], 135)
    arrow(cols[4] + bw / 2, 122, cols[4] + bw / 2, 72)
    return d


def alerts_chart(rows) -> Drawing:
    labels = {
        "SELF_EXCLUSION_BREACH": "Played while self-excluded",
        "MONTHLY_LOSS_LIMIT_BREACH": "Monthly limit breached",
        "NIGHT_MARATHON_SESSION": "Night marathon session",
        "STALE_ODDS_ACCEPTED": "Stale odds accepted",
        "DEPOSIT_VELOCITY": "Rapid deposits",
        "DAILY_LOSS_LIMIT_BREACH": "Daily limit breached",
        "DAILY_LOSS_LIMIT_80PCT": "80% of daily limit",
        "LOSS_CHASING": "Loss chasing",
    }
    rows = sorted(rows, key=lambda r: r[2])
    d = Drawing(WIDTH, 170)
    c = HorizontalBarChart()
    c.x, c.y, c.width, c.height = 135, 18, WIDTH - 175, 145
    c.data = [[r[2] for r in rows]]
    c.categoryAxis.categoryNames = [f"{labels.get(r[0], r[0])} ({r[1]})" for r in rows]
    c.categoryAxis.labels.fontName, c.categoryAxis.labels.fontSize = "Helvetica", 7.6
    c.categoryAxis.labels.fillColor = MUTED
    c.categoryAxis.strokeColor = RULE
    c.valueAxis.labels.fontName, c.valueAxis.labels.fontSize = "Helvetica", 7.2
    c.valueAxis.labels.fillColor = MUTED
    c.valueAxis.strokeColor = colors.white
    c.valueAxis.visibleGrid, c.valueAxis.gridStrokeColor = True, RULE
    c.valueAxis.valueMin = 0
    c.barWidth, c.groupSpacing = 9, 6
    c.bars.strokeColor = None
    for i, r in enumerate(rows):
        c.bars[(0, i)].fillColor = colors.HexColor(SEV[r[1]])
    c.barLabelFormat = "%d"
    c.barLabels.fontName, c.barLabels.fontSize, c.barLabels.fillColor = "Helvetica", 7.2, INK
    c.barLabels.nudge = 9
    d.add(c)
    return d


def shot(name: str, caption: str, height: float = 78 * mm):
    """Embed a dashboard screenshot, cropped below the shared page header so the charts stay readable."""
    import tempfile

    from PIL import Image as PILImage
    from reportlab.lib.utils import ImageReader

    src = PILImage.open(IMG / name)
    top = int(src.height * 0.355)
    path = Path(tempfile.gettempdir()) / f"crop_{name}"
    src.crop((0, top, src.width, src.height)).save(path)
    iw, ih = ImageReader(str(path)).getSize()
    crop_ratio = ih / iw
    img = Image(str(path), width=WIDTH, height=WIDTH * crop_ratio)
    if WIDTH * crop_ratio > height:
        img = Image(str(path), width=height / crop_ratio, height=height)
    img.hAlign = "CENTER"
    return KeepTogether([img, Spacer(1, 3), Paragraph(caption, SMALL), Spacer(1, 6)])


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(MARGIN, 10 * mm, "Pelivahti case study  |  Samwel Munyingi  |  github.com/munyingi/pelivahti")
    canvas.drawRightString(PAGE_W - MARGIN, 10 * mm, f"{doc.page}")
    canvas.restoreState()


def main() -> None:
    con = duckdb.connect(str(ROOT / "data" / "warehouse.duckdb"), read_only=True)
    ggr = con.execute(
        "select product, sum(turnover_eur), sum(winnings_eur), sum(ggr_eur) from marts.fct_ggr_daily "
        "group by 1 order by 1"
    ).fetchall()
    tax = dict(con.execute("select product, sum(tax_eur) from marts.fct_gambling_tax_monthly group by 1").fetchall())
    alerts = con.execute("select rule, severity, count(*) from marts.fct_rg_alerts group by all").fetchall()
    tiers = dict(con.execute("select risk_tier, count(*) from marts.dim_players group by 1").fetchall())
    dq = dict(con.execute("select metric, value from reports.rpt_data_quality").fetchall())
    gap = con.execute(
        "select sum(late_data_gap_eur), count(*) filter (where late_data_gap_eur <> 0) "
        "from reports.rpt_realtime_vs_batch_daily"
    ).fetchone()
    rps = con.execute(
        "select sum(records) / sum(seconds) from reports.rpt_stream_batches where records > 0"
    ).fetchone()[0]
    con.close()

    turnover = sum(float(r[1]) for r in ggr)
    total_ggr = sum(float(r[3]) for r in ggr)
    total_tax = sum(float(v) for v in tax.values())
    n_alerts = sum(r[2] for r in alerts if r[0] != "STALE_ODDS_ACCEPTED")

    s = []
    s += [
        Paragraph("Pelivahti", H1),
        Paragraph(
            "Real-time betting settlement and responsible gaming pipeline for a regulated gambling operator", SUB
        ),
        Paragraph(
            f"Case study by Samwel Munyingi, November 2026. All data is synthetic. Code: <link href='{REPO}' color='#2a78d6'>{REPO}</link>",
            SMALL,
        ),
        Spacer(1, 8),
    ]
    s += [
        kpis(
            [
                (compact(turnover), "turnover simulated"),
                (compact(total_ggr), "gross gaming revenue"),
                (compact(total_tax), "estimated gambling tax (22%)"),
                (f"{n_alerts:,}", "responsible gaming alerts"),
                ("0.00", "EUR stream vs batch difference"),
            ]
        )
    ]
    s += [
        Paragraph("The problem", H2),
        Paragraph(
            "A licensed betting and online casino operator moves real money on every event, corrects results after "
            "payout, and must report revenue and gambling tax (modelled here as 22% of the gambling margin, stakes "
            "minus winnings). It needs three things from its data platform: money that is settled "
            "correctly even when events arrive late, twice or broken; player protection rules that fire within "
            "seconds; and revenue and tax figures that can be proven.",
            BODY,
        ),
        Paragraph(
            "Pelivahti builds that platform end to end with free tools. "
            "A generator simulates 800 players over three days, about 79,000 messages across ten event types, "
            "and deliberately damages the stream the way real transport does.",
            BODY,
        ),
    ]
    s += [
        Paragraph("Architecture", H2),
        architecture(),
        Spacer(1, 4),
        Paragraph(
            "Two independent consumers read the same topic. The stream engine is the speed layer; the lander "
            "is the source of truth. dbt rebuilds the wallet ledger from bronze on its own and fails the build "
            "if the two disagree by a single cent.",
            SMALL,
        ),
    ]

    s += [
        Paragraph("What the pipeline had to survive", H2),
        table(
            [
                ["Problem injected", "Volume in the default run", "How it is handled"],
                ["Out-of-order delivery", "every event", "event time, 5 minute allowed lateness, watermark"],
                [
                    "Very late events (2 to 45 min)",
                    f"{dq['stream_late_events']:,} events",
                    "booked to the ledger, skipped by closed windows, logged and measured by the batch layer",
                ],
                [
                    "Redelivered duplicates",
                    f"{dq['stream_duplicates_dropped']:,} messages",
                    "dropped on event_id; deterministic output ids; idempotent Delta MERGE",
                ],
                [
                    "Contract violations, truncated JSON",
                    f"{dq['bronze_dlq']:,} messages",
                    "JSON Schema contracts, dead-letter queue with the reason",
                ],
                [
                    "Corrected match results",
                    "2.5% of matches",
                    "append-only ledger: REVERSAL entry plus a new settlement",
                ],
            ],
            [WIDTH * 0.3, WIDTH * 0.22, WIDTH * 0.48],
        ),
    ]
    s += [PageBreak()]

    s += [
        Paragraph("Key engineering decisions", H2),
        Paragraph(
            "<b>Exactly-once without Kafka transactions.</b> Every micro-batch commits in one DuckDB "
            "transaction: ledger rows, alerts, closed windows, DLQ and late logs, the next offset per partition, "
            "and a checkpoint of the engine state. Offsets are never committed to Kafka. After a crash the engine "
            "reloads the checkpoint and seeks to the stored offsets. A test crashes the engine inside a "
            "transaction, restarts it and asserts that every output table is identical to a clean run; another "
            "replays the whole stream on a finished database and asserts nothing changes.",
            BODY,
        ),
        Paragraph(
            "<b>Money is never dropped, windows are never reopened.</b> A late bet still settles and still hits "
            "the ledger, but it does not reopen a closed daily window. The batch layer is the correction path: "
            f"in the default run late events moved {eur(gap[0])} of net loss across {gap[1]} player-days, and "
            "one player crossed a daily limit only after the window had closed. A dbt warning test surfaces "
            "exactly that case instead of hiding it.",
            BODY,
        ),
        Paragraph(
            "<b>Corrections are append-only.</b> A corrected match result writes a reversal of the old "
            "settlement and a new settlement. Nothing is updated in place, so the ledger is an audit trail.",
            BODY,
        ),
        Paragraph(
            "<b>Same rules in both layers.</b> Loss limits are effective-dated in the engine and in dbt "
            "(lowering applies at once, raising waits 24 hours; an ASOF join finds the limit in force), so the "
            "stream and the warehouse can be compared row by row.",
            BODY,
        ),
    ]

    s += [
        Paragraph("Responsible gaming rules", H2),
        table(
            [
                ["Rule", "Severity", "Window"],
                ["Played while self-excluded", "critical", "none, checked even for late events"],
                ["Daily or monthly loss limit breached", "high", "local calendar day or month"],
                ["Loss chasing: deposit within 10 min of a loss, then 2x the usual stake", "high", "20 min sliding"],
                ["Rapid deposits: 3 or more within 15 minutes", "medium", "15 min sliding"],
                ["Night marathon: 90+ minute session touching 00:00 to 05:00", "medium", "session, 20 min gap"],
                ["80% of the daily loss limit reached", "low", "local calendar day"],
                ["Bet accepted at stale odds (operations)", "medium", "odds valid at bet time"],
            ],
            [WIDTH * 0.56, WIDTH * 0.14, WIDTH * 0.30],
        ),
        Spacer(1, 6),
        alerts_chart(alerts),
        Paragraph(
            f"Alerts raised in the default run, coloured by severity. Players by risk tier: {tiers.get('red', 0)} "
            f"red, {tiers.get('amber', 0)} amber, {tiers.get('green', 0)} green.",
            SMALL,
        ),
    ]
    s += [PageBreak()]

    s += [
        Paragraph("Revenue and tax", H2),
        table(
            [["Product", "Turnover", "Winnings", "GGR", "Hold", "Tax at 22%"]]
            + [[p, eur(t), eur(w), eur(g), f"{float(g) / float(t):.1%}", eur(tax.get(p, 0))] for p, t, w, g in ggr],
            [WIDTH * 0.16] + [WIDTH * 0.168] * 5,
        ),
        Paragraph(
            "Sports bets count on the day they settle; void and open bets are excluded. Tax is modelled as 22% "
            "of positive monthly GGR per product. Illustrative only, not tax advice.",
            SMALL,
        ),
        shot("01_revenue_and_tax.png", "Dashboard: revenue, hold and the tax estimate."),
        shot("02_responsible_gaming.png", "Dashboard: alerts by rule and players by risk tier."),
    ]
    s += [PageBreak()]
    s += [
        shot(
            "04_data_quality.png", "Dashboard: reconciliation status, dead letters, duplicates and the late-data gap."
        ),
        shot("03_player_drilldown.png", "Dashboard: one player's net loss against their daily limit, alerts and bets."),
    ]

    s += [
        Paragraph("Testing and delivery", H2),
        table(
            [
                ["Layer", "What is checked"],
                [
                    "pytest (29 + 2 Kafka)",
                    "contracts, settlement maths and rounding, every rule, watermark and window "
                    "closing, crash and resume, full replay, idempotent bronze, Kafka results equal file results",
                ],
                [
                    "dbt (46 data tests)",
                    "per-player and per-bet reconciliation with the stream, GGR identity, tax "
                    "formula, no bets after kickoff, no double-booked stakes, keys and accepted values",
                ],
                [
                    "GitHub Actions",
                    "lint and tests; full pipeline plus dbt build plus Dagster; end to end through "
                    "a Redpanda container",
                ],
            ],
            [WIDTH * 0.24, WIDTH * 0.76],
        ),
        Paragraph("Stack", H2),
        Paragraph(
            "Python 3.12, Redpanda (Kafka API) with confluent-kafka, Delta Lake (delta-rs), DuckDB, dbt-core "
            "with dbt-duckdb, Dagster, Streamlit and Plotly, JSON Schema, pytest, ruff, Docker Compose, GitHub "
            f"Actions. The stream engine processed about {rps:,.0f} records per second on a laptop.",
            BODY,
        ),
        Paragraph(
            "Run it: <font face='Courier' size='8.6'>make setup &amp;&amp; make run</font> builds "
            "everything without Kafka in about 40 seconds; <font face='Courier' size='8.6'>make kafka-up "
            "&amp;&amp; make kafka-run</font> runs it through a real broker.",
            BODY,
        ),
    ]

    doc = SimpleDocTemplate(
        str(OUT),
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title="Pelivahti case study",
        author="Samwel Munyingi",
        subject="Real-time betting settlement and responsible gaming data pipeline",
    )
    doc.build(s, onFirstPage=footer, onLaterPages=footer)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
