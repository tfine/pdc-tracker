import json
import re
import sqlite3
from pathlib import Path

import pdfplumber

from pdc.config import PDF_DIR

# Regex to match agenda items: 5-digit cert number followed by title
# Example: "30261:  Reconstruction of Comfort Station..."
ITEM_PATTERN = re.compile(
    r"(\d{5}):\s+"  # Certificate number
    r"(.+?)"        # Title (non-greedy)
    r"\.\s*\("      # Period + opening paren for level of review
    r"([^)]+)"      # Level of review
    r"\)\s*"         # Closing paren
)

# CC/CB pattern: (CC XX, CB YY) or (CC XX/YY, CB ZZ)
CC_CB_PATTERN = re.compile(
    r"\(CC\s+([^,)]+),\s*CB\s+([^)]+)\)"
)

# Agency codes at end of item (after CC/CB or after level of review)
AGENCY_PATTERN = re.compile(
    r"(?:\)\s*)([\w/%]+(?:/[\w/%]+)*)\s*$"
)

# Section headers in agendas
SECTION_HEADERS = [
    "Consent Items",
    "Renewed Motion to Vote",
    "Public Hearing",
    "Committee Meeting",
    "Committee and Consent",
]


def extract_text(pdf_path: Path) -> str:
    """Extract all text from a PDF."""
    with pdfplumber.open(pdf_path) as pdf:
        pages = [page.extract_text() or "" for page in pdf.pages]
    return "\n".join(pages)


def extract_presentation_urls(pdf_path: Path) -> list[str]:
    """Extract presentation PDF URLs from hyperlinks embedded in an agenda PDF."""
    urls = set()
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                for link in page.hyperlinks:
                    uri = link.get("uri", "")
                    if "pres" in uri.lower() and uri.endswith(".pdf"):
                        urls.add(uri)
    except Exception:
        pass
    return sorted(urls)


def parse_agenda_text(text: str, meeting_date: str | None = None) -> list[dict]:
    """Parse agenda text into structured items."""
    # Normalize whitespace: join lines that are continuations
    # (lines not starting with a cert number are continuations)
    lines = text.split("\n")
    merged_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        # If line starts with a 5-digit number followed by colon, it's a new item
        if re.match(r"\d{5}:", stripped):
            merged_lines.append(stripped)
        elif merged_lines:
            # Continuation of previous line
            merged_lines[-1] += " " + stripped
        else:
            merged_lines.append(stripped)

    # Determine current section
    current_section = None
    items = []

    for line in merged_lines:
        # Check for section headers
        for header in SECTION_HEADERS:
            if header.lower() in line.lower():
                current_section = header
                break

        # Try to match an agenda item
        match = ITEM_PATTERN.search(line)
        if not match:
            continue

        cert_number = match.group(1)
        title = match.group(2).strip()
        level_of_review = match.group(3).strip()

        # Extract CC/CB
        cc_match = CC_CB_PATTERN.search(line)
        cc_district = cc_match.group(1).strip() if cc_match else None
        community_board = cc_match.group(2).strip() if cc_match else None

        # Extract agency codes (after the last closing paren)
        # Find text after the last )
        last_paren = line.rfind(")")
        agency = None
        if last_paren >= 0 and last_paren < len(line) - 1:
            tail = line[last_paren + 1:].strip()
            if tail and re.match(r"[\w/%]+", tail):
                agency = tail.strip()

        # Extract scheduled time if present (e.g., "10:50 a.m.")
        time_match = re.search(r"(\d{1,2}:\d{2}\s*[ap]\.?m\.?)", line, re.IGNORECASE)
        scheduled_time = time_match.group(1) if time_match else None

        items.append({
            "certificate_number": cert_number,
            "title": title,
            "level_of_review": level_of_review,
            "cc_district": cc_district,
            "community_board": community_board,
            "agency": agency,
            "agenda_section": current_section,
            "scheduled_time": scheduled_time,
            "meeting_date": meeting_date,
        })

    return items


def parse_agenda_pdf(pdf_path: Path, meeting_date: str | None = None) -> list[dict]:
    """Parse an agenda PDF file into structured items."""
    text = extract_text(pdf_path)
    return parse_agenda_text(text, meeting_date)


def ingest_parsed_items(conn: sqlite3.Connection, items: list[dict]) -> dict:
    """Insert parsed agenda items into review_events and enrich projects."""
    inserted = 0
    for item in items:
        cert = item["certificate_number"]
        meeting_date = item["meeting_date"]
        if not meeting_date:
            continue

        # Try to find matching project by certificate number
        row = conn.execute(
            "SELECT project_id FROM review_events WHERE certificate_number = ? LIMIT 1",
            (cert,),
        ).fetchone()
        project_id = row["project_id"] if row else None

        # If no match by cert, create a placeholder project
        if not project_id:
            project_id = f"agenda_{cert}"
            conn.execute(
                """INSERT OR IGNORE INTO projects (project_id, title, borough)
                VALUES (?, ?, NULL)""",
                (project_id, item["title"]),
            )

        # Update project with CC/CB info from agenda
        if item["cc_district"] or item["community_board"]:
            conn.execute(
                """UPDATE projects SET
                    cc_district = COALESCE(?, cc_district),
                    community_board = COALESCE(?, community_board)
                WHERE project_id = ?""",
                (item["cc_district"], item["community_board"], project_id),
            )

        # Insert review event
        conn.execute(
            """INSERT OR IGNORE INTO review_events
                (project_id, certificate_number, meeting_date, level_of_review,
                 agenda_section, scheduled_time, data_source, raw_data)
            VALUES (?, ?, ?, ?, ?, ?, 'agenda_pdf', ?)""",
            (project_id, cert, meeting_date, item["level_of_review"],
             item["agenda_section"], item["scheduled_time"], json.dumps(item)),
        )
        inserted += 1

    conn.commit()
    return {"inserted": inserted}


def sync_parse_agendas(conn: sqlite3.Connection) -> dict:
    """Parse all downloaded agenda PDFs that haven't been parsed yet."""
    total_items = 0
    pdfs_parsed = 0

    # Get meetings with downloaded agendas
    rows = conn.execute(
        "SELECT meeting_date, agenda_pdf_url FROM meetings WHERE agenda_fetched_at IS NOT NULL"
    ).fetchall()

    for row in rows:
        meeting_date = row["meeting_date"]
        url = row["agenda_pdf_url"]
        if not url:
            continue

        # Check if we already have agenda_pdf events for this date
        existing = conn.execute(
            "SELECT COUNT(*) as cnt FROM review_events WHERE meeting_date = ? AND data_source = 'agenda_pdf'",
            (meeting_date,),
        ).fetchone()
        if existing["cnt"] > 0:
            continue

        pdf_path = PDF_DIR / Path(url).name
        if not pdf_path.exists():
            continue

        items = parse_agenda_pdf(pdf_path, meeting_date)
        if items:
            result = ingest_parsed_items(conn, items)
            total_items += result["inserted"]
            pdfs_parsed += 1

    return {"source": "agenda_parser", "pdfs_parsed": pdfs_parsed, "items_inserted": total_items}
