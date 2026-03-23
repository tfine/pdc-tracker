import re
import sqlite3
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from pdc.config import (
    PAST_AGENDAS_URL, CURRENT_MEETINGS_URL,
    PDF_DIR, PRESENTATION_PDF_DIR, MINUTES_PDF_DIR,
)


def discover_agenda_urls() -> list[dict]:
    """Scrape PDC pages for agenda PDF links."""
    pages = [PAST_AGENDAS_URL, CURRENT_MEETINGS_URL]
    agendas = []
    seen_urls = set()

    for page_url in pages:
        try:
            resp = httpx.get(page_url, timeout=30, follow_redirects=True)
            resp.raise_for_status()
        except httpx.HTTPError:
            continue
        soup = BeautifulSoup(resp.text, "lxml")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "agenda" in href.lower() and href.endswith(".pdf"):
                url = href if href.startswith("http") else f"https://www.nyc.gov{href}"
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                # Try to extract date from filename
                # Patterns: M-DD-YY, MM-DD-YY, M-DD-YYYY
                date_match = re.search(r"(\d{1,2})-(\d{1,2})-(\d{2,4})", Path(url).stem)
                meeting_date = None
                if date_match:
                    m, d, y = date_match.groups()
                    year = int(y) if len(y) == 4 else 2000 + int(y)
                    meeting_date = f"{year}-{int(m):02d}-{int(d):02d}"

                agendas.append({
                    "url": url,
                    "meeting_date": meeting_date,
                    "filename": Path(url).name,
                })

    return agendas


def download_agenda(url: str, dest_dir: Path | None = None) -> Path:
    """Download an agenda PDF, return the local path."""
    dest = (dest_dir or PDF_DIR) / Path(url).name
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = httpx.get(url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    return dest


def sync_agendas(conn: sqlite3.Connection) -> dict:
    """Discover and download all agenda PDFs, update meetings table."""
    agendas = discover_agenda_urls()
    downloaded = 0
    for agenda in agendas:
        meeting_date = agenda["meeting_date"]
        url = agenda["url"]
        if not meeting_date:
            continue

        # Check if we already have this agenda
        existing = conn.execute(
            "SELECT agenda_fetched_at FROM meetings WHERE meeting_date = ?",
            (meeting_date,),
        ).fetchone()
        if existing and existing["agenda_fetched_at"]:
            continue

        try:
            path = download_agenda(url)
            conn.execute(
                """INSERT INTO meetings (meeting_date, agenda_pdf_url, agenda_fetched_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(meeting_date) DO UPDATE SET
                    agenda_pdf_url = excluded.agenda_pdf_url,
                    agenda_fetched_at = excluded.agenda_fetched_at""",
                (meeting_date, url),
            )
            downloaded += 1
        except httpx.HTTPError:
            continue

    conn.commit()
    return {"source": "agendas", "discovered": len(agendas), "downloaded": downloaded}


def discover_minutes_urls() -> list[dict]:
    """Scrape both PDC meetings pages for minutes/certificates PDF links."""
    pages = [PAST_AGENDAS_URL, CURRENT_MEETINGS_URL]
    minutes = []
    seen_urls = set()

    for page_url in pages:
        try:
            resp = httpx.get(page_url, timeout=30, follow_redirects=True)
            resp.raise_for_status()
        except httpx.HTTPError:
            continue
        soup = BeautifulSoup(resp.text, "lxml")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            lower = href.lower()
            # Match minutes/certificates PDFs — various naming conventions
            if not href.endswith(".pdf"):
                continue
            if not ("minute" in lower or "cert" in lower):
                continue
            url = href if href.startswith("http") else f"https://www.nyc.gov{href}"
            if url in seen_urls:
                continue
            seen_urls.add(url)

            date_match = re.search(r"(\d{1,2})-(\d{1,2})-(\d{2,4})", Path(url).stem)
            meeting_date = None
            if date_match:
                m, d, y = date_match.groups()
                year = int(y) if len(y) == 4 else 2000 + int(y)
                meeting_date = f"{year}-{int(m):02d}-{int(d):02d}"

            minutes.append({
                "url": url,
                "meeting_date": meeting_date,
                "filename": Path(url).name,
            })

    return minutes


def sync_minutes(conn: sqlite3.Connection) -> dict:
    """Discover and download minutes/certificates PDFs, update meetings table."""
    all_minutes = discover_minutes_urls()
    downloaded = 0
    MINUTES_PDF_DIR.mkdir(parents=True, exist_ok=True)

    for item in all_minutes:
        meeting_date = item["meeting_date"]
        url = item["url"]
        if not meeting_date:
            continue

        # Skip if already fetched
        existing = conn.execute(
            "SELECT minutes_fetched_at FROM meetings WHERE meeting_date = ?",
            (meeting_date,),
        ).fetchone()
        if existing and existing["minutes_fetched_at"]:
            continue

        try:
            dest = MINUTES_PDF_DIR / item["filename"]
            if not dest.exists():
                resp = httpx.get(url, timeout=60, follow_redirects=True)
                resp.raise_for_status()
                dest.write_bytes(resp.content)

            conn.execute(
                """INSERT INTO meetings (meeting_date, minutes_pdf_url, minutes_fetched_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(meeting_date) DO UPDATE SET
                    minutes_pdf_url = excluded.minutes_pdf_url,
                    minutes_fetched_at = excluded.minutes_fetched_at""",
                (meeting_date, url),
            )
            downloaded += 1
        except httpx.HTTPError:
            continue

    conn.commit()
    return {"source": "minutes", "discovered": len(all_minutes), "downloaded": downloaded}


def download_presentation(url: str) -> Path | None:
    """Download a presentation PDF, return the local path."""
    dest = PRESENTATION_PDF_DIR / Path(url).name
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        resp = httpx.get(url, timeout=60, follow_redirects=True)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        return dest
    except httpx.HTTPError:
        return None


def sync_presentations(conn: sqlite3.Connection) -> dict:
    """Extract presentation PDF URLs from all agenda PDFs and download them."""
    from pdc.ingest.agenda_parser import extract_presentation_urls

    downloaded = 0
    already_had = 0
    failed = 0
    all_urls = []

    # Scan all downloaded agenda PDFs for presentation links
    for pdf_path in sorted(PDF_DIR.glob("*.pdf")):
        urls = extract_presentation_urls(pdf_path)
        all_urls.extend(urls)

    unique_urls = sorted(set(all_urls))

    for url in unique_urls:
        # Check if already downloaded
        dest = PRESENTATION_PDF_DIR / Path(url).name
        if dest.exists():
            already_had += 1
            continue

        path = download_presentation(url)
        if path:
            downloaded += 1
        else:
            failed += 1

    # Update review_events with presentation URLs where we can match
    # Presentation filenames encode: date-pres-AGENCY-LEVEL-ProjectName.pdf
    linked = 0
    for url in unique_urls:
        filename = Path(url).stem.lower()
        # Try to extract meeting date from filename
        # Patterns: MM-DD-YYYY-pres-... or M-DD-YYYY-pres-...
        date_match = re.search(r"(\d{1,2})-(\d{1,2})-(\d{4})-pres", filename)
        if not date_match:
            continue
        m, d, y = date_match.groups()
        meeting_date = f"{int(y)}-{int(m):02d}-{int(d):02d}"

        # Update review events for this meeting date that don't have a presentation_url yet
        # Match by agency code in the filename
        rows = conn.execute(
            """SELECT id, raw_data FROM review_events
               WHERE meeting_date = ? AND presentation_url IS NULL
                 AND data_source = 'agenda_pdf'""",
            (meeting_date,),
        ).fetchall()

        for row in rows:
            # Try to fuzzy-match: the presentation filename contains agency and project info
            conn.execute(
                "UPDATE review_events SET presentation_url = ? WHERE id = ?",
                (url, row["id"]),
            )
            linked += 1
            break  # One URL per event for now

    conn.commit()
    return {
        "source": "presentations",
        "discovered": len(unique_urls),
        "downloaded": downloaded,
        "already_had": already_had,
        "failed": failed,
        "linked": linked,
    }
