"""Backfill historical agendas and minutes from the Internet Archive.

The Art Commission (renamed the Public Design Commission in 2008) posted
meeting agendas at nyc.gov/html/artcom/downloads/pdf/ from roughly 2004
through 2015. nyc.gov no longer serves those files, but the Wayback
Machine preserved them. This module discovers the archived PDFs via the
CDX API, downloads the raw files, and registers them in the meetings
table so the regular agenda parser can pick them up.

Meetings that already have a live-scraped agenda are never overwritten.
"""

import re
import time
from pathlib import Path

import httpx

from pdc.config import PDF_DIR, MINUTES_PDF_DIR

CDX_API = "https://web.archive.org/cdx/search/cdx"
ARTCOM_PDF_PREFIX = "nyc.gov/html/artcom/downloads/pdf/"

# Later corrections of the same meeting's agenda supersede the original
_VARIANT_RANK = {"revised": 2, "corrected": 1}

_HEADERS = {"User-Agent": "pdc-tracker/0.1 (historical backfill; contact tdfine@gmail.com)"}

# The Internet Archive rate-limits aggressively and answers bursts with
# connection refusals that can persist for minutes. Space requests out and
# back off hard when refused; re-running the command resumes where it left
# off since already-downloaded meetings are skipped.
_DOWNLOAD_DELAY = 1.5


def _get_with_retry(url: str, *, params: dict | None = None,
                    timeout: float = 60, attempts: int = 5) -> httpx.Response:
    delay = 10.0
    last_exc: Exception = RuntimeError("unreachable")
    for attempt in range(attempts):
        try:
            resp = httpx.get(url, params=params, timeout=timeout,
                             follow_redirects=True, headers=_HEADERS)
            resp.raise_for_status()
            return resp
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = isinstance(exc, httpx.TransportError) or status in (
                429, 500, 502, 503, 504,
            )
            last_exc = exc
            if not retryable or attempt == attempts - 1:
                raise
            time.sleep(delay)
            delay *= 2
    raise last_exc


def _parse_date_from_filename(filename: str) -> str | None:
    """Extract an ISO meeting date from artcom-era filenames.

    Agendas look like ``1-11-10_public_agenda.pdf`` or
    ``10-16-06publicagenda.pdf``; minutes like ``Minutes_10_20_14.PDF``.
    """
    stem = Path(filename).stem
    m = re.search(r"(\d{1,2})[-_](\d{1,2})[-_](\d{2,4})", stem)
    if not m:
        return None
    month, day, year = (int(g) for g in m.groups())
    if year < 100:
        year += 2000
    if not (1 <= month <= 12 and 1 <= day <= 31 and 1998 <= year <= 2030):
        return None
    return f"{year}-{month:02d}-{day:02d}"


def _variant_rank(filename: str) -> int:
    lower = filename.lower()
    for marker, rank in _VARIANT_RANK.items():
        if marker in lower:
            return rank
    return 0


def _raw_snapshot_url(timestamp: str, original: str) -> str:
    """Replay URL with the id_ flag, which serves the original bytes."""
    return f"https://web.archive.org/web/{timestamp}id_/{original}"


def discover_wayback_files() -> dict:
    """Query the CDX API for archived artcom PDFs.

    Returns {"agendas": [...], "minutes": [...]} where each entry is
    {"meeting_date", "url", "filename"} with url pointing at the raw
    snapshot of the newest capture of the best variant for that date.
    """
    resp = _get_with_retry(
        CDX_API,
        params={
            "url": ARTCOM_PDF_PREFIX,
            "matchType": "prefix",
            "output": "json",
            "fl": "original,timestamp,statuscode",
            "limit": 20000,
        },
    )
    rows = resp.json()
    if rows:
        rows = rows[1:]  # first row is the header

    # Newest OK capture per filename (URLs vary only by :80 / scheme)
    latest: dict[str, tuple[str, str]] = {}  # filename -> (timestamp, original)
    for original, timestamp, statuscode in rows:
        if statuscode != "200":
            continue
        filename = Path(original).name
        key = filename.lower()
        if key not in latest or timestamp > latest[key][0]:
            latest[key] = (timestamp, original)

    # Best variant per meeting date
    best_agendas: dict[str, tuple[int, str, str, str]] = {}
    minutes: dict[str, tuple[str, str, str]] = {}
    for key, (timestamp, original) in latest.items():
        filename = Path(original).name
        meeting_date = _parse_date_from_filename(filename)
        if not meeting_date:
            continue
        if "agenda" in key:
            rank = _variant_rank(filename)
            current = best_agendas.get(meeting_date)
            if current is None or (rank, timestamp) > (current[0], current[1]):
                best_agendas[meeting_date] = (rank, timestamp, original, filename)
        elif "minute" in key:
            current = minutes.get(meeting_date)
            if current is None or timestamp > current[0]:
                minutes[meeting_date] = (timestamp, original, filename)

    return {
        "agendas": [
            {
                "meeting_date": date,
                "url": _raw_snapshot_url(ts, original),
                "filename": filename,
            }
            for date, (_rank, ts, original, filename) in sorted(best_agendas.items())
        ],
        "minutes": [
            {
                "meeting_date": date,
                "url": _raw_snapshot_url(ts, original),
                "filename": filename,
            }
            for date, (ts, original, filename) in sorted(minutes.items())
        ],
    }


def _download_pdf(url: str, dest: Path) -> bool:
    """Download a snapshot, verifying it is actually a PDF."""
    if dest.exists():
        return True
    resp = _get_with_retry(url, timeout=120)
    if not resp.content.startswith(b"%PDF"):
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(resp.content)
    time.sleep(_DOWNLOAD_DELAY)
    return True


def sync_wayback_agendas(conn) -> dict:
    """Download archived agendas for meetings we have no agenda for."""
    files = discover_wayback_files()
    downloaded = 0
    skipped = 0
    failed = 0

    for item in files["agendas"]:
        meeting_date = item["meeting_date"]
        existing = conn.execute(
            "SELECT agenda_fetched_at FROM meetings WHERE meeting_date = ?",
            (meeting_date,),
        ).fetchone()
        if existing and existing["agenda_fetched_at"]:
            skipped += 1
            continue

        try:
            ok = _download_pdf(item["url"], PDF_DIR / item["filename"])
        except httpx.HTTPError:
            ok = False
        if not ok:
            failed += 1
            continue

        conn.execute(
            """INSERT INTO meetings (meeting_date, agenda_pdf_url, agenda_fetched_at,
                                     notes)
               VALUES (?, ?, datetime('now'), 'Agenda recovered from Internet Archive')
               ON CONFLICT(meeting_date) DO UPDATE SET
                   agenda_pdf_url = excluded.agenda_pdf_url,
                   agenda_fetched_at = excluded.agenda_fetched_at""",
            (meeting_date, item["url"]),
        )
        downloaded += 1

    for item in files["minutes"]:
        meeting_date = item["meeting_date"]
        existing = conn.execute(
            "SELECT minutes_fetched_at FROM meetings WHERE meeting_date = ?",
            (meeting_date,),
        ).fetchone()
        if existing and existing["minutes_fetched_at"]:
            skipped += 1
            continue

        try:
            ok = _download_pdf(item["url"], MINUTES_PDF_DIR / item["filename"])
        except httpx.HTTPError:
            ok = False
        if not ok:
            failed += 1
            continue

        conn.execute(
            """INSERT INTO meetings (meeting_date, minutes_pdf_url, minutes_fetched_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(meeting_date) DO UPDATE SET
                   minutes_pdf_url = excluded.minutes_pdf_url,
                   minutes_fetched_at = excluded.minutes_fetched_at""",
            (meeting_date, item["url"]),
        )
        downloaded += 1

    conn.commit()
    return {
        "source": "wayback",
        "agendas_found": len(files["agendas"]),
        "minutes_found": len(files["minutes"]),
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
    }
