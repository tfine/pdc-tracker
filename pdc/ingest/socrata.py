import json
import sqlite3
from datetime import datetime

import httpx
from rich.progress import Progress, SpinnerColumn, TextColumn

from pdc.config import ENDPOINTS, SOCRATA_BATCH_SIZE


def fetch_all(endpoint_url: str, where_clause: str | None = None) -> list[dict]:
    """Paginate through a Socrata API endpoint."""
    records = []
    offset = 0
    with httpx.Client(timeout=60) as client:
        while True:
            params = {
                "$limit": SOCRATA_BATCH_SIZE,
                "$offset": offset,
                "$order": ":id",
            }
            if where_clause:
                params["$where"] = where_clause
            resp = client.get(endpoint_url, params=params)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            records.extend(batch)
            offset += len(batch)
            if len(batch) < SOCRATA_BATCH_SIZE:
                break
    return records


def _parse_date(val: str | None) -> str | None:
    """Normalize Socrata date fields to ISO date string."""
    if not val:
        return None
    # Socrata returns ISO datetime like "2025-06-15T00:00:00.000"
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return val[:10] if len(val) >= 10 else val


def sync_monthly_review(conn: sqlite3.Connection, full: bool = False) -> dict:
    """Fetch Monthly Design Review data and upsert into DB."""
    where = None
    if not full:
        row = conn.execute(
            "SELECT MAX(meeting_date) FROM review_events WHERE data_source='monthly_review_api'"
        ).fetchone()
        last_date = row[0] if row else None
        if last_date:
            where = f"date > '{last_date}'"

    records = fetch_all(ENDPOINTS["monthly_review"], where)
    inserted = 0
    updated = 0

    for rec in records:
        project_id = rec.get("project_id")
        if not project_id:
            continue

        meeting_date = _parse_date(rec.get("date"))
        title = rec.get("title", "").strip()
        borough = rec.get("borough")
        level_of_review = rec.get("level_of_review")
        agency = rec.get("agency")
        cert = rec.get("certificate_number")
        action = rec.get("action")
        project_type = rec.get("project_type")
        public_private = rec.get("public_private")

        # Upsert project
        existing = conn.execute(
            "SELECT project_id FROM projects WHERE project_id = ?", (project_id,)
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE projects SET
                    title = COALESCE(?, title),
                    borough = COALESCE(?, borough),
                    project_type = COALESCE(?, project_type),
                    lead_agency = COALESCE(?, lead_agency),
                    public_private = COALESCE(?, public_private),
                    last_seen_date = MAX(COALESCE(?, ''), COALESCE(last_seen_date, '')),
                    current_stage = ?,
                    updated_at = datetime('now')
                WHERE project_id = ?""",
                (title, borough, project_type, agency, public_private,
                 meeting_date, level_of_review, project_id),
            )
            updated += 1
        else:
            conn.execute(
                """INSERT INTO projects
                    (project_id, title, borough, project_type, lead_agency,
                     public_private, first_seen_date, last_seen_date, current_stage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (project_id, title, borough, project_type, agency,
                 public_private, meeting_date, meeting_date, level_of_review),
            )
            inserted += 1

        # Insert review event
        conn.execute(
            """INSERT OR IGNORE INTO review_events
                (project_id, certificate_number, meeting_date, level_of_review,
                 action, data_source, raw_data)
            VALUES (?, ?, ?, ?, ?, 'monthly_review_api', ?)""",
            (project_id, cert, meeting_date, level_of_review, action, json.dumps(rec)),
        )

    conn.commit()
    return {"source": "monthly_review", "fetched": len(records),
            "inserted": inserted, "updated": updated}


def sync_annual_report(conn: sqlite3.Connection, full: bool = False) -> dict:
    """Fetch Annual Report data and upsert into DB."""
    where = None
    if not full:
        row = conn.execute(
            "SELECT MAX(meeting_date) FROM review_events WHERE data_source='annual_report_api'"
        ).fetchone()
        last_date = row[0] if row else None
        if last_date:
            where = f"meeting_date > '{last_date}'"

    records = fetch_all(ENDPOINTS["annual_report"], where)
    inserted = 0
    updated = 0

    for rec in records:
        project_id = rec.get("project_id")
        if not project_id:
            continue

        meeting_date = _parse_date(rec.get("meeting_date"))
        title = rec.get("title", "").strip()
        borough = rec.get("borough")
        lead_agency = rec.get("lead_agency")
        secondary_agency = rec.get("secondary_agency")
        project_type = rec.get("project_type")
        construction_type = rec.get("construction_type")
        public_private = rec.get("public_private")
        result = rec.get("result")
        review_cycles = rec.get("review_cycles")

        # Upsert project
        existing = conn.execute(
            "SELECT project_id FROM projects WHERE project_id = ?", (project_id,)
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE projects SET
                    title = COALESCE(?, title),
                    borough = COALESCE(?, borough),
                    project_type = COALESCE(?, project_type),
                    construction_type = COALESCE(?, construction_type),
                    lead_agency = COALESCE(?, lead_agency),
                    secondary_agency = COALESCE(?, secondary_agency),
                    public_private = COALESCE(?, public_private),
                    final_result = COALESCE(?, final_result),
                    total_review_cycles = COALESCE(?, total_review_cycles),
                    last_seen_date = MAX(COALESCE(?, ''), COALESCE(last_seen_date, '')),
                    updated_at = datetime('now')
                WHERE project_id = ?""",
                (title, borough, project_type, construction_type, lead_agency,
                 secondary_agency, public_private, result, review_cycles,
                 meeting_date, project_id),
            )
            updated += 1
        else:
            conn.execute(
                """INSERT INTO projects
                    (project_id, title, borough, project_type, construction_type,
                     lead_agency, secondary_agency, public_private, final_result,
                     total_review_cycles, first_seen_date, last_seen_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (project_id, title, borough, project_type, construction_type,
                 lead_agency, secondary_agency, public_private, result,
                 review_cycles, meeting_date, meeting_date),
            )
            inserted += 1

        # Insert review event
        conn.execute(
            """INSERT OR IGNORE INTO review_events
                (project_id, meeting_date, level_of_review, result,
                 data_source, raw_data)
            VALUES (?, ?, ?, ?, 'annual_report_api', ?)""",
            (project_id, meeting_date, rec.get("level_of_review"), result, json.dumps(rec)),
        )

    conn.commit()
    return {"source": "annual_report", "fetched": len(records),
            "inserted": inserted, "updated": updated}


def sync_art_inventory(conn: sqlite3.Connection) -> dict:
    """Fetch Public Art Inventory data."""
    records = fetch_all(ENDPOINTS["art_inventory"])
    inserted = 0

    for rec in records:
        title = rec.get("title", "").strip()
        if not title:
            continue

        # Build artist name from parts
        artist_parts = [rec.get("last_name_1", ""), rec.get("first_name_1", "")]
        primary_artist = ", ".join(p for p in artist_parts if p).strip()

        conn.execute(
            """INSERT OR IGNORE INTO public_art
                (title, alternate_title, primary_artist, primary_architect,
                 architecture_firm, foundry, fabricator,
                 date_created, date_dedicated,
                 artwork_type1, artwork_type2, material,
                 location_name, address, borough,
                 latitude, longitude, block, lot,
                 subject_keyword, inscription, managing_agency,
                 acquisition, pdc_records)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (title, rec.get("alternate_title"), primary_artist,
             rec.get("architect_1"), rec.get("architecture_firm"),
             rec.get("foundry"), rec.get("fabricator"),
             rec.get("date_created"), rec.get("date_dedicated"),
             rec.get("artwork_type1"), rec.get("artwork_type2"),
             rec.get("material"),
             rec.get("location_name"), rec.get("address"), rec.get("borough"),
             rec.get("latitude"), rec.get("longitude"),
             rec.get("block"), rec.get("lot"),
             rec.get("subject_keyword"), rec.get("inscription"),
             rec.get("managing_city_agency"), rec.get("acquisition"),
             rec.get("pdc_records")),
        )
        inserted += 1

    conn.commit()
    return {"source": "art_inventory", "fetched": len(records), "inserted": inserted}


def sync_all(conn: sqlite3.Connection, full: bool = False) -> list[dict]:
    """Run all Socrata syncs."""
    results = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        task = progress.add_task("Syncing Monthly Design Review...", total=None)
        results.append(sync_monthly_review(conn, full))
        progress.update(task, description=f"Monthly Review: {results[-1]['fetched']} records")

        task = progress.add_task("Syncing Annual Report...", total=None)
        results.append(sync_annual_report(conn, full))
        progress.update(task, description=f"Annual Report: {results[-1]['fetched']} records")

        task = progress.add_task("Syncing Art Inventory...", total=None)
        results.append(sync_art_inventory(conn))
        progress.update(task, description=f"Art Inventory: {results[-1]['fetched']} records")

    return results
