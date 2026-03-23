import sqlite3

from rapidfuzz import fuzz


def match_agenda_to_api(conn: sqlite3.Connection, threshold: float = 80.0) -> dict:
    """Match agenda-sourced items to API-sourced items.

    First tries exact certificate_number match, then falls back to fuzzy title matching.
    """
    # Get agenda items without a real project_id (placeholder IDs start with "agenda_")
    agenda_items = conn.execute(
        """SELECT re.id, re.project_id, re.certificate_number, re.meeting_date,
                  p.title as agenda_title
           FROM review_events re
           JOIN projects p ON re.project_id = p.project_id
           WHERE re.data_source = 'agenda_pdf'
             AND re.project_id LIKE 'agenda_%'"""
    ).fetchall()

    matched = 0
    for item in agenda_items:
        cert = item["certificate_number"]
        date = item["meeting_date"]
        agenda_title = item["agenda_title"] or ""

        # Strategy 1: Match by certificate_number in API data
        api_match = conn.execute(
            """SELECT DISTINCT project_id FROM review_events
               WHERE certificate_number = ? AND data_source != 'agenda_pdf'
               LIMIT 1""",
            (cert,),
        ).fetchone()

        if api_match:
            _reassign_project(conn, item["project_id"], api_match["project_id"])
            matched += 1
            continue

        # Strategy 2: Fuzzy title match within the same meeting date
        candidates = conn.execute(
            """SELECT p.project_id, p.title
               FROM review_events re
               JOIN projects p ON re.project_id = p.project_id
               WHERE re.meeting_date = ?
                 AND re.data_source != 'agenda_pdf'
                 AND re.project_id NOT LIKE 'agenda_%'""",
            (date,),
        ).fetchall()

        best_score = 0.0
        best_id = None
        for cand in candidates:
            score = fuzz.token_sort_ratio(agenda_title, cand["title"] or "")
            if score > best_score:
                best_score = score
                best_id = cand["project_id"]

        if best_id and best_score >= threshold:
            _reassign_project(conn, item["project_id"], best_id)
            matched += 1

    conn.commit()
    return {"agenda_unmatched": len(agenda_items), "matched": matched}


def _reassign_project(conn: sqlite3.Connection, old_id: str, new_id: str):
    """Move review events from a placeholder project to a real one."""
    # Delete agenda events that would conflict with existing events on the target project
    conn.execute(
        """DELETE FROM review_events WHERE project_id = ? AND EXISTS (
            SELECT 1 FROM review_events e2
            WHERE e2.project_id = ?
              AND e2.meeting_date = review_events.meeting_date
              AND e2.level_of_review IS review_events.level_of_review
              AND e2.data_source = review_events.data_source
        )""",
        (old_id, new_id),
    )
    conn.execute(
        "UPDATE review_events SET project_id = ? WHERE project_id = ?",
        (new_id, old_id),
    )
    # Transfer CC/CB info
    old_proj = conn.execute(
        "SELECT cc_district, community_board FROM projects WHERE project_id = ?",
        (old_id,),
    ).fetchone()
    if old_proj and (old_proj["cc_district"] or old_proj["community_board"]):
        conn.execute(
            """UPDATE projects SET
                cc_district = COALESCE(?, cc_district),
                community_board = COALESCE(?, community_board)
            WHERE project_id = ?""",
            (old_proj["cc_district"], old_proj["community_board"], new_id),
        )
    # Remove placeholder project
    conn.execute("DELETE FROM projects WHERE project_id = ?", (old_id,))
