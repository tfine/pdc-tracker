import sqlite3

from rapidfuzz import fuzz

from pdc.compare.api_vs_agenda import compare_meeting


def find_unannounced_approvals(conn: sqlite3.Connection, threshold: float = 80.0) -> list[dict]:
    """Find approved projects with no matching announcement."""
    approved = conn.execute(
        """SELECT DISTINCT p.project_id, p.title, p.borough, p.lead_agency,
                  re.meeting_date, re.certificate_number
           FROM projects p
           JOIN review_events re ON p.project_id = re.project_id
           WHERE (p.final_result = 'Approved'
                  OR re.result = 'Approved'
                  OR re.level_of_review LIKE '%Final%')
             AND re.result = 'Approved'
           ORDER BY re.meeting_date DESC"""
    ).fetchall()

    announcements = conn.execute(
        "SELECT id, title, source_url, date_published FROM announcements"
    ).fetchall()

    unannounced = []
    for proj in approved:
        proj_title = proj["title"] or ""
        matched = False
        for ann in announcements:
            ann_title = ann["title"] or ""
            score = fuzz.token_sort_ratio(proj_title, ann_title)
            if score >= threshold:
                matched = True
                # Store the match
                conn.execute(
                    """UPDATE announcements SET
                        matched_project_id = ?,
                        matched_certificate = ?,
                        match_confidence = ?
                    WHERE id = ?""",
                    (proj["project_id"], proj["certificate_number"], score / 100.0, ann["id"]),
                )
                break

        if not matched:
            unannounced.append(dict(proj))

    conn.commit()
    return unannounced


def find_announcement_discrepancies(conn: sqlite3.Connection) -> list[dict]:
    """Find announcements that don't match the data accurately.

    Checks for timing differences and description mismatches.
    """
    matched = conn.execute(
        """SELECT a.*, p.title as project_title, p.final_result,
                  re.meeting_date as approval_date
           FROM announcements a
           JOIN projects p ON a.matched_project_id = p.project_id
           LEFT JOIN review_events re ON p.project_id = re.project_id
                AND re.result = 'Approved'
           WHERE a.matched_project_id IS NOT NULL"""
    ).fetchall()

    discrepancies = []
    for row in matched:
        issues = []

        # Check title accuracy
        if row["title"] and row["project_title"]:
            score = fuzz.token_sort_ratio(row["title"], row["project_title"])
            if score < 90:
                issues.append({
                    "type": "title_mismatch",
                    "announcement": row["title"],
                    "actual": row["project_title"],
                    "similarity": score,
                })

        # Check timing (announcement should come after approval)
        if row["date_published"] and row["approval_date"]:
            if row["date_published"] < row["approval_date"]:
                issues.append({
                    "type": "premature_announcement",
                    "announced": row["date_published"],
                    "approved": row["approval_date"],
                })

        if issues:
            discrepancies.append({
                "announcement_id": row["id"],
                "source_url": row["source_url"],
                "project_id": row["matched_project_id"],
                "issues": issues,
            })

    return discrepancies


def generate_summary(conn: sqlite3.Connection) -> dict:
    """Generate an overall summary report."""
    project_count = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    event_count = conn.execute("SELECT COUNT(*) FROM review_events").fetchone()[0]
    meeting_count = conn.execute("SELECT COUNT(*) FROM meetings").fetchone()[0]
    art_count = conn.execute("SELECT COUNT(*) FROM public_art").fetchone()[0]

    source_counts = conn.execute(
        """SELECT data_source, COUNT(*) as cnt
           FROM review_events GROUP BY data_source"""
    ).fetchall()

    result_counts = conn.execute(
        """SELECT final_result, COUNT(*) as cnt
           FROM projects WHERE final_result IS NOT NULL
           GROUP BY final_result ORDER BY cnt DESC"""
    ).fetchall()

    stage_counts = conn.execute(
        """SELECT current_stage, COUNT(*) as cnt
           FROM projects WHERE current_stage IS NOT NULL
           GROUP BY current_stage ORDER BY cnt DESC"""
    ).fetchall()

    borough_counts = conn.execute(
        """SELECT borough, COUNT(*) as cnt
           FROM projects WHERE borough IS NOT NULL
           GROUP BY borough ORDER BY cnt DESC"""
    ).fetchall()

    return {
        "totals": {
            "projects": project_count,
            "review_events": event_count,
            "meetings": meeting_count,
            "public_art": art_count,
        },
        "events_by_source": {r["data_source"]: r["cnt"] for r in source_counts},
        "projects_by_result": {r["final_result"]: r["cnt"] for r in result_counts},
        "projects_by_stage": {r["current_stage"]: r["cnt"] for r in stage_counts},
        "projects_by_borough": {r["borough"]: r["cnt"] for r in borough_counts},
    }
