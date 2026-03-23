import sqlite3
from datetime import date, datetime

from pdc.config import STAGE_ORDER


def get_project_timeline(conn: sqlite3.Connection, project_id: str) -> dict | None:
    """Get the full stage progression timeline for a project."""
    project = conn.execute(
        "SELECT * FROM projects WHERE project_id = ?", (project_id,)
    ).fetchone()
    if not project:
        return None

    events = conn.execute(
        """SELECT meeting_date, level_of_review, action, result, data_source
           FROM review_events
           WHERE project_id = ?
           ORDER BY meeting_date, data_source""",
        (project_id,),
    ).fetchall()

    stages = []
    for ev in events:
        stages.append({
            "date": ev["meeting_date"],
            "stage": ev["level_of_review"],
            "action": ev["action"],
            "result": ev["result"],
            "source": ev["data_source"],
        })

    # Compute durations
    stage_dates = {}
    for s in stages:
        lvl = s["stage"]
        if lvl and lvl not in stage_dates:
            stage_dates[lvl] = s["date"]

    first_date = stages[0]["date"] if stages else None
    last_date = stages[-1]["date"] if stages else None
    total_days = None
    if first_date and last_date:
        d1 = datetime.strptime(first_date, "%Y-%m-%d").date()
        d2 = datetime.strptime(last_date, "%Y-%m-%d").date()
        total_days = (d2 - d1).days

    return {
        "project_id": project_id,
        "title": project["title"],
        "borough": project["borough"],
        "project_type": project["project_type"],
        "lead_agency": project["lead_agency"],
        "stages": stages,
        "stage_dates": stage_dates,
        "total_days_in_review": total_days,
        "total_meetings": len(set(s["date"] for s in stages)),
        "current_stage": project["current_stage"],
        "final_result": project["final_result"],
    }


def find_stalled_projects(conn: sqlite3.Connection, days: int = 730) -> list[dict]:
    """Find projects that haven't advanced in `days` days (default 2 years)."""
    cutoff = date.today().isoformat()
    rows = conn.execute(
        """SELECT p.project_id, p.title, p.current_stage, p.last_seen_date,
                  p.borough, p.lead_agency,
                  CAST(julianday(?) - julianday(p.last_seen_date) AS INTEGER) as days_stalled
           FROM projects p
           WHERE p.final_result IS NULL
             AND p.last_seen_date IS NOT NULL
             AND julianday(?) - julianday(p.last_seen_date) > ?
           ORDER BY days_stalled DESC""",
        (cutoff, cutoff, days),
    ).fetchall()
    return [dict(r) for r in rows]


def compute_stage_stats(conn: sqlite3.Connection) -> dict:
    """Compute aggregate statistics about stage progression speed."""
    rows = conn.execute(
        """SELECT
            AVG(days_conceptual_to_preliminary) as avg_concept_to_prelim,
            AVG(days_preliminary_to_final) as avg_prelim_to_final,
            MIN(days_conceptual_to_preliminary) as min_concept_to_prelim,
            MAX(days_conceptual_to_preliminary) as max_concept_to_prelim,
            MIN(days_preliminary_to_final) as min_prelim_to_final,
            MAX(days_preliminary_to_final) as max_prelim_to_final,
            COUNT(CASE WHEN conceptual_date IS NOT NULL THEN 1 END) as projects_with_conceptual,
            COUNT(CASE WHEN preliminary_date IS NOT NULL THEN 1 END) as projects_with_preliminary,
            COUNT(CASE WHEN final_date IS NOT NULL THEN 1 END) as projects_with_final,
            COUNT(*) as total_projects
        FROM project_stage_timeline"""
    ).fetchone()
    return dict(rows)
