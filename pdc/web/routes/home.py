from flask import Blueprint, render_template, request

from pdc.db import get_db

bp = Blueprint("home", __name__)


@bp.route("/")
def index():
    with get_db() as conn:
        # Stats
        project_count = conn.execute("SELECT COUNT(*) AS n FROM projects").fetchone()[0]
        meeting_count = conn.execute("SELECT COUNT(*) AS n FROM meetings").fetchone()[0]
        approved_count = conn.execute(
            "SELECT COUNT(*) AS n FROM projects WHERE final_result = 'Approved'"
        ).fetchone()[0]

        # Upcoming meetings (future dates — meeting_date is TEXT in ISO format)
        from datetime import date
        today = date.today().isoformat()
        upcoming = conn.execute(
            """SELECT m.meeting_date, m.agenda_pdf_url, m.youtube_url,
                      COUNT(re.id) AS item_count
               FROM meetings m
               LEFT JOIN review_events re ON re.meeting_date = m.meeting_date
               WHERE m.meeting_date >= ?
               GROUP BY m.meeting_date, m.agenda_pdf_url, m.youtube_url
               ORDER BY m.meeting_date
               LIMIT 5""",
            (today,),
        ).fetchall()

        # Recent activity (last 10 review events with stage info)
        recent = conn.execute(
            """SELECT re.meeting_date, re.level_of_review, re.result,
                      p.project_id, p.title, p.borough
               FROM review_events re
               JOIN projects p ON re.project_id = p.project_id
               ORDER BY re.meeting_date DESC, re.id DESC
               LIMIT 10"""
        ).fetchall()

    return render_template(
        "home.html",
        project_count=project_count,
        meeting_count=meeting_count,
        approved_count=approved_count,
        upcoming=upcoming,
        recent=recent,
    )


@bp.route("/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return render_template("home.html", results=[], q="",
                               project_count=0, meeting_count=0,
                               approved_count=0, upcoming=[], recent=[])

    with get_db() as conn:
        like = f"%{q}%"
        results = conn.execute(
            """SELECT project_id, title, borough, lead_agency,
                      current_stage, final_result
               FROM projects
               WHERE title ILIKE ? OR address ILIKE ? OR lead_agency ILIKE ?
                  OR project_id ILIKE ?
               ORDER BY last_seen_date DESC NULLS LAST
               LIMIT 50""",
            (like, like, like, like),
        ).fetchall()

    return render_template("search_results.html", results=results, q=q)
