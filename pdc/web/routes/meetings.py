from flask import Blueprint, render_template, abort

from pdc.db import get_db
from pdc.config import DO_SPACES_CDN

bp = Blueprint("meetings", __name__, url_prefix="/meetings")


@bp.route("/")
def meeting_list():
    with get_db() as conn:
        meetings = conn.execute(
            """SELECT m.meeting_date, m.agenda_pdf_url, m.minutes_pdf_url,
                      m.youtube_url,
                      COUNT(re.id) AS item_count
               FROM meetings m
               LEFT JOIN review_events re ON re.meeting_date = m.meeting_date
               GROUP BY m.meeting_date, m.agenda_pdf_url, m.minutes_pdf_url,
                        m.youtube_url
               ORDER BY m.meeting_date DESC"""
        ).fetchall()

    return render_template("meeting_list.html", meetings=meetings)


@bp.route("/<meeting_date>")
def meeting_detail(meeting_date):
    with get_db() as conn:
        meeting = conn.execute(
            "SELECT * FROM meetings WHERE meeting_date = ?", (meeting_date,)
        ).fetchone()
        if not meeting:
            abort(404)

        items = conn.execute(
            """SELECT re.*, p.title, p.borough, p.lead_agency, p.project_type
               FROM review_events re
               JOIN projects p ON re.project_id = p.project_id
               WHERE re.meeting_date = ?
               ORDER BY re.agenda_section, re.scheduled_time, p.title""",
            (meeting_date,),
        ).fetchall()

        video = conn.execute(
            "SELECT * FROM youtube_videos WHERE meeting_date = ?",
            (meeting_date,),
        ).fetchone()

    return render_template(
        "meeting_detail.html",
        meeting=meeting,
        items=items,
        video=video,
        cdn=DO_SPACES_CDN,
    )
