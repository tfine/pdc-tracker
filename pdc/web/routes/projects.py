from flask import Blueprint, render_template, request, abort

from pdc.db import get_db
from pdc.config import STAGE_ORDER

bp = Blueprint("projects", __name__, url_prefix="/projects")

PER_PAGE = 50


@bp.route("/")
def project_list():
    page = request.args.get("page", 1, type=int)
    borough = request.args.get("borough", "")
    project_type = request.args.get("type", "")
    stage = request.args.get("stage", "")
    agency = request.args.get("agency", "")
    result_filter = request.args.get("result", "")
    sort = request.args.get("sort", "recent")

    clauses = []
    params = []

    if borough:
        clauses.append("p.borough = ?")
        params.append(borough)
    if project_type:
        clauses.append("p.project_type = ?")
        params.append(project_type)
    if stage:
        clauses.append("p.current_stage = ?")
        params.append(stage)
    if agency:
        clauses.append("p.lead_agency ILIKE ?")
        params.append(f"%{agency}%")
    if result_filter:
        clauses.append("p.final_result = ?")
        params.append(result_filter)

    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)

    order_map = {
        "recent": "p.last_seen_date DESC NULLS LAST",
        "oldest": "p.first_seen_date ASC NULLS LAST",
        "title": "p.title ASC",
        "borough": "p.borough ASC NULLS LAST",
    }
    order = order_map.get(sort, order_map["recent"])

    offset = (page - 1) * PER_PAGE

    with get_db() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM projects p {where}", tuple(params)
        ).fetchone()[0]

        projects = conn.execute(
            f"""SELECT p.project_id, p.title, p.borough, p.lead_agency,
                       p.project_type, p.current_stage, p.final_result,
                       p.first_seen_date, p.last_seen_date
                FROM projects p
                {where}
                ORDER BY {order}
                LIMIT ? OFFSET ?""",
            tuple(params) + (PER_PAGE, offset),
        ).fetchall()

        # Filter options
        boroughs = conn.execute(
            "SELECT DISTINCT borough FROM projects WHERE borough IS NOT NULL ORDER BY borough"
        ).fetchall()
        types = conn.execute(
            "SELECT DISTINCT project_type FROM projects WHERE project_type IS NOT NULL ORDER BY project_type"
        ).fetchall()
        stages = conn.execute(
            "SELECT DISTINCT current_stage FROM projects WHERE current_stage IS NOT NULL ORDER BY current_stage"
        ).fetchall()
        results = conn.execute(
            "SELECT DISTINCT final_result FROM projects WHERE final_result IS NOT NULL ORDER BY final_result"
        ).fetchall()

    total_pages = (total + PER_PAGE - 1) // PER_PAGE

    return render_template(
        "project_list.html",
        projects=projects,
        page=page,
        total_pages=total_pages,
        total=total,
        borough=borough,
        project_type=project_type,
        stage=stage,
        agency=agency,
        result_filter=result_filter,
        sort=sort,
        boroughs=[r["borough"] for r in boroughs],
        types=[r["project_type"] for r in types],
        stages=[r["current_stage"] for r in stages],
        results=[r["final_result"] for r in results],
    )


@bp.route("/<project_id>")
def project_detail(project_id):
    with get_db() as conn:
        project = conn.execute(
            "SELECT * FROM projects WHERE project_id = ?", (project_id,)
        ).fetchone()
        if not project:
            abort(404)

        events = conn.execute(
            """SELECT re.*, p.title AS project_title
               FROM review_events re
               JOIN projects p ON re.project_id = p.project_id
               WHERE re.project_id = ?
               ORDER BY re.meeting_date, re.data_source""",
            (project_id,),
        ).fetchall()

        # Get timeline dates from view
        timeline = conn.execute(
            "SELECT * FROM project_stage_timeline WHERE project_id = ?",
            (project_id,),
        ).fetchone()

        # Get YouTube videos for meetings this project appeared at
        meeting_dates = list({e["meeting_date"] for e in events})
        videos = []
        for md in meeting_dates:
            v = conn.execute(
                "SELECT * FROM youtube_videos WHERE meeting_date = ?", (md,)
            ).fetchone()
            if v:
                videos.append(v)

        # Get presentation PDF URLs
        presentations = [
            e["presentation_url"]
            for e in events
            if e["presentation_url"]
        ]

        # Get minutes/certificates URLs for each meeting date
        minutes_by_date = {}
        for md in meeting_dates:
            m = conn.execute(
                "SELECT minutes_pdf_url FROM meetings WHERE meeting_date = ?", (md,)
            ).fetchone()
            if m and m["minutes_pdf_url"]:
                minutes_by_date[md] = m["minutes_pdf_url"]

    return render_template(
        "project_detail.html",
        project=project,
        events=events,
        timeline=timeline,
        videos=videos,
        presentations=presentations,
        minutes_by_date=minutes_by_date,
        stage_order=STAGE_ORDER,
    )
