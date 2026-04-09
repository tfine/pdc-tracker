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

        # Get timeline: merge stages from same_project siblings
        # so e.g. Conceptual from project A + Preliminary from project B
        # shows the full progression on both pages
        sibling_ids = conn.execute(
            """SELECT CASE WHEN project_id_a = ? THEN project_id_b
                          ELSE project_id_a END AS sibling_id
               FROM project_links
               WHERE link_type = 'same_project'
                 AND (project_id_a = ? OR project_id_b = ?)""",
            (project_id, project_id, project_id),
        ).fetchall()
        all_ids = [project_id] + [r["sibling_id"] for r in sibling_ids]

        if len(all_ids) == 1:
            timeline = conn.execute(
                "SELECT * FROM project_stage_timeline WHERE project_id = ?",
                (project_id,),
            ).fetchone()
        else:
            # Build merged timeline from all siblings' review events
            placeholders = ",".join("?" for _ in all_ids)
            timelines = conn.execute(
                f"SELECT * FROM project_stage_timeline WHERE project_id IN ({placeholders})",
                tuple(all_ids),
            ).fetchall()

            # Merge: take the earliest non-null date for each stage
            def earliest(*dates):
                valid = [d for d in dates if d]
                return min(valid) if valid else None

            conceptual = earliest(*(t["conceptual_date"] for t in timelines))
            preliminary = earliest(*(t["preliminary_date"] for t in timelines))
            pf = earliest(*(t["preliminary_and_final_date"] for t in timelines))
            final = earliest(*(t["final_date"] for t in timelines))

            # Compute days between stages
            days_c_to_p = None
            days_p_to_f = None
            if conceptual and (preliminary or pf):
                from datetime import date as dt_date
                c = dt_date.fromisoformat(conceptual)
                p = dt_date.fromisoformat(preliminary or pf)
                days_c_to_p = (p - c).days
            if (preliminary or pf) and final:
                from datetime import date as dt_date
                p = dt_date.fromisoformat(preliminary or pf)
                f = dt_date.fromisoformat(final)
                days_p_to_f = (f - p).days

            total_meetings = sum(t["total_meetings"] for t in timelines)

            # Pick the most authoritative final_result:
            # Prefer approval-type results over non-approvals,
            # and later results over earlier ones
            _result_priority = {
                "Approved": 0, "Approved with conditions": 1,
                "Approved per delegation": 2, "Commented": 3,
                "Found incomplete": 4, "Found Incomplete": 4,
                "Withdrawn": 5, "Rejected": 6,
            }
            final_result = None
            best_prio = 99
            for t in timelines:
                r = t["final_result"]
                if r and _result_priority.get(r, 10) < best_prio:
                    best_prio = _result_priority[r]
                    final_result = r

            timeline = {
                "conceptual_date": conceptual,
                "preliminary_date": preliminary,
                "preliminary_and_final_date": pf,
                "final_date": final,
                "days_conceptual_to_preliminary": days_c_to_p,
                "days_preliminary_to_final": days_p_to_f,
                "total_meetings": total_meetings,
                "final_result": final_result,
            }

        # Compute "effective" stage/result from the merged timeline (for header)
        # and find the latest sibling (for banner pointing to the newer record)
        effective_stage = None
        effective_result = project["final_result"]
        latest_sibling = None

        if timeline:
            if timeline["final_date"] or timeline["preliminary_and_final_date"]:
                effective_stage = "Final"
            elif timeline["preliminary_date"]:
                effective_stage = "Preliminary"
            elif timeline["conceptual_date"]:
                effective_stage = "Conceptual"
            if timeline.get("final_result") if isinstance(timeline, dict) else timeline["final_result"]:
                effective_result = timeline["final_result"]

        if len(sibling_ids) > 0:
            # Find the sibling with the most recent last_seen_date
            sibling_id_list = [r["sibling_id"] for r in sibling_ids]
            placeholders = ",".join("?" for _ in sibling_id_list)
            siblings = conn.execute(
                f"""SELECT project_id, title, current_stage, final_result,
                           first_seen_date, last_seen_date
                    FROM projects WHERE project_id IN ({placeholders})
                    ORDER BY last_seen_date DESC NULLS LAST""",
                tuple(sibling_id_list),
            ).fetchall()
            if siblings:
                candidate = siblings[0]
                # Only show banner if the current project isn't already the latest
                current_last_seen = project["last_seen_date"] or ""
                if (candidate["last_seen_date"] or "") > current_last_seen:
                    latest_sibling = candidate

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

        # Get related projects (deduplicated: prefer most specific link type)
        related_raw = conn.execute(
            """SELECT p.project_id, p.title, p.current_stage, p.final_result,
                      p.first_seen_date, p.last_seen_date, p.lead_agency,
                      pl.link_type, pl.confidence
               FROM project_links pl
               JOIN projects p ON p.project_id = CASE
                   WHEN pl.project_id_a = ? THEN pl.project_id_b
                   ELSE pl.project_id_a END
               WHERE pl.project_id_a = ? OR pl.project_id_b = ?
               ORDER BY pl.link_type, p.first_seen_date""",
            (project_id, project_id, project_id),
        ).fetchall()

        # Deduplicate: keep the most specific link type per project
        # Priority: same_project > modification > same_site
        link_priority = {"same_project": 0, "modification": 1, "same_site": 2}
        best_by_pid = {}
        for r in related_raw:
            pid = r["project_id"]
            prio = link_priority.get(r["link_type"], 9)
            if pid not in best_by_pid or prio < best_by_pid[pid][0]:
                best_by_pid[pid] = (prio, r)
        related = [v[1] for v in sorted(
            best_by_pid.values(), key=lambda x: (x[0], x[1]["first_seen_date"] or "")
        )]

    return render_template(
        "project_detail.html",
        project=project,
        events=events,
        timeline=timeline,
        videos=videos,
        presentations=presentations,
        minutes_by_date=minutes_by_date,
        related=related,
        effective_stage=effective_stage,
        effective_result=effective_result,
        latest_sibling=latest_sibling,
        stage_order=STAGE_ORDER,
    )
