import sqlite3

from rapidfuzz import fuzz


def compare_meeting(conn: sqlite3.Connection, meeting_date: str) -> dict:
    """Compare API data vs agenda data for a specific meeting.

    Returns items only in API, only in agenda, and field differences.
    """
    api_events = conn.execute(
        """SELECT re.*, p.title as project_title
           FROM review_events re
           JOIN projects p ON re.project_id = p.project_id
           WHERE re.meeting_date = ? AND re.data_source != 'agenda_pdf'""",
        (meeting_date,),
    ).fetchall()

    agenda_events = conn.execute(
        """SELECT re.*, p.title as project_title
           FROM review_events re
           JOIN projects p ON re.project_id = p.project_id
           WHERE re.meeting_date = ? AND re.data_source = 'agenda_pdf'""",
        (meeting_date,),
    ).fetchall()

    api_certs = {e["certificate_number"]: dict(e) for e in api_events if e["certificate_number"]}
    agenda_certs = {e["certificate_number"]: dict(e) for e in agenda_events if e["certificate_number"]}

    only_in_api = []
    only_in_agenda = []
    differences = []

    for cert, api_ev in api_certs.items():
        if cert not in agenda_certs:
            only_in_api.append({
                "certificate_number": cert,
                "title": api_ev["project_title"],
                "level_of_review": api_ev["level_of_review"],
            })
        else:
            agenda_ev = agenda_certs[cert]
            diffs = {}
            if api_ev["level_of_review"] != agenda_ev["level_of_review"]:
                diffs["level_of_review"] = {
                    "api": api_ev["level_of_review"],
                    "agenda": agenda_ev["level_of_review"],
                }
            title_score = fuzz.token_sort_ratio(
                api_ev["project_title"] or "", agenda_ev["project_title"] or ""
            )
            if title_score < 90:
                diffs["title"] = {
                    "api": api_ev["project_title"],
                    "agenda": agenda_ev["project_title"],
                    "similarity": title_score,
                }
            if diffs:
                differences.append({
                    "certificate_number": cert,
                    "differences": diffs,
                })

    for cert, agenda_ev in agenda_certs.items():
        if cert not in api_certs:
            only_in_agenda.append({
                "certificate_number": cert,
                "title": agenda_ev["project_title"],
                "level_of_review": agenda_ev["level_of_review"],
            })

    return {
        "meeting_date": meeting_date,
        "api_items": len(api_certs),
        "agenda_items": len(agenda_certs),
        "only_in_api": only_in_api,
        "only_in_agenda": only_in_agenda,
        "differences": differences,
    }
