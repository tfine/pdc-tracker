"""Lightweight agenda checker — runs frequently to detect new agendas fast.

Only checks for new agenda/minutes PDFs and sends alerts. Does NOT run
the full sync pipeline (Socrata APIs, YouTube, etc.).
"""

import sys
from datetime import datetime

from pdc.db import get_db


def check_for_new_agendas():
    from pdc.ingest.agenda_scraper import sync_agendas, sync_minutes

    with get_db() as conn:
        # Snapshot current meeting dates with agendas
        old_agendas = {
            r["meeting_date"]
            for r in conn.execute(
                "SELECT meeting_date FROM meetings WHERE agenda_pdf_url IS NOT NULL"
            ).fetchall()
        }

        old_minutes = {
            r["meeting_date"]
            for r in conn.execute(
                "SELECT meeting_date FROM meetings WHERE minutes_pdf_url IS NOT NULL"
            ).fetchall()
        }

        # Check for new agendas and minutes
        agenda_result = sync_agendas(conn)
        minutes_result = sync_minutes(conn)

        # Parse any new agendas
        if agenda_result["downloaded"] > 0:
            from pdc.ingest.agenda_parser import sync_parse_agendas
            from pdc.transform.matcher import match_agenda_to_api
            sync_parse_agendas(conn)
            match_agenda_to_api(conn)

        # Detect what's new
        new_agendas = {
            r["meeting_date"]
            for r in conn.execute(
                "SELECT meeting_date FROM meetings WHERE agenda_pdf_url IS NOT NULL"
            ).fetchall()
        }

        new_minutes = {
            r["meeting_date"]
            for r in conn.execute(
                "SELECT meeting_date FROM meetings WHERE minutes_pdf_url IS NOT NULL"
            ).fetchall()
        }

        changes = []

        for md in new_agendas - old_agendas:
            changes.append({
                "trigger_type": "new_agenda",
                "project_id": None,
                "title": f"New PDC meeting agenda posted: {md}",
                "borough": None,
                "meeting_date": md,
                "detail": f"A new agenda has been published for the {md} meeting.",
            })

        for md in new_minutes - old_minutes:
            changes.append({
                "trigger_type": "new_agenda",
                "project_id": None,
                "title": f"Minutes & certificates posted: {md}",
                "borough": None,
                "meeting_date": md,
                "detail": f"Minutes and certificates for the {md} meeting are now available.",
            })

        if changes:
            try:
                from pdc.email_alerts import fan_out_alerts
                sent = fan_out_alerts(conn, changes)
                print(f"Sent alerts to {sent} subscribers for {len(changes)} new documents.")
            except Exception as exc:
                print(f"Alert sending failed: {exc}", file=sys.stderr)
        else:
            print("No new agendas or minutes.")

        print(
            f"Checked: {agenda_result['discovered']} agendas, "
            f"{minutes_result['discovered']} minutes. "
            f"New: {agenda_result['downloaded']} agendas, "
            f"{minutes_result['downloaded']} minutes."
        )


if __name__ == "__main__":
    check_for_new_agendas()
