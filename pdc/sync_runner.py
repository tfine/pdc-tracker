"""Headless sync entrypoint for Railway cron.

Runs the full sync pipeline, detects changes, and fans out email alerts.
"""

import sys
from datetime import datetime

from pdc.db import get_db


def run_sync():
    from pdc.ingest.socrata import sync_all
    from pdc.ingest.agenda_scraper import sync_agendas
    from pdc.ingest.agenda_parser import sync_parse_agendas
    from pdc.ingest.announcements import scrape_news_page
    from pdc.transform.matcher import match_agenda_to_api

    with get_db() as conn:
        # 1. Snapshot current state for diff
        old_stages = {}
        for row in conn.execute(
            "SELECT project_id, current_stage, final_result FROM projects"
        ).fetchall():
            old_stages[row["project_id"]] = {
                "stage": row["current_stage"],
                "result": row["final_result"],
            }

        old_meeting_dates = {
            r["meeting_date"]
            for r in conn.execute("SELECT meeting_date FROM meetings").fetchall()
        }

        # 2. Run sync pipeline
        log_id = conn.execute(
            "INSERT INTO sync_log (source, started_at) VALUES (?, ?)",
            ("cron_sync", datetime.utcnow().isoformat()),
        ).lastrowid

        try:
            sync_all(conn, full=False)
            sync_agendas(conn)
            sync_parse_agendas(conn)
            match_agenda_to_api(conn)
            scrape_news_page(conn)

            # Minutes/certificates
            from pdc.ingest.agenda_scraper import sync_minutes
            sync_minutes(conn)

            # Presentations
            from pdc.ingest.agenda_scraper import sync_presentations
            sync_presentations(conn)

            # YouTube
            from pdc.ingest.youtube import sync_youtube
            sync_youtube(conn)
        except Exception as exc:
            conn.execute(
                "UPDATE sync_log SET completed_at = ?, status = 'failed', error_message = ? WHERE id = ?",
                (datetime.utcnow().isoformat(), str(exc), log_id),
            )
            print(f"Sync failed: {exc}", file=sys.stderr)
            raise

        conn.execute(
            "UPDATE sync_log SET completed_at = ?, status = 'completed' WHERE id = ?",
            (datetime.utcnow().isoformat(), log_id),
        )

        # 3. Detect changes
        changes = []

        # New meetings (new agendas)
        new_meeting_dates = {
            r["meeting_date"]
            for r in conn.execute("SELECT meeting_date FROM meetings").fetchall()
        }
        for md in new_meeting_dates - old_meeting_dates:
            changes.append({
                "trigger_type": "new_agenda",
                "project_id": None,
                "title": f"New PDC meeting agenda: {md}",
                "borough": None,
                "meeting_date": md,
                "detail": f"A new agenda has been published for {md}.",
            })

        # Stage changes / approvals
        for row in conn.execute(
            "SELECT project_id, title, borough, current_stage, final_result FROM projects"
        ).fetchall():
            pid = row["project_id"]
            old = old_stages.get(pid)
            if not old:
                continue  # new project, already covered by new_agenda
            if row["current_stage"] != old["stage"]:
                changes.append({
                    "trigger_type": "stage_change",
                    "project_id": pid,
                    "title": row["title"],
                    "borough": row["borough"],
                    "meeting_date": None,
                    "detail": f"Stage changed: {old['stage']} → {row['current_stage']}",
                })
            if row["final_result"] and row["final_result"] != old["result"]:
                changes.append({
                    "trigger_type": "approval",
                    "project_id": pid,
                    "title": row["title"],
                    "borough": row["borough"],
                    "meeting_date": None,
                    "detail": f"Result: {row['final_result']}",
                })

        # 4. Fan out email alerts
        if changes:
            try:
                from pdc.email_alerts import fan_out_alerts
                sent = fan_out_alerts(conn, changes)
                print(f"Sent alerts to {sent} subscribers for {len(changes)} changes.")
            except Exception as exc:
                print(f"Alert sending failed: {exc}", file=sys.stderr)
        else:
            print("No changes detected.")


if __name__ == "__main__":
    run_sync()
