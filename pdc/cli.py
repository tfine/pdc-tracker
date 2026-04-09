import json
import subprocess
import sys
from datetime import datetime

import click
from rich.console import Console
from rich.table import Table

from pdc.config import DB_PATH
from pdc.db import get_db, init_db

console = Console()


@click.group()
def cli():
    """NYC Public Design Commission Tracker"""
    pass


@cli.command()
@click.option("--full", is_flag=True, help="Full sync (ignore last sync date)")
def sync(full):
    """Sync data from all sources."""
    from pdc.ingest.socrata import sync_all
    from pdc.ingest.agenda_scraper import sync_agendas
    from pdc.ingest.agenda_parser import sync_parse_agendas
    from pdc.ingest.announcements import scrape_news_page
    from pdc.transform.matcher import match_agenda_to_api

    with get_db() as conn:
        log_id = conn.execute(
            "INSERT INTO sync_log (source, started_at) VALUES ('full_sync', datetime('now'))",
        ).lastrowid

        console.print("[bold]Syncing Socrata APIs...[/bold]")
        api_results = sync_all(conn, full)
        for r in api_results:
            console.print(f"  {r['source']}: {r['fetched']} fetched")

        console.print("[bold]Discovering agenda PDFs...[/bold]")
        agenda_result = sync_agendas(conn)
        console.print(f"  {agenda_result['downloaded']} new agendas downloaded")

        console.print("[bold]Parsing agenda PDFs...[/bold]")
        parse_result = sync_parse_agendas(conn)
        console.print(f"  {parse_result['pdfs_parsed']} PDFs parsed, {parse_result['items_inserted']} items")

        console.print("[bold]Discovering minutes/certificates PDFs...[/bold]")
        from pdc.ingest.agenda_scraper import sync_minutes
        minutes_result = sync_minutes(conn)
        console.print(f"  {minutes_result['discovered']} found, {minutes_result['downloaded']} downloaded")

        console.print("[bold]Matching agenda items to API records...[/bold]")
        match_result = match_agenda_to_api(conn)
        console.print(f"  {match_result['matched']} of {match_result['agenda_unmatched']} matched")

        console.print("[bold]Scraping announcements...[/bold]")
        ann_result = scrape_news_page(conn)
        console.print(f"  {ann_result['inserted']} announcements found")

        console.print("[bold]Downloading presentation PDFs...[/bold]")
        from pdc.ingest.agenda_scraper import sync_presentations
        pres_result = sync_presentations(conn)
        console.print(f"  {pres_result['discovered']} found, {pres_result['downloaded']} downloaded, {pres_result['already_had']} already had, {pres_result['failed']} failed")

        console.print("[bold]Syncing YouTube videos & transcripts...[/bold]")
        from pdc.ingest.youtube import sync_youtube
        yt_result = sync_youtube(conn)
        console.print(f"  {yt_result['videos_found']} videos found, {yt_result['inserted']} new, {yt_result['transcripts_fetched']} transcripts")

        console.print("[bold]Building project links...[/bold]")
        from pdc.transform.linker import build_project_links
        link_result = build_project_links(conn)
        console.print(f"  {link_result['total']} links ({link_result['same_project']} same-project, {link_result['modification']} modifications, {link_result['same_site']} same-site)")

        total = sum(r["fetched"] for r in api_results)
        conn.execute(
            """UPDATE sync_log SET completed_at = datetime('now'),
               records_fetched = ?, status = 'completed'
               WHERE id = ?""",
            (total, log_id),
        )

    console.print("[bold green]Sync complete![/bold green]")


@cli.command()
@click.argument("project_id")
def track(project_id):
    """Show stage progression for a project."""
    from pdc.transform.stage_tracker import get_project_timeline

    with get_db() as conn:
        timeline = get_project_timeline(conn, project_id)

    if not timeline:
        console.print(f"[red]Project {project_id} not found[/red]")
        return

    console.print(f"\n[bold]{timeline['title']}[/bold]")
    console.print(f"Borough: {timeline['borough']}  |  Type: {timeline['project_type']}  |  Agency: {timeline['lead_agency']}")
    console.print(f"Result: {timeline['final_result'] or 'In progress'}  |  Total meetings: {timeline['total_meetings']}")
    if timeline["total_days_in_review"] is not None:
        console.print(f"Days in review: {timeline['total_days_in_review']}")

    table = Table(title="Stage Progression")
    table.add_column("Date")
    table.add_column("Stage")
    table.add_column("Action")
    table.add_column("Result")
    table.add_column("Source")

    for s in timeline["stages"]:
        table.add_row(
            s["date"], s["stage"] or "", s["action"] or "",
            s["result"] or "", s["source"],
        )

    console.print(table)


@cli.command()
@click.option("--date", "meeting_date", help="Meeting date (YYYY-MM-DD)")
def compare(meeting_date):
    """Compare API vs agenda data for a meeting."""
    from pdc.compare.api_vs_agenda import compare_meeting as do_compare

    if not meeting_date:
        console.print("[red]Please specify --date YYYY-MM-DD[/red]")
        return

    with get_db() as conn:
        result = do_compare(conn, meeting_date)

    console.print(f"\n[bold]Comparison for {result['meeting_date']}[/bold]")
    console.print(f"API items: {result['api_items']}  |  Agenda items: {result['agenda_items']}")

    if result["only_in_api"]:
        console.print(f"\n[yellow]Items only in API ({len(result['only_in_api'])}):[/yellow]")
        for item in result["only_in_api"]:
            console.print(f"  {item['certificate_number']}: {item['title']}")

    if result["only_in_agenda"]:
        console.print(f"\n[yellow]Items only in agenda ({len(result['only_in_agenda'])}):[/yellow]")
        for item in result["only_in_agenda"]:
            console.print(f"  {item['certificate_number']}: {item['title']}")

    if result["differences"]:
        console.print(f"\n[yellow]Field differences ({len(result['differences'])}):[/yellow]")
        for diff in result["differences"]:
            console.print(f"  {diff['certificate_number']}:")
            for field, vals in diff["differences"].items():
                console.print(f"    {field}: API={vals.get('api')} vs Agenda={vals.get('agenda')}")


@cli.command()
def report():
    """Generate summary report with comparison analysis."""
    from pdc.compare.report import generate_summary, find_unannounced_approvals, find_announcement_discrepancies

    with get_db() as conn:
        summary = generate_summary(conn)

        console.print("\n[bold]PDC Database Summary[/bold]")
        table = Table()
        table.add_column("Metric")
        table.add_column("Count", justify="right")
        for k, v in summary["totals"].items():
            table.add_row(k.replace("_", " ").title(), str(v))
        console.print(table)

        if summary["projects_by_borough"]:
            console.print("\n[bold]Projects by Borough[/bold]")
            for borough, cnt in summary["projects_by_borough"].items():
                console.print(f"  {borough}: {cnt}")

        if summary["projects_by_result"]:
            console.print("\n[bold]Projects by Result[/bold]")
            for result, cnt in summary["projects_by_result"].items():
                console.print(f"  {result}: {cnt}")

        console.print("\n[bold]Unannounced Approvals[/bold]")
        unannounced = find_unannounced_approvals(conn)
        if unannounced:
            table = Table()
            table.add_column("Project ID")
            table.add_column("Title")
            table.add_column("Date")
            table.add_column("Borough")
            for item in unannounced[:20]:  # Show top 20
                table.add_row(
                    item["project_id"], item["title"][:60],
                    item["meeting_date"], item["borough"],
                )
            console.print(table)
            if len(unannounced) > 20:
                console.print(f"  ... and {len(unannounced) - 20} more")
        else:
            console.print("  No unannounced approvals found.")

        console.print("\n[bold]Announcement Discrepancies[/bold]")
        discrepancies = find_announcement_discrepancies(conn)
        if discrepancies:
            for d in discrepancies[:10]:
                console.print(f"  Project {d['project_id']}:")
                for issue in d["issues"]:
                    console.print(f"    {issue['type']}: {issue}")
        else:
            console.print("  No discrepancies found.")


@cli.command()
def upload():
    """Upload all PDFs to DigitalOcean Spaces."""
    from pdc.storage import upload_all_pdfs

    with get_db() as conn:
        console.print("[bold]Uploading PDFs to DigitalOcean Spaces...[/bold]")
        try:
            result = upload_all_pdfs(conn)
            console.print(f"  Agendas uploaded: {result['uploaded_agendas']}")
            console.print(f"  Presentations uploaded: {result['uploaded_presentations']}")
            console.print(f"  Minutes uploaded: {result['uploaded_minutes']}")
            console.print(f"  Skipped (already uploaded): {result['skipped']}")
            console.print("[bold green]Upload complete![/bold green]")
        except RuntimeError as e:
            console.print(f"[red]{e}[/red]")


@cli.command()
@click.option("--port", default=8001, help="Port number")
def serve(port):
    """Launch Datasette web UI."""
    if not DB_PATH.exists():
        console.print("[red]Database not found. Run 'pdc sync' first.[/red]")
        return

    metadata = str(DB_PATH.parent / "metadata.yml")
    console.print(f"[bold]Starting Datasette on http://localhost:{port}[/bold]")
    subprocess.run(
        [sys.executable, "-m", "datasette", "serve", str(DB_PATH),
         "--metadata", metadata, "--port", str(port)],
    )


@cli.command()
@click.option("--days", default=730, help="Stalled threshold in days")
def stalled(days):
    """Show projects stalled for more than N days."""
    from pdc.transform.stage_tracker import find_stalled_projects

    with get_db() as conn:
        projects = find_stalled_projects(conn, days)

    if not projects:
        console.print(f"No projects stalled for more than {days} days.")
        return

    table = Table(title=f"Projects Stalled > {days} Days")
    table.add_column("Project ID")
    table.add_column("Title")
    table.add_column("Stage")
    table.add_column("Last Seen")
    table.add_column("Days Stalled", justify="right")

    for p in projects:
        table.add_row(
            p["project_id"], (p["title"] or "")[:50],
            p["current_stage"] or "", p["last_seen_date"] or "",
            str(p["days_stalled"]),
        )

    console.print(table)


@cli.command()
def link():
    """Build links between related projects."""
    from pdc.transform.linker import build_project_links

    with get_db() as conn:
        console.print("[bold]Building project links...[/bold]")
        result = build_project_links(conn)
        console.print(f"  Same project (different stages): {result['same_project']} pairs")
        console.print(f"  Modifications linked to originals: {result['modification']} pairs")
        console.print(f"  Same site: {result['same_site']} pairs")
        console.print(f"  [bold green]Total: {result['total']} links[/bold green]")


if __name__ == "__main__":
    cli()
