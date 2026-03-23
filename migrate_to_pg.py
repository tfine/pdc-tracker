#!/usr/bin/env python3
"""One-time migration: copy all data from local SQLite to Railway PostgreSQL.

Usage:
    DATABASE_URL=postgresql://... python migrate_to_pg.py

Set DATABASE_URL to your Railway PostgreSQL connection string.
"""

import os
import sqlite3
import sys
from pathlib import Path

from pdc.config import DB_PATH

# Tables in dependency order (parents before children)
TABLES = [
    "projects",
    "meetings",
    "review_events",
    "public_art",
    "announcements",
    "youtube_videos",
    "sync_log",
]


def migrate():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.startswith("postgresql"):
        print("ERROR: Set DATABASE_URL to a PostgreSQL connection string.", file=sys.stderr)
        sys.exit(1)

    if not DB_PATH.exists():
        print(f"ERROR: SQLite database not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    from sqlalchemy import create_engine, text

    # Connect to both databases
    src = sqlite3.connect(str(DB_PATH))
    src.row_factory = sqlite3.Row
    engine = create_engine(database_url)

    # Create PG tables (via our db module)
    from pdc.db import init_db
    os.environ["DATABASE_URL"] = database_url
    pg_conn = init_db()
    pg_conn.close()

    # Migrate each table
    with engine.connect() as dest:
        for table in TABLES:
            rows = src.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                print(f"  {table}: 0 rows (skipped)")
                continue

            columns = rows[0].keys()
            # Skip 'id' column for SERIAL tables
            serial_tables = {"review_events", "public_art", "announcements", "sync_log"}
            if table in serial_tables:
                columns = [c for c in columns if c != "id"]

            col_list = ", ".join(columns)
            param_list = ", ".join(f":{c}" for c in columns)

            sql = f"INSERT INTO {table} ({col_list}) VALUES ({param_list}) ON CONFLICT DO NOTHING"

            batch = []
            for row in rows:
                d = {c: row[c] for c in columns}
                for k, v in d.items():
                    # Convert SQLite boolean int → Python bool
                    if isinstance(v, int) and k in ("has_transcript", "verified"):
                        d[k] = bool(v)
                    # Clean numeric fields with trailing commas/whitespace
                    if k in ("latitude", "longitude", "match_confidence") and isinstance(v, str):
                        cleaned = v.strip().rstrip(",")
                        try:
                            d[k] = float(cleaned)
                        except (ValueError, TypeError):
                            d[k] = None
                batch.append(d)

            dest.execute(text(sql), batch)
            dest.commit()
            print(f"  {table}: {len(batch)} rows migrated")

        # Reset sequences for SERIAL columns
        for table in ["review_events", "public_art", "announcements", "sync_log"]:
            try:
                dest.execute(text(
                    f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                    f"COALESCE((SELECT MAX(id) FROM {table}), 1))"
                ))
                dest.commit()
            except Exception:
                pass

    src.close()
    print("\nMigration complete!")

    # Verify row counts
    with engine.connect() as dest:
        print("\nVerification:")
        for table in TABLES:
            sqlite_count = src_count(table)
            pg_row = dest.execute(text(f"SELECT COUNT(*) AS n FROM {table}")).fetchone()
            pg_count = pg_row[0]
            status = "OK" if pg_count >= sqlite_count else "MISMATCH"
            print(f"  {table}: SQLite={sqlite_count} PG={pg_count} [{status}]")


def src_count(table):
    src = sqlite3.connect(str(DB_PATH))
    count = src.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    src.close()
    return count


if __name__ == "__main__":
    migrate()
