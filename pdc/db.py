import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from pdc.config import DB_PATH, DATA_DIR, DATABASE_URL

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

_TABLES_SQLITE = """
-- Projects: one row per unique project
CREATE TABLE IF NOT EXISTS projects (
    project_id          TEXT PRIMARY KEY,
    title               TEXT NOT NULL,
    borough             TEXT,
    project_type        TEXT,
    construction_type   TEXT,
    public_private      TEXT,
    lead_agency         TEXT,
    secondary_agency    TEXT,
    location_name       TEXT,
    address             TEXT,
    latitude            REAL,
    longitude           REAL,
    block               TEXT,
    lot                 TEXT,
    cc_district         TEXT,
    community_board     TEXT,
    first_seen_date     TEXT,
    last_seen_date      TEXT,
    current_stage       TEXT,
    final_result        TEXT,
    total_review_cycles INTEGER,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

-- Review events: one row per project appearance at a meeting
CREATE TABLE IF NOT EXISTS review_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id          TEXT NOT NULL REFERENCES projects(project_id),
    certificate_number  TEXT,
    meeting_date        TEXT NOT NULL,
    level_of_review     TEXT,
    action              TEXT,
    result              TEXT,
    agenda_section      TEXT,
    scheduled_time      TEXT,
    presentation_url    TEXT,
    data_source         TEXT NOT NULL,
    raw_data            TEXT,
    created_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(project_id, meeting_date, level_of_review, data_source)
);

-- Meetings
CREATE TABLE IF NOT EXISTS meetings (
    meeting_date        TEXT PRIMARY KEY,
    agenda_pdf_url      TEXT,
    minutes_pdf_url     TEXT,
    agenda_fetched_at   TEXT,
    minutes_fetched_at  TEXT,
    location            TEXT,
    youtube_url         TEXT,
    notes               TEXT
);

-- Public art inventory
CREATE TABLE IF NOT EXISTS public_art (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    title               TEXT NOT NULL,
    alternate_title     TEXT,
    primary_artist      TEXT,
    secondary_artist    TEXT,
    primary_architect   TEXT,
    architecture_firm   TEXT,
    foundry             TEXT,
    fabricator          TEXT,
    date_created        TEXT,
    date_dedicated      TEXT,
    artwork_type1       TEXT,
    artwork_type2       TEXT,
    material            TEXT,
    location_name       TEXT,
    address             TEXT,
    borough             TEXT,
    latitude            REAL,
    longitude           REAL,
    block               TEXT,
    lot                 TEXT,
    subject_keyword     TEXT,
    inscription         TEXT,
    managing_agency     TEXT,
    acquisition         TEXT,
    pdc_records         TEXT,
    project_id          TEXT REFERENCES projects(project_id),
    created_at          TEXT DEFAULT (datetime('now'))
);

-- Announcements
CREATE TABLE IF NOT EXISTS announcements (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source              TEXT NOT NULL,
    source_url          TEXT,
    title               TEXT,
    date_published      TEXT,
    content_summary     TEXT,
    matched_project_id  TEXT REFERENCES projects(project_id),
    matched_certificate TEXT,
    match_confidence    REAL,
    created_at          TEXT DEFAULT (datetime('now'))
);

-- YouTube meeting videos and transcripts
CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id            TEXT PRIMARY KEY,
    title               TEXT,
    upload_date         TEXT,
    meeting_date        TEXT,
    url                 TEXT,
    duration_seconds    INTEGER,
    view_count          INTEGER,
    description         TEXT,
    has_transcript      BOOLEAN DEFAULT 0,
    transcript_text     TEXT,
    transcript_json     TEXT,
    fetched_at          TEXT DEFAULT (datetime('now'))
);

-- Sync log
CREATE TABLE IF NOT EXISTS sync_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source              TEXT NOT NULL,
    started_at          TEXT NOT NULL,
    completed_at        TEXT,
    records_fetched     INTEGER,
    records_inserted    INTEGER,
    records_updated     INTEGER,
    status              TEXT DEFAULT 'running',
    error_message       TEXT
);

-- Email alert subscribers
CREATE TABLE IF NOT EXISTS subscribers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    email               TEXT NOT NULL UNIQUE,
    verified            BOOLEAN DEFAULT 0,
    verify_token        TEXT,
    unsubscribe_token   TEXT NOT NULL,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    subscriber_id       INTEGER REFERENCES subscribers(id),
    subscription_type   TEXT NOT NULL,
    filter_value        TEXT,
    created_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(subscriber_id, subscription_type, filter_value)
);

CREATE TABLE IF NOT EXISTS alert_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    subscriber_id       INTEGER REFERENCES subscribers(id),
    subject             TEXT,
    trigger_type        TEXT,
    sent_at             TEXT DEFAULT (datetime('now'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_review_events_project ON review_events(project_id);
CREATE INDEX IF NOT EXISTS idx_review_events_date ON review_events(meeting_date);
CREATE INDEX IF NOT EXISTS idx_review_events_cert ON review_events(certificate_number);
CREATE INDEX IF NOT EXISTS idx_projects_borough ON projects(borough);
CREATE INDEX IF NOT EXISTS idx_projects_type ON projects(project_type);
CREATE INDEX IF NOT EXISTS idx_projects_stage ON projects(current_stage);
CREATE INDEX IF NOT EXISTS idx_public_art_pdc ON public_art(pdc_records);
CREATE INDEX IF NOT EXISTS idx_announcements_project ON announcements(matched_project_id);
CREATE INDEX IF NOT EXISTS idx_youtube_meeting ON youtube_videos(meeting_date);

-- Stage progression view
CREATE VIEW IF NOT EXISTS project_stage_timeline AS
SELECT
    p.project_id,
    p.title,
    p.borough,
    p.project_type,
    p.lead_agency,
    MIN(CASE WHEN re.level_of_review LIKE '%Conceptual%'
              AND re.level_of_review NOT LIKE '%Preliminary%'
         THEN re.meeting_date END) AS conceptual_date,
    MIN(CASE WHEN re.level_of_review LIKE '%Preliminary%'
              AND re.level_of_review NOT LIKE '%Conceptual%'
              AND re.level_of_review NOT LIKE '%Final%'
         THEN re.meeting_date END) AS preliminary_date,
    MIN(CASE WHEN re.level_of_review LIKE '%Final%'
              AND re.level_of_review NOT LIKE '%Preliminary%'
         THEN re.meeting_date END) AS final_date,
    MIN(CASE WHEN re.level_of_review = 'Preliminary and Final'
         THEN re.meeting_date END) AS preliminary_and_final_date,
    p.final_result,
    CAST(julianday(
        MIN(CASE WHEN re.level_of_review LIKE '%Preliminary%'
                  AND re.level_of_review NOT LIKE '%Conceptual%'
                  AND re.level_of_review NOT LIKE '%Final%'
             THEN re.meeting_date END)
    ) - julianday(
        MIN(CASE WHEN re.level_of_review LIKE '%Conceptual%'
                  AND re.level_of_review NOT LIKE '%Preliminary%'
             THEN re.meeting_date END)
    ) AS INTEGER) AS days_conceptual_to_preliminary,
    CAST(julianday(
        MIN(CASE WHEN re.level_of_review LIKE '%Final%'
                  AND re.level_of_review NOT LIKE '%Preliminary%'
             THEN re.meeting_date END)
    ) - julianday(
        MIN(CASE WHEN re.level_of_review LIKE '%Preliminary%'
                  AND re.level_of_review NOT LIKE '%Conceptual%'
                  AND re.level_of_review NOT LIKE '%Final%'
             THEN re.meeting_date END)
    ) AS INTEGER) AS days_preliminary_to_final,
    COUNT(DISTINCT re.meeting_date) AS total_meetings,
    p.total_review_cycles
FROM projects p
LEFT JOIN review_events re ON p.project_id = re.project_id
GROUP BY p.project_id;
"""

_TABLES_PG = """
CREATE TABLE IF NOT EXISTS projects (
    project_id          TEXT PRIMARY KEY,
    title               TEXT NOT NULL,
    borough             TEXT,
    project_type        TEXT,
    construction_type   TEXT,
    public_private      TEXT,
    lead_agency         TEXT,
    secondary_agency    TEXT,
    location_name       TEXT,
    address             TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    block               TEXT,
    lot                 TEXT,
    cc_district         TEXT,
    community_board     TEXT,
    first_seen_date     TEXT,
    last_seen_date      TEXT,
    current_stage       TEXT,
    final_result        TEXT,
    total_review_cycles INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS review_events (
    id                  SERIAL PRIMARY KEY,
    project_id          TEXT NOT NULL REFERENCES projects(project_id),
    certificate_number  TEXT,
    meeting_date        TEXT NOT NULL,
    level_of_review     TEXT,
    action              TEXT,
    result              TEXT,
    agenda_section      TEXT,
    scheduled_time      TEXT,
    presentation_url    TEXT,
    data_source         TEXT NOT NULL,
    raw_data            TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_id, meeting_date, level_of_review, data_source)
);

CREATE TABLE IF NOT EXISTS meetings (
    meeting_date        TEXT PRIMARY KEY,
    agenda_pdf_url      TEXT,
    minutes_pdf_url     TEXT,
    agenda_fetched_at   TEXT,
    minutes_fetched_at  TEXT,
    location            TEXT,
    youtube_url         TEXT,
    notes               TEXT
);

CREATE TABLE IF NOT EXISTS public_art (
    id                  SERIAL PRIMARY KEY,
    title               TEXT NOT NULL,
    alternate_title     TEXT,
    primary_artist      TEXT,
    secondary_artist    TEXT,
    primary_architect   TEXT,
    architecture_firm   TEXT,
    foundry             TEXT,
    fabricator          TEXT,
    date_created        TEXT,
    date_dedicated      TEXT,
    artwork_type1       TEXT,
    artwork_type2       TEXT,
    material            TEXT,
    location_name       TEXT,
    address             TEXT,
    borough             TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    block               TEXT,
    lot                 TEXT,
    subject_keyword     TEXT,
    inscription         TEXT,
    managing_agency     TEXT,
    acquisition         TEXT,
    pdc_records         TEXT,
    project_id          TEXT REFERENCES projects(project_id),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS announcements (
    id                  SERIAL PRIMARY KEY,
    source              TEXT NOT NULL,
    source_url          TEXT,
    title               TEXT,
    date_published      TEXT,
    content_summary     TEXT,
    matched_project_id  TEXT REFERENCES projects(project_id),
    matched_certificate TEXT,
    match_confidence    DOUBLE PRECISION,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id            TEXT PRIMARY KEY,
    title               TEXT,
    upload_date         TEXT,
    meeting_date        TEXT,
    url                 TEXT,
    duration_seconds    INTEGER,
    view_count          INTEGER,
    description         TEXT,
    has_transcript      BOOLEAN DEFAULT FALSE,
    transcript_text     TEXT,
    transcript_json     TEXT,
    fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sync_log (
    id                  SERIAL PRIMARY KEY,
    source              TEXT NOT NULL,
    started_at          TIMESTAMP NOT NULL,
    completed_at        TIMESTAMP,
    records_fetched     INTEGER,
    records_inserted    INTEGER,
    records_updated     INTEGER,
    status              TEXT DEFAULT 'running',
    error_message       TEXT
);

CREATE TABLE IF NOT EXISTS subscribers (
    id                  SERIAL PRIMARY KEY,
    email               TEXT NOT NULL UNIQUE,
    verified            BOOLEAN DEFAULT FALSE,
    verify_token        TEXT,
    unsubscribe_token   TEXT NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id                  SERIAL PRIMARY KEY,
    subscriber_id       INTEGER REFERENCES subscribers(id),
    subscription_type   TEXT NOT NULL,
    filter_value        TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(subscriber_id, subscription_type, filter_value)
);

CREATE TABLE IF NOT EXISTS alert_log (
    id                  SERIAL PRIMARY KEY,
    subscriber_id       INTEGER REFERENCES subscribers(id),
    subject             TEXT,
    trigger_type        TEXT,
    sent_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_review_events_project ON review_events(project_id);
CREATE INDEX IF NOT EXISTS idx_review_events_date ON review_events(meeting_date);
CREATE INDEX IF NOT EXISTS idx_review_events_cert ON review_events(certificate_number);
CREATE INDEX IF NOT EXISTS idx_projects_borough ON projects(borough);
CREATE INDEX IF NOT EXISTS idx_projects_type ON projects(project_type);
CREATE INDEX IF NOT EXISTS idx_projects_stage ON projects(current_stage);
CREATE INDEX IF NOT EXISTS idx_public_art_pdc ON public_art(pdc_records);
CREATE INDEX IF NOT EXISTS idx_announcements_project ON announcements(matched_project_id);
CREATE INDEX IF NOT EXISTS idx_youtube_meeting ON youtube_videos(meeting_date);
"""

_VIEW_PG = """
CREATE OR REPLACE VIEW project_stage_timeline AS
SELECT
    p.project_id,
    p.title,
    p.borough,
    p.project_type,
    p.lead_agency,
    MIN(CASE WHEN re.level_of_review LIKE '%Conceptual%'
              AND re.level_of_review NOT LIKE '%Preliminary%'
         THEN re.meeting_date END) AS conceptual_date,
    MIN(CASE WHEN re.level_of_review LIKE '%Preliminary%'
              AND re.level_of_review NOT LIKE '%Conceptual%'
              AND re.level_of_review NOT LIKE '%Final%'
         THEN re.meeting_date END) AS preliminary_date,
    MIN(CASE WHEN re.level_of_review LIKE '%Final%'
              AND re.level_of_review NOT LIKE '%Preliminary%'
         THEN re.meeting_date END) AS final_date,
    MIN(CASE WHEN re.level_of_review = 'Preliminary and Final'
         THEN re.meeting_date END) AS preliminary_and_final_date,
    p.final_result,
    (MIN(CASE WHEN re.level_of_review LIKE '%Preliminary%'
              AND re.level_of_review NOT LIKE '%Conceptual%'
              AND re.level_of_review NOT LIKE '%Final%'
         THEN re.meeting_date END)::date
    - MIN(CASE WHEN re.level_of_review LIKE '%Conceptual%'
              AND re.level_of_review NOT LIKE '%Preliminary%'
         THEN re.meeting_date END)::date
    ) AS days_conceptual_to_preliminary,
    (MIN(CASE WHEN re.level_of_review LIKE '%Final%'
              AND re.level_of_review NOT LIKE '%Preliminary%'
         THEN re.meeting_date END)::date
    - MIN(CASE WHEN re.level_of_review LIKE '%Preliminary%'
              AND re.level_of_review NOT LIKE '%Conceptual%'
              AND re.level_of_review NOT LIKE '%Final%'
         THEN re.meeting_date END)::date
    ) AS days_preliminary_to_final,
    COUNT(DISTINCT re.meeting_date) AS total_meetings,
    p.total_review_cycles
FROM projects p
LEFT JOIN review_events re ON p.project_id = re.project_id
GROUP BY p.project_id, p.title, p.borough, p.project_type, p.lead_agency,
         p.final_result, p.total_review_cycles;
"""

_FTS_INDEX_PG = """
CREATE INDEX IF NOT EXISTS idx_transcript_fts ON youtube_videos
    USING GIN(to_tsvector('english', COALESCE(transcript_text, '')));
"""

# ---------------------------------------------------------------------------
# SQL dialect translation (SQLite → PostgreSQL)
# ---------------------------------------------------------------------------

def _replace_julianday(sql):
    """Replace julianday(expr) with (expr)::date, handling nested parens."""
    result = []
    i = 0
    lower = sql.lower()
    while i < len(sql):
        pos = lower.find("julianday(", i)
        if pos == -1:
            result.append(sql[i:])
            break
        result.append(sql[i:pos])
        # Find matching closing paren
        paren_start = pos + len("julianday(")
        depth = 1
        j = paren_start
        while j < len(sql) and depth > 0:
            if sql[j] == "(":
                depth += 1
            elif sql[j] == ")":
                depth -= 1
            j += 1
        inner = sql[paren_start : j - 1]
        result.append(f"({inner})::date")
        i = j
    return "".join(result)


def _translate_sql(sql):
    """Translate SQLite SQL to PostgreSQL dialect."""
    # datetime('now') → CURRENT_TIMESTAMP
    sql = sql.replace("datetime('now')", "CURRENT_TIMESTAMP")

    # date('now') → CURRENT_DATE
    sql = sql.replace("date('now')", "CURRENT_DATE")

    # INSERT OR IGNORE INTO → INSERT INTO … ON CONFLICT DO NOTHING
    is_insert_or_ignore = bool(
        re.search(r"INSERT\s+OR\s+IGNORE\s+INTO", sql, re.IGNORECASE)
    )
    if is_insert_or_ignore:
        sql = re.sub(
            r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", sql, flags=re.IGNORECASE
        )

    # julianday(expr) → (expr)::date
    if "julianday" in sql.lower():
        sql = _replace_julianday(sql)

    # a IS b  (NULL-safe equality, not IS NULL / IS NOT NULL) →
    # a IS NOT DISTINCT FROM b
    sql = re.sub(
        r"\bIS\s+(?!NULL\b|NOT\b|TRUE\b|FALSE\b|DISTINCT\b)(\w+(?:\.\w+)?)",
        r"IS NOT DISTINCT FROM \1",
        sql,
        flags=re.IGNORECASE,
    )

    # ILIKE → LIKE (SQLite LIKE is already case-insensitive for ASCII)
    # This is a no-op for PostgreSQL; for SQLite we reverse it in the else branch
    # (handled below)

    # PRAGMA statements → no-op
    if sql.strip().upper().startswith("PRAGMA"):
        return "SELECT 1"

    # Append ON CONFLICT DO NOTHING (after VALUES / end of statement)
    if is_insert_or_ignore:
        sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"

    return sql


def _positional_to_named(sql, params):
    """Convert ? placeholders + tuple params → :p0 named params + dict."""
    if params is None:
        return sql, {}
    param_dict = {}
    counter = [0]

    def _replacer(_match):
        name = f"p{counter[0]}"
        counter[0] += 1
        return f":{name}"

    new_sql = re.sub(r"\?", _replacer, sql)
    for idx, val in enumerate(params):
        param_dict[f"p{idx}"] = val
    return new_sql, param_dict


# ---------------------------------------------------------------------------
# Row / Result wrappers for PostgreSQL (so row["col"] works everywhere)
# ---------------------------------------------------------------------------

class _DictRow:
    """Row that supports row["col"], row[0], dict(row), keys()."""

    __slots__ = ("_data", "_keys")

    def __init__(self, mapping):
        self._data = dict(mapping)
        self._keys = list(self._data.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._data[self._keys[key]]
        return self._data[key]

    def __contains__(self, key):
        return key in self._data

    def keys(self):
        return self._keys

    def __iter__(self):
        return iter(self._data.values())

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return f"_DictRow({self._data})"


class _PgCursorResult:
    """Wraps a SQLAlchemy CursorResult to expose fetchone/fetchall/_DictRow."""

    def __init__(self, sa_result, sa_conn):
        self._result = sa_result
        self._lastrowid = None
        # Attempt to retrieve lastrowid via lastval()
        if sa_result.returns_rows is False:
            try:
                from sqlalchemy import text as _t
                row = sa_conn.execute(_t("SELECT lastval()")).fetchone()
                if row:
                    self._lastrowid = row[0]
            except Exception:
                pass

    @property
    def lastrowid(self):
        return self._lastrowid

    @property
    def rowcount(self):
        return self._result.rowcount

    def fetchone(self):
        row = self._result.fetchone()
        if row is None:
            return None
        return _DictRow(row._mapping)

    def fetchall(self):
        return [_DictRow(r._mapping) for r in self._result.fetchall()]


class _PgConnection:
    """Wraps a SQLAlchemy connection to match the sqlite3.Connection interface."""

    def __init__(self, sa_conn):
        self._conn = sa_conn

    def execute(self, sql, params=None):
        from sqlalchemy import text
        sql = _translate_sql(sql)
        sql, param_dict = _positional_to_named(sql, params)
        result = self._conn.execute(text(sql), param_dict or {})
        return _PgCursorResult(result, self._conn)

    def executescript(self, sql):
        from sqlalchemy import text
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                self._conn.execute(text(stmt))

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


# ---------------------------------------------------------------------------
# Engine / init / get_db  (public API — unchanged interface)
# ---------------------------------------------------------------------------

class _SqliteConnection:
    """Thin wrapper around sqlite3.Connection to handle ILIKE → LIKE."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        sql = re.sub(r'\bILIKE\b', 'LIKE', sql)
        if params:
            return self._conn.execute(sql, params)
        return self._conn.execute(sql)

    def executescript(self, sql):
        return self._conn.executescript(sql)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        from sqlalchemy import create_engine
        _engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return _engine


def _use_pg():
    return DATABASE_URL.startswith("postgresql")


def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "pdfs" / "agendas").mkdir(parents=True, exist_ok=True)


def init_db(db_path: Path | None = None):
    """Initialise database and return a connection (sqlite3 or PG wrapper).

    For PostgreSQL the *db_path* argument is ignored.
    """
    if _use_pg():
        engine = _get_engine()
        from sqlalchemy import text
        raw = engine.connect()
        conn = _PgConnection(raw)
        # Create tables (idempotent)
        for stmt in _TABLES_PG.split(";"):
            stmt = stmt.strip()
            if stmt:
                raw.execute(text(stmt))
        # View (CREATE OR REPLACE)
        raw.execute(text(_VIEW_PG))
        # FTS index
        try:
            raw.execute(text(_FTS_INDEX_PG))
        except Exception:
            pass  # already exists
        raw.commit()
        return conn

    # SQLite path (local dev)
    path = db_path or DB_PATH
    ensure_data_dir()
    raw = sqlite3.connect(str(path))
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA journal_mode=WAL")
    raw.execute("PRAGMA foreign_keys=ON")
    raw.executescript(_TABLES_SQLITE)
    return _SqliteConnection(raw)


@contextmanager
def get_db(db_path: Path | None = None):
    """Context manager yielding a connection (sqlite3 or PG wrapper)."""
    conn = init_db(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# Keep old SCHEMA constant for backward compat (tests etc.)
SCHEMA = _TABLES_SQLITE
