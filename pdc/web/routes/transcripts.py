import json
import re

from flask import Blueprint, render_template, request

from pdc.db import get_db
from pdc.config import DATABASE_URL

bp = Blueprint("transcripts", __name__, url_prefix="/transcripts")


def _snippet(text, query, context_chars=120):
    """Extract a snippet around the first occurrence of query in text."""
    if not text or not query:
        return ""
    lower_text = text.lower()
    lower_q = query.lower()
    pos = lower_text.find(lower_q)
    if pos == -1:
        return text[:context_chars * 2] + "..."
    start = max(0, pos - context_chars)
    end = min(len(text), pos + len(query) + context_chars)
    snippet = text[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(text):
        snippet = snippet + "..."
    # Highlight the match
    snippet = re.sub(
        re.escape(query),
        lambda m: f"<mark>{m.group()}</mark>",
        snippet,
        flags=re.IGNORECASE,
    )
    return snippet


def _find_timestamp(transcript_json, query):
    """Find approximate timestamp for a query term in transcript segments."""
    if not transcript_json:
        return None
    try:
        segments = json.loads(transcript_json)
    except (json.JSONDecodeError, TypeError):
        return None
    lower_q = query.lower()
    for seg in segments:
        if lower_q in seg.get("text", "").lower():
            return int(seg.get("start", 0))
    return None


@bp.route("/search")
def search():
    q = request.args.get("q", "").strip()
    results = []

    if q:
        with get_db() as conn:
            if DATABASE_URL.startswith("postgresql"):
                # PostgreSQL full-text search
                rows = conn.execute(
                    """SELECT v.video_id, v.title, v.meeting_date, v.url,
                              v.transcript_text, v.transcript_json,
                              ts_rank(
                                  to_tsvector('english', COALESCE(v.transcript_text, '')),
                                  plainto_tsquery('english', ?)
                              ) AS rank
                       FROM youtube_videos v
                       WHERE v.has_transcript = true
                         AND to_tsvector('english', COALESCE(v.transcript_text, ''))
                             @@ plainto_tsquery('english', ?)
                       ORDER BY rank DESC
                       LIMIT 30""",
                    (q, q),
                ).fetchall()
            else:
                # SQLite fallback: LIKE search
                rows = conn.execute(
                    """SELECT video_id, title, meeting_date, url,
                              transcript_text, transcript_json
                       FROM youtube_videos
                       WHERE has_transcript = 1
                         AND transcript_text LIKE ?
                       ORDER BY meeting_date DESC
                       LIMIT 30""",
                    (f"%{q}%",),
                ).fetchall()

            for row in rows:
                timestamp = _find_timestamp(row["transcript_json"], q)
                url = row["url"] or ""
                if timestamp and url:
                    url = f"{url}&t={timestamp}s"
                results.append({
                    "video_id": row["video_id"],
                    "title": row["title"],
                    "meeting_date": row["meeting_date"],
                    "url": url,
                    "snippet": _snippet(row["transcript_text"], q),
                    "timestamp": timestamp,
                })

    return render_template("transcript_search.html", results=results, q=q)
