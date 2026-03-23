import json
import re
import sqlite3
from datetime import datetime

import yt_dlp
from youtube_transcript_api import YouTubeTranscriptApi

CHANNEL_URL = "https://www.youtube.com/@nycdesigncommission/streams"

# Patterns to extract meeting dates from video titles
# e.g. "Public Meeting January 20, 2026", "Design Commission Meeting 1/20/26"
DATE_PATTERNS = [
    # "January 20, 2026" or "Jan 20, 2026"
    re.compile(
        r"(January|February|March|April|May|June|July|August|September|October|November|December|"
        r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        r"\s+(\d{1,2}),?\s+(\d{4})",
        re.IGNORECASE,
    ),
    # "1/20/2026" or "1-20-2026" or "01/20/26"
    re.compile(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})"),
]

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _extract_meeting_date(title: str, upload_date: str | None = None) -> str | None:
    """Try to extract a meeting date from the video title."""
    for pattern in DATE_PATTERNS:
        match = pattern.search(title)
        if not match:
            continue
        groups = match.groups()
        if len(groups) == 3 and groups[0].isalpha():
            # Month name format
            month = MONTH_MAP.get(groups[0].lower())
            day = int(groups[1])
            year = int(groups[2])
            if month:
                return f"{year}-{month:02d}-{day:02d}"
        elif len(groups) == 3:
            # Numeric format
            m, d, y = int(groups[0]), int(groups[1]), int(groups[2])
            year = y if y > 100 else 2000 + y
            return f"{year}-{m:02d}-{d:02d}"

    # Fall back to upload date if available
    if upload_date and len(upload_date) == 8:
        # yt-dlp format: "20260120"
        return f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"
    return None


def fetch_channel_videos() -> list[dict]:
    """Fetch video metadata from the PDC YouTube channel."""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
        "skip_download": True,
        "ignoreerrors": True,
    }

    videos = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        result = ydl.extract_info(CHANNEL_URL, download=False)
        if not result:
            return videos

        entries = result.get("entries") or []
        for entry in entries:
            if not entry:
                continue
            video_id = entry.get("id")
            if not video_id:
                continue

            title = entry.get("title", "")
            upload_date = entry.get("upload_date")
            meeting_date = _extract_meeting_date(title, upload_date)

            videos.append({
                "video_id": video_id,
                "title": title,
                "upload_date": upload_date,
                "meeting_date": meeting_date,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "duration_seconds": entry.get("duration"),
                "view_count": entry.get("view_count"),
                "description": entry.get("description", ""),
            })

    return videos


def fetch_transcript(video_id: str) -> dict | None:
    """Fetch transcript for a single video. Returns dict with text and segments."""
    try:
        ytt_api = YouTubeTranscriptApi()
        transcript = ytt_api.fetch(video_id)
        segments = [
            {"text": seg.text, "start": seg.start, "duration": seg.duration}
            for seg in transcript.snippets
        ]
        full_text = " ".join(seg["text"] for seg in segments)
        return {
            "text": full_text,
            "segments": segments,
        }
    except Exception:
        return None


def sync_youtube(conn: sqlite3.Connection, include_transcripts: bool = True) -> dict:
    """Fetch all videos and optionally their transcripts."""
    videos = fetch_channel_videos()
    inserted = 0
    transcripts_fetched = 0

    for video in videos:
        vid = video["video_id"]

        # Check if already exists
        existing = conn.execute(
            "SELECT video_id, has_transcript FROM youtube_videos WHERE video_id = ?",
            (vid,),
        ).fetchone()

        if existing:
            # Update view count
            conn.execute(
                "UPDATE youtube_videos SET view_count = ? WHERE video_id = ?",
                (video["view_count"], vid),
            )
            # Fetch transcript if we don't have it yet
            if include_transcripts and not existing["has_transcript"]:
                transcript = fetch_transcript(vid)
                if transcript:
                    conn.execute(
                        """UPDATE youtube_videos SET
                            has_transcript = 1,
                            transcript_text = ?,
                            transcript_json = ?
                        WHERE video_id = ?""",
                        (transcript["text"], json.dumps(transcript["segments"]), vid),
                    )
                    transcripts_fetched += 1
            continue

        # Insert new video
        transcript_text = None
        transcript_json = None
        has_transcript = False

        if include_transcripts:
            transcript = fetch_transcript(vid)
            if transcript:
                transcript_text = transcript["text"]
                transcript_json = json.dumps(transcript["segments"])
                has_transcript = True
                transcripts_fetched += 1

        conn.execute(
            """INSERT OR IGNORE INTO youtube_videos
                (video_id, title, upload_date, meeting_date, url,
                 duration_seconds, view_count, description,
                 has_transcript, transcript_text, transcript_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (vid, video["title"], video["upload_date"], video["meeting_date"],
             video["url"], video["duration_seconds"], video["view_count"],
             video["description"], has_transcript, transcript_text, transcript_json),
        )
        inserted += 1

        # Link to meetings table
        if video["meeting_date"]:
            conn.execute(
                """UPDATE meetings SET youtube_url = ?
                WHERE meeting_date = ? AND youtube_url IS NULL""",
                (video["url"], video["meeting_date"]),
            )

    conn.commit()
    return {
        "source": "youtube",
        "videos_found": len(videos),
        "inserted": inserted,
        "transcripts_fetched": transcripts_fetched,
    }
