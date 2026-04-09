"""Link related PDC projects together.

Detects three types of relationships:
- same_project: Identical title, different project_id (same work at different review stages)
- modification: "Minor modifications to X" linked to the original project X
  (project_id_a = modification, project_id_b = original)
- same_site: Different work at the same address/location
"""

import re
import logging

from rapidfuzz import fuzz

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MINOR_MOD_RE = re.compile(
    r"^minor\s+modifications?\s+to\s+(?:the\s+)?", re.IGNORECASE
)

# Common abbreviation normalization for addresses
_ABBREVS = [
    (r"\bave\b", "avenue"),
    (r"\bst\b", "street"),
    (r"\bblvd\b", "boulevard"),
    (r"\bpl\b", "place"),
    (r"\bdr\b", "drive"),
    (r"\brd\b", "road"),
    (r"\bct\b", "court"),
    (r"\bpk\b", "park"),
    (r"\bn\.\s*", "north "),
    (r"\bs\.\s*", "south "),
    (r"\be\.\s*", "east "),
    (r"\bw\.\s*", "west "),
]


def extract_location(title: str) -> str | None:
    """Extract the location portion from a PDC project title.

    PDC titles follow: "Work description, Location/Address, Borough"
    We take everything after the first comma as the location.
    Returns None if title has no comma or location is too short.
    """
    if not title:
        return None
    idx = title.find(",")
    if idx == -1:
        return None
    loc = title[idx + 1:].strip().lower()
    if len(loc) < 10:
        return None
    return loc


def _normalize_location(loc: str) -> str:
    """Normalize a location string for robust comparison."""
    loc = loc.lower().strip()
    loc = re.sub(r"[,\.\-\(\)]", " ", loc)
    for pattern, replacement in _ABBREVS:
        loc = re.sub(pattern, replacement, loc)
    loc = re.sub(r"\s+", " ", loc)
    return loc.strip()


def _extract_work_description(title: str) -> str:
    """Extract just the work description (before the first comma)."""
    idx = title.find(",")
    if idx == -1:
        return title.lower().strip()
    return title[:idx].lower().strip()


def _insert_link(conn, id_a, id_b, link_type, confidence=1.0):
    """Insert a project link. For 'same_project' and 'same_site', ids are
    sorted for dedup. For 'modification', id_a is the modification project
    and id_b is the original."""
    a, b = str(id_a), str(id_b)
    if link_type != "modification":
        a, b = sorted([a, b])
    conn.execute(
        """INSERT OR IGNORE INTO project_links
           (project_id_a, project_id_b, link_type, confidence)
           VALUES (?, ?, ?, ?)""",
        (a, b, link_type, confidence),
    )


# ---------------------------------------------------------------------------
# Linking passes
# ---------------------------------------------------------------------------

def link_same_projects(conn) -> int:
    """Link projects with identical titles (same project at different stages)."""
    # Fetch all projects and group in Python (avoids GROUP_CONCAT which
    # differs between SQLite and PostgreSQL)
    rows = conn.execute(
        """SELECT project_id, LOWER(TRIM(title)) AS norm_title
           FROM projects
           WHERE title IS NOT NULL AND title != ''
           ORDER BY norm_title"""
    ).fetchall()

    groups: dict[str, list[str]] = {}
    for r in rows:
        groups.setdefault(r["norm_title"], []).append(r["project_id"])

    count = 0
    for ids in groups.values():
        if len(ids) < 2:
            continue
        for i, a in enumerate(ids):
            for b in ids[i + 1:]:
                _insert_link(conn, a, b, "same_project")
                count += 1
    return count


def link_modifications(conn) -> int:
    """Link 'minor modifications to X' projects to their original project X.

    Strategy: strip the 'minor modifications to (the)' prefix, then find the
    best-matching non-modification project at the same location.
    """
    mod_projects = conn.execute(
        """SELECT project_id, title FROM projects
           WHERE LOWER(title) LIKE 'minor modification%'"""
    ).fetchall()

    # Build a location index of non-modification projects for fast lookup
    all_projects = conn.execute(
        """SELECT project_id, title FROM projects
           WHERE LOWER(title) NOT LIKE 'minor modification%'"""
    ).fetchall()

    loc_index: dict[str, list[dict]] = {}
    for p in all_projects:
        loc = extract_location(p["title"])
        if not loc:
            continue
        norm = _normalize_location(loc)
        # Index by first 20 chars of normalized location for fast bucketing
        key = norm[:20]
        loc_index.setdefault(key, []).append({
            "project_id": p["project_id"],
            "title": p["title"],
            "norm_loc": norm,
        })

    count = 0
    for proj in mod_projects:
        title = proj["title"]
        original_desc = _MINOR_MOD_RE.sub("", title)
        if original_desc == title:
            continue

        mod_loc = extract_location(title)
        if not mod_loc:
            continue

        norm_mod_loc = _normalize_location(mod_loc)
        bucket_key = norm_mod_loc[:20]

        # Search the bucket and nearby buckets
        candidates = loc_index.get(bucket_key, [])

        best_score = 0.0
        best_id = None
        for cand in candidates:
            loc_score = fuzz.ratio(norm_mod_loc, cand["norm_loc"])
            if loc_score < 85:
                continue
            # Compare: the stripped mod description vs the candidate's full title
            title_score = fuzz.token_sort_ratio(
                original_desc.lower(), cand["title"].lower()
            )
            combined = (loc_score + title_score) / 2
            if combined > best_score:
                best_score = combined
                best_id = cand["project_id"]

        # If bucket miss, fall back to scanning all candidates at similar locations
        if not best_id:
            for key, bucket in loc_index.items():
                for cand in bucket:
                    loc_score = fuzz.ratio(norm_mod_loc, cand["norm_loc"])
                    if loc_score < 90:
                        continue
                    title_score = fuzz.token_sort_ratio(
                        original_desc.lower(), cand["title"].lower()
                    )
                    combined = (loc_score + title_score) / 2
                    if combined > best_score:
                        best_score = combined
                        best_id = cand["project_id"]

        if best_id and best_score >= 90:
            _insert_link(
                conn, proj["project_id"], best_id,
                "modification", best_score / 100,
            )
            count += 1
    return count


def link_same_site(conn, existing_pairs: set | None = None) -> int:
    """Link projects at the same physical location.

    Skips pairs that already have a more specific link type.
    """
    projects = conn.execute(
        "SELECT project_id, title FROM projects WHERE title IS NOT NULL"
    ).fetchall()

    loc_groups: dict[str, list[str]] = {}
    for p in projects:
        loc = extract_location(p["title"])
        if not loc:
            continue
        norm = _normalize_location(loc)
        loc_groups.setdefault(norm, []).append(p["project_id"])

    count = 0
    for norm_loc, ids in loc_groups.items():
        if len(ids) < 2 or len(ids) > 50:
            continue
        for i, a in enumerate(ids):
            for b in ids[i + 1:]:
                pair = tuple(sorted([str(a), str(b)]))
                if existing_pairs and pair in existing_pairs:
                    continue
                _insert_link(conn, a, b, "same_site", 0.9)
                count += 1
    return count


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_project_links(conn) -> dict:
    """Run all linking passes and return counts."""
    conn.execute("DELETE FROM project_links")

    same = link_same_projects(conn)
    log.info("Linked %d same-project pairs", same)

    mods = link_modifications(conn)
    log.info("Linked %d modification pairs", mods)

    # Collect existing pairs so same_site doesn't duplicate them
    existing = conn.execute(
        "SELECT project_id_a, project_id_b FROM project_links"
    ).fetchall()
    existing_pairs = {
        tuple(sorted([r["project_id_a"], r["project_id_b"]])) for r in existing
    }

    site = link_same_site(conn, existing_pairs)
    log.info("Linked %d same-site pairs", site)

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM project_links").fetchone()[0]
    return {
        "same_project": same,
        "modification": mods,
        "same_site": site,
        "total": total,
    }
