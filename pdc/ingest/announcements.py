import re
import sqlite3

import httpx
from bs4 import BeautifulSoup

from pdc.config import PDC_NEWS_URL


NEWS_URLS = [
    PDC_NEWS_URL,
    "https://www.nyc.gov/site/designcommission/about/news.page",
    "https://www.nyc.gov/site/designcommission/about/news/news.page",
]


def scrape_news_page(conn: sqlite3.Connection) -> dict:
    """Scrape PDC news page for announcements."""
    soup = None
    for url in NEWS_URLS:
        try:
            resp = httpx.get(url, timeout=30, follow_redirects=True)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            break
        except httpx.HTTPError:
            continue

    if soup is None:
        return {"source": "announcements", "inserted": 0, "error": "No news page reachable"}

    inserted = 0
    # Look for news article links and summaries
    for article in soup.find_all(["article", "div"], class_=re.compile(r"news|article|post", re.I)):
        title_el = article.find(["h2", "h3", "h4", "a"])
        if not title_el:
            continue

        title = title_el.get_text(strip=True)
        link = title_el.get("href") or (title_el.find("a") or {}).get("href", "")
        if link and not link.startswith("http"):
            link = f"https://www.nyc.gov{link}"

        # Try to find a date
        date_el = article.find(["time", "span", "p"], class_=re.compile(r"date", re.I))
        date_text = date_el.get_text(strip=True) if date_el else None

        # Get summary text
        summary_el = article.find("p")
        summary = summary_el.get_text(strip=True) if summary_el else None

        if not title:
            continue

        conn.execute(
            """INSERT OR IGNORE INTO announcements
                (source, source_url, title, date_published, content_summary)
            VALUES ('pdc_website', ?, ?, ?, ?)""",
            (link, title, date_text, summary),
        )
        inserted += 1

    # Also look for simple link lists (common on NYC.gov)
    for link_el in soup.find_all("a", href=True):
        href = link_el["href"]
        if "/news/" not in href or href == PDC_NEWS_URL:
            continue
        title = link_el.get_text(strip=True)
        if not title or len(title) < 10:
            continue
        url = href if href.startswith("http") else f"https://www.nyc.gov{href}"

        conn.execute(
            """INSERT OR IGNORE INTO announcements
                (source, source_url, title)
            VALUES ('pdc_website', ?, ?)""",
            (url, title),
        )
        inserted += 1

    conn.commit()
    return {"source": "announcements", "inserted": inserted}
