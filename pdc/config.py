import os
from pathlib import Path

from dotenv import load_dotenv

# Project root is the directory containing this package
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "pdc.db"
PDF_DIR = DATA_DIR / "pdfs" / "agendas"
PRESENTATION_PDF_DIR = DATA_DIR / "pdfs" / "presentations"

# Database — PostgreSQL URL or empty for SQLite fallback
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Flask
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")

# Email alerts via Resend
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "alerts@washingtonstreet.group")

# DigitalOcean Spaces CDN
DO_SPACES_CDN = os.getenv(
    "DO_SPACES_CDN",
    "https://pdc-archive.nyc3.cdn.digitaloceanspaces.com",
)

# Socrata API endpoints (NYC Open Data)
SOCRATA_BASE = "https://data.cityofnewyork.us/resource"
ENDPOINTS = {
    "monthly_review": f"{SOCRATA_BASE}/tfrc-rjtr.json",
    "annual_report": f"{SOCRATA_BASE}/5fsv-ze7v.json",
    "art_inventory": f"{SOCRATA_BASE}/2pg3-gcaa.json",
}
SOCRATA_BATCH_SIZE = 1000

# PDC website URLs
PDC_BASE = "https://www.nyc.gov/site/designcommission"
PAST_AGENDAS_URL = f"{PDC_BASE}/design-review/meetings/past-agendas-and-meeting-minutes.page"
PDC_NEWS_URL = f"{PDC_BASE}/about/news/"
AGENDA_PDF_BASE = "https://www.nyc.gov/assets/designcommission/downloads/pdf/agendas"
CURRENT_MEETINGS_URL = f"{PDC_BASE}/design-review/meetings/meetings.page"
MINUTES_PDF_DIR = DATA_DIR / "pdfs" / "minutes"

# Review stage ordering
STAGE_ORDER = {
    "Conceptual": 1,
    "Amended Conceptual": 1.5,
    "Conceptual and Preliminary": 2,
    "Preliminary": 2,
    "Amended Preliminary": 2.5,
    "Preliminary and Final": 3,
    "Final": 3,
    "Amended Final": 3.5,
}
