import os
import sqlite3
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv

from pdc.config import DATA_DIR, PDF_DIR, PRESENTATION_PDF_DIR, MINUTES_PDF_DIR

load_dotenv(DATA_DIR.parent / ".env")

# DigitalOcean Spaces config (S3-compatible)
DO_SPACES_KEY = os.getenv("DO_SPACES_KEY", "")
DO_SPACES_SECRET = os.getenv("DO_SPACES_SECRET", "")
DO_SPACES_REGION = os.getenv("DO_SPACES_REGION", "nyc3")
DO_SPACES_BUCKET = os.getenv("DO_SPACES_BUCKET", "pdc-archive")
DO_SPACES_ENDPOINT = f"https://{DO_SPACES_REGION}.digitaloceanspaces.com"
DO_SPACES_CDN = os.getenv(
    "DO_SPACES_CDN",
    f"https://{DO_SPACES_BUCKET}.{DO_SPACES_REGION}.cdn.digitaloceanspaces.com",
)


def get_s3_client():
    """Create an S3-compatible client for DigitalOcean Spaces."""
    if not DO_SPACES_KEY or not DO_SPACES_SECRET:
        raise RuntimeError(
            "DO_SPACES_KEY and DO_SPACES_SECRET must be set in .env file.\n"
            "Create a .env file in the project root with:\n"
            "  DO_SPACES_KEY=your_access_key\n"
            "  DO_SPACES_SECRET=your_secret_key\n"
            "  DO_SPACES_REGION=nyc3\n"
            "  DO_SPACES_BUCKET=pdc-archive"
        )
    return boto3.client(
        "s3",
        region_name=DO_SPACES_REGION,
        endpoint_url=DO_SPACES_ENDPOINT,
        aws_access_key_id=DO_SPACES_KEY,
        aws_secret_access_key=DO_SPACES_SECRET,
        config=BotoConfig(signature_version="s3v4"),
    )


def ensure_bucket(client):
    """Create the bucket if it doesn't exist."""
    try:
        client.head_bucket(Bucket=DO_SPACES_BUCKET)
    except client.exceptions.ClientError:
        client.create_bucket(
            Bucket=DO_SPACES_BUCKET,
            ACL="public-read",
        )


def upload_file(client, local_path: Path, key: str) -> str:
    """Upload a file to DO Spaces. Returns the public CDN URL."""
    content_type = "application/pdf" if local_path.suffix == ".pdf" else "application/octet-stream"
    client.upload_file(
        str(local_path),
        DO_SPACES_BUCKET,
        key,
        ExtraArgs={"ACL": "public-read", "ContentType": content_type},
    )
    return f"{DO_SPACES_CDN}/{key}"


def upload_all_pdfs(conn: sqlite3.Connection) -> dict:
    """Upload all agenda and presentation PDFs to DigitalOcean Spaces."""
    client = get_s3_client()
    ensure_bucket(client)

    uploaded_agendas = 0
    uploaded_presentations = 0
    skipped = 0

    # Upload agenda PDFs
    for pdf_path in sorted(PDF_DIR.glob("*.pdf")):
        key = f"agendas/{pdf_path.name}"
        # Check if already uploaded (simple: just try head_object)
        try:
            client.head_object(Bucket=DO_SPACES_BUCKET, Key=key)
            skipped += 1
            continue
        except client.exceptions.ClientError:
            pass

        cdn_url = upload_file(client, pdf_path, key)
        uploaded_agendas += 1

    # Upload presentation PDFs
    if PRESENTATION_PDF_DIR.exists():
        for pdf_path in sorted(PRESENTATION_PDF_DIR.glob("*.pdf")):
            key = f"presentations/{pdf_path.name}"
            try:
                client.head_object(Bucket=DO_SPACES_BUCKET, Key=key)
                skipped += 1
                continue
            except client.exceptions.ClientError:
                pass

            cdn_url = upload_file(client, pdf_path, key)
            uploaded_presentations += 1

    # Upload minutes/certificates PDFs
    uploaded_minutes = 0
    if MINUTES_PDF_DIR.exists():
        for pdf_path in sorted(MINUTES_PDF_DIR.glob("*.pdf")):
            key = f"minutes/{pdf_path.name}"
            try:
                client.head_object(Bucket=DO_SPACES_BUCKET, Key=key)
                skipped += 1
                continue
            except client.exceptions.ClientError:
                pass

            cdn_url = upload_file(client, pdf_path, key)
            uploaded_minutes += 1

    return {
        "uploaded_agendas": uploaded_agendas,
        "uploaded_presentations": uploaded_presentations,
        "uploaded_minutes": uploaded_minutes,
        "skipped": skipped,
    }
