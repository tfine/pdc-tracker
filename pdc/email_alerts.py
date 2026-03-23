"""Email alerts via Resend API."""

import resend

from pdc.config import RESEND_API_KEY, ALERT_FROM_EMAIL


def _ensure_api_key():
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY is not set")
    resend.api_key = RESEND_API_KEY


def send_verification_email(to_email: str, verify_token: str):
    """Send a verification email to a new subscriber."""
    _ensure_api_key()
    # Build verify URL — the actual domain is filled by the caller or
    # falls back to a relative path the user clicks from the site.
    verify_url = f"https://pdc.washingtonstreet.group/alerts/verify/{verify_token}"
    resend.Emails.send({
        "from": ALERT_FROM_EMAIL,
        "to": [to_email],
        "subject": "Verify your PDC Tracker subscription",
        "html": (
            "<h2>PDC Tracker — Email Verification</h2>"
            "<p>Click the link below to verify your email and activate alerts:</p>"
            f'<p><a href="{verify_url}">Verify my email</a></p>'
            "<p>If you didn't subscribe, you can ignore this email.</p>"
            "<p>&mdash; Washington Street Advocacy Group</p>"
        ),
    })


def send_alert_email(
    to_email: str,
    subject: str,
    html_body: str,
    unsubscribe_token: str,
):
    """Send an alert notification email."""
    _ensure_api_key()
    unsub_url = f"https://pdc.washingtonstreet.group/alerts/unsubscribe/{unsubscribe_token}"
    html = (
        html_body
        + f'<hr><p style="font-size:12px"><a href="{unsub_url}">Unsubscribe</a> '
        "from PDC Tracker alerts.</p>"
    )
    resend.Emails.send({
        "from": ALERT_FROM_EMAIL,
        "to": [to_email],
        "subject": subject,
        "html": html,
    })


def fan_out_alerts(conn, changes: list[dict]):
    """Send alert emails for a list of detected changes.

    Each change dict has: trigger_type, project_id, title, borough, meeting_date, detail.
    """
    if not changes:
        return 0

    sent = 0
    subscribers = conn.execute(
        "SELECT s.id, s.email, s.unsubscribe_token FROM subscribers s WHERE s.verified = 1"
    ).fetchall()

    for sub in subscribers:
        # Get this subscriber's subscriptions
        subs = conn.execute(
            "SELECT subscription_type, filter_value FROM subscriptions WHERE subscriber_id = ?",
            (sub["id"],),
        ).fetchall()

        matching = []
        for change in changes:
            for s in subs:
                if _matches(s, change):
                    matching.append(change)
                    break

        if not matching:
            continue

        subject = f"PDC Tracker: {len(matching)} update{'s' if len(matching) != 1 else ''}"
        body_parts = ["<h2>PDC Tracker Updates</h2><ul>"]
        for m in matching:
            body_parts.append(
                f'<li><strong>{m["trigger_type"]}</strong>: '
                f'{m["title"]} ({m.get("borough", "")}) &mdash; '
                f'{m.get("detail", "")}</li>'
            )
        body_parts.append("</ul>")

        try:
            send_alert_email(sub["email"], subject, "".join(body_parts), sub["unsubscribe_token"])
            for m in matching:
                conn.execute(
                    """INSERT INTO alert_log (subscriber_id, subject, trigger_type)
                       VALUES (?, ?, ?)""",
                    (sub["id"], subject, m["trigger_type"]),
                )
            sent += 1
        except Exception:
            continue

    return sent


def _matches(subscription, change):
    """Check if a subscription matches a change event."""
    stype = subscription["subscription_type"]
    fval = subscription["filter_value"]

    if stype == "all_meetings":
        return change["trigger_type"] == "new_agenda"
    if stype == "borough":
        return change.get("borough") == fval
    if stype == "project":
        return change.get("project_id") == fval
    return False
