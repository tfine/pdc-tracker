import secrets

from flask import Blueprint, render_template, request, redirect, url_for, flash

from pdc.db import get_db

bp = Blueprint("alerts", __name__, url_prefix="/alerts")


@bp.route("/subscribe", methods=["GET", "POST"])
def subscribe():
    if request.method == "GET":
        # Pre-fill options
        with get_db() as conn:
            boroughs = conn.execute(
                "SELECT DISTINCT borough FROM projects WHERE borough IS NOT NULL ORDER BY borough"
            ).fetchall()
        return render_template(
            "subscribe.html",
            boroughs=[r["borough"] for r in boroughs],
        )

    email = request.form.get("email", "").strip().lower()
    sub_type = request.form.get("type", "all_meetings")
    filter_value = request.form.get("filter_value", "").strip() or None

    if not email or "@" not in email:
        flash("Please enter a valid email address.", "error")
        return redirect(url_for("alerts.subscribe"))

    verify_token = secrets.token_urlsafe(32)
    unsub_token = secrets.token_urlsafe(32)

    with get_db() as conn:
        # Upsert subscriber
        existing = conn.execute(
            "SELECT id, verified, unsubscribe_token FROM subscribers WHERE email = ?",
            (email,),
        ).fetchone()

        if existing:
            sub_id = existing["id"]
            unsub_token = existing["unsubscribe_token"]
            # Update verify token if not yet verified
            if not existing["verified"]:
                conn.execute(
                    "UPDATE subscribers SET verify_token = ? WHERE id = ?",
                    (verify_token, sub_id),
                )
        else:
            conn.execute(
                """INSERT INTO subscribers (email, verify_token, unsubscribe_token)
                   VALUES (?, ?, ?)""",
                (email, verify_token, unsub_token),
            )
            sub_id = conn.execute(
                "SELECT id FROM subscribers WHERE email = ?", (email,)
            ).fetchone()["id"]

        # Add subscription (ignore duplicate)
        conn.execute(
            """INSERT INTO subscriptions (subscriber_id, subscription_type, filter_value)
               VALUES (?, ?, ?)
               ON CONFLICT(subscriber_id, subscription_type, filter_value) DO NOTHING""",
            (sub_id, sub_type, filter_value),
        )

    # Send verification email
    try:
        from pdc.email_alerts import send_verification_email
        send_verification_email(email, verify_token)
        flash("Check your email for a verification link.", "success")
    except Exception:
        flash("Subscription saved. Verification email could not be sent.", "warning")

    return redirect(url_for("alerts.subscribe"))


@bp.route("/verify/<token>")
def verify(token):
    with get_db() as conn:
        sub = conn.execute(
            "SELECT id, email FROM subscribers WHERE verify_token = ?", (token,)
        ).fetchone()
        if not sub:
            flash("Invalid or expired verification link.", "error")
            return redirect(url_for("home.index"))
        conn.execute(
            "UPDATE subscribers SET verified = 1, verify_token = NULL WHERE id = ?",
            (sub["id"],),
        )
    flash("Email verified! You'll receive alerts when items you watch are updated.", "success")
    return redirect(url_for("home.index"))


@bp.route("/unsubscribe/<token>")
def unsubscribe(token):
    with get_db() as conn:
        sub = conn.execute(
            "SELECT id, email FROM subscribers WHERE unsubscribe_token = ?", (token,)
        ).fetchone()
        if not sub:
            flash("Invalid unsubscribe link.", "error")
            return redirect(url_for("home.index"))
        conn.execute("DELETE FROM subscriptions WHERE subscriber_id = ?", (sub["id"],))
        conn.execute("DELETE FROM subscribers WHERE id = ?", (sub["id"],))
    flash("You've been unsubscribed.", "success")
    return redirect(url_for("home.index"))
