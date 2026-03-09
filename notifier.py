"""
Email-to-SMS notifications via Gmail SMTP.

Each contact in WORLDCUP_NOTIFY_PHONES receives an individual SMS by sending
a short email to their carrier's email-to-SMS gateway address.  No third-party
SMS API or paid service is required — just the same Gmail App Password used by
the other bots in the monorepo.

Supported carriers and their gateways:
  att       → number@txt.att.net
  verizon   → number@vtext.com
  tmobile   → number@tmomail.net
  sprint    → number@messaging.sprintpcs.com
  cricket   → number@mms.cricketwireless.net

Contact list format (WORLDCUP_NOTIFY_PHONES env var):
  "2125551234:verizon,6465559876:att"

SMS body is intentionally kept under 160 characters so it arrives as a single
message on all carriers.

Gmail setup:
  1. Enable 2-Step Verification on your Google account.
  2. Create an App Password at https://myaccount.google.com/apppasswords
  3. Set GMAIL_APP_PASSWORD to that 16-character password.
"""

import logging
import smtplib
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

GATEWAYS: dict[str, str] = {
    "att": "@txt.att.net",
    "verizon": "@vtext.com",
    "tmobile": "@tmomail.net",
    "sprint": "@messaging.sprintpcs.com",
    "cricket": "@mms.cricketwireless.net",
}

_SECTION_LABELS = {
    "field_level": "field level",
    "lower_bowl": "lower bowl",
    "upper_deck": "upper deck",
    "any": "general",
}


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------


def _format_sms(listing: dict) -> str:
    """
    Build a sub-160-character SMS body for one ticket listing.

    Example:
      FIFA26 DEAL [stubhub]: $142 lower bowl
      USA vs Mexico — Jun 15
      stubhub.com/fifa-world-cup...
    """
    source = listing["source"].title()
    price = listing["price"]
    section = _SECTION_LABELS.get(listing["section"], listing["section"])
    game = listing["game"]
    date = listing["date"]  # ISO "2026-06-15"
    url = listing["url"]

    # Format date as "Jun 15" for SMS brevity
    try:
        from datetime import date as date_cls

        d = date_cls.fromisoformat(date)
        date_fmt = d.strftime("%b %-d")
    except Exception:
        date_fmt = date

    # Truncate URL domain for character budget
    url_short = url.split("?")[0][:55] if url else "(no link)"

    body = (
        f"FIFA26 DEAL [{source}]: ${price:.0f} {section}\n"
        f"{game[:40]} — {date_fmt}\n"
        f"{url_short}"
    )
    return body


# ---------------------------------------------------------------------------
# Contact parsing
# ---------------------------------------------------------------------------


def _parse_contacts(raw: str) -> list[tuple[str, str]]:
    """
    Parse "2125551234:verizon,6465559876:att" into
    [("2125551234", "verizon"), ("6465559876", "att")].

    Entries that reference an unsupported carrier are logged and skipped.
    """
    contacts: list[tuple[str, str]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            logger.warning("Skipping malformed contact entry (no colon): %r", part)
            continue
        phone, carrier = part.split(":", 1)
        phone = phone.strip()
        carrier = carrier.strip().lower()
        if carrier not in GATEWAYS:
            logger.warning(
                "Unsupported carrier %r for %s — supported: %s",
                carrier,
                phone,
                ", ".join(GATEWAYS),
            )
            continue
        contacts.append((phone, carrier))
    return contacts


def _gateway_address(phone: str, carrier: str) -> str:
    return f"{phone}{GATEWAYS[carrier]}"


# ---------------------------------------------------------------------------
# SMTP delivery
# ---------------------------------------------------------------------------


def _send_single(
    gmail_user: str,
    gmail_app_password: str,
    to_address: str,
    body: str,
) -> None:
    """Send one email-to-SMS message.  Raises on SMTP failure."""
    msg = MIMEText(body, "plain")
    msg["From"] = gmail_user
    msg["To"] = to_address
    # Subject is intentionally blank — most SMS gateways prepend it to the body
    # and it wastes character budget.
    msg["Subject"] = ""

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(gmail_user, gmail_app_password)
        server.sendmail(gmail_user, to_address, msg.as_string())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def send_alerts(
    gmail_user: str,
    gmail_app_password: str,
    notify_phones_raw: str,
    listings: list[dict],
) -> None:
    """
    Send one SMS per contact per listing that crossed a price threshold.

    Each SMS is sent in its own SMTP connection so a failure for one contact
    does not block the others.  All errors are logged; none are raised.
    """
    if not listings:
        return

    contacts = _parse_contacts(notify_phones_raw)
    if not contacts:
        logger.warning("No valid contacts parsed from WORLDCUP_NOTIFY_PHONES — no SMS sent.")
        return

    for listing in listings:
        body = _format_sms(listing)
        logger.debug("SMS body (%d chars):\n%s", len(body), body)

        for phone, carrier in contacts:
            address = _gateway_address(phone, carrier)
            try:
                _send_single(gmail_user, gmail_app_password, address, body)
                logger.info(
                    "SMS sent to %s (%s) — %s @ $%.2f",
                    phone,
                    carrier,
                    listing["game"][:40],
                    listing["price"],
                )
            except smtplib.SMTPAuthenticationError:
                logger.error(
                    "Gmail authentication failed. "
                    "Ensure GMAIL_APP_PASSWORD is a valid App Password — "
                    "see https://myaccount.google.com/apppasswords"
                )
                return  # No point retrying other contacts with bad credentials
            except Exception as exc:
                logger.error("Failed to send SMS to %s: %s", phone, exc)
