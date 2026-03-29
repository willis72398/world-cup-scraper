"""
Email-to-SMS notifications and daily digest emails via Gmail SMTP.

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

Daily digest emails (WORLDCUP_DIGEST_EMAILS env var):
  Comma-separated email addresses that receive a daily HTML summary of the
  lowest price per game across TickPick and Gametime.

Gmail setup:
  1. Enable 2-Step Verification on your Google account.
  2. Create an App Password at https://myaccount.google.com/apppasswords
  3. Set GMAIL_APP_PASSWORD to that 16-character password.
"""

import logging
import smtplib
from collections import defaultdict
from datetime import date as date_cls
from email.mime.multipart import MIMEMultipart
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


# ---------------------------------------------------------------------------
# Daily digest email
# ---------------------------------------------------------------------------


def _format_date(iso: str) -> str:
    """'2026-06-15' → 'Sun Jun 15'."""
    try:
        d = date_cls.fromisoformat(iso)
        return d.strftime("%a %b %-d")
    except Exception:
        return iso


def _build_digest_html(listings: list[dict]) -> str:
    """
    Build an HTML email body showing the lowest price per game from each
    source, with a side-by-side comparison.
    """
    # Group by (date, game_normalized) so we can compare across sources
    games: dict[str, dict] = {}  # key = date|game_name → {date, game, sources}
    for l in listings:
        # Normalize game key: strip source-specific prefixes
        game = l["game"]
        date = l["date"]
        key = f"{date}|{game[:50].lower()}"

        # Fuzzy: group by date if game names differ across sources
        if key not in games:
            # Check if we already have this date with a similar game
            matched = False
            for k, v in games.items():
                if k.startswith(date + "|"):
                    # Same date — merge if it looks like the same match
                    v["sources"][l["source"]] = l
                    matched = True
                    break
            if not matched:
                games[key] = {
                    "date": date,
                    "game": game,
                    "sources": {l["source"]: l},
                }
        else:
            games[key]["sources"][l["source"]] = l

    # Sort by date
    sorted_games = sorted(games.values(), key=lambda g: g["date"])

    rows = []
    for g in sorted_games:
        date_fmt = _format_date(g["date"])
        game_name = g["game"]

        tp = g["sources"].get("tickpick")
        gt = g["sources"].get("gametime")

        tp_price = f'<a href="{tp["url"]}">${tp["price"]:,.0f}</a>' if tp else "—"
        gt_price = f'<a href="{gt["url"]}">${gt["price"]:,.0f}</a>' if gt else "—"

        # Highlight the cheaper source
        if tp and gt:
            if tp["price"] < gt["price"]:
                tp_price = f"<b>{tp_price}</b>"
            elif gt["price"] < tp["price"]:
                gt_price = f"<b>{gt_price}</b>"

        rows.append(
            f"<tr>"
            f'<td style="padding:6px 12px">{date_fmt}</td>'
            f'<td style="padding:6px 12px">{game_name}</td>'
            f'<td style="padding:6px 12px;text-align:right">{tp_price}</td>'
            f'<td style="padding:6px 12px;text-align:right">{gt_price}</td>'
            f"</tr>"
        )

    today = date_cls.today().strftime("%B %-d, %Y")
    table_rows = "\n".join(rows) if rows else '<tr><td colspan="4" style="padding:12px">No listings found.</td></tr>'

    return f"""\
<html><body style="font-family:Arial,sans-serif;color:#222">
<h2>World Cup 2026 — MetLife Stadium Price Watch</h2>
<p style="color:#666">{today}</p>
<table border="0" cellspacing="0" style="border-collapse:collapse;width:100%">
<thead>
<tr style="background:#1a1a2e;color:#fff">
<th style="padding:8px 12px;text-align:left">Date</th>
<th style="padding:8px 12px;text-align:left">Game</th>
<th style="padding:8px 12px;text-align:right">TickPick</th>
<th style="padding:8px 12px;text-align:right">Gametime</th>
</tr>
</thead>
<tbody>
{table_rows}
</tbody>
</table>
<p style="color:#999;font-size:12px;margin-top:20px">
Prices are the lowest available listing per source. <b>Bold</b> = cheaper option.
Click a price to go directly to the listing.
</p>
</body></html>"""


def send_digest(
    gmail_user: str,
    gmail_app_password: str,
    digest_emails: list[str],
    listings: list[dict],
) -> None:
    """
    Send a daily HTML digest email to each address in *digest_emails*.
    """
    if not digest_emails:
        logger.warning("No digest email addresses configured — skipping digest.")
        return

    html = _build_digest_html(listings)

    for addr in digest_emails:
        try:
            msg = MIMEMultipart("alternative")
            msg["From"] = gmail_user
            msg["To"] = addr
            msg["Subject"] = f"World Cup 2026 Ticket Prices — {date_cls.today().strftime('%b %-d')}"
            msg.attach(MIMEText(html, "html"))

            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.ehlo()
                server.starttls()
                server.login(gmail_user, gmail_app_password)
                server.sendmail(gmail_user, addr, msg.as_string())

            logger.info("Digest email sent to %s", addr)
        except smtplib.SMTPAuthenticationError:
            logger.error(
                "Gmail authentication failed. "
                "Ensure GMAIL_APP_PASSWORD is a valid App Password."
            )
            return
        except Exception as exc:
            logger.error("Failed to send digest to %s: %s", addr, exc)
