"""
FIFA World Cup 2026 Ticket Price Bot

Polls six ticketing sites (Ticketmaster, SeatGeek, StubHub, Vivid Seats,
TickPick, Gametime) for World Cup games at MetLife Stadium in East Rutherford, NJ.
When a listing's price drops at or below a configured per-section threshold,
an SMS is sent to every contact in WORLDCUP_NOTIFY_PHONES via email-to-SMS
gateway.  Duplicate alerts within a 6-hour cooldown window are suppressed
(see state.py for the full deduplication logic).

Usage
-----
Continuous (Pi / local):     python main.py
Single poll (CI / testing):  python main.py --once

Environment variables  (see .env.example for full docs)
-------------------------------------------------------
GMAIL_USER                    Gmail address used to send SMS messages
GMAIL_APP_PASSWORD            16-char Gmail App Password
WORLDCUP_NOTIFY_PHONES        Comma-separated PHONE:CARRIER pairs
WORLDCUP_PRICE_FIELD_LEVEL    Alert threshold for field-level tickets  (USD)
WORLDCUP_PRICE_LOWER_BOWL     Alert threshold for lower-bowl tickets   (USD)
WORLDCUP_PRICE_UPPER_DECK     Alert threshold for upper-deck tickets   (USD)
WORLDCUP_POLL_INTERVAL_MINUTES How often to poll all sites             (min)
TICKETMASTER_API_KEY          Ticketmaster Discovery API key
SEATGEEK_CLIENT_ID            SeatGeek API client ID
SEATGEEK_CLIENT_SECRET        SeatGeek API client secret (optional)
DATABASE_URL                  PostgreSQL connection string
"""

import argparse
import logging
import os
import sys
import time

from dotenv import find_dotenv, load_dotenv

from notifier import send_alerts
from scraper import fetch_all
from state import record_alert, should_alert

load_dotenv(find_dotenv(usecwd=True))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


def _require(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        logger.error("Required environment variable %s is not set.", key)
        sys.exit(1)
    return val


def _optional(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip() or default


# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------

GMAIL_USER = _require("GMAIL_USER")
GMAIL_APP_PASSWORD = _require("GMAIL_APP_PASSWORD")
NOTIFY_PHONES = _require("WORLDCUP_NOTIFY_PHONES")

POLL_INTERVAL = int(_optional("WORLDCUP_POLL_INTERVAL_MINUTES", "30"))

# Per-section price thresholds — alert when a listing's price is AT OR BELOW.
# "any" is used when a source only returns event-level min prices (no section
# data).  We use the upper_deck threshold as the conservative baseline.
THRESHOLDS: dict[str, float] = {
    "field_level": float(_optional("WORLDCUP_PRICE_FIELD_LEVEL", "200")),
    "lower_bowl":  float(_optional("WORLDCUP_PRICE_LOWER_BOWL",  "200")),
    "upper_deck":  float(_optional("WORLDCUP_PRICE_UPPER_DECK",  "200")),
}
THRESHOLDS["any"] = THRESHOLDS["upper_deck"]


# ---------------------------------------------------------------------------
# Threshold check
# ---------------------------------------------------------------------------


def _below_threshold(listing: dict) -> bool:
    """Return True if the listing price is at or below its section threshold."""
    threshold = THRESHOLDS.get(listing.get("section", "any"), THRESHOLDS["any"])
    return listing["price"] <= threshold


# ---------------------------------------------------------------------------
# Poll
# ---------------------------------------------------------------------------


def _do_poll() -> None:
    """Fetch all sites, filter by threshold, deduplicate, and fire alerts."""
    logger.info("Polling all sites for World Cup tickets at MetLife…")

    try:
        all_listings = fetch_all()
    except Exception as exc:
        logger.error("fetch_all raised unexpectedly: %s", exc)
        return

    logger.info("Fetched %d total listing(s) across all sources.", len(all_listings))

    # Filter by price threshold
    candidates = [l for l in all_listings if _below_threshold(l)]
    logger.info(
        "%d listing(s) are at or below threshold (field≤$%.0f / lower≤$%.0f / upper≤$%.0f).",
        len(candidates),
        THRESHOLDS["field_level"],
        THRESHOLDS["lower_bowl"],
        THRESHOLDS["upper_deck"],
    )

    if not candidates:
        return

    # Deduplicate via DB cooldown logic
    to_alert = [l for l in candidates if should_alert(l)]
    if not to_alert:
        logger.info("All candidates suppressed by cooldown — no SMS sent.")
        return

    logger.info("%d listing(s) cleared deduplication — sending SMS.", len(to_alert))

    send_alerts(
        gmail_user=GMAIL_USER,
        gmail_app_password=GMAIL_APP_PASSWORD,
        notify_phones_raw=NOTIFY_PHONES,
        listings=to_alert,
    )

    # Record each sent alert so future polls can apply the cooldown
    for listing in to_alert:
        record_alert(listing)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="FIFA World Cup 2026 Ticket Bot")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single poll and exit (useful for testing or CI).",
    )
    args = parser.parse_args()

    if args.once:
        logger.info("Running in single-poll mode (--once).")
        _do_poll()
        return

    logger.info(
        "Starting continuous polling every %d minute(s). Press Ctrl+C to stop.",
        POLL_INTERVAL,
    )
    try:
        while True:
            _do_poll()
            logger.info("Sleeping %d minute(s) until next poll…", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL * 60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")


if __name__ == "__main__":
    main()
