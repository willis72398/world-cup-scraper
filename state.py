"""
PostgreSQL-backed state for the World Cup 2026 ticket bot.

Each row in worldcup_alerts represents one SMS alert that was sent.
Before firing a new alert for a (game, section, source) triple, we check:

  1. Cooldown  — was an alert sent for this combo within the last
                 COOLDOWN_HOURS (default 6)?  If yes, suppress unless…
  2. Reprice   — the new price is at least REPRICE_PCT (default 10%) lower
                 than the price we last alerted on.  A meaningful further drop
                 always breaks through the cooldown.

Connection is configured via the DATABASE_URL environment variable:
  postgresql://worldcup:<password>@postgres:5432/worldcup

Public interface:
  should_alert(listing)  → bool
  record_alert(listing)  → None
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

COOLDOWN_HOURS = 6
REPRICE_PCT = 0.10  # suppress re-alert unless price fell >= 10% from last alert


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def should_alert(listing: dict) -> bool:
    """
    Return True if this listing warrants a new SMS alert.

    Suppression rules:
      - An alert for the same (game, section, source) was already sent within
        COOLDOWN_HOURS, AND the price has not dropped by REPRICE_PCT or more.

    On any DB error the function defaults to True so we don't silently miss
    a real price drop.
    """
    game = listing["game"]
    section = listing["section"]
    source = listing["source"]
    price = float(listing["price"])
    cutoff = datetime.now(timezone.utc) - timedelta(hours=COOLDOWN_HOURS)

    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT price
                FROM   worldcup_alerts
                WHERE  game    = %s
                  AND  section = %s
                  AND  source  = %s
                  AND  alerted_at >= %s
                ORDER  BY alerted_at DESC
                LIMIT  1
                """,
                (game, section, source, cutoff),
            )
            row = cur.fetchone()
    except Exception as exc:
        logger.warning("DB read failed (%s) — defaulting to allow alert.", exc)
        return True

    if row is None:
        return True

    last_price = float(row[0])
    if last_price <= 0:
        return True

    drop_fraction = (last_price - price) / last_price
    return drop_fraction >= REPRICE_PCT


def record_alert(listing: dict) -> None:
    """
    Persist a sent alert to the worldcup_alerts table.

    ON CONFLICT is not used here because each alert is a new row (we want
    the full history). Errors are logged but do not raise so the poll loop
    can continue.
    """
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO worldcup_alerts
                    (game, game_date, section, source, price, url)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    listing["game"],
                    listing["date"],
                    listing["section"],
                    listing["source"],
                    listing["price"],
                    listing["url"],
                ),
            )
        logger.debug(
            "Recorded alert: %s / %s / %s @ $%.2f",
            listing["source"],
            listing["game"],
            listing["section"],
            listing["price"],
        )
    except Exception as exc:
        logger.error("Failed to record alert to DB: %s", exc)
