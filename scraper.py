"""
Multi-site ticket scraper for FIFA World Cup 2026 at MetLife Stadium.

Five adapters are implemented, each returning a list of normalized listing dicts:

  {
      "game":    "FIFA World Cup 2026 - Match 23",
      "date":    "2026-06-15",          # ISO date string
      "section": "lower_bowl",          # field_level | lower_bowl | upper_deck | any
      "price":   142.00,                # lowest available price from this source
      "url":     "https://...",
      "source":  "stubhub",
  }

Adapter strategy
----------------
  Ticketmaster  — official Discovery API (JSON, no scraping)
  SeatGeek      — official public API   (JSON, no scraping)
  StubHub       — __NEXT_DATA__ JSON embedded in Next.js HTML page
  Vivid Seats   — __NEXT_DATA__ JSON embedded in Next.js HTML page
  Gametime      — __NEXT_DATA__ JSON embedded in Next.js HTML page

All three HTTP-scraped adapters fail gracefully: if the page structure has
changed or the site blocks the request, the adapter logs a warning and returns
an empty list.  The other adapters continue unaffected.

A 7-second inter-request delay is inserted between the three scraped sites to
avoid triggering rate-limit detection on the bot's residential IP.
"""

import json
import logging
import os
import re
import time

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VENUE_NAME = "MetLife Stadium"
_VENUE_KEYWORDS = {"metlife", "east rutherford"}
_WC_KEYWORDS = {"fifa", "world cup"}

_INTER_SITE_DELAY_SECS = 7

_MAX_RETRIES = 3

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_API_HEADERS = {
    **_BROWSER_HEADERS,
    "Accept": "application/json",
}

# Section-tier keyword sets (checked in priority order: field → lower → upper)
_FIELD_KEYWORDS = {"field", "floor", "pitch", "sideline", "field level", "ha "}
_LOWER_KEYWORDS = {"lower", "club", "loge", "mezzanine", "mezz", "100", "200"}
_UPPER_KEYWORDS = {"upper", "300", "400", "500", "nosebleed", "ga", "general"}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _is_world_cup_at_metlife(event_name: str, venue_name: str) -> bool:
    name_lower = event_name.lower()
    venue_lower = venue_name.lower()
    is_wc = any(kw in name_lower for kw in _WC_KEYWORDS)
    is_metlife = any(kw in venue_lower for kw in _VENUE_KEYWORDS)
    return is_wc and is_metlife


def normalize_section(section_name: str) -> str:
    """
    Map a raw section/zone name to one of:
      field_level | lower_bowl | upper_deck

    Falls back to "upper_deck" when nothing matches (the most conservative
    assumption — do not over-promise on section quality).
    """
    name = section_name.lower()
    for kw in _FIELD_KEYWORDS:
        if kw in name:
            return "field_level"
    for kw in _LOWER_KEYWORDS:
        if kw in name:
            return "lower_bowl"
    return "upper_deck"


def _fetch_html(url: str) -> str:
    """GET a URL with browser headers and exponential-backoff retries."""
    backoff = 5
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=20)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            if attempt == _MAX_RETRIES:
                raise
            logger.warning(
                "HTTP request failed (attempt %d/%d): %s — retrying in %ds…",
                attempt,
                _MAX_RETRIES,
                exc,
                backoff,
            )
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError("Unreachable")


def _fetch_json(url: str, params: dict | None = None) -> dict:
    """GET a JSON API endpoint with retries."""
    backoff = 5
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_API_HEADERS, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == _MAX_RETRIES:
                raise
            logger.warning(
                "API request failed (attempt %d/%d): %s — retrying in %ds…",
                attempt,
                _MAX_RETRIES,
                exc,
                backoff,
            )
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError("Unreachable")


def _extract_next_data(html: str) -> dict:
    """
    Pull the __NEXT_DATA__ JSON blob from a Next.js page.
    Returns an empty dict if the tag is absent or unparseable.
    """
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        try:
            return json.loads(tag.string)
        except json.JSONDecodeError as exc:
            logger.debug("__NEXT_DATA__ JSON parse error: %s", exc)
    return {}


def _extract_json_ld(html: str) -> list[dict]:
    """Extract all application/ld+json blocks from a page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                results.append(data)
        except json.JSONDecodeError:
            pass
    return results


# ---------------------------------------------------------------------------
# Ticketmaster adapter  (official Discovery API)
# ---------------------------------------------------------------------------


def fetch_ticketmaster() -> list[dict]:
    """
    Query the Ticketmaster Discovery API for FIFA World Cup events in NJ.
    Requires TICKETMASTER_API_KEY to be set.

    API docs: https://developer.ticketmaster.com/products-and-docs/apis/discovery-api/v2/
    """
    api_key = os.getenv("TICKETMASTER_API_KEY", "").strip()
    if not api_key:
        logger.warning("TICKETMASTER_API_KEY not set — skipping Ticketmaster.")
        return []

    try:
        data = _fetch_json(
            "https://app.ticketmaster.com/discovery/v2/events.json",
            params={
                "apikey": api_key,
                "keyword": "FIFA World Cup",
                "stateCode": "NJ",
                "classificationName": "sports",
                "size": 50,
            },
        )
    except Exception as exc:
        logger.error("Ticketmaster API error: %s", exc)
        return []

    events = data.get("_embedded", {}).get("events", [])
    listings: list[dict] = []

    for event in events:
        name = event.get("name", "")
        venues = event.get("_embedded", {}).get("venues", [{}])
        venue_name = venues[0].get("name", "") if venues else ""

        if not _is_world_cup_at_metlife(name, venue_name):
            continue

        date = event.get("dates", {}).get("start", {}).get("localDate", "")
        price_ranges = event.get("priceRanges", [])
        if not price_ranges:
            logger.debug("Ticketmaster event %r has no priceRanges — skipping.", name)
            continue

        min_price = min(pr.get("min", 9999) for pr in price_ranges)
        url = event.get("url", "")

        # TM Discovery API returns event-level ranges, not per-section data.
        # We report section as "any"; main.py compares against the upper_deck
        # threshold as the conservative baseline.
        listings.append(
            {
                "game": name,
                "date": date,
                "section": "any",
                "price": float(min_price),
                "url": url,
                "source": "ticketmaster",
            }
        )

    logger.info("Ticketmaster: %d World Cup listing(s) at MetLife.", len(listings))
    return listings


# ---------------------------------------------------------------------------
# SeatGeek adapter  (official public API)
# ---------------------------------------------------------------------------


def fetch_seatgeek() -> list[dict]:
    """
    Query the SeatGeek public API for FIFA World Cup events at MetLife.
    Requires SEATGEEK_CLIENT_ID (and optionally SEATGEEK_CLIENT_SECRET).

    API docs: https://platform.seatgeek.com/
    """
    client_id = os.getenv("SEATGEEK_CLIENT_ID", "").strip()
    if not client_id:
        logger.warning("SEATGEEK_CLIENT_ID not set — skipping SeatGeek.")
        return []

    params: dict = {
        "q": "FIFA World Cup 2026",
        "venue.state": "NJ",
        "per_page": 25,
        "client_id": client_id,
    }
    client_secret = os.getenv("SEATGEEK_CLIENT_SECRET", "").strip()
    if client_secret:
        params["client_secret"] = client_secret

    try:
        data = _fetch_json("https://api.seatgeek.com/2/events", params=params)
    except Exception as exc:
        logger.error("SeatGeek API error: %s", exc)
        return []

    listings: list[dict] = []

    for event in data.get("events", []):
        title = event.get("title", "")
        venue_name = event.get("venue", {}).get("name", "")

        if not _is_world_cup_at_metlife(title, venue_name):
            continue

        min_price = event.get("stats", {}).get("lowest_price")
        if not min_price:
            continue

        date_str = (event.get("datetime_local") or "")[:10]
        url = event.get("url", "")

        listings.append(
            {
                "game": title,
                "date": date_str,
                "section": "any",
                "price": float(min_price),
                "url": url,
                "source": "seatgeek",
            }
        )

    logger.info("SeatGeek: %d World Cup listing(s) at MetLife.", len(listings))
    return listings


# ---------------------------------------------------------------------------
# StubHub adapter  (__NEXT_DATA__ scrape)
# ---------------------------------------------------------------------------


def fetch_stubhub() -> list[dict]:
    """
    Scrape StubHub's search results page for World Cup tickets.

    StubHub is a Next.js app that embeds its search payload in a
    <script id="__NEXT_DATA__"> tag. We extract that JSON and navigate
    to the event card list.  If the page structure changes, this adapter
    logs a warning and returns [] without crashing the poll.
    """
    url = "https://www.stubhub.com/find/s/?q=FIFA+World+Cup+2026+MetLife+Stadium"
    try:
        html = _fetch_html(url)
    except Exception as exc:
        logger.error("StubHub fetch error: %s", exc)
        return []

    listings: list[dict] = []
    try:
        data = _extract_next_data(html)
        if not data:
            logger.warning("StubHub: __NEXT_DATA__ not found — site structure may have changed.")
            return []

        props = data.get("props", {}).get("pageProps", {})

        # StubHub's pageProps key for search results has varied — try several paths.
        events_raw = (
            props.get("events")
            or props.get("searchResults", {}).get("events")
            or props.get("initialData", {}).get("events")
            or []
        )

        for ev in events_raw:
            name = ev.get("name") or ev.get("description", "")
            if not any(kw in name.lower() for kw in _WC_KEYWORDS):
                continue

            venue_name = (ev.get("venue") or {}).get("name", "")
            if not any(kw in venue_name.lower() for kw in _VENUE_KEYWORDS):
                continue

            raw_price = ev.get("minTicketPrice") or ev.get("minPrice") or ev.get("ticketInfo", {}).get("minPrice")
            if not raw_price:
                continue

            date = (ev.get("eventDateLocal") or ev.get("startDate", ""))[:10]
            event_url = ev.get("eventUrl") or ev.get("url", "")
            if event_url and not event_url.startswith("http"):
                event_url = "https://www.stubhub.com" + event_url

            listings.append(
                {
                    "game": name,
                    "date": date,
                    "section": "any",
                    "price": float(raw_price),
                    "url": event_url,
                    "source": "stubhub",
                }
            )
    except Exception as exc:
        logger.warning("StubHub data parsing failed: %s", exc)

    logger.info("StubHub: %d World Cup listing(s) at MetLife.", len(listings))
    return listings


# ---------------------------------------------------------------------------
# Vivid Seats adapter  (__NEXT_DATA__ scrape)
# ---------------------------------------------------------------------------


def fetch_vividseats() -> list[dict]:
    """
    Scrape Vivid Seats for World Cup tickets.

    Like StubHub, Vivid Seats uses Next.js with a __NEXT_DATA__ payload.
    Falls back to JSON-LD structured data if __NEXT_DATA__ doesn't yield results.
    """
    url = "https://www.vividseats.com/search?searchTerm=FIFA+World+Cup+2026"
    try:
        html = _fetch_html(url)
    except Exception as exc:
        logger.error("Vivid Seats fetch error: %s", exc)
        return []

    listings: list[dict] = []
    try:
        data = _extract_next_data(html)
        props = data.get("props", {}).get("pageProps", {})

        productions = (
            props.get("productions")
            or props.get("searchResults")
            or props.get("initialProps", {}).get("productions")
            or []
        )

        for prod in productions:
            name = prod.get("name", "")
            if not any(kw in name.lower() for kw in _WC_KEYWORDS):
                continue

            venue_name = (prod.get("venue") or {}).get("name", "")
            if not any(kw in venue_name.lower() for kw in _VENUE_KEYWORDS):
                continue

            min_price = prod.get("minPrice") or prod.get("minTicketPrice")
            if not min_price:
                continue

            date = (prod.get("localDate") or prod.get("startDate", ""))[:10]
            slug = prod.get("url") or prod.get("webPath", "")
            event_url = (
                f"https://www.vividseats.com{slug}"
                if slug and not slug.startswith("http")
                else slug or "https://www.vividseats.com"
            )

            listings.append(
                {
                    "game": name,
                    "date": date,
                    "section": "any",
                    "price": float(min_price),
                    "url": event_url,
                    "source": "vividseats",
                }
            )
    except Exception as exc:
        logger.warning("Vivid Seats data parsing failed: %s", exc)

    logger.info("Vivid Seats: %d World Cup listing(s) at MetLife.", len(listings))
    return listings


# ---------------------------------------------------------------------------
# Gametime adapter  (__NEXT_DATA__ scrape)
# ---------------------------------------------------------------------------


def fetch_gametime() -> list[dict]:
    """
    Scrape Gametime for World Cup tickets.

    Gametime renders its search page via Next.js; we attempt __NEXT_DATA__
    extraction and fall back gracefully on failure.
    """
    url = "https://gametime.co/search?q=FIFA+World+Cup+2026"
    try:
        html = _fetch_html(url)
    except Exception as exc:
        logger.error("Gametime fetch error: %s", exc)
        return []

    listings: list[dict] = []
    try:
        data = _extract_next_data(html)
        props = data.get("props", {}).get("pageProps", {})

        events = (
            props.get("events")
            or props.get("results")
            or props.get("initialData", {}).get("events")
            or []
        )

        for ev in events:
            name = ev.get("name", "")
            if not any(kw in name.lower() for kw in _WC_KEYWORDS):
                continue

            venue_name = (ev.get("venue") or {}).get("name", "")
            if not any(kw in venue_name.lower() for kw in _VENUE_KEYWORDS):
                continue

            min_price = ev.get("min_price") or ev.get("minPrice")
            if not min_price:
                continue

            date = (ev.get("starts_at") or ev.get("date", ""))[:10]
            event_id = ev.get("id", "")
            event_url = (
                f"https://gametime.co/events/{event_id}"
                if event_id
                else "https://gametime.co"
            )

            listings.append(
                {
                    "game": name,
                    "date": date,
                    "section": "any",
                    "price": float(min_price),
                    "url": event_url,
                    "source": "gametime",
                }
            )
    except Exception as exc:
        logger.warning("Gametime data parsing failed: %s", exc)

    logger.info("Gametime: %d World Cup listing(s) at MetLife.", len(listings))
    return listings


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# Scraped adapters only — no API keys required.
# A delay is inserted between each to avoid rate-limit detection.
_ADAPTERS: list[tuple[str, callable]] = [
    ("StubHub",     fetch_stubhub),
    ("Vivid Seats", fetch_vividseats),
    ("Gametime",    fetch_gametime),
]


def fetch_all() -> list[dict]:
    """
    Poll all scraped sites and return a combined list of normalized listings.

    Each adapter is isolated — an exception or empty result from one does not
    affect the others.  A delay is inserted between adapters to avoid
    triggering rate-limit detection.
    """
    combined: list[dict] = []

    for i, (name, fn) in enumerate(_ADAPTERS):
        if i > 0:
            logger.debug("Waiting %ds before hitting %s…", _INTER_SITE_DELAY_SECS, name)
            time.sleep(_INTER_SITE_DELAY_SECS)
        try:
            results = fn()
            combined.extend(results)
        except Exception as exc:
            logger.error("Adapter %s raised unexpectedly: %s", name, exc)

    logger.info("fetch_all: %d total listing(s) across all sources.", len(combined))
    return combined
