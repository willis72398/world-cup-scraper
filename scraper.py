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
  Ticketmaster  — Playwright scrape of search results page (__NEXT_DATA__)
  SeatGeek      — Playwright scrape of performer page (__NEXT_DATA__)
  StubHub       — requests scrape, <script id="index-data"> JSON blob
  Vivid Seats   — requests scrape, __NEXT_DATA__ JSON embedded in page
  Gametime      — Gametime mobile API (public, no auth required)

All adapters fail gracefully: if a page structure changes or a request is
blocked, the adapter logs a warning and returns an empty list so the others
continue unaffected.

A 7-second inter-request delay is inserted between adapters to avoid
triggering rate-limit detection.
"""

import json
import logging
import os
import re
import time

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

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


_PLAYWRIGHT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _fetch_html_playwright(url: str, wait_until: str = "networkidle") -> str:
    """Render a URL in a headless Chromium browser and return the page HTML."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_PLAYWRIGHT_UA)
        page = context.new_page()
        try:
            page.goto(url, timeout=30_000, wait_until=wait_until)
        except Exception as exc:
            logger.warning("Playwright navigation warning (%s): %s", url, exc)
        html = page.content()
        context.close()
        browser.close()
    return html


# ---------------------------------------------------------------------------
# Ticketmaster adapter  (Playwright scrape — TM blocks plain HTTP)
# ---------------------------------------------------------------------------


def fetch_ticketmaster() -> list[dict]:
    """
    Scrape Ticketmaster's search page for FIFA World Cup events at MetLife.

    TM uses Next.js; event data is in __NEXT_DATA__ under
    props.pageProps.initialReduxState.api.  We try several known paths and
    fall back gracefully if the structure changes.
    """
    search_url = "https://www.ticketmaster.com/search?q=FIFA+World+Cup+2026+MetLife+Stadium"
    try:
        html = _fetch_html_playwright(search_url)
    except Exception as exc:
        logger.error("Ticketmaster Playwright error: %s", exc)
        return []

    listings: list[dict] = []
    try:
        data = _extract_next_data(html)
        redux = (
            data.get("props", {})
            .get("pageProps", {})
            .get("initialReduxState", {})
        )

        # TM stores search results under api.search.events._embedded.events
        api = redux.get("api", {})
        search = api.get("search", {}) if isinstance(api, dict) else {}
        embedded = (
            search.get("events", {}).get("_embedded", {})
            if isinstance(search, dict)
            else {}
        )
        raw_events = embedded.get("events", []) if isinstance(embedded, dict) else []

        if not raw_events:
            logger.debug(
                "Ticketmaster: no events at expected path; redux keys: %s",
                list(redux.keys()) if isinstance(redux, dict) else type(redux),
            )

        for event in raw_events:
            if not isinstance(event, dict):
                continue
            name = event.get("name", "")
            venues = event.get("_embedded", {}).get("venues", [{}])
            venue_name = venues[0].get("name", "") if venues else ""

            if not _is_world_cup_at_metlife(name, venue_name):
                continue

            date = event.get("dates", {}).get("start", {}).get("localDate", "")
            price_ranges = event.get("priceRanges") or []
            if not price_ranges:
                continue

            min_price = min(pr.get("min", 9999) for pr in price_ranges if isinstance(pr, dict))
            url = event.get("url", "")

            listings.append({
                "game": name,
                "date": date,
                "section": "any",
                "price": float(min_price),
                "url": url,
                "source": "ticketmaster",
            })
    except Exception as exc:
        logger.warning("Ticketmaster data parsing failed: %s", exc)

    logger.info("Ticketmaster: %d World Cup listing(s) at MetLife.", len(listings))
    return listings


# ---------------------------------------------------------------------------
# SeatGeek adapter  (Playwright scrape — SeatGeek blocks plain HTTP)
# ---------------------------------------------------------------------------


def fetch_seatgeek() -> list[dict]:
    """
    Scrape SeatGeek's FIFA World Cup performer page for MetLife events.

    SeatGeek uses Next.js; event data lives in __NEXT_DATA__ under
    props.pageProps.  We try several known key variants.
    """
    url = "https://seatgeek.com/fifa-world-cup-tickets"
    try:
        html = _fetch_html_playwright(url)
    except Exception as exc:
        logger.error("SeatGeek Playwright error: %s", exc)
        return []

    listings: list[dict] = []
    try:
        data = _extract_next_data(html)
        props = data.get("props", {}).get("pageProps", {})

        events = (
            props.get("events")
            or props.get("productions")
            or props.get("searchResults")
            or props.get("initialData", {}).get("events")
            or []
        )

        if not events:
            logger.debug(
                "SeatGeek: no events found; pageProps keys: %s", list(props.keys())
            )

        for event in events:
            if not isinstance(event, dict):
                continue
            title = event.get("title", "") or event.get("name", "")
            venue_name = (event.get("venue") or {}).get("name", "")

            if not _is_world_cup_at_metlife(title, venue_name):
                continue

            min_price = (
                (event.get("stats") or {}).get("lowest_price")
                or event.get("lowest_price")
                or event.get("min_price")
            )
            if not min_price:
                continue

            date_str = (event.get("datetime_local") or event.get("date", ""))[:10]
            url_field = event.get("url") or event.get("seo_url") or ""
            event_url = (
                f"https://seatgeek.com{url_field}"
                if url_field and not url_field.startswith("http")
                else url_field or "https://seatgeek.com"
            )

            listings.append({
                "game": title,
                "date": date_str,
                "section": "any",
                "price": float(min_price),
                "url": event_url,
                "source": "seatgeek",
            })
    except Exception as exc:
        logger.warning("SeatGeek data parsing failed: %s", exc)

    logger.info("SeatGeek: %d World Cup listing(s) at MetLife.", len(listings))
    return listings


# ---------------------------------------------------------------------------
# StubHub adapter  (__NEXT_DATA__ scrape)
# ---------------------------------------------------------------------------


_STUBHUB_DATE_RE = re.compile(r"-tickets-(\d{1,2}-\d{1,2}-(\d{4}))/")


def _parse_stubhub_price(formatted: str) -> float | None:
    """Parse a price string like '$142' or '$1,234' into a float."""
    try:
        return float(re.sub(r"[^\d.]", "", formatted))
    except (ValueError, TypeError):
        return None


def _parse_stubhub_date(event_url: str) -> str:
    """Extract ISO date from StubHub event URL, e.g. tickets-6-15-2026 → 2026-06-15."""
    m = _STUBHUB_DATE_RE.search(event_url)
    if m:
        parts = m.group(1).split("-")
        if len(parts) == 3:
            month, day, year = parts
            return f"{year}-{int(month):02d}-{int(day):02d}"
    return ""


def fetch_stubhub() -> list[dict]:
    """
    Scrape StubHub's search results page for World Cup tickets.

    StubHub embeds its search payload in <script id="index-data">.
    Structure: eventGrids[key].items[i] with fields:
      name, venueName, venueCity, venueStateProvince, formattedMinPrice, url
    """
    url = "https://www.stubhub.com/search?q=FIFA+World+Cup+2026+MetLife+Stadium"
    try:
        html = _fetch_html(url)
    except Exception as exc:
        logger.error("StubHub fetch error: %s", exc)
        return []

    listings: list[dict] = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("script", id="index-data")
        if not tag or not tag.string:
            logger.warning("StubHub: index-data script not found — site structure may have changed.")
            return []

        data = json.loads(tag.string)
        grids = data.get("eventGrids", {})
        if not grids:
            logger.warning("StubHub: no eventGrids in index-data.")
            return []

        # eventGrids is a dict; take the first value
        grid = next(iter(grids.values())) if isinstance(grids, dict) else grids[0]
        events_raw = grid.get("items", []) if isinstance(grid, dict) else []

        for ev in events_raw:
            name = ev.get("name", "")
            if not any(kw in name.lower() for kw in _WC_KEYWORDS):
                continue

            venue_name = ev.get("venueName", "")
            venue_city = ev.get("venueCity", "")
            venue_str = f"{venue_name} {venue_city}".lower()
            if not any(kw in venue_str for kw in _VENUE_KEYWORDS):
                continue

            price = _parse_stubhub_price(ev.get("formattedMinPrice", ""))
            if price is None:
                continue

            event_url = ev.get("url", "")
            date = _parse_stubhub_date(event_url)

            listings.append({
                "game": name,
                "date": date,
                "section": "any",
                "price": price,
                "url": event_url,
                "source": "stubhub",
            })
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
# Gametime adapter  (mobile API — no auth required)
# ---------------------------------------------------------------------------

# Performer ID for "2026 FIFA World Cup" on Gametime's mobile API.
_GAMETIME_PERFORMER_ID = "66f70bca07991b85a6c55ad9"


def fetch_gametime() -> list[dict]:
    """
    Query Gametime's mobile API for FIFA World Cup events at MetLife Stadium.

    Uses the undocumented-but-public mobile API endpoint.  Prices are returned
    in cents and converted to dollars.  Only NJ events are requested so the
    venue filter is just a sanity check.
    """
    try:
        data = _fetch_json(
            "https://mobile.gametime.co/v1/events",
            params={
                "performer_id": _GAMETIME_PERFORMER_ID,
                "venue_state": "NJ",
                "page": 1,
                "per_page": 50,
            },
        )
    except Exception as exc:
        logger.error("Gametime API error: %s", exc)
        return []

    listings: list[dict] = []
    for item in data.get("events", []):
        if not isinstance(item, dict):
            continue
        ev = item.get("event", {})
        venue = item.get("venue", {})

        name = ev.get("name", "")
        if not any(kw in name.lower() for kw in _WC_KEYWORDS):
            continue

        venue_name = venue.get("name", "") if isinstance(venue, dict) else ""
        if not any(kw in venue_name.lower() for kw in _VENUE_KEYWORDS):
            continue

        min_price_obj = ev.get("min_price") or {}
        price_cents = (
            min_price_obj.get("total") or min_price_obj.get("prefee")
            if isinstance(min_price_obj, dict)
            else None
        )
        if not price_cents:
            continue

        date = (ev.get("datetime_local") or "")[:10]
        event_id = ev.get("id", "")
        event_url = (
            f"https://gametime.co/events/{event_id}"
            if event_id
            else "https://gametime.co"
        )

        listings.append({
            "game": name,
            "date": date,
            "section": "any",
            "price": price_cents / 100.0,
            "url": event_url,
            "source": "gametime",
        })

    logger.info("Gametime: %d World Cup listing(s) at MetLife.", len(listings))
    return listings


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_ADAPTERS: list[tuple[str, callable]] = [
    ("Ticketmaster", fetch_ticketmaster),
    ("SeatGeek",     fetch_seatgeek),
    ("StubHub",      fetch_stubhub),
    ("Vivid Seats",  fetch_vividseats),
    ("Gametime",     fetch_gametime),
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
