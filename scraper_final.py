"""
Booking.com Vietnam Scraper — Production Edition 
=====================================================
Changelog from v4:
  - ARCHITECTURE: deep_scrape is now the DEFAULT and primary data path.
    The search card is used only to collect hotel URLs; all rich data
    (address, description, rooms, facilities, review sub-scores, area
    info, taxes, check-in/out times, images) is extracted from the
    individual hotel detail page.
  - NEW: deep_scrape_hotel() completely rewritten — now extracts:
      location.address, location.neighborhood, location.area_info,
      location.latitude, location.longitude
      detail.description, detail.checkin_time, detail.checkout_time,
      detail.languages, detail.taxes_fees, detail.meal_plan
      property.type, property.star_rating
      facilities (full list, grouped by category)
      popular_facilities (top highlighted)
      room_types (name, beds, size, price, inclusions)
      rating.score, rating.label, rating.review_count
      rating.review_categories (staff/facilities/cleanliness/comfort/
                                 value/location/wifi — each scored 1–10)
      images (full-resolution gallery URLs, up to 20)
  - NEW: VIETNAM_TOURISM_CITIES — expanded to 25 cities, randomly
    sampled per run
  - NEW: _parse_review_categories() — maps sub-score text blocks to
    structured dict keyed by category name
  - NEW: _dp_* extractor family — isolated deep-page extractors
    (same isolation pattern as card extractors)
  - CHANGED: default deep_scrape=True in scrape_vietnam()
  - CHANGED: max_pages_per_city default lowered to 1 (more cities,
    fewer pages each, balanced against deep-scrape latency)

Requirements:
    pip install playwright playwright-stealth
    playwright install chromium
"""

import argparse
import asyncio
import csv
import json
import logging
import random
import re
import os
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, urlparse, urlunparse

from playwright.async_api import (
    async_playwright,
    Page,
    Browser,
    BrowserContext,
    Locator,
    TimeoutError as PWTimeout,
)

try:
    from playwright_stealth import stealth_async
    HAS_STEALTH = True
except ImportError:
    HAS_STEALTH = False
    print("Warning: playwright-stealth not installed. Run: pip install playwright-stealth")

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
BASE_SEARCH_URL = "https://www.booking.com/searchresults.html"
BASE_ORIGIN     = "https://www.booking.com"

VIETNAM_CITIES = [
    # Major hubs
    "Ho Chi Minh City", "Hanoi", "Da Nang",
    # Central coast & heritage
    "Hoi An", "Hue", "Quy Nhon", "Quang Ngai", "Dong Ha",
    # South Central coast
    "Nha Trang", "Phan Rang", "Phan Thiet", "Mui Ne", "Tuy Hoa",
    # Southern beach & islands
    "Vung Tau", "Phu Quoc", "Con Dao",
    # Northern highlights
    "Ha Long", "Cat Ba", "Hai Phong", "Ninh Binh", "Sa Pa", "Ha Giang",
    # Central Highlands
    "Da Lat", "Buon Ma Thuot", "Pleiku",
    # Mekong Delta
    "Can Tho", "My Tho", "Ben Tre", "Chau Doc",
    # Gateway cities
    "Dong Hoi", "Dien Bien Phu",
]
# De-duplicate while preserving order
_seen: set = set()
VIETNAM_CITIES = [c for c in VIETNAM_CITIES if not (c in _seen or _seen.add(c))]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

# Populate with "http://user:pass@host:port" strings to enable proxy rotation
PROXIES: list[str] = []

# ─────────────────────────────────────────────────────────────────────────────
# CSV COLUMNS  (neighborhood / meal_plan / area_info excluded per requirements)
# ─────────────────────────────────────────────────────────────────────────────
CSV_COLUMNS = [
    "scrape_date", "scraped_at", "hotel_id", "name", "url",
    "city", "country", "address", "distance_from_center", "latitude", "longitude",
    "price_per_night_vnd", "original_price_vnd", "discount_pct",
    "currency", "taxes_included", "taxes_fees_text",
    "rating_score", "rating_label", "review_count", "review_categories",
    "property_type", "star_rating", "availability_status", "rooms_left", "room_types",
    "checkin_time", "checkout_time", "languages",
    "description",
    "facilities_popular", "facilities_all",
    "amenities", "badges", "images",
]


# ─────────────────────────────────────────────────────────────────────────────
# CSV DATA STORE — single append-only file, Windows-safe
# ─────────────────────────────────────────────────────────────────────────────

def _safe_replace(src: Path, dst: Path) -> None:
    """Atomic rename. If dst is locked (Excel open on Windows) save a backup."""
    try:
        src.replace(dst)
    except PermissionError:
        backup = dst.with_name(
            f"{dst.stem}_locked_{datetime.now().strftime('%H%M%S')}{dst.suffix}"
        )
        try:
            src.rename(backup)
        except Exception:
            pass
        logger.warning(
            f"⚠  {dst.name} is open in another program (Excel?).\n"
            f"   Data saved to: {backup.name}\n"
            f"   Close the file, then run: python scraper_final.py --rebuild-csv"
        )


def _is_richer(new_val, old_val) -> bool:
    nv = str(new_val).strip() if new_val is not None else ""
    ov = str(old_val).strip() if old_val is not None else ""
    if nv in ("", "None", "null", "[]", "{}"): return False
    if ov in ("", "None", "null", "[]", "{}"): return True
    if len(ov) < 20: return True
    return len(nv) > len(ov)


def _flatten(hotel: dict, scrape_date: str) -> dict:
    loc  = hotel.get("location", {})
    pr   = hotel.get("pricing",  {})
    rat  = hotel.get("rating",   {})
    prop = hotel.get("property", {})
    det  = hotel.get("detail",   {})
    fac  = hotel.get("facilities", {})
    js   = lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else (v if v is not None else "")
    return {
        "scrape_date":          scrape_date,
        "scraped_at":           hotel.get("scraped_at", ""),
        "hotel_id":             hotel.get("hotel_id", ""),
        "name":                 hotel.get("name", ""),
        "url":                  hotel.get("url", ""),
        "city":                 loc.get("city", ""),
        "country":              loc.get("country", "Vietnam"),
        "address":              loc.get("address", ""),
        "distance_from_center": loc.get("distance_from_center", ""),
        "latitude":             loc.get("latitude", ""),
        "longitude":            loc.get("longitude", ""),
        "price_per_night_vnd":  pr.get("price_per_night_vnd", ""),
        "original_price_vnd":   pr.get("original_price_vnd", ""),
        "discount_pct":         pr.get("discount_pct", ""),
        "currency":             pr.get("currency", "VND"),
        "taxes_included":       pr.get("taxes_included", ""),
        "taxes_fees_text":      pr.get("taxes_fees_text", ""),
        "rating_score":         rat.get("score", ""),
        "rating_label":         rat.get("label", ""),
        "review_count":         rat.get("review_count", ""),
        "review_categories":    js(rat.get("review_categories", {})),
        "property_type":        prop.get("type", ""),
        "star_rating":          prop.get("star_rating", ""),
        "availability_status":  prop.get("availability_status", ""),
        "rooms_left":           prop.get("rooms_left", ""),
        "room_types":           js(prop.get("room_types", [])),
        "checkin_time":         det.get("checkin_time", ""),
        "checkout_time":        det.get("checkout_time", ""),
        "languages":            js(det.get("languages", [])),
        "description":          det.get("description", ""),
        "facilities_popular":   js(fac.get("popular", [])),
        "facilities_all":       js(fac.get("all", {})),
        "amenities":            js(hotel.get("amenities", [])),
        "badges":               js(hotel.get("badges", [])),
        "images":               js(hotel.get("images", [])),
    }


class DataStore:
    """
    Single append-only CSV + JSONL pair.
    Key = (hotel_id, scrape_date)
      Same hotel, same date  → smart merge (never overwrite good data with null)
      Same hotel, new date   → new row (tracks price/availability over time)
    """

    def __init__(self, data_dir: str = "."):
        self.dir        = Path(data_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.dir / "hotels_vietnam_all.jsonl"
        self.csv_path   = self.dir / "hotels_vietnam_all.csv"

    def _load(self) -> dict[tuple, dict]:
        records: dict[tuple, dict] = {}
        if not self.jsonl_path.exists():
            return records
        with open(self.jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    key = (obj.get("hotel_id", ""), obj.get("scrape_date", ""))
                    records[key] = obj
                except Exception:
                    continue
        return records

    def _write(self, records: dict[tuple, dict]) -> None:
        tmp_j = self.jsonl_path.with_suffix(".tmp")
        with open(tmp_j, "w", encoding="utf-8") as f:
            for obj in records.values():
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        _safe_replace(tmp_j, self.jsonl_path)

        tmp_c = self.csv_path.with_suffix(".tmp")
        with open(tmp_c, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for obj in records.values():
                writer.writerow(obj)
        _safe_replace(tmp_c, self.csv_path)

    def upsert(self, hotels: list[dict], scrape_date: str) -> dict:
        existing = self._load()
        inserted = updated = 0
        for hotel in hotels:
            row = _flatten(hotel, scrape_date)
            key = (row["hotel_id"], scrape_date)
            if key not in existing:
                existing[key] = row
                inserted += 1
            else:
                merged = dict(existing[key])
                for field, nv in row.items():
                    if _is_richer(nv, merged.get(field)):
                        merged[field] = nv
                existing[key] = merged
                updated += 1
        self._write(existing)
        return {"inserted": inserted, "updated": updated, "total": len(existing)}

    def rebuild_csv(self) -> int:
        records = self._load()
        if not records:
            logger.warning("rebuild_csv: no records found in JSONL.")
            return 0
        tmp_c = self.csv_path.with_suffix(".tmp")
        with open(tmp_c, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for obj in records.values():
                writer.writerow(obj)
        _safe_replace(tmp_c, self.csv_path)
        logger.info(f"✓ CSV rebuilt: {len(records)} records → {self.csv_path}")
        return len(records)

    def stats(self) -> dict:
        records = self._load()
        if not records:
            return {"total": 0}
        dates  = sorted({v["scrape_date"] for v in records.values() if v.get("scrape_date")})
        cities = sorted({v["city"]        for v in records.values() if v.get("city")})
        return {"total": len(records), "dates": f"{dates[0]} → {dates[-1]}" if dates else "—", "cities": cities}


# ─────────────────────────────────────────────────────────────────────────────
# CITY ROTATION (Airflow daily — every city guaranteed every ~5 days)
# ─────────────────────────────────────────────────────────────────────────────

def pick_today_cities(n: int = 6) -> list[str]:
    """
    Deterministic date-based round-robin.
    With n=6 and 30 cities, every city appears at least once every 5 days.
    Override: BOOKING_CITIES="Ninh Binh,Hue,Da Lat" env var.
    """
    env = os.environ.get("BOOKING_CITIES", "").strip()
    if env:
        cities = [c.strip() for c in env.split(",") if c.strip()]
        logger.info(f"City override from BOOKING_CITIES env: {cities}")
        return cities
    day    = date.today().timetuple().tm_yday
    start  = ((day - 1) * n) % len(VIETNAM_CITIES)
    cities = [VIETNAM_CITIES[(start + i) % len(VIETNAM_CITIES)] for i in range(n)]
    logger.info(f"Day {day} → cities: {cities}")
    return cities


# ─────────────────────────────────────────────────────────────────────────────
# PRIMITIVE EXTRACTORS
# ─────────────────────────────────────────────────────────────────────────────

async def _first_visible_text(locator: Locator, timeout: int = 3000) -> Optional[str]:
    """
    Iterate matched elements, return inner_text of the first visible one.
    Uses inner_text() so CSS-rendered text (::before, ::after) is included.
    """
    try:
        count = await locator.count()
        for i in range(count):
            try:
                el = locator.nth(i)
                if await el.is_visible(timeout=timeout):
                    txt = (await el.inner_text()).strip()
                    if txt:
                        return txt
            except Exception:
                continue
    except Exception:
        pass
    return None


async def _first_attr(locator: Locator, attr: str, timeout: int = 3000) -> Optional[str]:
    """Return first non-empty value of `attr` across all matched elements."""
    try:
        count = await locator.count()
        for i in range(count):
            try:
                val = await locator.nth(i).get_attribute(attr, timeout=timeout)
                if val and val.strip():
                    return val.strip()
            except Exception:
                continue
    except Exception:
        pass
    return None


async def _try_selectors(
    root: Locator | Page,
    selectors: list[str],
    timeout: int = 3000,
) -> Optional[str]:
    """
    Core multi-fallback primitive.
    Tries each CSS selector in order, returns first non-empty inner_text found.
    Scoped to `root` (a card locator or a page) so searches stay contained.
    """
    for sel in selectors:
        try:
            loc = root.locator(sel)
            txt = await _first_visible_text(loc, timeout=timeout)
            if txt:
                return txt
        except Exception:
            continue
    return None


async def _try_selectors_attr(
    root: Locator | Page,
    selectors: list[str],
    attr: str,
    timeout: int = 3000,
) -> Optional[str]:
    """Same as _try_selectors but returns an attribute value instead of text."""
    for sel in selectors:
        try:
            loc = root.locator(sel)
            val = await _first_attr(loc, attr, timeout=timeout)
            if val:
                return val
        except Exception:
            continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
# TYPE-SAFE PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_float(text: Optional[str]) -> Optional[float]:
    """
    Extract a Booking review score (1.0–10.0) from a string.
    Handles: "8.5", "Scored 8.5", "8,5" (European comma), "8.5 Excellent"
    Rejects values outside valid score range.
    """
    if not text:
        return None
    # Normalise European comma decimal separator
    normalised = text.replace(",", ".")
    # Match a 1–2 digit number optionally followed by one decimal place
    m = re.search(r"\b(\d{1,2}(?:\.\d)?)\b", normalised)
    if m:
        try:
            val = float(m.group(1))
            if 1.0 <= val <= 10.0:
                return round(val, 1)
        except ValueError:
            pass
    return None


def _parse_review_count(text: Optional[str]) -> Optional[int]:
    """
    Extract review count from strings like:
      "1,234 reviews", "1234 reviews", "Based on 200 reviews"
    Rejects implausibly small numbers (< 5) to filter noise like "2 nights".
    """
    if not text:
        return None
    matches = re.findall(r"\b(\d[\d,]*)\b", text)
    for raw in matches:
        try:
            val = int(raw.replace(",", ""))
            if val >= 5:
                return val
        except ValueError:
            continue
    return None


def _parse_price(text: Optional[str]) -> Optional[float]:
    """
    Extract numeric price from various currency string formats:
      VND dot-thousands:  "1.500.000"    → 1500000.0
      Comma-thousands:    "1,500,000"    → 1500000.0
      USD decimal:        "US$45.50"     → 45.5
      Bare number:        "1500000"      → 1500000.0
      VND with decimal:   "1.500.000,00" → 1500000.0
    Returns None for zero or unparseable input.
    """
    if not text:
        return None
    # Strip currency symbols, whitespace, and non-numeric characters
    t = re.sub(r"[^\d.,]", "", text.strip())
    if not t:
        return None

    # VND format: dots as thousands separators → "1.500.000" or "1.500.000,00"
    if re.match(r"^\d{1,3}(\.\d{3})+(,\d+)?$", t):
        t = t.replace(".", "").replace(",", ".")
    # USD/international comma-thousands → "1,500,000" or "1,500,000.50"
    elif re.match(r"^\d{1,3}(,\d{3})+(\.\d+)?$", t):
        t = t.replace(",", "")
    # Decimal comma only (no dot): "1500000,50" → "1500000.50"
    elif "," in t and "." not in t:
        t = t.replace(",", ".")
    # Fallback: strip anything that isn't a digit or decimal point
    else:
        t = re.sub(r"[^\d.]", "", t)

    try:
        val = float(t)
        return val if val > 0 else None
    except ValueError:
        return None


def _parse_rooms_left(text: Optional[str]) -> Optional[int]:
    """
    Extract integer from urgency messages:
      "Only 2 rooms left!" → 2
      "Last room!"         → 1
      "3 rooms left at this price" → 3
    """
    if not text:
        return None
    lower = text.lower()
    m = re.search(r"(\d+)\s+room", lower)
    if m:
        return int(m.group(1))
    if "last room" in lower or "last available" in lower:
        return 1
    return None


def _parse_star_count(aria_label: Optional[str], span_count: int) -> Optional[int]:
    """
    Resolve star rating from:
      1. aria-label parse  →  "Rated 4 out of 5"  /  "4-star hotel"
      2. span child count  →  fallback when aria-label absent

    Rejects values outside 1–5. Returns None for unrated properties.
    """
    if aria_label:
        # "Rated N out of 5" / "N stars"
        m = re.search(r"(\d)\s+(?:out of|stars?)", aria_label.lower())
        if m:
            val = int(m.group(1))
            if 1 <= val <= 5:
                return val
        # "4-star hotel" / "4 star"
        m2 = re.search(r"(\d)-?\s*star", aria_label.lower())
        if m2:
            val = int(m2.group(1))
            if 1 <= val <= 5:
                return val

    if 1 <= span_count <= 5:
        return span_count

    return None


def _clean_hotel_url(raw_href: Optional[str]) -> Optional[str]:
    """
    Strip ALL query parameters and language suffixes from a hotel URL.

    Input:  /hotel/vn/some-slug.en-gb.html?aid=123&label=abc&sr_order=...
    Output: https://www.booking.com/hotel/vn/some-slug.html

    The language suffix (.en-gb, .vi, etc.) is removed so the canonical URL
    is language-neutral and stable across repeated scrape runs.
    """
    if not raw_href:
        return None
    if raw_href.startswith("/"):
        raw_href = BASE_ORIGIN + raw_href

    parsed = urlparse(raw_href)
    path   = parsed.path

    # Remove language tag before .html  e.g. ".en-gb.html" → ".html"
    path = re.sub(r"\.[a-z]{2}(?:-[a-z]{2,4})?(?=\.html$)", "", path, flags=re.IGNORECASE)

    # Rebuild with no query string or fragment
    return urlunparse(("https", "www.booking.com", path, "", "", ""))


def _extract_hotel_id(clean_url: Optional[str]) -> Optional[str]:
    """Extract slug: https://www.booking.com/hotel/vn/<slug>.html → <slug>"""
    if not clean_url:
        return None
    m = re.search(r"/hotel/vn/([^/.]+)", clean_url)
    return m.group(1) if m else None


# ─────────────────────────────────────────────────────────────────────────────
# ISOLATED FIELD EXTRACTORS
# One function per field group → surgical selector updates, isolated failures.
# ─────────────────────────────────────────────────────────────────────────────

async def _extract_name_and_url(
    card: Locator,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Returns (name, clean_url, hotel_id)."""

    name = await _try_selectors(card, [
        "[data-testid='title']",
        "h3[data-testid='title']",
        ".sr-hotel__name",
        "h3.title",
        "[class*='hotelName']",
    ])

    raw_href = await _try_selectors_attr(card, [
        "[data-testid='title-link']",
        "a[href*='/hotel/vn/']",
        "a[href*='booking.com/hotel']",
    ], attr="href")

    clean_url = _clean_hotel_url(raw_href)
    hotel_id  = _extract_hotel_id(clean_url)

    return name, clean_url, hotel_id


async def _extract_location(card: Locator) -> dict:
    """
    Location extraction strategy (v4).

    address:
      [data-testid='address'] still works on some card variants.
      When it fails, we walk the card's inner_text blocks and apply
      Vietnamese address pattern matching:
        - Contains "District", "Phường", "Quận", "Street", "Đường",
          "Ward", or common HCMC/HN district names
        - Short text block (< 80 chars) near the hotel name

    distance_from_center:
      [data-testid='distance'] is stable — keep as-is.
    """

    # ── Address: primary selectors ────────────────────────────────────────────
    address = await _try_selectors(card, [
        "[data-testid='address']",
        "[data-testid='location']",
        ".sr_card_address_line",
        ".bui-location__address",
        "[aria-label*='located in']",
        "[aria-label*='address']",
    ])

    # ── Address: text-block fallback ──────────────────────────────────────────
    # If selectors fail, collect all short text blocks from the card and
    # keyword-match for Vietnamese/English address patterns.
    if address is None:
        _VN_ADDRESS_SIGNALS = re.compile(
            r"\b(district|phường|quận|ward|street|đường|road|alley|hẻm"
            r"|avenue|boulevard|floor|p\.\d|q\.\d)\b",
            re.IGNORECASE | re.UNICODE,
        )
        try:
            # Gather inner_text from all leaf-level text nodes in the card
            # by querying small containers that are likely to hold address text
            candidate_selectors = [
                "[data-testid='property-card-header'] span",
                "[data-testid='property-card-header'] div",
                "span[class*='address']", "div[class*='address']",
                "span[class*='location']", "div[class*='location']",
                "span[class*='subtitle']", "div[class*='subtitle']",
            ]
            for sel in candidate_selectors:
                els = card.locator(sel)
                n   = await els.count()
                for i in range(n):
                    try:
                        txt = (await els.nth(i).inner_text()).strip()
                        # Must be short, non-empty, and look like an address
                        if txt and 5 < len(txt) < 100 and _VN_ADDRESS_SIGNALS.search(txt):
                            address = txt
                            break
                    except Exception:
                        continue
                if address:
                    break
        except Exception:
            pass

    # ── Distance ──────────────────────────────────────────────────────────────
    distance = await _try_selectors(card, [
        "[data-testid='distance']",
        ".bui-location__distance",
        "[data-testid='location-description']",
        "[class*='distance']",
        "[class*='Distance']",
    ])

    return {
        "address":              address,
        "city":                 None,       # Set by caller from city loop
        "country":              "Vietnam",
        "distance_from_center": distance,
        "latitude":             None,       # Populated by deep scrape only
        "longitude":            None,
    }


async def _extract_pricing(card: Locator) -> dict:
    """
    Pricing extraction strategy (v4).

    Current price:
      [data-testid='price-and-discounted-price'] is stable and works.
      Fallback to nearby price containers.

    Original (struck-out) price:
      Booking renders the original as a <s> or <del> element adjacent to the
      current price block. We scope the search INSIDE the price container first,
      then try card-level <s>/<del> as fallback.
      Always validate: original > current. Discard on failure.

    Discount pct (NEW):
      NOT scraped — COMPUTED from (original - current) / original * 100.
      Booking does not expose this number directly on search cards.
      If original_price is missing, discount_pct = None (not 0).

    Taxes:
      [data-testid='taxes-and-charges'] remains stable.
    """

    # ── Current price ─────────────────────────────────────────────────────────
    current_text = await _try_selectors(card, [
        "[data-testid='price-and-discounted-price']",
        ".bui-price-display__value",
        ".prco-valign-middle-helper",
        "[data-testid='price']",
        "[class*='actualPrice']",
    ])
    current_price = _parse_price(current_text)

    # ── Original (crossed-out) price ──────────────────────────────────────────
    # Strategy 1: <s> or <del> scoped INSIDE the price display container.
    # This is the most specific — avoids picking up unrelated strikethrough text.
    original_price: Optional[float] = None

    # Try scoped inside the price block first
    price_block = card.locator(
        "[data-testid='price-and-discounted-price'], "
        "[data-testid='recommended-units'], "
        "[class*='priceWrapper'], [class*='price-wrapper']"
    ).first

    if await price_block.count() > 0:
        for sel in ["s", "del", "[class*='crossedOut']", "[class*='crossed-out']",
                    "[class*='originalPrice']", "[class*='original-price']"]:
            try:
                el = price_block.locator(sel).first
                if await el.count() > 0:
                    txt = (await el.inner_text()).strip()
                    val = _parse_price(txt)
                    if val and val > 0:
                        original_price = val
                        break
            except Exception:
                continue

    # Strategy 2: card-level <s>/<del> if scoped search found nothing
    if original_price is None:
        for sel in [
            "s", "del",
            "[class*='crossedOutPrice']",
            "[class*='originalPrice']",
            "s[class*='price']",
            "del[class*='price']",
            "[aria-label*='original price']",
            "[data-testid='price-for-x-nights'] s",
            "[data-testid='price-for-x-nights'] del",
            ".bui-price-display__original",
        ]:
            try:
                els = card.locator(sel)
                n = await els.count()
                for i in range(n):
                    txt = (await els.nth(i).inner_text()).strip()
                    val = _parse_price(txt)
                    if val and val > 0:
                        original_price = val
                        break
                if original_price is not None:
                    break
            except Exception:
                continue

    # Sanity: original must exceed current
    if original_price is not None and current_price is not None:
        if original_price <= current_price:
            logger.debug(f"Discarding original_price={original_price} (≤ current={current_price})")
            original_price = None

    # ── Discount pct (COMPUTED, not scraped) ──────────────────────────────────
    discount_pct = _compute_discount(original_price, current_price)

    # ── Taxes ─────────────────────────────────────────────────────────────────
    taxes_text = await _try_selectors(card, [
        "[data-testid='taxes-and-charges']",
        ".prd-taxes-and-fees-under-price",
        "[class*='taxesAndFees']",
        "[class*='taxes']",
    ])
    taxes_included: Optional[bool] = None
    if taxes_text:
        lower = taxes_text.lower()
        if "excl" in lower or "+ taxes" in lower or "not included" in lower:
            taxes_included = False
        elif "incl" in lower or "included" in lower:
            taxes_included = True

    return {
        "price_per_night_vnd": current_price,
        "original_price_vnd":  original_price,
        "discount_pct":        discount_pct,   # float e.g. 20.0, or None
        "currency":            "VND",
        "taxes_included":      taxes_included,
    }


# Known Booking rating label words (lowercase). Used by _parse_review_block
# and fallback aria-label scanning. Keep sorted longest-first so 'very good'
# matches before 'good'.
_RATING_WORDS: list[str] = [
    "exceptional", "superb", "fabulous", "wonderful",
    "very good", "good", "pleasant", "okay", "poor",
]

# Known property types Booking displays on cards.
# Used by _extract_property_type text-based matching.
_PROPERTY_TYPES: list[str] = [
    "hotel", "apartment", "hostel", "guesthouse", "villa",
    "resort", "motel", "homestay", "serviced apartment",
    "ryokan", "capsule hotel", "boat", "camp", "farm stay",
]

# Amenity keyword map: canonical_key → list[match_phrase] (all lowercase).
# Used by _extract_amenities to normalize card text into structured keys.
# Ordered: longer/more-specific phrases first within each group.
AMENITY_MAP: dict[str, list[str]] = {
    "free_cancellation":     ["free cancellation", "cancel for free", "cancellations are free"],
    "no_prepayment":         ["no prepayment needed", "no prepayment", "pay at the property",
                              "pay nothing until", "no credit card needed"],
    "free_breakfast":        ["free breakfast", "breakfast included", "breakfast is included",
                              "complimentary breakfast", "breakfast free"],
    "swimming_pool":         ["swimming pool", "outdoor pool", "indoor pool",
                              "rooftop pool", "infinity pool", "pool"],
    "airport_shuttle":       ["airport shuttle", "airport transfer", "shuttle service"],
    "free_wifi":             ["free wifi", "free wi-fi", "free internet", "wifi included",
                              "wi-fi included"],
    "parking":               ["free parking", "private parking", "parking available", "car park"],
    "spa":                   ["spa", "wellness center", "wellness centre", "massage"],
    "fitness_center":        ["fitness center", "fitness centre", "gym", "gymnasium"],
    "restaurant":            ["restaurant", "on-site restaurant"],
    "air_conditioning":      ["air conditioning", "air conditioned", "air-conditioned"],
    "kitchen":               ["full kitchen", "kitchenette", "kitchen"],
    "limited_time_deal":     ["limited-time deal", "limited time deal"],
    "genius_discount":       ["genius discount", "genius level", "genius"],
    "mobile_only_price":     ["mobile-only price", "app-only", "mobile only"],
    "non_smoking":           ["non-smoking", "non smoking", "no smoking"],
    "family_rooms":          ["family rooms", "family room"],
    "pets_allowed":          ["pets allowed", "pet friendly", "pets welcome"],
    "ev_charging":           ["electric vehicle charging", "ev charging", "ev station"],
}


def _compute_discount(original: Optional[float], current: Optional[float]) -> Optional[float]:
    """
    Derive discount_pct from original and current prices.
    Returns rounded percentage (e.g. 20.0 for 20%) or None if not computable.
    Booking does NOT expose discount_pct directly on search cards —
    it must always be computed from the two price values.
    """
    if not original or not current:
        return None
    if original <= 0 or current <= 0:
        return None
    if original <= current:
        # Sanity: original must exceed current for a valid discount
        return None
    return round((original - current) / original * 100, 1)


def _parse_review_block(full_text: str) -> tuple[Optional[float], Optional[str], Optional[int]]:
    """
    Parse the inner_text of [data-testid='review-score'] into (score, label, count).

    The block always contains the three pieces of information as newline-separated
    text regardless of which obfuscated class names wrap them internally.
    Example inner_text values (real Booking output):
      "8.5\\nVery good\\n406 reviews"
      "Review score\\n8.5\\nVery good\\n1,234 reviews"
      "Scored 8.5\\nGood\\nBased on 200 reviews"
      "Very good\\n8.1\\n512 reviews"  ← label can appear before score

    Strategy:
      Split by newline. Scan each line independently.
      Score:  first line that is a standalone decimal in range 1–10.
              Also handle "Scored 8.5" / "Score: 8.5" prefix patterns.
      Label:  first line that matches a known Booking rating word exactly.
      Count:  first line matching '<digits> review(s)' or 'Based on <N> reviews'.
    """
    if not full_text:
        return None, None, None

    lines = [l.strip() for l in re.split(r"[\n\r]+", full_text) if l.strip()]
    score: Optional[float] = None
    label: Optional[str]   = None
    count: Optional[int]   = None

    for line in lines:
        lower = line.lower()

        # ── Score ─────────────────────────────────────────────────────────────
        if score is None:
            # Standalone number: "8.5", "10", "7" (possibly with &nbsp;)
            clean = line.replace("\xa0", "").strip()
            m = re.fullmatch(r"(\d{1,2}(?:[.,]\d)?)", clean)
            if m:
                try:
                    val = float(m.group(1).replace(",", "."))
                    if 1.0 <= val <= 10.0:
                        score = round(val, 1)
                        continue
                except ValueError:
                    pass

            # Prefixed: "Scored 8.5", "Score: 8.5", "Rating: 8.5"
            if score is None:
                m2 = re.search(r"(?:scored?|rating)[:\s]+(\d{1,2}(?:[.,]\d)?)", lower)
                if m2:
                    try:
                        val = float(m2.group(1).replace(",", "."))
                        if 1.0 <= val <= 10.0:
                            score = round(val, 1)
                    except ValueError:
                        pass

        # ── Label ─────────────────────────────────────────────────────────────
        if label is None:
            for word in _RATING_WORDS:
                # Exact match or match at start (e.g. "Very good " with trailing space)
                if lower == word or lower.startswith(word) and len(lower) <= len(word) + 3:
                    label = line
                    break

        # ── Count ─────────────────────────────────────────────────────────────
        if count is None:
            m3 = re.search(r"([\d,]+)\s+reviews?", lower)
            if m3:
                try:
                    count = int(m3.group(1).replace(",", ""))
                except ValueError:
                    pass

        if score is not None and label is not None and count is not None:
            break  # All three found — stop scanning

    return score, label, count


async def _extract_rating(card: Locator) -> dict:
    """
    Selectors derived from live Booking.com HTML (March 2026).

    Actual DOM structure inside [data-testid='review-score']:

        <div data-testid="review-score">
          <div class="bc946a29db">Scored 10</div>          ← screen-reader text (no aria-hidden)
          <div aria-hidden="true"  class="...">10</div>    ← SCORE  (machine-readable duplicate)
          <div aria-hidden="false" class="...">            ← visible wrapper
            <div class="...">Exceptional</div>             ← LABEL  (first child)
            <div class="...">2 reviews</div>               ← COUNT  (second child)
          </div>
        </div>

    Key insight: Booking deliberately places the numeric score in a
    div with aria-hidden="true" as a machine-readable duplicate.
    The label and count live inside the aria-hidden="false" wrapper
    as its first and last child divs respectively.
    These aria-hidden attributes are part of Booking's accessibility
    contract and have been stable across multiple deployments — they
    are far more reliable than the obfuscated class names which rotate
    with every frontend deployment.

    Selector strategy (3 layers, each independent):
      SCORE  → [aria-hidden='true']  inside the review-score block
               Fallback: parse "Scored N" from screen-reader text div
      LABEL  → [aria-hidden='false'] > div:first-child
      COUNT  → [aria-hidden='false'] > div:last-child  (parse "N reviews")
    """
    review_block = card.locator("[data-testid='review-score']")
    if await review_block.count() == 0:
        return {"score": None, "label": None, "review_count": None}

    rb = review_block.first

    # ── SCORE: aria-hidden="true" child (machine-readable numeric duplicate) ──
    score: Optional[float] = None
    try:
        score_el   = rb.locator("[aria-hidden='true']").first
        score_text = (await score_el.inner_text()).strip() if await score_el.count() > 0 else ""
        score = _parse_float(score_text)
    except Exception:
        pass

    # Score fallback: screen-reader text "Scored 10" (div with no aria-hidden attr)
    if score is None:
        try:
            # The SR div has no aria-hidden attribute — select it via :not()
            sr_el   = rb.locator("div:not([aria-hidden])").first
            sr_text = (await sr_el.inner_text()).strip() if await sr_el.count() > 0 else ""
            # Parse "Scored 10" or bare "10"
            m = re.search(r"(?:scored?\s+)?(\d{1,2}(?:[.,]\d)?)", sr_text, re.IGNORECASE)
            if m:
                val = float(m.group(1).replace(",", "."))
                if 1.0 <= val <= 10.0:
                    score = round(val, 1)
        except Exception:
            pass

    # ── LABEL + COUNT: inside aria-hidden="false" wrapper ─────────────────────
    label: Optional[str] = None
    count: Optional[int] = None
    try:
        visible = rb.locator("[aria-hidden='false']").first
        if await visible.count() > 0:
            children = visible.locator("> div")
            n = await children.count()

            # Label → first child div
            if n >= 1:
                raw_label = (await children.nth(0).inner_text()).strip()
                # Guard: must match a known rating word, not be a number
                if raw_label and not re.match(r"^\d", raw_label):
                    label = raw_label

            # Count → last child div  (text: "N reviews" or "N,NNN reviews")
            if n >= 2:
                raw_count = (await children.nth(n - 1).inner_text()).strip()
                m = re.search(r"([\d,]+)\s+reviews?", raw_count, re.IGNORECASE)
                if m:
                    count = int(m.group(1).replace(",", ""))
    except Exception:
        pass

    # ── Final fallback: parse entire block inner_text if anything still missing ─
    if score is None or label is None or count is None:
        try:
            block_text = (await rb.inner_text()).strip()
            fb_score, fb_label, fb_count = _parse_review_block(block_text)
            if score is None:
                score = fb_score
            if label is None:
                label = fb_label
            if count is None:
                count = fb_count
        except Exception:
            pass

    return {"score": score, "label": label, "review_count": count}


async def _extract_stars(card: Locator) -> Optional[int]:
    """
    Star rating extraction (v4).

    Booking renders star ratings in one of several ways:
      A) aria-label="4 stars" on the container div
         → [data-testid='rating-stars'] aria-label
      B) aria-label on an inner <span> or <svg> element
         → [data-testid='rating-stars'] span[aria-label]
      C) SVG <title> child: "4 stars"
         → [data-testid='rating-stars'] svg title
      D) Count of filled-star <span>/<svg> children (legacy)

    Strategy: try all known containers, for each try all methods in order.
    Return the first valid integer 1–5 found. None for unrated properties.
    """
    CONTAINERS = [
        "[data-testid='rating-stars']",
        "[data-testid='star-rating']",
        ".bui-rating",
        "[class*='starRating']",
        "[class*='star-rating']",
        "[class*='StarRating']",
    ]

    for sel in CONTAINERS:
        try:
            container = card.locator(sel).first
            if await container.count() == 0:
                continue

            # Method A: aria-label on the container itself
            aria = await container.get_attribute("aria-label")
            result = _parse_star_count(aria, 0)
            if result:
                return result

            # Method B: aria-label on any direct child span/div
            for child_sel in ["span[aria-label]", "div[aria-label]", "a[aria-label]"]:
                try:
                    child = container.locator(child_sel).first
                    if await child.count() > 0:
                        child_aria = await child.get_attribute("aria-label")
                        result = _parse_star_count(child_aria, 0)
                        if result:
                            return result
                except Exception:
                    continue

            # Method C: SVG <title> text (e.g. "4 stars")
            try:
                title_el = container.locator("svg title, title").first
                if await title_el.count() > 0:
                    title_text = (await title_el.inner_text()).strip()
                    result = _parse_star_count(title_text, 0)
                    if result:
                        return result
            except Exception:
                pass

            # Method D: count filled-star child elements
            for star_sel in ["span", "svg", "i[class*='star']"]:
                try:
                    stars = container.locator(star_sel)
                    n = await stars.count()
                    result = _parse_star_count(None, n)
                    if result:
                        return result
                except Exception:
                    continue

        except Exception:
            continue

    return None


async def _extract_availability(
    card: Locator,
) -> tuple[Optional[str], Optional[int]]:
    """
    Returns (availability_status, rooms_left).

    Status values:
      "available"  — no urgency message
      "limited"    — "Only N rooms left" or "In high demand"
      "last_room"  — "Last room!" / rooms_left == 1
      "sold_out"   — explicit sold-out overlay

    rooms_left is an integer when we can parse a number, else None.
    """
    # Sold-out is the most critical state — check it first
    sold_out = card.locator(
        ".sold_out_property, [data-testid='sold-out'], [class*='soldOut']"
    )
    if await sold_out.count() > 0:
        return "sold_out", None

    urgency_text = await _try_selectors(card, [
        "[data-testid='availability-rate-information']",
        "[data-testid='urgency-message']",
        ".urgency_message",
        ".sr_card_availability",
        "[class*='urgency']",
        "[class*='scarcity']",
        "[class*='availabilityCount']",
    ])

    if not urgency_text:
        return "available", None

    rooms_left = _parse_rooms_left(urgency_text)
    lower      = urgency_text.lower()

    if rooms_left == 1 or "last room" in lower:
        return "last_room", 1
    elif rooms_left is not None:
        return "limited", rooms_left
    elif "high demand" in lower or "booked" in lower or "popular" in lower:
        return "limited", None

    return "available", None


async def _extract_property_type(card: Locator) -> Optional[str]:
    """
    Property type extraction (v4).

    Primary: [data-testid='property-type-badge'] if it exists.
    Fallback: walk candidate containers in the card, collect short text blocks,
    keyword-match against known property type words.
    This approach is immune to class-name changes.
    """
    # Primary: stable data-testid
    prop_type = await _try_selectors(card, [
        "[data-testid='property-type-badge']",
        "[data-testid='recommended-units'] [class*='propertyType']",
        "[data-testid='recommended-units'] [class*='property-type']",
        ".sr_card__property_type",
    ])
    if prop_type and len(prop_type) <= 40:
        return prop_type

    # Fallback: text-based keyword match
    # Look for short text blocks in the card header / near title
    candidate_selectors = [
        "[data-testid='property-card-header'] span",
        "[data-testid='property-card-header'] div",
        "[data-testid='title'] ~ span",
        "[data-testid='title'] ~ div",
        "span[class*='type']", "div[class*='type']",
        "span[class*='category']", "div[class*='category']",
        "span[class*='label']",
    ]

    for sel in candidate_selectors:
        try:
            els = card.locator(sel)
            n   = await els.count()
            for i in range(n):
                try:
                    txt = (await els.nth(i).inner_text()).strip().lower()
                    if not txt or len(txt) > 40:
                        continue
                    for pt in _PROPERTY_TYPES:
                        if pt in txt:
                            # Return properly capitalised
                            return pt.title()
                except Exception:
                    continue
        except Exception:
            continue

    return None


async def _extract_badges(card: Locator) -> list[str]:
    """
    Badge extraction (v4) — raw human-readable badge strings.

    Strategy: collect ALL text from candidate sections of the card,
    filter to short strings that look like feature badges.
    No reliance on obfuscated class names.

    Returned list contains raw strings as Booking displays them,
    e.g. ["Free cancellation", "Limited-time Deal", "Genius"].
    For structured/normalized output see _extract_amenities.
    """
    JUNK = re.compile(r"^\d+$|^https?://|^[\W_]+$|^\s*$", re.IGNORECASE)

    # Known badge phrase fragments — used to validate candidate text
    BADGE_SIGNALS = re.compile(
        r"cancell|prepayment|breakfast|wifi|wi-fi|pool|shuttle|parking|spa|gym"
        r"|fitness|deal|genius|mobile|non.smoking|pets|family|kitchen|airport"
        r"|limited|included|free|no \w+|available",
        re.IGNORECASE
    )

    CANDIDATE_SELECTORS = [
        # Stable data-testid patterns
        "[data-testid='property-card-feature-highlight']",
        "[data-testid='property-card-feature-highlight-list'] li",
        "[data-testid='benefits'] span",
        "[data-testid='benefits'] div",
        "[data-testid='facility-icons'] span",
        "[data-testid='facility-icons'] div",
        "[data-testid='recommended-units'] li",
        # Broad structural
        "ul[class*='benefit'] li",
        "ul[class*='feature'] li",
        "ul[class*='highlight'] li",
        "li[class*='benefit']",
        "li[class*='feature']",
        "li[class*='highlight']",
        "span[class*='badge']",
        "div[class*='badge']",
        ".bui-badge",
    ]

    seen:   set[str]  = set()
    badges: list[str] = []

    for sel in CANDIDATE_SELECTORS:
        try:
            els = card.locator(sel)
            n   = await els.count()
            for i in range(n):
                try:
                    txt = (await els.nth(i).inner_text()).strip()
                    if (txt
                            and txt not in seen
                            and not JUNK.match(txt)
                            and len(txt) < 80
                            and BADGE_SIGNALS.search(txt)):
                        seen.add(txt)
                        badges.append(txt)
                except Exception:
                    continue
        except Exception:
            continue
        if len(badges) >= 12:
            break

    return badges


async def _extract_amenities(card: Locator) -> list[str]:
    """
    Amenity extraction (v4) — normalized structured keys.

    Booking renders amenities as independent text nodes scattered across
    the card with no single stable container. We therefore:
      1. Collect ALL inner_text from the entire card
      2. Apply AMENITY_MAP keyword matching (case-insensitive)
      3. Return deduplicated list of canonical snake_case keys

    Example output: ["free_cancellation", "swimming_pool", "free_breakfast"]

    This approach is fully immune to DOM structure changes — as long as
    Booking renders the amenity text anywhere in the card, we find it.
    """
    try:
        # Get entire card text — O(1) DOM call, much faster than many locators
        full_text = (await card.inner_text()).lower()
    except Exception:
        return []

    found: list[str] = []
    for key, phrases in AMENITY_MAP.items():
        for phrase in phrases:
            if phrase in full_text:
                found.append(key)
                break  # matched this key — move to next key

    return found


async def _extract_images(card: Locator, max_images: int = 3) -> list[str]:
    SELECTORS = [
        "img[data-testid='image']",
        "img[class*='hotel_image']",
        "[data-testid='gallery'] img",
        ".sr_card_img img",
    ]
    images: list[str] = []
    for sel in SELECTORS:
        try:
            els = card.locator(sel)
            n   = await els.count()
            for i in range(min(n, max_images)):
                src = (
                    await els.nth(i).get_attribute("src")
                    or await els.nth(i).get_attribute("data-src")
                    or await els.nth(i).get_attribute("data-lazy")
                )
                if src and src.startswith("http") and src not in images:
                    images.append(src)
                if len(images) >= max_images:
                    break
        except Exception:
            continue
        if len(images) >= max_images:
            break
    return images


# ─────────────────────────────────────────────────────────────────────────────
# MAIN CARD PARSER
# ─────────────────────────────────────────────────────────────────────────────

async def parse_hotel_card(card: Locator) -> dict:
    """
    Orchestrates extraction from a single property card.
    Collects basic fields available on the search results page.
    All rich detail (description, rooms, facilities, review sub-scores,
    area info, full images) is populated by deep_scrape_hotel().
    """
    hotel: dict = {
        "scraped_at": datetime.utcnow().isoformat(),
        "hotel_id":   None,
        "name":       None,
        "url":        None,
        "location": {
            "address":              None,
            "city":                 None,
            "country":              "Vietnam",
            "distance_from_center": None,
            "latitude":             None,
            "longitude":            None,
        },
        "detail": {
            "description":   None,
            "checkin_time":  None,
            "checkout_time": None,
            "languages":     [],
        },
        "pricing": {
            "price_per_night_vnd": None,
            "original_price_vnd":  None,
            "discount_pct":        None,
            "currency":            "VND",
            "taxes_included":      None,
            "taxes_fees_text":     None,
        },
        "rating": {
            "score":              None,
            "label":              None,
            "review_count":       None,
            "review_categories":  {},
        },
        "property": {
            "type":                None,
            "star_rating":         None,
            "availability_status": None,
            "rooms_left":          None,
            "room_types":          [],
        },
        "facilities": {
            "popular": [],
            "all":     {},
        },
        "badges":    [],
        "amenities": [],
        "images":    [],
    }

    try:
        name, url, hotel_id  = await _extract_name_and_url(card)
        hotel["name"]        = name
        hotel["url"]         = url
        hotel["hotel_id"]    = hotel_id
    except Exception as e:
        logger.debug(f"name/url: {e}")

    try:
        loc = await _extract_location(card)
        for k, v in loc.items():
            if v is not None:
                hotel["location"][k] = v
    except Exception as e:
        logger.debug(f"location: {e}")

    try:
        pricing = await _extract_pricing(card)
        hotel["pricing"].update({k: v for k, v in pricing.items() if v is not None})
    except Exception as e:
        logger.debug(f"pricing: {e}")

    try:
        rating = await _extract_rating(card)
        hotel["rating"].update({k: v for k, v in rating.items() if v is not None})
    except Exception as e:
        logger.debug(f"rating: {e}")

    try:
        stars = await _extract_stars(card)
        if stars:
            hotel["property"]["star_rating"] = stars
    except Exception as e:
        logger.debug(f"stars: {e}")

    try:
        prop_type = await _extract_property_type(card)
        if prop_type:
            hotel["property"]["type"] = prop_type
    except Exception as e:
        logger.debug(f"property_type: {e}")

    try:
        status, rooms_left                       = await _extract_availability(card)
        hotel["property"]["availability_status"] = status
        hotel["property"]["rooms_left"]          = rooms_left
    except Exception as e:
        logger.debug(f"availability: {e}")

    try:
        hotel["badges"] = await _extract_badges(card)
    except Exception as e:
        logger.debug(f"badges: {e}")

    try:
        hotel["amenities"] = await _extract_amenities(card)
    except Exception as e:
        logger.debug(f"amenities: {e}")

    try:
        hotel["images"] = await _extract_images(card)
    except Exception as e:
        logger.debug(f"images: {e}")

    return hotel


# ─────────────────────────────────────────────────────────────────────────────
# REVIEW CATEGORY PARSING
# ─────────────────────────────────────────────────────────────────────────────

# Maps canonical key → list of match phrases (lowercase).
# Used to identify which sub-score category each text block belongs to.
REVIEW_CATEGORY_MAP: dict[str, list[str]] = {
    "staff":       ["staff", "service", "personnel"],
    "facilities":  ["facilities", "facility", "amenities"],
    "cleanliness": ["cleanliness", "cleaning", "clean"],
    "comfort":     ["comfort", "comfortable"],
    "value":       ["value for money", "value"],
    "location":    ["location"],
    "wifi":        ["free wifi", "wifi", "wi-fi", "internet"],
}


def _parse_review_categories(blocks: list[str]) -> dict[str, float]:
    """
    Parse a list of text blocks from review sub-score elements into a dict.
    Each block looks like: 'Staff\n9.2' or 'Facilities 8.1'.
    Returns e.g. {'staff': 9.2, 'facilities': 8.1, ...}
    """
    result: dict[str, float] = {}
    for block in blocks:
        lower = block.lower().strip()
        # Extract numeric score (1.0–10.0)
        m = re.search(r"(\d{1,2}(?:\.\d)?)", block)
        if not m:
            continue
        try:
            score = float(m.group(1))
        except ValueError:
            continue
        if not (1.0 <= score <= 10.0):
            continue
        # Match to category key
        for key, phrases in REVIEW_CATEGORY_MAP.items():
            if key not in result:
                for phrase in phrases:
                    if phrase in lower:
                        result[key] = score
                        break
    return result


# ─────────────────────────────────────────────────────────────────────────────
# DETAIL PAGE EXTRACTORS  (_dp_* prefix = detail-page scoped)
# Each function takes `page: Page` and returns isolated field data.
# ─────────────────────────────────────────────────────────────────────────────

async def _dp_coordinates(page: Page) -> tuple[Optional[float], Optional[float]]:
    """Extract lat/lon from JSON-LD schema or meta itemprop tags."""
    # Strategy 1: JSON-LD (most reliable — structured data)
    try:
        blobs = await page.locator("script[type='application/ld+json']").all_inner_texts()
        for blob in blobs:
            try:
                obj = json.loads(blob)
                # Handle both @graph arrays and direct objects
                items = obj.get("@graph", [obj]) if isinstance(obj, dict) else [obj]
                for item in items:
                    geo = item.get("geo", {}) if isinstance(item, dict) else {}
                    lat = geo.get("latitude") or item.get("latitude")
                    lon = geo.get("longitude") or item.get("longitude")
                    if lat and lon:
                        return float(lat), float(lon)
            except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
                continue
    except Exception:
        pass

    # Strategy 2: meta itemprop tags
    try:
        lat_el = page.locator("meta[itemprop='latitude']").first
        lon_el = page.locator("meta[itemprop='longitude']").first
        if await lat_el.count() > 0 and await lon_el.count() > 0:
            lat_str = await lat_el.get_attribute("content")
            lon_str = await lon_el.get_attribute("content")
            if lat_str and lon_str:
                return float(lat_str), float(lon_str)
    except Exception:
        pass

    # Strategy 3: data-atlas-latlng attribute sometimes embedded in map elements
    try:
        el = page.locator("[data-atlas-latlng]").first
        if await el.count() > 0:
            latlng = await el.get_attribute("data-atlas-latlng")
            if latlng and "," in latlng:
                parts = latlng.split(",")
                return float(parts[0].strip()), float(parts[1].strip())
    except Exception:
        pass

    return None, None


async def _dp_address(page: Page) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (full_address, neighborhood).

    Primary: JSON-LD streetAddress — Booking always embeds this, immune to DOM changes.
    Fallback: DOM selectors for older layouts.
    Note: Booking renders address inside a Google Maps button in newer layouts,
    so DOM text selectors are unreliable. JSON-LD is the only stable source.
    """
    # Strategy 1: JSON-LD (most reliable)
    try:
        blobs = await page.locator("script[type='application/ld+json']").all_inner_texts()
        for blob in blobs:
            try:
                obj = json.loads(blob)
                items = obj.get("@graph", [obj]) if isinstance(obj, dict) else [obj]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    addr = item.get("address", {})
                    if isinstance(addr, dict):
                        # streetAddress is the full formatted address
                        street = addr.get("streetAddress", "").strip()
                        if street:
                            neighborhood = addr.get("addressLocality", "").strip() or None
                            return street, neighborhood
                    elif isinstance(addr, str) and addr.strip():
                        return addr.strip(), None
            except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
                continue
    except Exception:
        pass

    # Strategy 2: DOM selectors (older Booking layouts)
    address = await _try_selectors(page, [
        "[data-testid='address']",
        "span[data-testid='address']",
        ".hp_address_subtitle",
        "#showMap2 span.hp_address_subtitle",
        "p.address",
        "[itemprop='streetAddress']",
        "[itemprop='address'] span",
    ])
    neighborhood = await _try_selectors(page, [
        "[data-testid='location-block-title']",
        ".hp_location_block_title",
        "[itemprop='addressLocality']",
        ".bui-breadcrumb__item:last-child",
        ".hp-address-subtitle--neighborhood",
    ])
    return address, neighborhood


async def _dp_description(page: Page) -> Optional[str]:
    """Hotel description / about section."""
    desc = await _try_selectors(page, [
        "[data-testid='property-description']",
        "#property_description_content",
        ".hp_desc_main_content",
        ".hotel_description_wrapper_exp",
        "[data-testid='hotel-description']",
        ".hp-hotel-description",
    ])
    # Clean: strip excessive whitespace
    if desc:
        desc = re.sub(r"\s{3,}", "\n\n", desc.strip())
    return desc


async def _dp_property_info(page: Page) -> dict:
    """
    Check-in/out times, languages spoken, property type.

    Booking changed the DOM layout in 2024/25 — check-in/out times are no longer
    in <dd> tags. They now appear in:
    1. JSON-LD checkinTime / checkoutTime fields (most reliable)
    2. "House Rules" section inner_text — parse label lines like "Check-in: From 14:00"
    3. "Good to know" / property info box
    4. Old DOM selectors as final fallback
    """

    def _norm_time(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        # Handles "14:00", "2:00 PM", "From 14:00", "14:00 – 24:00"
        # Convert 12h to 24h if needed
        m12 = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM)", raw, re.IGNORECASE)
        if m12:
            h, mn, mer = int(m12.group(1)), int(m12.group(2)), m12.group(3).upper()
            if mer == "PM" and h != 12: h += 12
            elif mer == "AM" and h == 12: h = 0
            return f"{h:02d}:{mn:02d}"
        m24 = re.search(r"(\d{1,2}:\d{2})", raw)
        return m24.group(1) if m24 else None

    checkin  = None
    checkout = None

    # Strategy 1: JSON-LD
    try:
        blobs = await page.locator("script[type='application/ld+json']").all_inner_texts()
        for blob in blobs:
            try:
                obj = json.loads(blob)
                items = obj.get("@graph", [obj]) if isinstance(obj, dict) else [obj]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if not checkin and item.get("checkinTime"):
                        checkin = _norm_time(str(item["checkinTime"]))
                    if not checkout and item.get("checkoutTime"):
                        checkout = _norm_time(str(item["checkoutTime"]))
                if checkin and checkout:
                    break
            except Exception:
                continue
    except Exception:
        pass

    # Strategy 2: House Rules / Good to know section inner_text
    # Booking renders "Check-in: From 14:00" as plain text in this block
    if not checkin or not checkout:
        try:
            for section_sel in [
                "[data-testid='property-checkin-checkout-section']",
                "[data-testid='property-info-row']",
                "[data-testid='HouseRules-wrapper']",
                "#hp_policies_box",
                ".c-policy-block",
                "[data-testid='good-to-know-section']",
                ".hp__hotel-details",
            ]:
                section = page.locator(section_sel).first
                if await section.count() == 0:
                    continue
                text = await section.inner_text()
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                for line in lines:
                    ll = line.lower()
                    if not checkin and ("check-in" in ll or "check in" in ll):
                        t = _norm_time(line)
                        if t:
                            checkin = t
                    if not checkout and ("check-out" in ll or "check out" in ll):
                        t = _norm_time(line)
                        if t:
                            checkout = t
                if checkin and checkout:
                    break
        except Exception:
            pass

    # Strategy 3: legacy DOM selectors
    if not checkin:
        raw = await _try_selectors(page, [
            "[data-testid='checkin-time']",
            ".checkin_checkout-time--checkin",
            "dd[data-testid='checkin']",
        ])
        checkin = _norm_time(raw)
    if not checkout:
        raw = await _try_selectors(page, [
            "[data-testid='checkout-time']",
            ".checkin_checkout-time--checkout",
            "dd[data-testid='checkout']",
        ])
        checkout = _norm_time(raw)

    # Languages
    languages: list[str] = []
    for sel in [
        "[data-testid='property-language-spoken'] li",
        ".spoken_languages li",
        "[data-testid='language-list'] li",
        ".languagesList li",
    ]:
        try:
            els = page.locator(sel)
            n   = await els.count()
            for i in range(n):
                txt = (await els.nth(i).inner_text()).strip()
                if txt and txt not in languages:
                    languages.append(txt)
            if languages:
                break
        except Exception:
            continue

    property_type = await _try_selectors(page, [
        "[data-testid='property-type']",
        ".hp__hotel-type-badge",
        "span[data-testid='property-type-badge']",
        "[class*='propertyTypeBadge']",
        ".bui-breadcrumb__item:nth-child(2)",
    ])
    if property_type and len(property_type) > 40:
        property_type = None

    return {
        "checkin_time":  checkin,
        "checkout_time": checkout,
        "languages":     languages,
        "property_type": property_type,
    }


async def _dp_star_rating(page: Page) -> Optional[int]:
    """Star rating from detail page — multiple strategies."""
    for sel in [
        "[data-testid='rating-stars']",
        "[data-testid='star-rating']",
        ".bui-rating",
        "[class*='starRating']",
    ]:
        try:
            container = page.locator(sel).first
            if await container.count() == 0:
                continue
            aria = await container.get_attribute("aria-label")
            spans = await container.locator("span").count()
            result = _parse_star_count(aria, spans)
            if result:
                return result
        except Exception:
            continue

    # SVG title fallback
    try:
        title_el = page.locator("[data-testid='rating-stars'] svg title").first
        if await title_el.count() > 0:
            txt = (await title_el.inner_text()).strip()
            result = _parse_star_count(txt, 0)
            if result:
                return result
    except Exception:
        pass

    return None


async def _dp_facilities(page: Page) -> dict:
    """
    Full facility list, grouped by category.
    Returns {'popular': [...], 'all': {'Category Name': [items...]}}
    """
    popular: list[str] = []
    for sel in [
        "[data-testid='property-most-popular-facilities-wrapper'] span",
        "[data-testid='property-most-popular-facilities-wrapper'] li",
        ".hp-facility-highlights span",
        ".important_facility",
        ".hp_popular_facilities li",
    ]:
        try:
            els = page.locator(sel)
            n   = await els.count()
            for i in range(n):
                txt = (await els.nth(i).inner_text()).strip()
                if txt and txt not in popular and len(txt) < 60:
                    popular.append(txt)
            if popular:
                break
        except Exception:
            continue

    # Full grouped facility list
    all_facilities: dict[str, list[str]] = {}
    try:
        # Each facility group has a heading + list of items
        groups = page.locator(
            "[data-testid='facility-group'], "
            ".hotel-facilities-group, "
            ".facilitiesChecklistSection"
        )
        n_groups = await groups.count()
        for i in range(n_groups):
            try:
                group = groups.nth(i)
                # Group heading
                heading_el = group.locator("h3, h4, [class*='heading'], [class*='title']").first
                heading = (await heading_el.inner_text()).strip() if await heading_el.count() > 0 else f"Group {i+1}"

                # Items in this group
                items: list[str] = []
                item_els = group.locator("li, [data-testid='facility-item'], span[class*='item']")
                n_items  = await item_els.count()
                for j in range(n_items):
                    txt = (await item_els.nth(j).inner_text()).strip()
                    if txt and len(txt) < 80 and txt != heading:
                        items.append(txt)

                if items:
                    all_facilities[heading] = items
            except Exception:
                continue
    except Exception:
        pass

    return {"popular": popular, "all": all_facilities}


async def _dp_room_types(page: Page) -> list[dict]:
    """
    Extract available room types from the rooms/rates table.
    Returns list of {name, beds, size_sqm, max_guests, price_vnd, cancellation}.

    max_guests is the critical field for chatbot queries like
    "có phòng cho 3 người không?" — extracted from occupancy icons
    or text like "2 adults", "Sleeps 3", "×3".
    """
    rooms: list[dict] = []
    seen_names: set[str] = set()

    room_name_selectors = [
        "[data-testid='room-row'] [data-testid='room-name']",
        "[data-testid='roomtype-listing'] h3",
        "table#maxotel_rooms td.ftd a",
        "[data-testid='room-info'] h3",
        ".hprt-roomtype-icon-link",
    ]

    for sel in room_name_selectors:
        try:
            els = page.locator(sel)
            n   = await els.count()
            if n == 0:
                continue
            for i in range(min(n, 10)):
                try:
                    room_name_el = els.nth(i)
                    name = (await room_name_el.inner_text()).strip()
                    if not name or name in seen_names:
                        continue
                    seen_names.add(name)

                    room_row = room_name_el.locator(
                        "xpath=ancestor::tr[1], ancestor::[data-testid='room-row'][1]"
                    ).first

                    room: dict = {
                        "name":         name,
                        "beds":         None,
                        "size_sqm":     None,
                        "max_guests":   None,
                        "price_vnd":    None,
                        "cancellation": None,
                    }

                    if await room_row.count() > 0:
                        row_text = (await room_row.inner_text()).lower()

                        # Bed info
                        m_bed = re.search(
                            r"(\d+\s+(?:single|double|twin|king|queen|bunk)\s+bed[s]?)",
                            row_text, re.IGNORECASE
                        )
                        if m_bed:
                            room["beds"] = m_bed.group(1).strip()

                        # Size
                        m_size = re.search(r"(\d+)\s*m[²2]", row_text)
                        if m_size:
                            room["size_sqm"] = int(m_size.group(1))

                        # Max guests — multiple patterns Booking uses
                        # Pattern 1: "×3", "x3", "x 3" (occupancy icon count)
                        m_x = re.search(r"[×x]\s*(\d+)", row_text)
                        # Pattern 2: "sleeps 3", "max 3 guests", "3 adults", "2 guests"
                        m_guests = re.search(
                            r"(?:sleeps?|max\.?\s*|(?:up to\s+)?)(\d+)\s*(?:guest|adult|person|people|khách)",
                            row_text, re.IGNORECASE
                        )
                        # Pattern 3: occupancy data attribute
                        occ_attr = None
                        try:
                            occ_el = room_row.locator(
                                "[data-testid='occupancy'], "
                                "[class*='occupancy'], "
                                "[data-max-occupancy]"
                            ).first
                            if await occ_el.count() > 0:
                                occ_attr = await occ_el.get_attribute("data-max-occupancy")
                                if not occ_attr:
                                    occ_text = (await occ_el.inner_text()).strip()
                                    m_occ = re.search(r"(\d+)", occ_text)
                                    occ_attr = m_occ.group(1) if m_occ else None
                        except Exception:
                            pass

                        if occ_attr:
                            try:
                                room["max_guests"] = int(occ_attr)
                            except ValueError:
                                pass
                        elif m_guests:
                            room["max_guests"] = int(m_guests.group(1))
                        elif m_x:
                            room["max_guests"] = int(m_x.group(1))

                        # Price
                        price_el = room_row.locator(
                            "[data-testid='price-and-discounted-price'], "
                            ".bui-price-display__value, "
                            "strong.price"
                        ).first
                        if await price_el.count() > 0:
                            price_txt = (await price_el.inner_text()).strip()
                            room["price_vnd"] = _parse_price(price_txt)

                        # Cancellation policy
                        if "free cancellation" in row_text:
                            room["cancellation"] = "free"
                        elif "non-refundable" in row_text or "no refund" in row_text:
                            room["cancellation"] = "non_refundable"
                        elif "partially refundable" in row_text:
                            room["cancellation"] = "partial"

                    rooms.append(room)
                except Exception:
                    continue
            if rooms:
                break
        except Exception:
            continue

    return rooms


async def _dp_rating(page: Page) -> dict:
    """Full rating block from detail page including sub-category scores."""
    # Overall score/label/count — same aria-hidden strategy as card
    score: Optional[float] = None
    label: Optional[str]   = None
    count: Optional[int]   = None

    review_block = page.locator("[data-testid='review-score']").first
    if await review_block.count() > 0:
        try:
            score_el = review_block.locator("[aria-hidden='true']").first
            if await score_el.count() > 0:
                score = _parse_float((await score_el.inner_text()).strip())
        except Exception:
            pass

        try:
            visible = review_block.locator("[aria-hidden='false']").first
            if await visible.count() > 0:
                children = visible.locator("> div")
                n = await children.count()
                if n >= 1:
                    raw = (await children.nth(0).inner_text()).strip()
                    if not re.match(r"^\d", raw):
                        label = raw
                if n >= 2:
                    raw_c = (await children.nth(n-1).inner_text()).strip()
                    m = re.search(r"([\d,]+)\s+reviews?", raw_c, re.IGNORECASE)
                    if m:
                        count = int(m.group(1).replace(",", ""))
        except Exception:
            pass

        if score is None:
            try:
                block_text = (await review_block.inner_text()).strip()
                score, label, count = _parse_review_block(block_text)
            except Exception:
                pass

    # Fallback selectors for score on detail page
    if score is None:
        score_text = await _try_selectors(page, [
            "[data-testid='review-score-right-component'] [aria-hidden='true']",
            ".bui-review-score__badge",
            "[class*='reviewScore'] [class*='badge']",
        ])
        score = _parse_float(score_text)

    # Review category sub-scores
    categories: dict[str, float] = {}
    try:
        sub_blocks = page.locator(
            "[data-testid='review-subscore'], "
            ".review_score_breakdown_row, "
            "[class*='ReviewSubscore'], "
            "[class*='reviewSubscore']"
        )
        n = await sub_blocks.count()
        block_texts: list[str] = []
        for i in range(n):
            try:
                txt = (await sub_blocks.nth(i).inner_text()).strip()
                if txt:
                    block_texts.append(txt)
            except Exception:
                continue
        categories = _parse_review_categories(block_texts)
    except Exception:
        pass

    return {
        "score":              score,
        "label":              label,
        "review_count":       count,
        "review_categories":  categories,
    }


async def _dp_area_info(page: Page) -> list[dict]:
    """
    Nearby points of interest: airports, train stations, beaches, attractions.
    Returns list of {name, distance_km, type}.
    """
    pois: list[dict] = []

    # Booking renders area info as a list of location items
    # Each item has a name and a distance
    SELECTORS = [
        "[data-testid='location-block'] li",
        "[data-testid='surroundings-section'] li",
        ".hp_location_block li",
        ".location_block_wrapper li",
        "[data-testid='nearby-locations'] li",
    ]

    POI_TYPES = {
        "airport": ["airport", "airfield", "aerodrome", "terminal"],
        "train":   ["train", "railway", "station", "rail"],
        "bus":     ["bus", "coach"],
        "beach":   ["beach", "coast", "shore", "bay"],
        "center":  ["city centre", "city center", "downtown", "center"],
        "hospital":["hospital", "clinic", "medical"],
        "restaurant": ["restaurant", "dining", "food"],
        "attraction": ["museum", "temple", "pagoda", "monument", "park", "market"],
    }

    for sel in SELECTORS:
        try:
            els = page.locator(sel)
            n   = await els.count()
            if n == 0:
                continue
            for i in range(min(n, 20)):
                try:
                    txt = (await els.nth(i).inner_text()).strip()
                    if not txt or len(txt) > 120:
                        continue

                    poi: dict = {"name": txt, "distance_km": None, "type": "other"}

                    # Extract distance
                    m = re.search(r"(\d+(?:\.\d+)?)\s*(km|m\b|mile)", txt, re.IGNORECASE)
                    if m:
                        dist_val = float(m.group(1))
                        unit = m.group(2).lower()
                        if unit == "m":
                            dist_val = round(dist_val / 1000, 2)
                        elif "mile" in unit:
                            dist_val = round(dist_val * 1.609, 2)
                        poi["distance_km"] = dist_val

                    # Classify type
                    lower = txt.lower()
                    for poi_type, keywords in POI_TYPES.items():
                        if any(kw in lower for kw in keywords):
                            poi["type"] = poi_type
                            break

                    pois.append(poi)
                except Exception:
                    continue
            if pois:
                break
        except Exception:
            continue

    return pois


async def _dp_taxes_fees(page: Page) -> Optional[str]:
    """Extract taxes and fees note from detail page."""
    return await _try_selectors(page, [
        "[data-testid='excluded-charges']",
        ".prd-taxes-and-fees-under-price",
        ".fee-table",
        "[data-testid='property-fee-details']",
        ".taxesandfees_disc",
        "[data-testid='taxes-and-charges']",
        "[class*='taxExcl']",
    ])


async def _dp_images(page: Page, max_images: int = 20) -> list[str]:
    """
    Collect full-resolution image URLs from the detail page gallery.
    Detail page has many more images than the search card (which shows 1–3).
    Prefers large/original sizes over thumbnails.
    """
    images: list[str] = []
    seen: set[str] = set()

    SELECTORS = [
        "[data-testid='bh-photo-modal-grid-item-img']",
        "[data-testid='photo-grid-item'] img",
        ".bh-photo-grid img",
        "#photos_distinct img",
        ".hp-gallery-photos img",
        "[data-testid='hp-gallery'] img",
        "a.bh-photo-grid-image img",
        "img[data-testid='gallery-photo']",
        ".fotorama__img",
    ]

    for sel in SELECTORS:
        try:
            els = page.locator(sel)
            n   = await els.count()
            for i in range(n):
                try:
                    el  = els.nth(i)
                    src = (
                        await el.get_attribute("src")
                        or await el.get_attribute("data-src")
                        or await el.get_attribute("data-original")
                        or await el.get_attribute("data-lazy-src")
                    )
                    if not src or not src.startswith("http"):
                        continue
                    # Upgrade thumbnail URLs to full-size:
                    # Booking thumbnail: square240 / square60 / max300 → max1280 / max500
                    src = re.sub(r"square\d+", "max1280", src)
                    src = re.sub(r"max\d+", "max1280", src)
                    if src not in seen:
                        seen.add(src)
                        images.append(src)
                    if len(images) >= max_images:
                        break
                except Exception:
                    continue
        except Exception:
            continue
        if len(images) >= max_images:
            break

    return images


async def _dp_pricing(page: Page) -> dict:
    """
    Prices shown on the detail page — often more accurate than the card.
    Also extracts meal plan info visible on the detail page.
    """
    current_text = await _try_selectors(page, [
        "[data-testid='price-and-discounted-price']",
        ".bui-price-display__value",
        ".hprt-price-price",
        "strong.price",
        "[class*='finalPrice']",
    ])
    current_price = _parse_price(current_text)

    # Struck-through original price
    original_price: Optional[float] = None
    for sel in ["s", "del", "[class*='crossedOutPrice']", "[class*='originalPrice']",
                ".bui-price-display__original"]:
        try:
            els = page.locator(sel)
            n   = await els.count()
            for i in range(n):
                txt = (await els.nth(i).inner_text()).strip()
                val = _parse_price(txt)
                if val and val > 0:
                    original_price = val
                    break
            if original_price:
                break
        except Exception:
            continue

    if original_price and current_price and original_price <= current_price:
        original_price = None

    meal_plan = await _try_selectors(page, [
        "[data-testid='meal-plan']",
        ".meal_plan_text",
        "[class*='mealPlan']",
        "[data-testid='room-listing-meal-included']",
    ])

    taxes_text = await _dp_taxes_fees(page)
    taxes_included: Optional[bool] = None
    if taxes_text:
        lower = taxes_text.lower()
        if "excl" in lower or "+ taxes" in lower or "not included" in lower:
            taxes_included = False
        elif "incl" in lower or "included" in lower:
            taxes_included = True

    return {
        "price_per_night_vnd": current_price,
        "original_price_vnd":  original_price,
        "discount_pct":        _compute_discount(original_price, current_price),
        "currency":            "VND",
        "taxes_included":      taxes_included,
        "taxes_fees_text":     taxes_text,
        "meal_plan":           meal_plan,
    }


# ─────────────────────────────────────────────────────────────────────────────
# DEEP SCRAPE — HOTEL DETAIL PAGE  (complete rewrite for v5)
# ─────────────────────────────────────────────────────────────────────────────

async def deep_scrape_hotel(page: Page, hotel: dict) -> dict:
    """
    Visit the hotel detail page and populate ALL rich fields that the
    search card cannot provide.

    Fields populated:
      location.address, location.neighborhood, location.area_info,
      location.latitude, location.longitude
      detail.description, detail.checkin_time, detail.checkout_time,
      detail.languages, detail.property_type
      property.star_rating, property.room_types
      pricing (full: current, original, discount_pct, taxes, meal_plan)
      rating (score, label, review_count, review_categories)
      facilities.popular, facilities.all
      images (up to 20 full-resolution)

    The function is structured as a set of isolated _dp_* calls so that
    a failure in one field group never aborts the others.
    """
    url = hotel.get("url")
    if not url:
        return hotel

    await human_delay(2.5, 5.0)
    logger.info(f"    ↳ detail: {hotel.get('name', url)[:50]}")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=35000)
        # Wait for a key element that confirms the detail page loaded
        await page.wait_for_selector(
            "[data-testid='rating-stars'], "
            "[data-testid='property-description'], "
            "#hp_hotel_name, "
            "h2.hp__hotel-name",
            timeout=15000,
        )
    except PWTimeout:
        logger.warning(f"    ↳ timeout loading detail page: {url}")
        return hotel
    except Exception as e:
        logger.warning(f"    ↳ failed to load detail page: {e}")
        return hotel

    await handle_cookie_popup(page)
    # Scroll to trigger lazy-loaded content (facilities, reviews, area info)
    await human_scroll(page)
    await human_delay(0.8, 1.5)

    # ── Coordinates ───────────────────────────────────────────────────────────
    try:
        lat, lon = await _dp_coordinates(page)
        hotel["location"]["latitude"]  = lat
        hotel["location"]["longitude"] = lon
    except Exception as e:
        logger.debug(f"dp_coordinates: {e}")

    # ── Address ───────────────────────────────────────────────────────────────
    try:
        address, _nbhd = await _dp_address(page)
        if address:
            hotel["location"]["address"] = address
    except Exception as e:
        logger.debug(f"dp_address: {e}")

    # ── Description ──────────────────────────────────────────────────────────
    try:
        hotel["detail"]["description"] = await _dp_description(page)
    except Exception as e:
        logger.debug(f"dp_description: {e}")

    # ── Property info (type, checkin, checkout, languages) ────────────────────
    try:
        prop_info = await _dp_property_info(page)
        hotel["detail"]["checkin_time"]  = prop_info["checkin_time"]
        hotel["detail"]["checkout_time"] = prop_info["checkout_time"]
        hotel["detail"]["languages"]     = prop_info["languages"]
        if prop_info["property_type"]:
            hotel["property"]["type"] = prop_info["property_type"]
    except Exception as e:
        logger.debug(f"dp_property_info: {e}")

    # ── Star rating ───────────────────────────────────────────────────────────
    try:
        stars = await _dp_star_rating(page)
        if stars:
            hotel["property"]["star_rating"] = stars
    except Exception as e:
        logger.debug(f"dp_stars: {e}")

    # ── Facilities ────────────────────────────────────────────────────────────
    try:
        facilities = await _dp_facilities(page)
        hotel["facilities"] = facilities
        # Merge popular facilities into amenities list (add any new ones)
        if facilities.get("popular"):
            hotel["amenities"] = list(set(hotel.get("amenities", []) +
                                         [f.lower().replace(" ", "_")
                                          for f in facilities["popular"]
                                          if len(f) < 40]))
    except Exception as e:
        logger.debug(f"dp_facilities: {e}")

    # ── Room types ────────────────────────────────────────────────────────────
    try:
        hotel["property"]["room_types"] = await _dp_room_types(page)
    except Exception as e:
        logger.debug(f"dp_rooms: {e}")

    # ── Pricing (detail page — more accurate) ─────────────────────────────────
    try:
        dp_pricing = await _dp_pricing(page)
        # Only override card pricing if detail page has values
        if dp_pricing.get("price_per_night_vnd"):
            hotel["pricing"].update(dp_pricing)
        elif dp_pricing.get("taxes_fees_text"):
            hotel["pricing"]["taxes_fees_text"] = dp_pricing["taxes_fees_text"]
    except Exception as e:
        logger.debug(f"dp_pricing: {e}")

    # ── Rating with category sub-scores ──────────────────────────────────────
    try:
        dp_rating = await _dp_rating(page)
        # Merge — fill any gaps from card scrape
        if dp_rating.get("score") is not None:
            hotel["rating"]["score"] = dp_rating["score"]
        if dp_rating.get("label"):
            hotel["rating"]["label"] = dp_rating["label"]
        if dp_rating.get("review_count") is not None:
            hotel["rating"]["review_count"] = dp_rating["review_count"]
        hotel["rating"]["review_categories"] = dp_rating.get("review_categories", {})
    except Exception as e:
        logger.debug(f"dp_rating: {e}")

    # ── Images (full gallery, up to 20) ───────────────────────────────────────
    try:
        dp_images = await _dp_images(page)
        if dp_images:
            hotel["images"] = dp_images
    except Exception as e:
        logger.debug(f"dp_images: {e}")

    return hotel


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER / CONTEXT FACTORY
# ─────────────────────────────────────────────────────────────────────────────

async def create_browser_context(
    playwright,
    proxy: Optional[str] = None,
) -> tuple[Browser, BrowserContext]:
    """Launch Chromium with anti-detection flags and a realistic browsing context."""
    ua = random.choice(USER_AGENTS)

    launch_kwargs: dict = {
        "headless": True,
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--disable-extensions",
            "--disable-gpu",
            "--window-size=1920,1080",
            "--lang=en-US",
        ],
    }
    if proxy:
        launch_kwargs["proxy"] = {"server": proxy}

    browser = await playwright.chromium.launch(**launch_kwargs)
    context = await browser.new_context(
        user_agent=ua,
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
        timezone_id="Asia/Ho_Chi_Minh",
        geolocation={"latitude": 10.8231, "longitude": 106.6297},
        permissions=["geolocation"],
        extra_http_headers={
            "Accept-Language":           "en-US,en;q=0.9",
            "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding":           "gzip, deflate, br",
            "DNT":                       "1",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest":            "document",
            "Sec-Fetch-Mode":            "navigate",
            "Sec-Fetch-Site":            "none",
            "Sec-Fetch-User":            "?1",
        },
    )
    return browser, context


async def new_stealth_page(context: BrowserContext) -> Page:
    """Create a new page with stealth patches applied per-page (not per-context)."""
    page = await context.new_page()
    if HAS_STEALTH:
        await stealth_async(page)
    return page


# ─────────────────────────────────────────────────────────────────────────────
# HUMAN BEHAVIOUR HELPERS
# ─────────────────────────────────────────────────────────────────────────────

async def human_delay(min_s: float = 1.5, max_s: float = 4.0) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))


async def human_scroll(page: Page) -> None:
    """Progressive scroll with short pauses, plus a small scroll-back at the end."""
    total = random.randint(4, 7)
    for i in range(total):
        amount = random.randint(250, 600) if i < total - 1 else random.randint(100, 300)
        await page.evaluate(f"window.scrollBy(0, {amount})")
        await asyncio.sleep(random.uniform(0.2, 0.7))
    # Scroll back up slightly — real users do this
    await page.evaluate(f"window.scrollBy(0, -{random.randint(200, 500)})")
    await asyncio.sleep(random.uniform(0.3, 0.6))


async def handle_cookie_popup(page: Page) -> None:
    SELECTORS = [
        "button#onetrust-accept-btn-handler",
        "button[data-gdpr-consent='accept']",
        "#accept-cookies",
        "[data-testid='accept-cookies-button']",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "button:has-text('I agree')",
        "button:has-text('Agree')",
    ]
    for sel in SELECTORS:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=2000):
                await btn.click()
                logger.debug("Cookie popup dismissed")
                await human_delay(0.5, 1.2)
                return
        except Exception:
            continue


async def handle_captcha_check(page: Page) -> bool:
    title   = (await page.title()).lower()
    url     = page.url.lower()
    signals = ["captcha", "robot", "blocked", "access denied", "403", "unusual traffic"]
    return any(s in title or s in url for s in signals)


# ─────────────────────────────────────────────────────────────────────────────
# URL BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_search_url(city: str, checkin: str, checkout: str, offset: int = 0) -> str:
    ci = datetime.strptime(checkin,  "%Y-%m-%d")
    co = datetime.strptime(checkout, "%Y-%m-%d")
    params = {
        "ss":                f"{city}, Vietnam",
        "checkin_year":      ci.year,
        "checkin_month":     ci.month,
        "checkin_monthday":  ci.day,
        "checkout_year":     co.year,
        "checkout_month":    co.month,
        "checkout_monthday": co.day,
        "group_adults":      2,
        "no_rooms":          1,
        "group_children":    0,
        "offset":            offset,
        "lang":              "en-us",
        "currency":          "VND",
    }
    return f"{BASE_SEARCH_URL}?{urlencode(params)}"


# ─────────────────────────────────────────────────────────────────────────────
# PAGE SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

# All known property-card selectors across Booking's A/B layout variants.
# The scraper tries them in order and uses whichever returns results.
CARD_SELECTORS = [
    "[data-testid='property-card']",
    ".sr_property_block",
    ".a826ba81c4",
    "[data-hotelid]",
]


async def scrape_search_page(
    page: Page,
    city: str,
    checkin: str,
    checkout: str,
    offset: int = 0,
    deep_scrape: bool = False,
    context: Optional[BrowserContext] = None,
) -> list[dict]:
    """
    Two-phase scrape of one search-result page.

    Phase 1 — stay on the search page and collect ALL card data.
    Phase 2 — open each hotel detail page in its own NEW TAB (context.new_page()),
              extract rich fields, then close that tab.
              The search results page is never navigated away from.
              This fixes the stale-locator bug that caused 0% detail fields
              in all prior versions.
    """
    url = build_search_url(city, checkin, checkout, offset)
    logger.info(f"  → offset={offset}: {url[:90]}...")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=35000)
    except PWTimeout:
        logger.warning("  Page load timeout — proceeding with partial DOM")

    await handle_cookie_popup(page)

    if await handle_captcha_check(page):
        logger.error("  ✗ Bot-check triggered. Rotate proxy or increase delays.")
        return []

    card_appeared = False
    for sel in CARD_SELECTORS:
        try:
            await page.wait_for_selector(sel, timeout=12000)
            card_appeared = True
            break
        except PWTimeout:
            continue

    if not card_appeared:
        logger.warning("  No property cards found — empty page or layout change.")
        return []

    await human_scroll(page)
    await human_delay(0.8, 1.8)

    cards_locator: Optional[Locator] = None
    total = 0
    for sel in CARD_SELECTORS:
        loc = page.locator(sel)
        n   = await loc.count()
        if n > 0:
            cards_locator = loc
            total         = n
            logger.info(f"  → {n} cards (selector: {sel!r})")
            break

    if cards_locator is None:
        logger.warning("  No cards matched any known selector.")
        return []

    # ── Phase 1: collect all card data without leaving the search page ────────
    hotels: list[dict] = []
    for i in range(total):
        try:
            card  = cards_locator.nth(i)
            hotel = await parse_hotel_card(card)
            hotel["location"]["city"] = city
            hotels.append(hotel)
        except Exception as e:
            logger.warning(f"  Card {i} error: {e}")

    logger.info(f"  Phase 1 complete: {len(hotels)} cards")

    # ── Phase 2: deep scrape each hotel in its own fresh tab ─────────────────
    if deep_scrape and context is not None:
        enriched = failed = 0
        for i, hotel in enumerate(hotels):
            if not hotel.get("url"):
                continue
            detail_page: Optional[Page] = None
            try:
                await human_delay(1.5, 3.5)
                detail_page = await context.new_page()
                if HAS_STEALTH:
                    await stealth_async(detail_page)
                hotels[i] = await deep_scrape_hotel(detail_page, hotel)
                enriched += 1
            except Exception as e:
                failed += 1
                logger.debug(f"  detail {hotel.get('name','?')[:30]}: {e}")
            finally:
                if detail_page:
                    try:
                        await detail_page.close()
                    except Exception:
                        pass
        logger.info(f"  Phase 2 complete: {enriched} enriched, {failed} failed")

    return hotels


# ─────────────────────────────────────────────────────────────────────────────
# CITY SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_city(
    city: str,
    checkin: str,
    checkout: str,
    max_pages: int = 5,
    proxy: Optional[str] = None,
    deep_scrape: bool = False,
) -> list[dict]:
    """Scrape all result pages for one city using a fresh browser session."""
    all_hotels: list[dict] = []

    async with async_playwright() as pw:
        browser, context = await create_browser_context(pw, proxy=proxy)
        page = await new_stealth_page(context)

        # Warm-up: land on homepage first (more human-like entry point)
        try:
            await page.goto(BASE_ORIGIN, wait_until="domcontentloaded", timeout=20000)
            await handle_cookie_popup(page)
            await human_delay(2.0, 4.0)
        except Exception:
            pass

        for page_num in range(max_pages):
            offset = page_num * 25
            logger.info(f"  [{city}] Page {page_num + 1}/{max_pages} | offset={offset}")

            hotels = await scrape_search_page(
                page, city, checkin, checkout,
                offset=offset,
                deep_scrape=deep_scrape,
                context=context,
            )

            if not hotels:
                logger.info(f"  [{city}] No results at offset {offset} — stopping.")
                break

            all_hotels.extend(hotels)
            logger.info(f"  [{city}] +{len(hotels)} | total: {len(all_hotels)}")

            if page_num < max_pages - 1:
                await human_delay(3.0, 7.0)

        await browser.close()

    return all_hotels


# ─────────────────────────────────────────────────────────────────────────────
# PROXY HELPER
# ─────────────────────────────────────────────────────────────────────────────

def get_proxy(index: int) -> Optional[str]:
    if not PROXIES:
        return None
    return PROXIES[index % len(PROXIES)]


# ─────────────────────────────────────────────────────────────────────────────
# TOP-LEVEL ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_vietnam(
    checkin: str,
    checkout: str,
    cities: Optional[list[str]] = None,
    n_cities: int = 8,
    max_pages_per_city: int = 1,
    deep_scrape: bool = True,
    random_cities: bool = True,
) -> list[dict]:
    """
    Scrape Vietnamese tourism cities.

    Args:
        checkin:             Check-in date "YYYY-MM-DD"
        checkout:            Check-out date "YYYY-MM-DD"
        cities:              Explicit city list — overrides random sampling.
        n_cities:            Number of cities to randomly sample from
                             VIETNAM_CITIES (used when cities=None).
        max_pages_per_city:  Search result pages per city (25 hotels each).
                             Default 1 — with deep_scrape=True each hotel
                             takes ~5s extra, so 1 page × 25 hotels × n_cities
                             gives a manageable run time.
        deep_scrape:         Visit each hotel's detail page.
                             Default True — this is how we get description,
                             room types, facilities, review sub-scores,
                             coordinates, taxes, and full images.
        random_cities:       If True and cities=None, randomly sample n_cities
                             from VIETNAM_CITIES each run.
    """
    if cities is not None:
        target_cities = cities
    elif random_cities:
        target_cities = random.sample(VIETNAM_CITIES, min(n_cities, len(VIETNAM_CITIES)))
        logger.info(f"Randomly selected cities: {target_cities}")
    else:
        target_cities = VIETNAM_CITIES[:n_cities]

    all_hotels: list[dict] = []

    for i, city in enumerate(target_cities):
        logger.info(f"\n{'═' * 60}")
        logger.info(f"[{i + 1}/{len(target_cities)}] {city}")

        proxy = get_proxy(i)
        if proxy:
            logger.info(f"  Proxy: ...{proxy.split('@')[-1]}")

        try:
            hotels = await scrape_city(
                city=city,
                checkin=checkin,
                checkout=checkout,
                max_pages=max_pages_per_city,
                proxy=proxy,
                deep_scrape=deep_scrape,
            )
            all_hotels.extend(hotels)
            logger.info(f"  ✓ {city}: {len(hotels)} hotels")
        except Exception as e:
            logger.error(f"  ✗ {city} failed: {e}", exc_info=True)

        if i < len(target_cities) - 1:
            delay = random.uniform(6.0, 14.0)
            logger.info(f"  Cooldown: {delay:.1f}s")
            await asyncio.sleep(delay)

    return all_hotels


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSTIC MODE  — run: python scraper_playwright_v4.py --diagnose
# ─────────────────────────────────────────────────────────────────────────────

async def run_diagnostics(checkin: str = "2026-05-01", checkout: str = "2026-05-03") -> None:
    """
    Loads one search results page and dumps the outerHTML + inner_text of the
    first hotel card to diagnostic_card.html / diagnostic_card.txt.

    Use this whenever fields go null after a Booking.com deployment:
      1. Run:  python scraper_playwright_v4.py --diagnose
      2. Open: diagnostic_card.html in browser DevTools or a text editor
      3. Search for the text near rating/address/badges to find new selectors
      4. Update the relevant _extract_* function selector list
    """
    logger.info("=== DIAGNOSTIC MODE ===")
    logger.info("Loading one search page and dumping first card HTML...")

    async with async_playwright() as pw:
        browser, context = await create_browser_context(pw)
        page = await new_stealth_page(context)

        try:
            await page.goto(BASE_ORIGIN, wait_until="domcontentloaded", timeout=20000)
            await handle_cookie_popup(page)
            await human_delay(2.0, 3.0)
        except Exception:
            pass

        url = build_search_url("Ho Chi Minh City", checkin, checkout, offset=0)
        await page.goto(url, wait_until="domcontentloaded", timeout=35000)
        await handle_cookie_popup(page)

        for sel in CARD_SELECTORS:
            try:
                await page.wait_for_selector(sel, timeout=12000)
                break
            except PWTimeout:
                continue

        await human_scroll(page)
        await human_delay(1.0, 2.0)

        dumped = False
        for sel in CARD_SELECTORS:
            loc = page.locator(sel)
            if await loc.count() > 0:
                card = loc.first
                html = await card.evaluate("el => el.outerHTML")
                text = await card.inner_text()

                with open("diagnostic_card.html", "w", encoding="utf-8") as f:
                    f.write(f"<!-- Scraped: {datetime.utcnow().isoformat()} -->\n")
                    f.write(html)
                with open("diagnostic_card.txt", "w", encoding="utf-8") as f:
                    f.write(f"# inner_text dump — {datetime.utcnow().isoformat()}\n\n")
                    f.write(text)

                logger.info("✓ diagnostic_card.html — full card HTML")
                logger.info("✓ diagnostic_card.txt  — inner_text of card")
                logger.info("")
                logger.info("NEXT STEPS:")
                logger.info("  1. Search 'review-score' in diagnostic_card.html")
                logger.info("     → find the class names of its child divs")
                logger.info("     → update _extract_rating selectors")
                logger.info("  2. Search 'address' / 'district' / 'street' text")
                logger.info("     → find its parent element's data-testid or class")
                logger.info("     → update _extract_location selectors")
                logger.info("  3. Search badge/benefit text like 'cancellation'")
                logger.info("     → find its container data-testid")
                logger.info("     → update _extract_badges selectors")
                dumped = True
                break

        if not dumped:
            logger.error("Could not find any hotel card. Page may be blocked.")
            full_html = await page.content()
            with open("diagnostic_full_page.html", "w", encoding="utf-8") as f:
                f.write(full_html)
            logger.info("Saved full page HTML → diagnostic_full_page.html")

        await browser.close()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    parser = argparse.ArgumentParser(
        description="Booking.com Vietnam Hotel Scraper — Final Edition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
# Daily Airflow run (auto rotation, saves to ./data/)
  python scraper_final.py --data-dir ./data

# Test specific cities
  python scraper_final.py --cities "Ninh Binh" "Quang Binh" --pages 2

# Card-only, fast
  python scraper_final.py --no-deep --pages 1

# Fix CSV after Excel lock error
  python scraper_final.py --rebuild-csv

Airflow BashOperator
--------------------
  python /opt/scraper/scraper_final.py \\
      --checkin  {{ ds }} \\
      --checkout {{ macros.ds_add(ds, 2) }} \\
      --data-dir /data/booking
""",
    )
    parser.add_argument("--checkin",     default="2026-05-01")
    parser.add_argument("--checkout",    default="2026-05-03")
    parser.add_argument("--cities",      nargs="+", metavar="CITY",
                        help="Explicit cities — skips auto rotation")
    parser.add_argument("--n-cities",    type=int, default=6,
                        help="Cities per run using auto rotation (default 6)")
    parser.add_argument("--pages",       type=int, default=1,
                        help="Search result pages per city (default 1 ≈ 25 hotels)")
    parser.add_argument("--no-deep",     action="store_true",
                        help="Skip detail page scraping (card data only, faster)")
    parser.add_argument("--data-dir",    default=".",
                        help="Directory for hotels_vietnam_all.csv/.jsonl")
    parser.add_argument("--rebuild-csv", action="store_true",
                        help="Rebuild CSV from JSONL then exit")
    parser.add_argument("--diagnose",    action="store_true",
                        help="Dump first card HTML for selector debugging")
    args = parser.parse_args()

    if args.diagnose:
        asyncio.run(run_diagnostics())
        sys.exit(0)

    store = DataStore(data_dir=args.data_dir)

    if args.rebuild_csv:
        n = store.rebuild_csv()
        logger.info(f"Done. {n} records → {store.csv_path}")
        sys.exit(0)

    today         = date.today().isoformat()
    target_cities = args.cities if args.cities else pick_today_cities(args.n_cities)

    results = asyncio.run(
        scrape_vietnam(
            checkin            = args.checkin,
            checkout           = args.checkout,
            cities             = target_cities,
            max_pages_per_city = args.pages,
            deep_scrape        = not args.no_deep,
            random_cities      = False,
        )
    )

    if not results:
        logger.warning("No results scraped — check for bot detection or network issues.")
        sys.exit(0)

    stats = store.upsert(results, scrape_date=today)
    st    = store.stats()

    logger.info(f"\n{'═' * 60}")
    logger.info(f"✓ DataStore updated")
    logger.info(f"  Inserted : {stats['inserted']}  |  Updated: {stats['updated']}  |  Total: {stats['total']}")
    logger.info(f"  CSV  → {store.csv_path}")
    logger.info(f"  JSONL→ {store.jsonl_path}")
    logger.info(f"  Date range : {st.get('dates', '—')}")
    logger.info(f"  All cities : {st.get('cities', [])}")

    total = len(results)
    pct   = lambda n: f"{100 * n // total}%" if total else "0%"

    def count_field(path: str) -> int:
        keys = path.split(".")
        n = 0
        for h in results:
            v = h
            try:
                for k in keys:
                    v = v[k]
                if v is not None and v != [] and v != {}:
                    n += 1
            except (KeyError, TypeError):
                pass
        return n

    logger.info(f"\n  ── Today: {total} hotels, cities: {sorted({h['location']['city'] for h in results if h['location']['city']})}")
    logger.info(f"  ── Basic (search card) ──────────────────────────")
    logger.info(f"  name               : {count_field('name')}/{total}  ({pct(count_field('name'))})")
    logger.info(f"  price_per_night_vnd: {count_field('pricing.price_per_night_vnd')}/{total}  ({pct(count_field('pricing.price_per_night_vnd'))})")
    logger.info(f"  rating.score       : {count_field('rating.score')}/{total}  ({pct(count_field('rating.score'))})")
    logger.info(f"  availability_status: {count_field('property.availability_status')}/{total}  ({pct(count_field('property.availability_status'))})")
    logger.info(f"  rooms_left         : {count_field('property.rooms_left')}/{total}  (urgency badge only — not all hotels show this)")
    logger.info(f"  ── Detail page fields ───────────────────────────")
    logger.info(f"  location.address   : {count_field('location.address')}/{total}  ({pct(count_field('location.address'))})")
    logger.info(f"  location.latitude  : {count_field('location.latitude')}/{total}  ({pct(count_field('location.latitude'))})")
    logger.info(f"  distance_center    : {count_field('location.distance_from_center')}/{total}  ({pct(count_field('location.distance_from_center'))})")
    logger.info(f"  detail.description : {count_field('detail.description')}/{total}  ({pct(count_field('detail.description'))})")
    logger.info(f"  detail.checkin_time: {count_field('detail.checkin_time')}/{total}  ({pct(count_field('detail.checkin_time'))})")
    logger.info(f"  detail.checkout    : {count_field('detail.checkout_time')}/{total}  ({pct(count_field('detail.checkout_time'))})")
    logger.info(f"  property.room_types: {count_field('property.room_types')}/{total}  ({pct(count_field('property.room_types'))})")
    logger.info(f"  property.star_rating:{count_field('property.star_rating')}/{total}  ({pct(count_field('property.star_rating'))})")
    logger.info(f"  review_categories  : {count_field('rating.review_categories')}/{total}  ({pct(count_field('rating.review_categories'))})")
    logger.info(f"  facilities.popular : {count_field('facilities.popular')}/{total}  ({pct(count_field('facilities.popular'))})")
    logger.info(f"  images             : {count_field('images')}/{total}  ({pct(count_field('images'))})")
    avg_imgs = sum(len(h.get("images", [])) for h in results) / total
    logger.info(f"  avg images/hotel   : {avg_imgs:.1f}")

    low = [f for f in ["name", "pricing.price_per_night_vnd", "location.address",
                        "detail.description", "rating.score", "detail.checkin_time"]
           if count_field(f) < total // 2]
    if low:
        logger.warning(f"  Still low: {low} — run --diagnose and check detail page HTML")
