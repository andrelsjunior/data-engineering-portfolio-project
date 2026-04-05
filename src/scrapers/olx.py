"""
OLX real estate listing scraper.

Usage:
    python -m src.scrapers.olx <url> [--output listings.json] [--dedup] [--no-polite]

Example:
    python -m src.scrapers.olx \
        "https://www.olx.com.br/imoveis/venda/estado-es/norte-do-espirito-santo/vitoria/fradinhos?sf=1" \
        --output data/fradinhos.json --dedup

Deduplication
-------------
OLX sellers routinely re-post the same property daily to stay at the top of
results, generating many listings with identical physical attributes but
different ad IDs and slightly varying prices.

A "property fingerprint" is (area_m2, rooms, bathrooms, garage_spots).
When --dedup is enabled, only the most recent listing per fingerprint is kept.
All other variants are attached as `price_history` so the price evolution is
not lost.
"""

from __future__ import annotations

import json
import logging
import random
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

logger = logging.getLogger(__name__)

PAGE_SIZE = 50  # OLX shows 50 listings per page

# ---------------------------------------------------------------------------
# Anti-blocking constants
# ---------------------------------------------------------------------------

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
]

BLOCK_SIGNALS = [
    "access denied", "cloudflare", "just a moment",
    "verificação de segurança", "please verify", "captcha",
]

DETAIL_BATCH_SIZE = 10
DETAIL_INTRA_DELAY_MS = (3000, 5000)   # between individual detail pages
DETAIL_INTER_BATCH_SECS = (25, 45)     # between batches of 10

# ---------------------------------------------------------------------------
# Anti-blocking helpers
# ---------------------------------------------------------------------------

def _human_delay(base_ms: int = 2500, jitter_ms: int = 2000) -> None:
    """Sleep for base_ms ± jitter_ms/2, minimum 0.5s."""
    delay = (base_ms + random.uniform(-jitter_ms / 2, jitter_ms / 2)) / 1000
    time.sleep(max(0.5, delay))


def _is_blocked(page: Page) -> bool:
    """Return True if the page looks like a CAPTCHA / bot-challenge page."""
    try:
        title = (page.title() or "").lower()
        body = page.evaluate("() => document.body.innerText.slice(0, 2000)").lower()
        return any(signal in title or signal in body for signal in BLOCK_SIGNALS)
    except Exception:
        return False


def _has_listings(page: Page) -> bool:
    """Return True if at least one olx-adcard element is present in the DOM."""
    try:
        return page.evaluate(
            "() => document.querySelectorAll('[class*=\"olx-adcard\"]').length > 0"
        )
    except Exception:
        return False


def _simulate_reading(page: Page) -> None:
    """Scroll down and back up to trigger lazy-load and mimic human scanning."""
    try:
        page.mouse.wheel(0, 600)
        time.sleep(random.uniform(0.3, 0.7))
        page.mouse.wheel(0, 800)
        time.sleep(random.uniform(0.3, 0.6))
        page.mouse.wheel(0, -1400)
    except Exception:
        pass


def _build_context(browser: Browser) -> BrowserContext:
    """Create a hardened browser context with human-like fingerprint."""
    viewport = random.choice(VIEWPORTS)
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport=viewport,
        locale="pt-BR",
        timezone_id="America/Sao_Paulo",
        color_scheme="light",
        extra_http_headers={
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        },
    )
    # Patch navigator properties exposed by Playwright's CDP automation
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en'] });
    """)
    return context


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _property_fingerprint(listing: dict) -> tuple:
    """Stable key that identifies the same physical property across re-listings."""
    return (
        listing.get("area_m2"),
        str(listing.get("rooms")),
        str(listing.get("bathrooms")),
        str(listing.get("garage_spots")),
    )


def deduplicate(listings: list[dict]) -> list[dict]:
    """
    Collapse re-listings of the same property into a single record.

    Strategy:
    - Group by (area_m2, rooms, bathrooms, garage_spots).
    - Keep the most recent listing as the canonical record (OLX returns
      results newest-first, so index 0 of each group is the most recent).
    - Attach all observed prices as `price_history` (sorted desc by date)
      so downstream models can track price evolution.
    - Add `listing_count` (how many times the property was posted).

    # TODO: improve dedup signal — current fingerprint (area_m2, rooms, bathrooms, garage_spots)
    # misses same-property re-listings where area drifts between posts.
    # Option B: bucket area ±10m² + price within ±15%
    # Option C: OLX URL slug prefix (strip trailing numeric ad_id from URL)
    # Combined B+C: any 2-of-3 signals matching → duplicate
    # Long-term: CEP + image dhash from scrape_detail_pages()
    """
    from collections import defaultdict

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for listing in listings:
        groups[_property_fingerprint(listing)].append(listing)

    deduped = []
    for fp, group in groups.items():
        canonical = group[0]  # most recent (OLX default sort = newest first)
        canonical["listing_count"] = len(group)
        canonical["price_history"] = [
            {"price_brl": l["price_brl"], "date_listed": l["date_listed"], "url": l["url"]}
            for l in group
            if l["price_brl"] is not None
        ]
        deduped.append(canonical)

    removed = len(listings) - len(deduped)
    logger.info(
        "Dedup: %d → %d unique properties (%d duplicates removed)",
        len(listings), len(deduped), removed,
    )
    return deduped


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_price(text: str) -> int | None:
    """Extract first R$ price from text, returning integer BRL cents-free."""
    match = re.search(r"R\$\s*([\d.]+(?:,\d{2})?)", text)
    if not match:
        return None
    raw = match.group(1).replace(".", "").replace(",", ".")
    try:
        value = int(float(raw))
        # Sanity check: prices below R$10k are almost certainly parsing errors
        return value if value >= 10_000 else None
    except ValueError:
        return None


def _parse_int(text: str | None) -> int | None:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def _add_page_param(url: str, page: int) -> str:
    """Return URL with &o=<page> appended (page 1 = no param)."""
    if page <= 1:
        return url
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    qs["o"] = [str(page)]
    new_query = urlencode({k: v[0] for k, v in qs.items()})
    return urlunparse(parsed._replace(query=new_query))


# ---------------------------------------------------------------------------
# Core extraction — single page
# ---------------------------------------------------------------------------

def _extract_cards(playwright_page: Page) -> list[dict]:
    """Extract all listing cards from the current page."""
    raw = playwright_page.evaluate("""() => {
        const results = [];
        const cards = document.querySelectorAll('[class*="olx-adcard"]');
        const seen = new Set();

        cards.forEach(card => {
            const link = card.querySelector('[data-testid="adcard-link"]');
            if (!link || !link.getAttribute('href')) return;

            const url = link.getAttribute('href');
            if (seen.has(url)) return;
            seen.add(url);

            const details = card.querySelectorAll('[class*="adcard__detail"]');
            const detailLabels = Array.from(details)
                .map(d => d.getAttribute('aria-label'))
                .filter(Boolean);

            results.push({
                title: link.getAttribute('title'),
                url: url,
                detail_labels: detailLabels,
                full_text: card.innerText
            });
        });

        return results;
    }""")

    listings = []
    for card in raw:
        text: str = card["full_text"]
        detail_labels: list[str] = card["detail_labels"]

        def find_detail(keyword: str) -> str | None:
            return next((d for d in detail_labels if keyword in d), None)

        area_label = find_detail("metro")
        area_match = re.search(r"(\d+)", area_label) if area_label else None

        rooms_label = find_detail("quarto")
        rooms_match = re.search(r"(\d+)", rooms_label) if rooms_label else None
        rooms_is_5plus = rooms_label and "5 ou mais" in rooms_label

        baths_label = find_detail("banheiro")
        baths_match = re.search(r"(\d+)", baths_label) if baths_label else None
        baths_is_5plus = baths_label and "5 ou mais" in baths_label

        garage_label = find_detail("vaga")
        garage_match = re.search(r"(\d+)", garage_label) if garage_label else None
        garage_is_5plus = garage_label and "5 ou mais" in garage_label

        price = _parse_price(text)
        area = int(area_match.group(1)) if area_match else None

        iptu_match = re.search(r"IPTU R\$\s*([\d.,]+)", text)
        iptu = _parse_int(iptu_match.group(1)) if iptu_match else None

        condo_match = re.search(r"Condom[íi]nio R\$\s*([\d.,]+)", text)
        condo = _parse_int(condo_match.group(1)) if condo_match else None

        date_match = re.search(r"(\d+ de \w+(?:, \d{2}:\d{2})?)", text)
        date_str = date_match.group(1) if date_match else None

        price_per_m2 = round(price / area) if price and area else None

        listings.append({
            "title": card["title"],
            "price_brl": price,
            "price_per_m2": price_per_m2,
            "area_m2": area,
            "rooms": "5+" if rooms_is_5plus else (int(rooms_match.group(1)) if rooms_match else None),
            "bathrooms": "5+" if baths_is_5plus else (int(baths_match.group(1)) if baths_match else None),
            "garage_spots": "5+" if garage_is_5plus else (int(garage_match.group(1)) if garage_match else None),
            "iptu_brl": iptu,
            "condo_fee_brl": condo,
            "date_listed": date_str,
            "url": card["url"],
        })

    return listings


def _get_total_count(playwright_page: Page) -> int | None:
    """Parse 'X - Y de Z resultados' from page."""
    try:
        text = playwright_page.evaluate("""() => {
            const el = Array.from(document.querySelectorAll('*')).find(
                e => e.children.length === 0 && /\\d+ de \\d+ resultado/.test(e.innerText || '')
            );
            return el ? el.innerText : null;
        }""")
        if text:
            match = re.search(r"de (\d+) resultado", text)
            if match:
                return int(match.group(1))
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scrape(
    url: str,
    *,
    max_pages: int = 20,
    headless: bool = True,
    dedup: bool = False,
    polite_mode: bool = True,
) -> list[dict]:
    """
    Scrape all OLX real estate listings from a search URL.

    Args:
        url:          OLX search URL (first page).
        max_pages:    Safety cap on pagination (default 20).
        headless:     Run browser headlessly (default True).
        dedup:        Collapse re-listings of the same property (default False).
        polite_mode:  Enable jittered delays and scroll simulation (default True).
                      Set False for fast local testing.

    Returns:
        List of listing dicts, each with:
        title, price_brl, price_per_m2, area_m2, rooms, bathrooms,
        garage_spots, iptu_brl, condo_fee_brl, date_listed, url,
        scraped_at (ISO 8601 UTC).

        When dedup=True, each record also includes:
        listing_count (int) and price_history (list of {price_brl, date_listed, url}).
    """
    scraped_at = datetime.now(timezone.utc).isoformat()
    all_listings: list[dict] = []
    seen_urls: set[str] = set()

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=headless)
        context = _build_context(browser)
        page = context.new_page()

        total_pages = max_pages

        for page_num in range(1, max_pages + 1):
            page_url = _add_page_param(url, page_num)
            logger.info("Fetching page %d: %s", page_num, page_url)

            page.goto(page_url, wait_until="domcontentloaded", timeout=30_000)

            if polite_mode:
                _human_delay()
            else:
                time.sleep(0.5)

            # Check for bot detection
            if _is_blocked(page):
                logger.warning("Block detected on page %d — sleeping 60s and retrying", page_num)
                time.sleep(60)
                page.reload(wait_until="domcontentloaded", timeout=30_000)
                if polite_mode:
                    _human_delay()
                if _is_blocked(page):
                    logger.error("Still blocked after retry — stopping scrape")
                    break

            # Dismiss cookie banner on first page
            if page_num == 1:
                try:
                    page.click('button:has-text("Aceitar")', timeout=2000)
                    time.sleep(0.5)
                except Exception:
                    pass

            # On first page, determine total pages needed
            if page_num == 1:
                total = _get_total_count(page)
                if total is not None:
                    total_pages = min(max_pages, -(-total // PAGE_SIZE))  # ceil div
                    logger.info("Total listings: %d → %d pages", total, total_pages)

            if polite_mode:
                _simulate_reading(page)

            listings = _extract_cards(page)

            if not listings:
                if page_num > 1 and not _has_listings(page):
                    logger.warning("No cards found on page %d, stopping.", page_num)
                else:
                    logger.info("No cards found on page %d, stopping.", page_num)
                break

            new = 0
            for listing in listings:
                if listing["url"] not in seen_urls:
                    seen_urls.add(listing["url"])
                    listing["scraped_at"] = scraped_at
                    all_listings.append(listing)
                    new += 1

            logger.info("Page %d: %d listings (%d new)", page_num, len(listings), new)

            if page_num >= total_pages:
                break

        browser.close()

    logger.info("Done. Total unique listings: %d", len(all_listings))

    if dedup:
        return deduplicate(all_listings)
    return all_listings


def _extract_detail_page(playwright_page: Page) -> dict:
    """
    Extract enrichment fields from a loaded OLX property detail page.

    Returns a dict with keys:
        ad_id, cep, city, state, description,
        amenidades_imovel (list[str]), amenidades_condominio (list[str]),
        seller_type ("PROFISSIONAL" | "PARTICULAR" | None), seller_name.

    All fields are nullable — never raises; missing data returns None / [].
    """
    return playwright_page.evaluate(r"""() => {
        const text = document.body.innerText;

        // Ad ID: trailing digits in the URL path
        const ad_id = window.location.pathname.match(/(\d+)$/)?.[1] ?? null;

        // CEP: 8 digits or NNNNN-NNN anywhere in the body
        const cepMatch = text.match(/\b(\d{5}-?\d{3})\b/);
        const cep = cepMatch ? cepMatch[1].replace('-', '') : null;

        // City and state: "City Name, UF, NNNNN" on one line (no newlines in city)
        const locMatch = text.match(/([A-ZÀ-Ú][a-zà-ú]+(?:[^\S\n][A-ZÀ-Ú][a-zà-ú]+)*),\s*([A-Z]{2}),\s*\d{5}/);
        const city  = locMatch ? locMatch[1].trim() : null;
        const state = locMatch ? locMatch[2] : null;

        // Description: content between the ad-code line and the first stop marker
        const descMatch = text.match(
            /Código do anúncio:[^\n]*\n([\s\S]*?)(?=Ver descrição completa|Consórcio fácil|Localização\n|Detalhes\n)/
        );
        const description = descMatch ? descMatch[1].trim() || null : null;

        // Amenidades do imóvel (property features list)
        const amenImovelMatch = text.match(
            /Características do imóvel\n([\s\S]*?)(?=Características do condomínio|R\$\s*[\d.,]|Simular|\n{2,}|$)/
        );
        const amenidades_imovel = amenImovelMatch
            ? amenImovelMatch[1].trim().split('\n').map(s => s.trim()).filter(Boolean)
            : [];

        // Amenidades do condomínio (condo features list)
        const amenCondMatch = text.match(
            /Características do condomínio\n([\s\S]*?)(?=R\$\s*[\d.,]|Simular|\n{2,}|$)/
        );
        const amenidades_condominio = amenCondMatch
            ? amenCondMatch[1].trim().split('\n').map(s => s.trim()).filter(Boolean)
            : [];

        // Seller type
        const sellerTypeMatch = text.match(/\b(PROFISSIONAL|PARTICULAR)\b/);
        const seller_type = sellerTypeMatch ? sellerTypeMatch[1] : null;

        // Seller name: first non-empty line after the type label
        const sellerNameMatch = text.match(/(?:PROFISSIONAL|PARTICULAR)\n([^\n]+)/);
        const seller_name = sellerNameMatch ? sellerNameMatch[1].trim() : null;

        return {
            ad_id, cep, city, state, description,
            amenidades_imovel, amenidades_condominio,
            seller_type, seller_name,
        };
    }""")


_EMPTY_DETAIL: dict = {
    "ad_id": None, "cep": None, "city": None, "state": None,
    "description": None, "amenidades_imovel": [], "amenidades_condominio": [],
    "seller_type": None, "seller_name": None,
}


def scrape_detail_pages(
    listings: list[dict],
    *,
    headless: bool = True,
) -> list[dict]:
    """
    Visit individual OLX property pages to enrich listings with:
    CEP, amenidades (imóvel + condomínio), full description, ad_id,
    city, state, seller_type, seller_name.

    Batches of DETAIL_BATCH_SIZE with DETAIL_INTER_BATCH_SECS pause between batches.
    Skips blocked pages (logs warning, continues) — blocked listings receive
    null values for all enrichment fields so the output schema is consistent.

    Uses a fresh browser context (separate from the listing scrape session) to
    avoid tying detail-page activity to the listing-page fingerprint.
    """
    enriched = []

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=headless)
        context = _build_context(browser)
        page = context.new_page()

        for i, listing in enumerate(listings):
            detail_url = listing.get("url")
            if not detail_url:
                enriched.append({**_EMPTY_DETAIL, **listing})
                continue

            # Intra-batch delay (skip first item)
            if i > 0:
                delay_ms = random.randint(*DETAIL_INTRA_DELAY_MS)
                _human_delay(base_ms=delay_ms, jitter_ms=500)

            # Inter-batch pause
            if i > 0 and i % DETAIL_BATCH_SIZE == 0:
                pause = random.uniform(*DETAIL_INTER_BATCH_SECS)
                logger.info(
                    "Batch boundary at listing %d/%d — pausing %.1fs",
                    i, len(listings), pause,
                )
                time.sleep(pause)

            logger.info("Fetching detail page %d/%d: %s", i + 1, len(listings), detail_url)

            try:
                page.goto(detail_url, wait_until="domcontentloaded", timeout=30_000)
                _human_delay()

                if _is_blocked(page):
                    logger.warning("Block detected on detail page %s — skipping", detail_url)
                    enriched.append({**_EMPTY_DETAIL, **listing})
                    continue

                _simulate_reading(page)

                detail = _extract_detail_page(page)
                enriched.append({**listing, **detail})

            except Exception as exc:
                logger.warning("Failed to fetch detail page %s: %s", detail_url, exc)
                enriched.append({**_EMPTY_DETAIL, **listing})

        browser.close()

    return enriched


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Scrape OLX real estate listings")
    parser.add_argument("url", help="OLX search URL")
    parser.add_argument("--output", "-o", default="-", help="Output file (default: stdout)")
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    parser.add_argument("--no-polite", action="store_true", help="Disable jitter/scroll (fast local testing)")
    parser.add_argument("--dedup", action="store_true", help="Collapse re-listings of same property")
    parser.add_argument("--enrich", action="store_true", help="Visit detail pages to add CEP, amenidades, description")
    args = parser.parse_args()

    listings = scrape(
        args.url,
        max_pages=args.max_pages,
        headless=not args.no_headless,
        dedup=args.dedup,
        polite_mode=not args.no_polite,
    )

    if args.enrich:
        listings = scrape_detail_pages(listings, headless=not args.no_headless)

    output = json.dumps(listings, ensure_ascii=False, indent=2)

    if args.output == "-":
        print(output)
    else:
        import pathlib
        pathlib.Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(args.output).write_text(output, encoding="utf-8")
        print(f"Saved {len(listings)} listings → {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
