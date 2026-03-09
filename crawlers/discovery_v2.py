#!/usr/bin/env python3
"""
Discovery v3 - Architettura a 3 livelli:
  1. Sitemap (veloce, copre ~80% dei siti, supporta sitemap index)
  2. Crawl4AI AsyncUrlSeeder (async, nessun browser, migliaia di URL in secondi)
  3. Camoufox BFS (solo fallback per siti anti-bot senza sitemap, max 60s)
"""

from urllib.parse import urljoin, urlparse
import re
import asyncio
import time
from typing import Set, List
from bs4 import BeautifulSoup
import requests
import json
import os

# ============================================================================
# JUNK URL FILTER
# ============================================================================

_DISCOVERY_JUNK = re.compile(
    r'(?:feed|rss|atom|sitemap|\.xml|wp-json|wp-admin|wp-login'
    r'|/tag/|/tags/|/category/|/categories/|/author/|/page/\d+'
    r'|/search|/cart|/checkout|/account|/login|/register'
    r'|/wp-content/uploads/'
    r'|\.(css|js|png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|pdf|zip|webp|avif|mp4|mp3|webm)$'
    r'|utm_|#)',
    re.IGNORECASE
)

def _is_valid_url(url: str, base_domain: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.netloc != base_domain:
            return False
        return not bool(_DISCOVERY_JUNK.search(url))
    except Exception:
        return False


# ============================================================================
# BRAND SEED URLs
# ============================================================================

def load_brand_seeds():
    seeds_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'brand_seeds.json')
    if os.path.exists(seeds_file):
        with open(seeds_file, 'r') as f:
            return json.load(f)
    return {}


def get_brand_seeds(domain: str) -> List[str]:
    BRAND_SEEDS = load_brand_seeds()
    for brand_key, seeds in BRAND_SEEDS.items():
        if brand_key in domain:
            return seeds
    return []


# ============================================================================
# LIVELLO 1: SITEMAP (supporta sitemap index + sub-sitemap)
# ============================================================================

_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def _fetch_sitemap_urls(sitemap_url: str, base_domain: str, depth: int = 0) -> Set[str]:
    """Scarica un sitemap, segue sub-sitemap se è un index."""
    found = set()
    if depth > 3:
        return found
    try:
        resp = requests.get(sitemap_url, timeout=15, headers=_HEADERS)
        if resp.status_code != 200:
            return found
        soup = BeautifulSoup(resp.text, 'xml')
        # Sitemap index: tag <sitemap><loc>
        sub_sitemaps = soup.find_all('sitemap')
        if sub_sitemaps:
            for s in sub_sitemaps:
                loc = s.find('loc')
                if loc:
                    sub_url = loc.text.strip()
                    found.update(_fetch_sitemap_urls(sub_url, base_domain, depth + 1))
        else:
            # Sitemap normale: tag <url><loc>
            for loc in soup.find_all('loc'):
                url = loc.text.strip()
                if url.startswith('http') and _is_valid_url(url, base_domain):
                    found.add(url)
    except Exception:
        pass
    return found


def _get_sitemaps_from_robots(base_url: str) -> List[str]:
    """Legge robots.txt e restituisce le sitemap dichiarate."""
    domain = urlparse(base_url).netloc
    found = []
    try:
        resp = requests.get(f"https://{domain}/robots.txt", timeout=10, headers=_HEADERS)
        if resp.status_code == 200:
            for line in resp.text.splitlines():
                if line.lower().startswith('sitemap:'):
                    url = line.split(':', 1)[1].strip()
                    if url.startswith('http'):
                        found.append(url)
                        print(f"[{domain}] robots.txt → sitemap: {url}")
    except Exception:
        pass
    return found


def try_sitemap(base_url: str) -> Set[str]:
    domain = urlparse(base_url).netloc

    # Prima leggi robots.txt per trovare sitemap dichiarate
    robots_sitemaps = _get_sitemaps_from_robots(base_url)

    sitemap_candidates = robots_sitemaps + [
        f"https://{domain}/sitemap.xml",
        f"https://{domain}/sitemap_index.xml",
        f"https://{domain}/post-sitemap.xml",
        f"https://{domain}/sitemap-posts.xml",
        f"https://{domain}/news-sitemap.xml",
    ]

    # Deduplicazione mantenendo l'ordine
    seen: Set[str] = set()
    sitemap_candidates = [x for x in sitemap_candidates if not (x in seen or seen.add(x))]  # type: ignore

    for sitemap_url in sitemap_candidates:
        print(f"[{domain}] Sitemap: Provando {sitemap_url}")
        urls = _fetch_sitemap_urls(sitemap_url, domain)
        if urls:
            print(f"[{domain}] Sitemap: {sitemap_url} → {len(urls)} URLs")
            return urls
    return set()


# ============================================================================
# LIVELLO 2: CRAWL4AI AsyncUrlSeeder (async, no browser, velocissimo)
# ============================================================================

try:
    from crawl4ai import AsyncUrlSeeder, SeedingConfig
    CRAWL4AI_AVAILABLE = True
except ImportError:
    CRAWL4AI_AVAILABLE = False


async def _crawl4ai_discover_async(base_url: str, site_type: str) -> Set[str]:
    domain = urlparse(base_url).netloc
    found = set()
    try:
        seeder = AsyncUrlSeeder()
        # Pattern per blog: solo articoli
        # Pattern per brand: tutto il sito
        if site_type == 'blog':
            pattern = None  # prende tutto e filtriamo dopo
        else:
            pattern = None

        config = SeedingConfig(
            source="sitemap",       # prima sitemap, poi fallback BFS interno
            filter_nonsense_urls=True,  # filtra media, admin, api ecc.
            hits_per_sec=5,
            concurrency=10,
        )
        urls = await seeder.urls(base_url, config)

        for u in urls:
            url_str = u if isinstance(u, str) else u.get('url', '')
            if url_str and _is_valid_url(url_str, domain):
                found.add(url_str)

        print(f"[crawl4ai] {domain}: {len(found)} URLs trovati")
    except Exception as e:
        print(f"[crawl4ai] Error {domain}: {e}")
    return found


def _crawl4ai_discover(base_url: str, site_type: str, timeout: int = 120) -> Set[str]:
    """Wrapper sync con timeout globale."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                asyncio.wait_for(_crawl4ai_discover_async(base_url, site_type), timeout=timeout)
            )
        finally:
            loop.close()
    except asyncio.TimeoutError:
        print(f"[crawl4ai] Timeout ({timeout}s) su {base_url}")
        return set()
    except Exception as e:
        print(f"[crawl4ai] Error: {e}")
        return set()


# ============================================================================
# LIVELLO 3: CAMOUFOX BFS (solo fallback, max 300s, depth=7, max_per_level=100)
# ============================================================================

try:
    from camoufox.sync_api import Camoufox
    CAMOUFOX_AVAILABLE = True
except ImportError:
    CAMOUFOX_AVAILABLE = False

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


def _extract_links_from_html(html: str, base_url: str, base_domain: str) -> Set[str]:
    found = set()
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a', href=True):
        full_url = urljoin(base_url, link['href'])
        if _is_valid_url(full_url, base_domain):
            found.add(full_url.rstrip('/'))
    return found


def _browser_bfs(base_url: str, max_depth: int = 7, max_per_level: int = 100,
                  timeout_per_page: int = 20000) -> Set[str]:
    """BFS leggero con Camoufox — usato solo come ultimo fallback."""
    base_domain = urlparse(base_url).netloc
    all_found: Set[str] = set()
    to_visit = {base_url.rstrip('/')}
    visited: Set[str] = set()
    start = time.time()
    MAX_TOTAL = 300  # secondi massimi totali (5 minuti)

    def _run_with_browser(browser):
        nonlocal to_visit, visited
        for depth in range(max_depth):
            if time.time() - start > MAX_TOTAL:
                print(f"[browser_bfs] Timeout globale {MAX_TOTAL}s raggiunto")
                break
            level_urls = list(to_visit - visited)[:max_per_level]
            next_level: Set[str] = set()
            for url in level_urls:
                if url in visited:
                    continue
                if time.time() - start > MAX_TOTAL:
                    break
                visited.add(url)
                page = None
                try:
                    page = browser.new_page()
                    page.goto(url, wait_until='domcontentloaded', timeout=timeout_per_page)
                    page.wait_for_timeout(1000)
                    html = page.content()
                    new_links = _extract_links_from_html(html, url, base_domain)
                    all_found.update(new_links)
                    next_level.update(new_links - visited)
                except Exception as e:
                    print(f"[browser_bfs] Error {url}: {str(e)[:60]}")
                finally:
                    if page:
                        try:
                            page.close()
                        except Exception:
                            pass
            to_visit = next_level
            print(f"[browser_bfs] Depth {depth+1}/{max_depth}: {len(all_found)} URLs")
            if not to_visit:
                break

    if CAMOUFOX_AVAILABLE:
        print(f"[discovery] Fallback: Camoufox BFS (max {MAX_TOTAL}s)")
        try:
            with Camoufox(headless=True, humanize=False) as browser:
                _run_with_browser(browser)
        except Exception as e:
            print(f"[browser_bfs] Camoufox error: {e}")
    elif PLAYWRIGHT_AVAILABLE:
        print(f"[discovery] Fallback: Playwright BFS (max {MAX_TOTAL}s)")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--disable-blink-features=AutomationControlled',
                          '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
                )
                ctx = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                )
                ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
                _run_with_browser(ctx)
                browser.close()
        except Exception as e:
            print(f"[browser_bfs] Playwright error: {e}")

    return all_found


# ============================================================================
# ORCHESTRATOR PRINCIPALE
# ============================================================================

def discover_urls(base_url: str, site_type: str, max_limit: int = None) -> Set[str]:
    domain = urlparse(base_url).netloc
    all_urls: Set[str] = set()

    # LIVELLO 1: Sitemap
    sitemap_urls = try_sitemap(base_url)
    if sitemap_urls:
        all_urls.update(sitemap_urls)
        print(f"[{domain}] ✅ Sitemap: {len(all_urls)} URLs")

    # Brand seeds — aggiunti sempre per siti brand
    if site_type == 'brand':
        seeds = get_brand_seeds(domain)
        if seeds:
            print(f"[{domain}] 🌱 {len(seeds)} seed URLs")
            all_urls.update(seeds)

    # LIVELLO 2: Crawl4AI AsyncUrlSeeder se sitemap ha trovato poco
    if len(all_urls) < 50:
        if CRAWL4AI_AVAILABLE:
            print(f"[{domain}] ⚠️  Solo {len(all_urls)} URLs → Crawl4AI AsyncUrlSeeder")
            c4a_urls = _crawl4ai_discover(base_url, site_type, timeout=120)
            all_urls.update(c4a_urls)
            print(f"[{domain}] Dopo Crawl4AI: {len(all_urls)} URLs")
        else:
            print(f"[{domain}] ⚠️  Crawl4AI non disponibile")

    # LIVELLO 3: Camoufox BFS solo se ancora pochissimi URL
    if len(all_urls) < 20:
        print(f"[{domain}] ⚠️  Solo {len(all_urls)} URLs → Fallback browser BFS")
        bfs_urls = _browser_bfs(base_url, max_depth=7, max_per_level=100)
        all_urls.update(bfs_urls)
        print(f"[{domain}] Dopo BFS: {len(all_urls)} URLs")

    if max_limit and len(all_urls) > max_limit:
        all_urls = set(list(all_urls)[:max_limit])

    return all_urls


# ============================================================================
# BACKWARD COMPATIBILITY
# ============================================================================

def discover_all(base_url: str, mode: str, logger=None, max_urls: int = None) -> List[str]:
    domain = urlparse(base_url).netloc
    if logger:
        logger.info(f"[{domain}] 🔍 Discovery: {base_url} ({mode})")
    try:
        urls_set = discover_urls(base_url, mode, max_urls)
        return list(urls_set)
    except Exception as e:
        print(f"[{domain}] ⚠️ discover_all exception: {e}")
        return []


if __name__ == '__main__':
    import sys
    url = sys.argv[1] if len(sys.argv) > 1 else 'https://www.watchinsanity.it'
    mode = sys.argv[2] if len(sys.argv) > 2 else 'blog'
    urls = discover_urls(url, mode)
    print(f"\nTrovati {len(urls)} URLs")
    for u in list(urls)[:10]:
        print(f"  - {u}")
