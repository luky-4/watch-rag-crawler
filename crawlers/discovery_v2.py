#!/usr/bin/env python3
"""
Discovery v2 - Camoufox (primary) + Playwright Chromium (fallback)
Patch 3: BFS multi-livello, filtro junk negativo, anti-detect migliorato
"""

from urllib.parse import urljoin, urlparse
import time
import random
import re
from typing import Set, List
from bs4 import BeautifulSoup
import requests
import json
import os

# ============================================================================
# JUNK URL FILTER (negativo — scarta solo spazzatura nota)
# ============================================================================

_DISCOVERY_JUNK = re.compile(
    r'(?:feed|rss|atom|sitemap|\.xml|wp-json|wp-admin|wp-login'
    r'|/tag/|/tags/|/category/|/categories/|/author/|/page/\d+'
    r'|/search|/cart|/checkout|/account|/login|/register'
    r'|\.(css|js|png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|pdf|zip)$'
    r'|utm_|#)',
    re.IGNORECASE
)

def _is_valid_url(url: str, base_domain: str) -> bool:
    """URL valido = stesso dominio + non junk"""
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
# SHARED LINK EXTRACTOR
# ============================================================================

def _extract_links_from_html(html: str, base_url: str, base_domain: str) -> Set[str]:
    found = set()
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a', href=True):
        full_url = urljoin(base_url, link['href'])
        if _is_valid_url(full_url, base_domain):
            found.add(full_url.rstrip('/'))
    return found


# ============================================================================
# CAMOUFOX DISCOVERY (primary)
# ============================================================================

try:
    from camoufox.sync_api import Camoufox
    CAMOUFOX_AVAILABLE = True
except ImportError:
    CAMOUFOX_AVAILABLE = False


def _camoufox_discover(base_url: str, max_depth: int = 3, max_per_level: int = 20) -> Set[str]:
    base_domain = urlparse(base_url).netloc
    all_found: Set[str] = set()
    to_visit = {base_url.rstrip('/')}
    visited: Set[str] = set()

    with Camoufox(headless=True, humanize=True) as browser:
        for depth in range(max_depth):
            level_urls = list(to_visit - visited)[:max_per_level]
            next_level: Set[str] = set()

            for url in level_urls:
                if url in visited:
                    continue
                visited.add(url)
                page = None
                try:
                    page = browser.new_page()
                    page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    page.wait_for_timeout(2000)
                    try:
                        page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
                        page.wait_for_timeout(800)
                    except Exception:
                        pass
                    html = page.content()
                    new_links = _extract_links_from_html(html, url, base_domain)
                    all_found.update(new_links)
                    next_level.update(new_links - visited)
                except Exception as e:
                    print(f"[camoufox] Error {url}: {e}")
                finally:
                    if page:
                        try:
                            page.close()
                        except Exception:
                            pass

            to_visit = next_level
            print(f"[camoufox] Depth {depth+1}/{max_depth}: {len(all_found)} URLs trovati")
            if not to_visit:
                break

    return all_found


# ============================================================================
# PLAYWRIGHT CHROMIUM FALLBACK
# ============================================================================

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


def _playwright_discover(base_url: str, max_depth: int = 3, max_per_level: int = 20) -> Set[str]:
    base_domain = urlparse(base_url).netloc
    all_found: Set[str] = set()
    to_visit = {base_url.rstrip('/')}
    visited: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
        )
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York',
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

        for depth in range(max_depth):
            level_urls = list(to_visit - visited)[:max_per_level]
            next_level: Set[str] = set()

            for url in level_urls:
                if url in visited:
                    continue
                visited.add(url)
                page = None
                try:
                    page = context.new_page()
                    page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    page.wait_for_timeout(2000)
                    try:
                        page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
                        page.wait_for_timeout(800)
                    except Exception:
                        pass
                    html = page.content()
                    new_links = _extract_links_from_html(html, url, base_domain)
                    all_found.update(new_links)
                    next_level.update(new_links - visited)
                except Exception as e:
                    print(f"[playwright] Error {url}: {e}")
                finally:
                    if page:
                        try:
                            page.close()
                        except Exception:
                            pass

            to_visit = next_level
            print(f"[playwright] Depth {depth+1}/{max_depth}: {len(all_found)} URLs trovati")
            if not to_visit:
                break

        browser.close()

    return all_found


def stealth_discover(base_url: str, site_type: str, max_depth: int = 3) -> Set[str]:
    if CAMOUFOX_AVAILABLE:
        print(f"[discovery] Usando Camoufox (Firefox anti-detect)")
        return _camoufox_discover(base_url, max_depth=max_depth)
    elif PLAYWRIGHT_AVAILABLE:
        print(f"[discovery] Camoufox non disponibile → fallback Playwright Chromium")
        return _playwright_discover(base_url, max_depth=max_depth)
    else:
        print(f"[discovery] Né Camoufox né Playwright disponibili — skip stealth")
        return set()


# ============================================================================
# SITEMAP DISCOVERY
# ============================================================================

def try_sitemap(base_url: str) -> Set[str]:
    found = set()
    domain = urlparse(base_url).netloc
    base_domain = domain

    sitemap_urls = [
        f"https://{domain}/sitemap.xml",
        f"https://{domain}/sitemap_index.xml",
        f"https://{domain}/post-sitemap.xml",
        f"https://{domain}/sitemap-posts.xml",
    ]

    for sitemap_url in sitemap_urls:
        try:
            print(f"[{domain}] Sitemap: Provando {sitemap_url}")
            resp = requests.get(sitemap_url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })

            if resp.status_code == 200 and 'xml' in resp.headers.get('content-type', ''):
                soup = BeautifulSoup(resp.text, 'xml')
                for loc in soup.find_all('loc'):
                    url = loc.text.strip()
                    if url.startswith('http') and _is_valid_url(url, base_domain):
                        found.add(url)

                if found:
                    print(f"[{domain}] Sitemap: {sitemap_url} → {len(found)} URLs")
                    break

        except Exception:
            pass

    return found


# ============================================================================
# MAIN DISCOVERY ORCHESTRATOR
# ============================================================================

def discover_urls(base_url: str, site_type: str, max_limit: int = None) -> Set[str]:
    domain = urlparse(base_url).netloc
    all_urls: Set[str] = set()

    # STEP 1: Sitemap
    sitemap_urls = try_sitemap(base_url)
    if sitemap_urls:
        all_urls.update(sitemap_urls)

    # STEP 2: Brand seeds — sempre aggiunti per siti brand
    if site_type == 'brand':
        seeds = get_brand_seeds(domain)
        if seeds:
            print(f"[{domain}]   🌱 Adding {len(seeds)} seed URLs")
            all_urls.update(seeds)

    # STEP 3: Stealth BFS se <50 URL (soglia alzata da 10 a 50)
    if len(all_urls) < 50:
        print(f"[{domain}] ⚠️  Solo {len(all_urls)} URLs → Attivando Stealth BFS")
        stealth_urls = stealth_discover(base_url, site_type, max_depth=3)
        all_urls.update(stealth_urls)

    if max_limit and len(all_urls) > max_limit:
        all_urls = set(list(all_urls)[:max_limit])

    return all_urls


# ============================================================================
# BACKWARD COMPATIBILITY
# ============================================================================

def discover_all(base_url: str, mode: str, logger=None, max_urls: int = None) -> List[str]:
    if logger:
        logger.info(f"[{urlparse(base_url).netloc}] 🔍 Discovery: {base_url} ({mode})")
    urls_set = discover_urls(base_url, mode, max_urls)
    return list(urls_set)


if __name__ == '__main__':
    urls = discover_urls('https://www.rolex.com', 'brand')
    print(f"\nTrovati {len(urls)} URLs")
    for url in list(urls)[:10]:
        print(f"  - {url}")
