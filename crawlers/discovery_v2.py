"""
Discovery v2 - PRODUCTION

Ottimizzazioni:
- Depth 7 per stealth (era 3)
- No limit 10000 hardcoded
- Timeout aumentati per brand lenti
"""

import re
import time
import logging
import requests
from typing import Set, List
from urllib.parse import urlparse
from trafilatura import fetch_url
from trafilatura.feeds import find_feed_urls

# Playwright (opzionale)
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


def fetch_with_timeout(url: str, timeout: int = 15) -> str:
    """Fetch URL con timeout"""
    try:
        resp = requests.get(url, timeout=timeout, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; bot/1.0)'
        }, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except:
        pass
    return None


def discover_via_sitemap_xml(base_url: str, logger: logging.Logger, timeout: int = 60) -> Set[str]:
    """Sitemap XML - Timeout aumentato a 60s per brand lenti"""
    urls = set()
    domain = urlparse(base_url).netloc
    
    sitemap_paths = [
        '/sitemap.xml',
        '/sitemap_index.xml',
        '/post-sitemap.xml',
        '/sitemap-posts.xml',
    ]
    
    start = time.time()
    
    for path in sitemap_paths:
        if time.time() - start > timeout:
            break
        
        sitemap_url = base_url.rstrip('/') + path
        
        try:
            logger.info(f"Sitemap: Provando {sitemap_url}")
            content = fetch_with_timeout(sitemap_url, timeout=30)
            
            if not content:
                continue
            
            locs = re.findall(r'<loc>(.+?)</loc>', content, re.DOTALL)
            logger.info(f"Sitemap: {path} → {len(locs)} URLs")
            
            # Index sitemap
            if any('sitemap' in loc.lower() for loc in locs):
                for sub_url in locs[:20]:
                    if 'sitemap' in sub_url.lower():
                        try:
                            sub_content = fetch_url(sub_url)
                            if sub_content:
                                sub_locs = re.findall(r'<loc>(.+?)</loc>', sub_content, re.DOTALL)
                                locs.extend(sub_locs)
                        except:
                            pass
            
            for loc in locs:
                loc = loc.strip()
                if domain in loc and 'sitemap' not in loc.lower():
                    urls.add(loc)
            
            if len(urls) > 100:
                break
                
        except Exception as e:
            logger.debug(f"Sitemap {path} error: {e}")
    
    return urls


def discover_fast(base_url: str, mode: str, logger: logging.Logger, max_urls: int = 999999) -> List[str]:
    """
    Discovery veloce - NO LIMIT hardcoded
    """
    all_urls = set()
    
    # Sitemap
    sitemap_urls = discover_via_sitemap_xml(base_url, logger, timeout=60)
    all_urls.update(sitemap_urls)
    
    # RSS (solo blog)
    if mode == 'blog' and len(all_urls) < 1000:
        try:
            rss_urls = find_feed_urls(base_url)
            domain = urlparse(base_url).netloc
            for url in rss_urls[:100]:
                if domain in url:
                    all_urls.add(url)
        except:
            pass
    
    # Normalizza
    normalized = set()
    for url in all_urls:
        clean = url.strip().split('?')[0].split('#')[0]
        if clean and len(clean) > 15:
            normalized.add(clean)
    
    return list(normalized)[:max_urls]


def stealth_discover_site(base_url: str, logger: logging.Logger, max_pages: int = 999999) -> List[str]:
    """
    Stealth discovery - DEPTH 7, NO LIMIT
    """
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright non disponibile")
        return []
    
    logger.info("🎭 Stealth Discovery (depth 7, no limit)")
    
    domain = urlparse(base_url).netloc
    all_urls = set()
    visited = set()
    to_visit = {base_url}
    
    CATALOG_PATTERNS = ['/watch', '/collection', '/product', '/catalog', '/model', '/timepiece']
    CONTENT_PATTERNS = ['/blog', '/news', '/story', '/article', '/about', '/heritage', '/history']
    
    # PRIORITY PATTERNS - Press release, novelties, media
    PRESS_PATTERNS = [
        '/press', '/press-room', '/press-release', '/press-releases',
        '/news', '/novelties', '/new-watches', '/new-models',
        '/media', '/media-center', '/media-kit',
        '/innovation', '/latest', '/announcements'
    ]
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox'],
        )
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080},
            locale='it-IT',
            timezone_id='Europe/Rome',
        )
        
        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        
        depth = 0
        max_depth = 7  # AUMENTATO a 7!
        
        while to_visit and len(all_urls) < max_pages and depth < max_depth:
            current_batch = list(to_visit)[:10]  # 10 per batch
            to_visit.difference_update(current_batch)
            
            logger.info(f"   Depth {depth}/{max_depth}: {len(current_batch)} URLs (trovato: {len(all_urls)}, coda: {len(to_visit)})")
            
            for url in current_batch:
                if url in visited or len(all_urls) >= max_pages:
                    continue
                
                visited.add(url)
                
                try:
                    # CRITICAL: usa domcontentloaded invece di networkidle
                    # Brand come Omega/Cartier non raggiungono mai networkidle
                    page.goto(url, wait_until='domcontentloaded', timeout=45000)
                    
                    # Aspetta che il DOM sia caricato
                    page.wait_for_timeout(4000)  # 4s per JS iniziale
                    
                    # Scroll per triggerare lazy loading
                    try:
                        page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
                        page.wait_for_timeout(1000)
                    except:
                        pass
                    
                    # Estrai link
                    hrefs = page.evaluate('''
                        () => Array.from(document.querySelectorAll('a[href]'))
                              .map(a => a.href)
                              .filter(h => h && h.startsWith('http'))
                    ''')
                    
                    # Filtra
                    base_norm = domain.replace('www.', '')
                    for href in hrefs:
                        try:
                            href_domain = urlparse(href).netloc.replace('www.', '')
                            if base_norm in href_domain or href_domain in base_norm:
                                clean = href.split('#')[0].split('?')[0].rstrip('/')
                                if clean:
                                    all_urls.add(clean)
                        except:
                            pass
                    
                    # Aggiungi link interessanti per prossimo depth
                    if depth < max_depth - 1:
                        for link in list(all_urls)[-30:]:  # Ultimi 30 link trovati
                            if link not in visited:
                                link_lower = link.lower()
                                
                                # MASSIMA priorità: Press/News/Novelties
                                if any(p in link_lower for p in PRESS_PATTERNS):
                                    to_visit.add(link)
                                    logger.info(f"      🔥 PRIORITY: {link}")
                                
                                # Alta priorità: cataloghi
                                elif any(p in link_lower for p in CATALOG_PATTERNS):
                                    if len(to_visit) < 50:
                                        to_visit.add(link)
                                
                                # Media priorità: contenuti
                                elif any(p in link_lower for p in CONTENT_PATTERNS):
                                    if len(to_visit) < 100:
                                        to_visit.add(link)
                    
                    time.sleep(1)
                    
                except Exception as e:
                    logger.debug(f"   Stealth error: {e}")
            
            depth += 1
        
        browser.close()
    
    logger.info(f"✅ Stealth: {len(all_urls)} URLs")
    return list(all_urls)


def discover_all(base_url: str, mode: str, logger: logging.Logger, max_urls: int = 999999) -> List[str]:
    """
    Discovery a 2 FASI con SEED URLs
    """
    logger.info(f"🔍 Discovery: {base_url} ({mode})")
    
    start = time.time()
    domain = urlparse(base_url).netloc.replace('www.', '')
    
    # ============================================
    # SEED URLs - Garantiti per brand importanti
    # ============================================
    BRAND_SEEDS = {
        # Luxury Brands - Press/News
        'patek.com': [
            '/en/company/news-events',
            '/en/collection/new-models',
            '/en/company/savoir-faire'
        ],
        'audemarspiguet.com': [
            '/com/en/journal.html',
            '/com/en/news.html',
            '/com/en/collection.html'
        ],
        'vacheron-constantin.com': [
            '/en/news.html',
            '/en/collections.html',
            '/en/maison.html'
        ],
        'rolex.com': [
            '/watches/new-watches',
            '/about-rolex/history',
            '/watches',
            '/watchmaking'
        ],
        'omegawatches.com': [
            '/watches',
            '/planet-omega/heritage',
            '/planet-omega/innovation'
        ],
        'iwc.com': [
            '/en/journal.html',
            '/en/collections.html'
        ],
        'jaeger-lecoultre.com': [
            '/eu/en/watches.html',
            '/eu/en/journal.html'
        ],
        'panerai.com': [
            '/us/en/collections.html',
            '/us/en/experience.html'
        ],
        'breguet.com': [
            '/en/collections',
            '/en/world-breguet/news'
        ],
        'blancpain.com': [
            '/en/watches',
            '/en/watchmaking/news'
        ],
        'a-lange-soehne.com': [
            '/en/timepieces',
            '/en/news'
        ],
        'fp-journe.com': [
            '/en/collection',
            '/en/actualites'
        ],
        'richardmille.com': [
            '/collections',
            '/news'
        ],
        'cartier.com': [
            '/en-us/watches.html',
            '/en-us/maison.html'
        ],
        'girard-perregaux.com': [
            '/collections',
            '/news'
        ],
        'zenith-watches.com': [
            '/en_en/collections.html',
            '/en_en/news.html'
        ],
        'tag-heuer.com': [
            '/us/en/watches.html',
            '/us/en/news.html'
        ],
        'hublot.com': [
            '/en-us/watches',
            '/en-us/news'
        ],
        'breitling.com': [
            '/us/watches',
            '/us/news'
        ],
        'chopard.com': [
            '/intl/en/collections/watches.html',
            '/intl/en/news.html'
        ],
        'ulysse-nardin.com': [
            '/usa_en/collections',
            '/usa_en/news-events'
        ],
        'grand-seiko.com': [
            '/global-en/collections',
            '/global-en/news'
        ],
        'tudor.com': [
            '/en/watches',
            '/en/inside-tudor'
        ],
        'bell-ross.com': [
            '/collections',
            '/news'
        ],
        'oris.ch': [
            '/en/watches',
            '/en/news'
        ],
        'baume-et-mercier.com': [
            '/us/watches.html',
            '/us/news.html'
        ],
        'piaget.com': [
            '/watches',
            '/news-events'
        ]
    }
    
    # FASE 1: Fast discovery
    urls_fast = discover_fast(base_url, mode, logger, max_urls)
    logger.info(f"  Fast: {len(urls_fast)} URLs")
    
    # Aggiungi SEED URLs se disponibili
    seed_urls = set()
    for seed_domain, paths in BRAND_SEEDS.items():
        if seed_domain in domain or domain in seed_domain:
            logger.info(f"  🌱 Adding {len(paths)} seed URLs for {seed_domain}")
            for path in paths:
                seed_url = urljoin(base_url, path)
                seed_urls.add(seed_url)
                logger.info(f"     → {seed_url}")
    
    # Merge seed con discovery
    urls_combined = set(urls_fast)
    urls_combined.update(seed_urls)
    
    # FASE 2: Stealth se necessario (e non abbiamo già abbastanza con seeds)
    if len(urls_combined) < 200:
        logger.warning(f"⚠️  Solo {len(urls_combined)} URLs → Attivando Stealth")
        
        if PLAYWRIGHT_AVAILABLE:
            urls_stealth = stealth_discover_site(base_url, logger, max_pages=max_urls)
            
            if len(urls_stealth) > len(urls_combined):
                urls_combined.update(urls_stealth)
                urls_final = list(urls_combined)[:max_urls]
                
                elapsed = time.time() - start
                logger.info(f"✅ Discovery: {len(urls_final)} URLs (stealth + seeds) in {elapsed:.1f}s")
                return urls_final
        else:
            logger.warning("Playwright non disponibile")
    
    # Ritorna combined
    urls_final = list(urls_combined)[:max_urls]
    elapsed = time.time() - start
    logger.info(f"✅ Discovery: {len(urls_final)} URLs (fast + seeds) in {elapsed:.1f}s")
    return urls_final
