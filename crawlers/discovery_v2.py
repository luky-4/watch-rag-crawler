#!/usr/bin/env python3
"""
Discovery v2 - Playwright Stealth con Anti-Detection
"""

from playwright.sync_api import sync_playwright
from urllib.parse import urljoin, urlparse
import time
import random
from typing import Set, List
from bs4 import BeautifulSoup
import requests

# ============================================================================
# BRAND SEED URLs - Hardcoded per garantire crawling brand pages
# ============================================================================
BRAND_SEEDS = {
    'rolex.com': [
        'https://www.rolex.com/en-us/watches',
        'https://www.rolex.com/en-us/about-rolex/history',
        'https://www.rolex.com/en-us/new-watches',
        'https://www.rolex.com/en-us/sustainability'
    ],
    'patek.com': [
        'https://www.patek.com/en/collection',
        'https://www.patek.com/en/company/news',
        'https://www.patek.com/en/company/history'
    ],
    'audemarspiguet.com': [
        'https://www.audemarspiguet.com/com/en/universe/ap-house.html',
        'https://www.audemarspiguet.com/com/en/watch-collection.html',
        'https://www.audemarspiguet.com/com/en/news-room.html'
    ],
    'breguet.com': [
        'https://www.breguet.com/en/history',
        'https://www.breguet.com/en/news'
    ],
    'blancpain.com': [
        'https://www.blancpain.com/en/news',
        'https://www.blancpain.com/en/our-universe/history'
    ],
    'cartier.com': [
        'https://www.cartier.com/en-us/maison/news.html',
        'https://www.cartier.com/en-us/maison/history.html'
    ],
    'girard-perregaux.com': [
        'https://www.girard-perregaux.com/int-en/news',
        'https://www.girard-perregaux.com/int-en/heritage'
    ],
    'piaget.com': [
        'https://www.piaget.com/news',
        'https://www.piaget.com/piaget-society'
    ],
    'omegawatches.com': [
        'https://www.omegawatches.com/planet-omega/stories',
        'https://www.omegawatches.com/planet-omega/heritage',
        'https://www.omegawatches.com/news'
    ],
}


def get_brand_seeds(domain: str) -> List[str]:
    """Ritorna seed URLs hardcoded per un brand"""
    for brand_key, seeds in BRAND_SEEDS.items():
        if brand_key in domain:
            return seeds
    return []


# ============================================================================
# STEALTH PLAYWRIGHT SETUP - Anti Bot Detection
# ============================================================================

def create_stealth_browser(p):
    """
    Crea browser Playwright con massima stealth per evitare detection
    """
    browser = p.chromium.launch(
        headless=True,  # DEVE essere headless su GitHub Actions
        args=[
            # Anti-detection core
            '--disable-blink-features=AutomationControlled',
            
            # Performance
            '--disable-dev-shm-usage',
            '--disable-gpu',
            
            # Security bypass (necessari per GitHub Actions)
            '--no-sandbox',
            '--disable-setuid-sandbox',
            
            # Stealth extra
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-infobars',
            
            # User agent realistico
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
    )
    
    context = browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        locale='en-US',
        timezone_id='America/New_York',
        permissions=['geolocation'],
        geolocation={'latitude': 40.7128, 'longitude': -74.0060},  # NYC
        color_scheme='light',
        has_touch=False,
        is_mobile=False,
        java_script_enabled=True,
    )
    
    return browser, context


def inject_stealth_scripts(page):
    """
    Inietta script per nascondere automazione e sembrare browser umano
    """
    page.add_init_script("""
        // Rimuovi webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        
        // Simula platform Windows
        Object.defineProperty(navigator, 'platform', {
            get: () => 'Win32'
        });
        
        // Simula plugins come browser vero
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        
        // Simula lingue
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en']
        });
        
        // Chrome property
        window.chrome = {
            runtime: {}
        };
        
        // Permissions API
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
    """)


# ============================================================================
# DISCOVERY FUNCTION
# ============================================================================

def stealth_discover(base_url: str, site_type: str, max_depth: int = 7) -> Set[str]:
    """
    Discovery con Playwright in stealth mode per evitare bot detection
    """
    found = set()
    
    with sync_playwright() as p:
        try:
            browser, context = create_stealth_browser(p)
            page = context.new_page()
            
            # Inietta stealth scripts
            inject_stealth_scripts(page)
            
            # Naviga con timeout aumentato
            page.goto(base_url, wait_until='domcontentloaded', timeout=30000)
            
            # Comportamento umano: scroll random
            page.evaluate("""
                window.scrollTo(0, Math.floor(Math.random() * 500));
            """)
            time.sleep(random.uniform(1, 2))
            
            # Estrai links
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(base_url, href)
                
                # Filtra URL validi
                if urlparse(full_url).netloc == urlparse(base_url).netloc:
                    # Filtri per blog vs brand
                    if site_type == 'blog':
                        if any(x in full_url.lower() for x in ['/blog', '/news', '/article', '/post', '/watch', '/review']):
                            found.add(full_url)
                    else:  # brand
                        if any(x in full_url.lower() for x in ['/news', '/story', '/collection', '/history', '/universe', '/maison']):
                            found.add(full_url)
            
            browser.close()
            
        except Exception as e:
            print(f"Discovery error: {e}")
    
    return found


# ============================================================================
# SITEMAP DISCOVERY (FAST)
# ============================================================================

def try_sitemap(base_url: str) -> Set[str]:
    """Prova sitemap.xml variants"""
    found = set()
    domain = urlparse(base_url).netloc
    
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
                
                # Estrai URLs da <loc>
                for loc in soup.find_all('loc'):
                    url = loc.text.strip()
                    if url.startswith('http'):
                        found.add(url)
                
                if found:
                    print(f"[{domain}] Sitemap: {sitemap_url} → {len(found)} URLs")
                    break  # Trovato sitemap valido, stop
                    
        except Exception:
            pass
    
    return found


# ============================================================================
# MAIN DISCOVERY ORCHESTRATOR
# ============================================================================

def discover_urls(base_url: str, site_type: str, max_limit: int = None) -> Set[str]:
    """
    Orchestrator: try sitemap first, then stealth crawl
    """
    domain = urlparse(base_url).netloc
    all_urls = set()
    
    # STEP 1: Sitemap (veloce)
    sitemap_urls = try_sitemap(base_url)
    if sitemap_urls:
        all_urls.update(sitemap_urls)
    
    # STEP 2: Brand seeds (se brand e pochi URL da sitemap)
    if site_type == 'brand':
        seeds = get_brand_seeds(domain)
        if seeds:
            print(f"[{domain}]   🌱 Adding {len(seeds)} seed URLs for {domain}")
            all_urls.update(seeds)
    
    # STEP 3: Se pochi URL (<10), usa stealth discovery
    if len(all_urls) < 10:
        print(f"[{domain}] ⚠️  Solo {len(all_urls)} URLs → Attivando Stealth")
        print(f"[{domain}] 🎭 Stealth Discovery (depth 7, no limit)")
        
        stealth_urls = stealth_discover(base_url, site_type, max_depth=7)
        all_urls.update(stealth_urls)
    
    return all_urls


if __name__ == '__main__':
    # Test
    urls = discover_urls('https://www.rolex.com', 'brand')
    print(f"\nTrovati {len(urls)} URLs")
    for url in list(urls)[:10]:
        print(f"  - {url}")
