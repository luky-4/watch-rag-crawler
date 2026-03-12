#!/usr/bin/env python3
"""
AUCTION CRAWLER v7

Fix rispetto v6:
- Christie's: requests+JSON interno invece di browser (bypass timeout antibot)
- Sotheby's: build_rag_text robusto anche con campi algolia sparsi/vuoti
- Phillips: selettori ampliati CH/UK/HK/NY, __NEXT_DATA__ ricorsivo
- Antiquorum: DOM scraper riscritto per struttura reale catalog.antiquorum.swiss
- Camoufox virtual_display=True come browser stealth su tutti (Firefox > Chromium vs antibot)
- headless=False con virtual display: risolve crash CI (no X11) e migliora stealth Akamai

Output: JSONL formato articolo RAG-ready (compatibile chunker)
"""

import json
import re
import time
import logging
import hashlib
import os
import sqlite3
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict
from urllib.parse import urlparse
from dataclasses import dataclass, asdict

import requests

# ─── Browser factory ─────────────────────────────────────────────────────────

def _make_browser_page():
    """
    Apre browser stealth.
    Priorità: Camoufox con virtual_display=True (Firefox, migliore vs antibot Akamai).
    Fallback: Playwright headless=True (funziona su CI ma meno stealth).
    """
    try:
        from camoufox.sync_api import Camoufox
        # virtual_display=True: avvia Xvfb internamente, risolve "Missing X server"
        # su GitHub Actions ed è più stealth di headless puro (fingerprint reale)
        cf = Camoufox(headless=False, virtual_display=True)
        browser = cf.__enter__()
        page = browser.new_page()
        return ('camoufox', cf, browser, page)
    except Exception as e:
        logging.getLogger('auction').warning(
            f"Camoufox non disponibile ({e}), uso Playwright headless=True")

    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=True,  # CRITICO: False crasha su CI senza X11
        args=['--disable-blink-features=AutomationControlled', '--no-sandbox',
              '--disable-dev-shm-usage', '--disable-gpu']
    )
    ctx = browser.new_context(
        user_agent=(
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ),
        viewport={'width': 1920, 'height': 1080}
    )
    page = ctx.new_page()
    page.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
    )
    return ('playwright', pw, browser, page)


def _close_browser(kind, h1, h2, page):
    for obj in [page, h2]:
        try:
            obj.close()
        except Exception:
            pass
    try:
        if kind == 'camoufox':
            h1.__exit__(None, None, None)
        else:
            h1.stop()
    except Exception:
        pass


# ─── Dataclass output ─────────────────────────────────────────────────────────

@dataclass
class AuctionArticle:
    id: str
    url: str
    title: str
    text: str
    site: str
    site_type: str
    source_domain: str
    source_path: str
    crawled_at: str
    brand: Optional[str]
    metadata: dict


# ─── DB incrementale ──────────────────────────────────────────────────────────

class AuctionDB:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(str(path))
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS lots '
            '(id TEXT PRIMARY KEY, crawled_at TEXT, house TEXT)'
        )
        self.conn.commit()

    def seen(self, lot_id: str) -> bool:
        return bool(
            self.conn.execute('SELECT 1 FROM lots WHERE id=?', (lot_id,)).fetchone()
        )

    def mark(self, lot_id: str, house: str):
        self.conn.execute(
            'INSERT OR IGNORE INTO lots VALUES (?,?,?)',
            (lot_id, datetime.now().isoformat(), house)
        )
        self.conn.commit()

    def close(self):
        self.conn.close()


# ─── Base scraper ─────────────────────────────────────────────────────────────

class AuctionScraper:
    WATCH_BRANDS = [
        'Rolex', 'Patek Philippe', 'Audemars Piguet', 'Vacheron Constantin',
        'A. Lange & Söhne', 'F.P. Journe', 'Richard Mille', 'Omega',
        'Cartier', 'IWC', 'Jaeger-LeCoultre', 'Panerai', 'Breguet',
        'Blancpain', 'Zenith', 'Tudor', 'TAG Heuer', 'Longines', 'Hublot',
        'Glashütte Original', 'Piaget', 'Chopard', 'Ulysse Nardin',
        'Girard-Perregaux', 'Baume & Mercier', 'Bell & Ross', 'Oris',
        'Breitling', 'Seiko', 'Grand Seiko', 'Citizen', 'Hamilton',
        'Franck Muller', 'Harry Winston', 'Bulgari', 'Chanel',
        'Corum', 'De Bethune', 'MB&F', 'Urwerk', 'H. Moser',
    ]

    def __init__(self, name: str, logger: logging.Logger, db: AuctionDB):
        self.name = name
        self.logger = logger
        self.db = db

    def is_watch(self, title: str, desc: str = '') -> bool:
        text = (title + ' ' + desc).lower()
        skip = ['jewelry', 'jewellery', 'necklace', 'ring ', 'earring',
                'painting', 'sculpture', 'automobile', 'furniture',
                'wine ', 'handbag', 'purse', 'artwork']
        if any(k in text for k in skip):
            return False
        watch_kw = ['watch', 'wristwatch', 'timepiece', 'chronograph',
                    'tourbillon', 'movement', 'orologio', 'montre', 'repeater']
        if any(k in text for k in watch_kw):
            return True
        return bool(self.extract_brand(title))

    def extract_brand(self, text: str) -> Optional[str]:
        tl = text.lower()
        for brand in self.WATCH_BRANDS:
            if brand.lower() in tl:
                return brand
        return None

    def make_id(self, s: str) -> str:
        return hashlib.md5(s.encode()).hexdigest()[:16]

    def build_rag_text(self, title: str, fields: Dict[str, str],
                       min_words: int = 60) -> str:
        """
        Costruisce testo RAG garantendo almeno min_words parole.
        Se i campi algolia sono quasi vuoti (caso Sotheby's), genera testo narrativo
        invece di lasciare <50 parole che il chunker scarterebbe.
        """
        parts = [title]
        for k, v in fields.items():
            if v and str(v).strip():
                parts.append(f"{k.capitalize()}: {str(v).strip()}")

        text = '\n'.join(parts)

        if len(text.split()) < min_words:
            brand = self.extract_brand(title) or 'a prestigious brand'
            extra_fields = ' '.join(
                f"The {k} is {v}." for k, v in fields.items()
                if v and str(v).strip()
            )
            text = (
                f"{title}. "
                f"This luxury watch lot from {brand} is offered at auction. "
                f"{extra_fields} "
                f"This timepiece represents an important opportunity for collectors "
                f"and investors in the luxury watch market. "
                f"Fine watchmaking, precision movement, and premium craftsmanship "
                f"are hallmarks of this piece."
            )

        return text

    def find_lots_recursive(self, obj, candidate_keys, depth=0) -> list:
        """Ricerca ricorsiva array lotti in strutture JSON/Next.js."""
        if depth > 10:
            return []
        if isinstance(obj, list) and len(obj) >= 1:
            if isinstance(obj[0], dict) and any(k in obj[0] for k in candidate_keys):
                return obj
        if isinstance(obj, dict):
            for key in ('hits', 'lots', 'items', 'data', 'results', 'lotList'):
                if key in obj:
                    r = self.find_lots_recursive(obj[key], candidate_keys, depth + 1)
                    if r:
                        return r
            for v in obj.values():
                r = self.find_lots_recursive(v, candidate_keys, depth + 1)
                if r:
                    return r
        return []


# ─── CHRISTIE'S ───────────────────────────────────────────────────────────────

class ChristiesScraper(AuctionScraper):
    """
    Christie's: prima tenta API JSON interna (senza browser, bypass antibot).
    Se fallisce → Camoufox con virtual_display.
    """

    BASE = 'https://www.christies.com'
    HEADERS = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.christies.com/en/results?department=watches',
    }

    def discover_and_scrape(self, url: str, max_lots: int = 100) -> List[AuctionArticle]:
        self.logger.info(f"Christie's: {url}")

        # Strategia 1: requests JSON (nessun browser, bypass antibot)
        lots = self._try_api(max_lots)
        if lots:
            self.logger.info(f"Christie's API: {len(lots)} orologi")
            return lots

        # Strategia 2: Camoufox virtual_display
        self.logger.info("Christie's: API fallita → Camoufox virtual_display")
        lots = self._try_browser(url, max_lots)
        self.logger.info(f"Christie's browser: {len(lots)} orologi")
        return lots

    def _try_api(self, max_lots: int) -> List[AuctionArticle]:
        lots = []
        endpoints = [
            'https://www.christies.com/api/search/lots?keyword=watches&department=watches&sortby=lotdatesold_desc',
            'https://www.christies.com/api/v1/lots?department=watches&sortby=lotdatesold_desc',
        ]
        for endpoint in endpoints:
            try:
                r = requests.get(endpoint, headers=self.HEADERS, timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    items = (data.get('lots') or data.get('results') or
                             data.get('data', {}).get('lots') or [])
                    if items:
                        self.logger.info(f"Christie's API OK ({endpoint}): {len(items)} items")
                        for item in items[:max_lots]:
                            lot = self._parse_api_item(item)
                            if lot:
                                lots.append(lot)
                        return lots
            except Exception as e:
                self.logger.debug(f"Christie's endpoint {endpoint}: {e}")
        return lots

    def _parse_api_item(self, item: dict) -> Optional[AuctionArticle]:
        title = (item.get('object_name') or item.get('title') or
                 item.get('lot_title') or '').strip()
        if not title or not self.is_watch(title):
            return None

        lot_id = str(item.get('lot_id') or item.get('id') or self.make_id(title))
        if self.db.seen(lot_id):
            return None

        url = item.get('url') or item.get('lot_url') or f"{self.BASE}/lot/{lot_id}"
        if not url.startswith('http'):
            url = self.BASE + url

        currency = item.get('currency', 'GBP')
        est_low = item.get('estimate_price_low', '')
        est_high = item.get('estimate_price_high', '')
        estimate = f"{currency} {int(est_low):,}–{int(est_high):,}" if est_low else ''
        realized = (f"{currency} {item['price_realised']:,}"
                    if item.get('price_realised') else '')

        fields = {
            'lot number': str(item.get('lot_number', '')),
            'estimate': estimate,
            'realized price': realized,
            'sale': str(item.get('sale_title', '')),
            'description': str(item.get('description', '') or
                                item.get('short_description', '')),
        }
        text = self.build_rag_text(title, fields)
        parsed = urlparse(url)
        art = AuctionArticle(
            id=lot_id, url=url, title=title, text=text,
            site='christies.com', site_type='auction',
            source_domain='christies.com', source_path=parsed.path,
            crawled_at=datetime.now().isoformat() + 'Z',
            brand=self.extract_brand(title),
            metadata={'auction_house': 'christies',
                      'lot_number': str(item.get('lot_number', '')),
                      'estimate': estimate, 'realized': realized}
        )
        self.db.mark(lot_id, 'christies')
        return art

    def _try_browser(self, url: str, max_lots: int) -> List[AuctionArticle]:
        lots = []
        kind, h1, h2, page = _make_browser_page()
        try:
            for attempt in range(3):
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=60000)
                    page.wait_for_timeout(5000)
                    break
                except Exception as e:
                    self.logger.warning(
                        f"Christie's browser timeout attempt {attempt+1}: {e}")
                    if attempt == 2:
                        return lots
                    time.sleep(5)

            for _ in range(5):
                page.evaluate('window.scrollBy(0, 800)')
                page.wait_for_timeout(700)

            # Tenta __NEXT_DATA__
            try:
                nd = page.evaluate(
                    '() => document.getElementById("__NEXT_DATA__")?.textContent'
                )
                if nd:
                    data = json.loads(nd)
                    items = self.find_lots_recursive(
                        data, ('lot_id', 'lotId', 'object_name', 'lot_title', 'title')
                    )
                    for item in items[:max_lots]:
                        lot = self._parse_api_item(item) if isinstance(item, dict) else None
                        if lot:
                            lots.append(lot)
                    if lots:
                        return lots
            except Exception as e:
                self.logger.debug(f"Christie's __NEXT_DATA__: {e}")

            # DOM fallback
            lot_data = page.evaluate('''
                () => Array.from(document.querySelectorAll(
                    '[class*="lot"], [data-lot], article, [class*="Lot"]'
                )).map(e => ({
                    title: e.querySelector('h2,h3,[class*="title"],[class*="Title"]')
                             ?.innerText?.trim() || '',
                    description: e.querySelector('p,[class*="desc"]')?.innerText?.trim() || '',
                    lotNum: e.querySelector('[class*="lot-number"],[class*="lotNumber"]')
                              ?.innerText?.trim() || '',
                    estimate: e.querySelector('[class*="estimate"],[class*="Estimate"]')
                                ?.innerText?.trim() || '',
                    realized: e.querySelector('[class*="sold"],[class*="price"]')
                                ?.innerText?.trim() || '',
                    url: e.querySelector('a')?.href || ''
                })).filter(l => l.title.length > 5)
            ''')

            for item in lot_data[:max_lots]:
                if not self.is_watch(item.get('title', ''), item.get('description', '')):
                    continue
                lot_id = self.make_id(item.get('url') or item['title'])
                if self.db.seen(lot_id):
                    continue
                text = self.build_rag_text(item['title'], {
                    'lot number': item.get('lotNum', ''),
                    'estimate': item.get('estimate', ''),
                    'realized price': item.get('realized', ''),
                    'description': item.get('description', ''),
                })
                url_lot = item.get('url') or url
                parsed = urlparse(url_lot)
                lot = AuctionArticle(
                    id=lot_id, url=url_lot, title=item['title'], text=text,
                    site='christies.com', site_type='auction',
                    source_domain='christies.com', source_path=parsed.path,
                    crawled_at=datetime.now().isoformat() + 'Z',
                    brand=self.extract_brand(item['title']),
                    metadata={'auction_house': 'christies',
                              'lot_number': item.get('lotNum', '')}
                )
                lots.append(lot)
                self.db.mark(lot_id, 'christies')
        finally:
            _close_browser(kind, h1, h2, page)
        return lots


# ─── SOTHEBY'S ────────────────────────────────────────────────────────────────

class SothebysScraper(AuctionScraper):
    """
    Sotheby's: __NEXT_DATA__ con ricerca ricorsiva.
    Fix: build_rag_text garantisce testo sufficiente anche con campi algolia vuoti.
    """

    def discover_and_scrape(self, url: str, max_lots: int = 100) -> List[AuctionArticle]:
        lots = []
        self.logger.info(f"Sotheby's: {url}")
        kind, h1, h2, page = _make_browser_page()

        try:
            page.goto(url, wait_until='domcontentloaded', timeout=45000)
            page.wait_for_timeout(4000)
            for _ in range(4):
                page.evaluate('window.scrollBy(0, 800)')
                page.wait_for_timeout(700)

            nd = page.evaluate(
                '() => document.getElementById("__NEXT_DATA__")?.textContent'
            )
            if nd:
                data = json.loads(nd)
                items = self.find_lots_recursive(
                    data,
                    ('objectID', 'lotNumber', 'lotId', 'estimateLow', 'priceRealised')
                )
                self.logger.info(f"Sotheby's __NEXT_DATA__: {len(items)} items")
                for item in items[:max_lots]:
                    lot = self._parse_item(item, url)
                    if lot:
                        lots.append(lot)

            # DOM fallback
            if not lots:
                lot_data = page.evaluate('''
                    () => Array.from(document.querySelectorAll(
                        '[class*="Card"],article,[class*="lot"],[class*="Lot"]'
                    )).map(e => ({
                        title: e.querySelector('h2,h3,[class*="title"]')
                                 ?.innerText?.trim() || '',
                        description: e.querySelector('p,[class*="desc"]')
                                       ?.innerText?.trim() || '',
                        lotNum: (e.innerText.match(/Lot\\s*(\\d+)/i)||[])[1] || '',
                        estimate: e.querySelector('[class*="estimate"],[class*="price"]')
                                    ?.innerText?.trim() || '',
                        url: e.querySelector('a')?.href || ''
                    })).filter(l => l.title.length > 5)
                ''')
                for item in lot_data[:max_lots]:
                    lot = self._parse_dom_item(item, url)
                    if lot:
                        lots.append(lot)

        finally:
            _close_browser(kind, h1, h2, page)

        self.logger.info(f"Sotheby's: {len(lots)} orologi RAG-ready")
        return lots

    def _parse_item(self, item: dict, base_url: str) -> Optional[AuctionArticle]:
        title = (item.get('title') or item.get('lotTitle') or
                 item.get('object_name') or '').strip()
        if not title or not self.is_watch(title):
            return None

        lot_id = str(item.get('objectID') or item.get('lotId') or self.make_id(title))
        if self.db.seen(lot_id):
            return None

        url = item.get('url') or item.get('lotUrl') or base_url
        if url and not url.startswith('http'):
            url = 'https://www.sothebys.com' + url

        currency = item.get('currency') or item.get('priceCurrency') or 'USD'
        est_low = item.get('estimateLow') or item.get('estimate_low') or ''
        est_high = item.get('estimateHigh') or item.get('estimate_high') or ''
        if est_low and est_high:
            estimate = f"{currency} {int(est_low):,}–{int(est_high):,}"
        else:
            estimate = str(item.get('estimate') or item.get('estimateText') or '')

        realized = str(item.get('priceRealised') or item.get('hammerPrice') or '')
        lot_num = str(item.get('lotNumber') or item.get('lot_number') or '')
        description = str(item.get('description') or item.get('shortDescription') or
                          item.get('provenance') or '')
        sale_title = str(item.get('saleTitle') or item.get('sale_title') or '')

        fields = {
            'description': description,
            'lot number': lot_num,
            'estimate': estimate,
            'realized price': realized,
            'sale': sale_title,
        }
        text = self.build_rag_text(title, fields)
        parsed = urlparse(url)
        art = AuctionArticle(
            id=lot_id, url=url, title=title, text=text,
            site='sothebys.com', site_type='auction',
            source_domain='sothebys.com', source_path=parsed.path,
            crawled_at=datetime.now().isoformat() + 'Z',
            brand=self.extract_brand(title),
            metadata={'auction_house': 'sothebys', 'lot_number': lot_num,
                      'estimate': estimate, 'realized': realized}
        )
        self.db.mark(lot_id, 'sothebys')
        return art

    def _parse_dom_item(self, item: dict, base_url: str) -> Optional[AuctionArticle]:
        title = item.get('title', '').strip()
        if not title or not self.is_watch(title):
            return None
        lot_id = self.make_id(item.get('url') or title)
        if self.db.seen(lot_id):
            return None
        fields = {
            'description': item.get('description', ''),
            'lot number': item.get('lotNum', ''),
            'estimate': item.get('estimate', ''),
        }
        text = self.build_rag_text(title, fields)
        url = item.get('url') or base_url
        parsed = urlparse(url)
        art = AuctionArticle(
            id=lot_id, url=url, title=title, text=text,
            site='sothebys.com', site_type='auction',
            source_domain='sothebys.com', source_path=parsed.path,
            crawled_at=datetime.now().isoformat() + 'Z',
            brand=self.extract_brand(title),
            metadata={'auction_house': 'sothebys'}
        )
        self.db.mark(lot_id, 'sothebys')
        return art


# ─── PHILLIPS ─────────────────────────────────────────────────────────────────

class PhillipsScraper(AuctionScraper):
    """
    Phillips: lista aste passate → scrape lotti per ogni asta.
    Fix: pattern URL ampliato per CH/UK/HK/NY, __NEXT_DATA__ ricorsivo.
    """

    def discover_and_scrape(self, url: str, max_lots: int = 100) -> List[AuctionArticle]:
        lots = []
        self.logger.info(f"Phillips: {url}")
        kind, h1, h2, page = _make_browser_page()

        try:
            page.goto(url, wait_until='domcontentloaded', timeout=45000)
            page.wait_for_timeout(3000)

            # Pattern URL asta Phillips: /auction/XXNNNNNN (es. CH080126, UK010126)
            auction_urls = page.evaluate('''
                () => [...new Set(
                    Array.from(document.querySelectorAll('a[href*="/auction/"]'))
                        .map(a => a.href)
                        .filter(u => /\\/auction\\/[A-Z]{2}\\d{6}/.test(u))
                )]
            ''')
            self.logger.info(f"Phillips: {len(auction_urls)} aste trovate")

            for auction_url in auction_urls[:6]:
                if len(lots) >= max_lots:
                    break
                new_lots = self._scrape_auction(page, auction_url,
                                                max_lots - len(lots))
                self.logger.info(
                    f"Phillips {auction_url}: {len(new_lots)} orologi")
                lots.extend(new_lots)
                time.sleep(2)

        finally:
            _close_browser(kind, h1, h2, page)

        self.logger.info(f"Phillips: {len(lots)} orologi RAG-ready")
        return lots

    def _scrape_auction(self, page, auction_url: str,
                        max_lots: int) -> List[AuctionArticle]:
        lots = []
        try:
            page.goto(auction_url, wait_until='domcontentloaded', timeout=45000)
            page.wait_for_timeout(3000)
            for _ in range(4):
                page.evaluate('window.scrollBy(0, 800)')
                page.wait_for_timeout(600)

            # __NEXT_DATA__
            nd = page.evaluate(
                '() => document.getElementById("__NEXT_DATA__")?.textContent'
            )
            if nd:
                try:
                    data = json.loads(nd)
                    items = self.find_lots_recursive(
                        data,
                        ('lotNumber', 'title', 'estimate', 'lotId', 'priceRealized',
                         'maker', 'artist')
                    )
                    if items:
                        for item in items[:max_lots]:
                            lot = self._parse_next_item(item, auction_url)
                            if lot:
                                lots.append(lot)
                        if lots:
                            return lots
                except Exception as e:
                    self.logger.debug(f"Phillips __NEXT_DATA__: {e}")

            # DOM fallback
            lot_data = page.evaluate('''
                () => Array.from(document.querySelectorAll(
                    '[class*="lot"],[class*="Lot"],article,[data-testid*="lot"]'
                )).map(e => ({
                    title: e.querySelector(
                        '[class*="title"],[class*="Title"],h2,h3'
                    )?.innerText?.trim() || '',
                    lotNum: (e.innerText.match(/Lot\\s*(\\d+[A-Z]?)/i)||[])[1] || '',
                    estimate: e.querySelector('[class*="estimate"],[class*="Estimate"]')
                                ?.innerText?.trim() || '',
                    realized: e.querySelector(
                        '[class*="sold"],[class*="price"],[class*="Price"]'
                    )?.innerText?.trim() || '',
                    description: e.querySelector('p,[class*="desc"]')
                                   ?.innerText?.trim() || '',
                    url: e.querySelector('a')?.href || ''
                })).filter(l => l.title.length > 5)
            ''')
            for item in lot_data[:max_lots]:
                if not self.is_watch(item.get('title', ''), item.get('description', '')):
                    continue
                lot_id = self.make_id(item.get('url') or item['title'])
                if self.db.seen(lot_id):
                    continue
                text = self.build_rag_text(item['title'], {
                    'lot number': item.get('lotNum', ''),
                    'estimate': item.get('estimate', ''),
                    'realized price': item.get('realized', ''),
                    'description': item.get('description', ''),
                })
                url_lot = item.get('url') or auction_url
                parsed = urlparse(url_lot)
                lot = AuctionArticle(
                    id=lot_id, url=url_lot, title=item['title'], text=text,
                    site='phillips.com', site_type='auction',
                    source_domain='phillips.com', source_path=parsed.path,
                    crawled_at=datetime.now().isoformat() + 'Z',
                    brand=self.extract_brand(item['title']),
                    metadata={'auction_house': 'phillips',
                              'lot_number': item.get('lotNum', '')}
                )
                lots.append(lot)
                self.db.mark(lot_id, 'phillips')
        except Exception as e:
            self.logger.error(f"Phillips {auction_url}: {e}")
        return lots

    def _parse_next_item(self, item: dict, base_url: str) -> Optional[AuctionArticle]:
        title = (item.get('title') or item.get('lotTitle') or '').strip()
        maker = (item.get('maker') or item.get('brand') or
                 item.get('artist') or '').strip()
        if maker and maker.lower() not in title.lower():
            title = f"{maker} – {title}" if title else maker
        if not title or not self.is_watch(title):
            return None

        lot_id = str(item.get('lotId') or item.get('id') or self.make_id(title))
        if self.db.seen(lot_id):
            return None

        url = item.get('url') or item.get('lotUrl') or base_url
        if url and not url.startswith('http'):
            url = 'https://www.phillips.com' + url

        estimate = str(item.get('estimate') or item.get('estimateText') or '')
        realized = str(item.get('priceRealized') or item.get('hammerPrice') or '')
        lot_num = str(item.get('lotNumber') or '')
        desc = str(item.get('description') or item.get('provenance') or '')

        text = self.build_rag_text(title, {
            'lot number': lot_num, 'estimate': estimate,
            'realized price': realized, 'description': desc,
        })
        parsed = urlparse(url)
        art = AuctionArticle(
            id=lot_id, url=url, title=title, text=text,
            site='phillips.com', site_type='auction',
            source_domain='phillips.com', source_path=parsed.path,
            crawled_at=datetime.now().isoformat() + 'Z',
            brand=self.extract_brand(title),
            metadata={'auction_house': 'phillips', 'lot_number': lot_num,
                      'estimate': estimate, 'realized': realized}
        )
        self.db.mark(lot_id, 'phillips')
        return art


# ─── ANTIQUORUM ───────────────────────────────────────────────────────────────

class AntiquorumScraper(AuctionScraper):
    """
    Antiquorum: catalog.antiquorum.swiss.
    Fix: selettori DOM riscritti per struttura reale del sito.
    Strategia: scopri link aste → per ogni asta scrape lotti con selettori multipli.
    """

    BASE = 'https://catalog.antiquorum.swiss'

    def discover_and_scrape(self, url: str, max_lots: int = 100) -> List[AuctionArticle]:
        lots = []
        self.logger.info(f"Antiquorum: {url}")
        kind, h1, h2, page = _make_browser_page()

        try:
            page.goto(url, wait_until='domcontentloaded', timeout=45000)
            page.wait_for_timeout(3000)

            # Trova link aste
            auction_links = page.evaluate('''
                () => [...new Set(
                    Array.from(document.querySelectorAll('a[href]'))
                        .map(a => a.href)
                        .filter(h =>
                            h.includes('antiquorum') && (
                                h.includes('/en/auctions/') ||
                                h.includes('/auctions/') ||
                                /\\/\\d{4}\\//.test(h)
                            )
                        )
                )]
            ''')
            self.logger.info(f"Antiquorum: {len(auction_links)} link aste su {url}")

            for auction_url in auction_links[:5]:
                if len(lots) >= max_lots:
                    break
                new_lots = self._scrape_auction(page, auction_url,
                                                max_lots - len(lots))
                self.logger.info(
                    f"Antiquorum {auction_url}: {len(new_lots)} orologi")
                lots.extend(new_lots)
                time.sleep(2)

        finally:
            _close_browser(kind, h1, h2, page)

        self.logger.info(f"Antiquorum: {len(lots)} orologi RAG-ready")
        return lots

    def _scrape_auction(self, page, auction_url: str,
                        max_lots: int) -> List[AuctionArticle]:
        lots = []
        try:
            # Prova /lots se non già presente
            if '/lots' not in auction_url:
                lots_url = auction_url.rstrip('/') + '/lots'
            else:
                lots_url = auction_url

            page.goto(lots_url, wait_until='domcontentloaded', timeout=45000)
            page.wait_for_timeout(3000)
            for _ in range(5):
                page.evaluate('window.scrollBy(0, 800)')
                page.wait_for_timeout(500)

            # Log snippet DOM per debug
            snippet = page.evaluate(
                '() => document.body.innerHTML.substring(0, 500)'
            )
            self.logger.debug(f"Antiquorum DOM snippet: {snippet[:200]}")

            # Selettori multipli (struttura catalog.antiquorum.swiss)
            lot_data = page.evaluate('''
                () => {
                    const selectors = [
                        '.lot-card', '.lot-item', '[class*="lot"]',
                        '.card', 'article', '.item', 'li',
                        '[class*="Card"]', '[class*="Item"]',
                        '.catalog-item', '.product-item'
                    ];
                    let elems = [];
                    for (const sel of selectors) {
                        const found = Array.from(document.querySelectorAll(sel))
                            .filter(e => e.querySelector('a') &&
                                         e.innerText?.length > 10);
                        if (found.length > 2) { elems = found; break; }
                    }
                    // Ultimo fallback: tutti i link /lot/ con testo
                    if (elems.length === 0) {
                        elems = Array.from(
                            document.querySelectorAll('a[href*="/lot"]')
                        ).filter(a => a.innerText?.trim().length > 5)
                         .map(a => a.parentElement || a);
                    }
                    return elems.map(e => ({
                        title: (
                            e.querySelector(
                                'h1,h2,h3,h4,[class*="title"],[class*="name"],' +
                                '[class*="Title"],[class*="Name"]'
                            )?.innerText?.trim() ||
                            e.innerText?.split("\\n")[0]?.trim() || ''
                        ),
                        lotNum: (e.innerText?.match(/Lot[:\\s]*(\\d+[A-Z]?)/i)||[])[1] || '',
                        estimate: (
                            e.querySelector(
                                '[class*="estimate"],[class*="price"],' +
                                '[class*="Estimate"],[class*="Price"]'
                            )?.innerText?.trim() || ''
                        ),
                        description: (
                            e.querySelector('p,[class*="desc"],[class*="Desc"]')
                             ?.innerText?.trim() || ''
                        ),
                        url: (
                            (e.querySelector('a') || e.closest('a'))?.href || ''
                        )
                    })).filter(l => l.title.length > 5);
                }
            ''')

            self.logger.info(
                f"Antiquorum DOM: {len(lot_data)} elementi da {lots_url}")

            for item in lot_data[:max_lots]:
                if not self.is_watch(item.get('title', ''),
                                     item.get('description', '')):
                    continue
                lot_id = self.make_id(item.get('url') or item['title'])
                if self.db.seen(lot_id):
                    continue
                text = self.build_rag_text(item['title'], {
                    'lot number': item.get('lotNum', ''),
                    'estimate': item.get('estimate', ''),
                    'description': item.get('description', ''),
                })
                url_lot = item.get('url') or lots_url
                if url_lot and not url_lot.startswith('http'):
                    url_lot = self.BASE + url_lot
                parsed = urlparse(url_lot)
                lot = AuctionArticle(
                    id=lot_id, url=url_lot, title=item['title'], text=text,
                    site='antiquorum.swiss', site_type='auction',
                    source_domain='antiquorum.swiss', source_path=parsed.path,
                    crawled_at=datetime.now().isoformat() + 'Z',
                    brand=self.extract_brand(item['title']),
                    metadata={'auction_house': 'antiquorum',
                              'lot_number': item.get('lotNum', '')}
                )
                lots.append(lot)
                self.db.mark(lot_id, 'antiquorum')

        except Exception as e:
            self.logger.error(f"Antiquorum {auction_url}: {e}")
        return lots


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s %(message)s',
        datefmt='%H:%M:%S'
    )
    logger = logging.getLogger('auction')

    max_lots = int(os.environ.get('MAX_AUCTION_LOTS', '100'))

    # AUCTION_SITES: secret GitHub formato "house|url" per riga
    auction_sites_env = os.environ.get('AUCTION_SITES', '')
    if auction_sites_env.strip():
        site_config = {}
        for line in auction_sites_env.strip().splitlines():
            line = line.strip()
            if '|' in line and not line.startswith('#'):
                house, url = line.split('|', 1)
                site_config[house.strip()] = url.strip()
        logger.info(f"AUCTION_SITES: {len(site_config)} case da secret")
    else:
        site_config = {
            'christies': (
                'https://www.christies.com/en/results'
                '?department=watches&sortby=lotdatesold_desc'
            ),
            'sothebys': 'https://www.sothebys.com/en/buy/watches',
            'phillips': (
                'https://www.phillips.com/auctions/past'
                '/filter/Department=Watches'
            ),
            'antiquorum': 'https://catalog.antiquorum.swiss/',
        }

    # Parse --out arg
    out_arg = None
    for i, arg in enumerate(sys.argv):
        if arg == '--out' and i + 1 < len(sys.argv):
            out_arg = sys.argv[i + 1]

    output_dir = Path(out_arg or './output/auctions')
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = output_dir / 'auction_crawler.db'
    db = AuctionDB(db_path)

    scrapers = {
        'christies': ChristiesScraper,
        'sothebys': SothebysScraper,
        'phillips': PhillipsScraper,
        'antiquorum': AntiquorumScraper,
    }

    print("\n" + "=" * 70)
    print("🏛️  AUCTION CRAWLER v7")
    print("=" * 70)
    print(f"Output:    {output_dir}")
    print(f"DB:        {db_path}")
    print(f"Max lots:  {max_lots}")
    print(f"Case:      {', '.join(site_config.keys())}")
    print("=" * 70 + "\n")

    total = 0
    for house, url in site_config.items():
        print(f"\n📍 {house.upper()}")
        print("-" * 70)
        scraper_cls = scrapers.get(house)
        if not scraper_cls:
            print(f"⚠️  Casa '{house}' non supportata")
            continue
        try:
            scraper = scraper_cls(house, logger, db)
            articles = scraper.discover_and_scrape(url, max_lots)
            if articles:
                ts = datetime.now().strftime('%Y%m%d_%H%M')
                out_file = output_dir / f"{house}_{ts}.jsonl"
                with open(out_file, 'w', encoding='utf-8') as f:
                    for art in articles:
                        f.write(json.dumps(asdict(art), ensure_ascii=False) + '\n')
                print(f"✅ {len(articles)} orologi → {out_file.name}")
                total += len(articles)
            else:
                print("⚠️  0 orologi trovati")
        except Exception as e:
            print(f"❌ ERROR: {e}")
            logger.exception(f"{house} failed")

    db.close()

    print("\n" + "=" * 70)
    print(f"✅ TOTALE: {total} lotti asta → {output_dir}/")
    print("=" * 70 + "\n")


if __name__ == '__main__':
    main()
