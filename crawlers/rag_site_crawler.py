#!/usr/bin/env python3
"""
RAG Site Crawler v14 - Con Playwright Extraction

Novità v14:
- Playwright extraction per siti brand heavy-JS (Rolex, Cartier, ecc.)
- Discovery a 2 fasi (fast sitemap → stealth playwright)
- Extraction intelligente (trafilatura per blog, Playwright per brand)
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
import hashlib
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import List, Tuple, Optional, Set, Dict
from urllib.parse import urlparse, urljoin
from dataclasses import dataclass
from collections import Counter

# Playwright (opzionale)
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# Nascondi warning urllib3
warnings.filterwarnings('ignore', message='Connection pool is full')
warnings.filterwarnings('ignore', category=DeprecationWarning)

import trafilatura
from trafilatura import feeds, extract, fetch_url, bare_extraction
from trafilatura.settings import use_config
from trafilatura.spider import focused_crawler

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    from usp.tree import sitemap_tree_for_homepage
    USP_AVAILABLE = True
except ImportError:
    USP_AVAILABLE = False

# Crawl4AI opzionale - skip se da errori
CRAWL4AI_AVAILABLE = False
try:
    from crawl4ai import WebCrawler
    CRAWL4AI_AVAILABLE = True
except:
    pass

# Timeout aggressivi
SPIDER_TIMEOUT = 30  # Ridotto, tanto non serve molto
SITEMAP_TIMEOUT = 90  # Ridotto da 120 per evitare attese lunghe
RSS_TIMEOUT = 20
FETCH_TIMEOUT = 60

config = use_config()
config.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")

# --- Filtro URL spazzatura (Patch 2) ---
import re as _re
_JUNK_URL_PATTERNS = _re.compile(
    r'(?:feed|rss|atom|sitemap|\.xml|wp-json|wp-admin|wp-login'
    r'|/tag/|/tags/|/category/|/categories/|/author/|/page/\d+'
    r'|/search|/cart|/checkout|/account|/login|/register'
    r'|\.(css|js|png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|pdf|zip)$'
    r'|utm_|#)',
    _re.IGNORECASE
)
_MIN_WORDS = 80  # Articoli con meno parole = 404/landing vuote

def is_junk_url(url: str) -> bool:
    return bool(_JUNK_URL_PATTERNS.search(url))


# -------------- Chunking (uguale) --------------

@dataclass
class Chunk:
    id: str
    article_id: str
    chunk_index: int
    total_chunks: int
    text: str
    token_count: int
    metadata: Dict


class SemanticChunker:
    def __init__(self, target_tokens: int = 300, max_tokens: int = 450, 
                 min_tokens: int = 150, overlap_sentences: int = 1):
        self.target_tokens = target_tokens
        self.max_tokens = max_tokens
        self.min_tokens = min_tokens
        self.overlap_sentences = overlap_sentences
    
    def estimate_tokens(self, text: str) -> int:
        return int(len(text.split()) / 1.3)
    
    def split_into_sentences(self, text: str) -> List[str]:
        text = re.sub(r'\bADVERTISEMENT\b', '', text)
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def create_chunks(self, sentences: List[str]) -> List[str]:
        chunks = []
        current_chunk = []
        current_tokens = 0
        
        i = 0
        while i < len(sentences):
            sentence = sentences[i]
            sentence_tokens = self.estimate_tokens(sentence)
            
            if sentence_tokens > self.max_tokens:
                if current_chunk:
                    chunks.append(' '.join(current_chunk))
                    current_chunk = []
                    current_tokens = 0
                
                sub_parts = re.split(r'[,;]\s+', sentence)
                temp_chunk = []
                temp_tokens = 0
                
                for part in sub_parts:
                    part_tokens = self.estimate_tokens(part)
                    if temp_tokens + part_tokens > self.max_tokens:
                        if temp_chunk:
                            chunks.append(' '.join(temp_chunk))
                        temp_chunk = [part]
                        temp_tokens = part_tokens
                    else:
                        temp_chunk.append(part)
                        temp_tokens += part_tokens
                
                if temp_chunk:
                    chunks.append(' '.join(temp_chunk))
                i += 1
                continue
            
            if current_tokens + sentence_tokens <= self.max_tokens:
                current_chunk.append(sentence)
                current_tokens += sentence_tokens
                i += 1
            else:
                chunks.append(' '.join(current_chunk))
                overlap_start = max(0, len(current_chunk) - self.overlap_sentences)
                current_chunk = current_chunk[overlap_start:]
                current_tokens = sum(self.estimate_tokens(s) for s in current_chunk)
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks
    
    def extract_metadata(self, article: Dict) -> Dict:
        text = article.get('text', '')
        
        title = article.get('title')
        if not title and text:
            first_para = text.split('\n')[0][:100]
            if len(first_para) > 20:
                title = first_para
        
        brands_mentioned = []
        brand_patterns = [
            r'\b(Rolex|Omega|Seiko|Breitling|Patek Philippe|Audemars Piguet|'
            r'Vacheron Constantin|IWC|Panerai|Cartier|Tudor|Grand Seiko|'
            r'Urwerk|Doxa|Hamilton|Marathon|Heuer|TAG Heuer|Longines|Blancpain)\b'
        ]
        for pattern in brand_patterns:
            brands_mentioned.extend(re.findall(pattern, text, re.IGNORECASE))
        brands_mentioned = list(set([b.title() for b in brands_mentioned]))
        
        topics = []
        text_lower = text.lower()
        topic_keywords = {
            'dive_watches': ['dive', 'diving', 'submariner', 'seamaster'],
            'chronographs': ['chronograph', 'daytona', 'speedmaster'],
            'vintage': ['vintage', '1950s', '1960s', '1970s'],
        }
        for topic, keywords in topic_keywords.items():
            if any(kw in text_lower for kw in keywords):
                topics.append(topic)
        if not topics:
            topics = ['general']
        
        return {
            'source': {
                'url': article.get('source_url'),
                'domain': article.get('source_domain'),
                'type': article.get('site_type', 'blog'),
                'path': article.get('source_path')
            },
            'article': {
                'title': title,
                'date': article.get('date'),
                'authors': article.get('authors'),
                'id': article.get('id')
            },
            'taxonomy': {
                'brand': article.get('brand') or (brands_mentioned[0] if brands_mentioned else None),
                'brands_mentioned': brands_mentioned,
                'topics': topics,
                'site': article.get('site')
            },
            'crawled_at': article.get('crawled_at')
        }
    
    def process_article(self, article: Dict) -> List[Chunk]:
        text = article.get('text', '')
        if not text:
            return []
        
        sentences = self.split_into_sentences(text)
        if not sentences:
            return []
        
        chunk_texts = self.create_chunks(sentences)
        metadata = self.extract_metadata(article)
        
        chunks = []
        article_id = article.get('id', article.get('source_url', ''))
        
        for idx, chunk_text in enumerate(chunk_texts):
            chunk_id = hashlib.md5(f"{article_id}_{idx}".encode()).hexdigest()
            token_count = self.estimate_tokens(chunk_text)
            
            chunk = Chunk(
                id=chunk_id,
                article_id=article_id,
                chunk_index=idx,
                total_chunks=len(chunk_texts),
                text=chunk_text,
                token_count=token_count,
                metadata=metadata
            )
            chunks.append(chunk)
        
        return chunks


# -------------- Database --------------

class CrawlerDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.lock = Lock()
        self._init_db()
    
    def _init_db(self):
        with self.lock:
            c = self.conn.cursor()
            c.execute('''
                CREATE TABLE IF NOT EXISTS urls (
                    url TEXT PRIMARY KEY,
                    domain TEXT,
                    status TEXT,
                    crawled_at TIMESTAMP,
                    error_msg TEXT
                )
            ''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain)')
            self.conn.commit()
    
    def mark_url_crawled(self, url: str, domain: str, status: str, error_msg: str = None):
        with self.lock:
            c = self.conn.cursor()
            c.execute('''
                INSERT OR REPLACE INTO urls (url, domain, status, crawled_at, error_msg)
                VALUES (?, ?, ?, ?, ?)
            ''', (url, domain, status, datetime.utcnow().isoformat(), error_msg))
            self.conn.commit()
    
    def get_crawled_urls(self, domain: str) -> Set[str]:
        with self.lock:
            c = self.conn.cursor()
            c.execute('SELECT url FROM urls WHERE domain = ? AND status = "success"', (domain,))
            return {row[0] for row in c.fetchall()}
    
    def close(self):
        self.conn.close()


# -------------- Discovery CON TIMEOUT AGGRESSIVI --------------

def normalize_url(url: str) -> str:
    if not url:
        return None
    url = url.split('#')[0].split('?')[0]
    if url.endswith('/'):
        url = url[:-1]
    return url


def discover_spider(base_url: str, logger: logging.Logger, mode: str, max_urls: int) -> Set[str]:
    """Spider con timeout - NESSUN LIMITE reale"""
    urls = set()
    domain = urlparse(base_url).netloc
    
    def _crawl():
        try:
            # NESSUN LIMITE - trova tutto
            max_seen = 999999
            max_known = 999999
            
            to_visit, known_links = focused_crawler(
                base_url, 
                max_seen_urls=max_seen,
                max_known_urls=max_known
            )
            
            # FIX: converti liste in set
            spider_urls = set(to_visit) | set(known_links)
            spider_urls = {u for u in spider_urls if domain in urlparse(u).netloc}
            urls.update(spider_urls)
        except Exception as e:
            logger.debug(f"Spider error: {e}")
    
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_crawl)
        try:
            fut.result(timeout=SPIDER_TIMEOUT)
        except FutureTimeoutError:
            logger.warning(f"Spider timeout ({SPIDER_TIMEOUT}s)")
        except Exception:
            pass
    
    return urls


def discover_sitemap(base_url: str, logger: logging.Logger, max_urls: int, mode: str = 'blog') -> Set[str]:
    """Sitemap con DOPPIO metodo: USP + parsing diretto XML + scraping fallback"""
    import re  # Import qui per evitare scope issues
    
    urls = set()
    domain = urlparse(base_url).netloc
    
    def _fetch():
        # METODO 1: ultimate-sitemap-parser con TIMEOUT DINAMICO
        if USP_AVAILABLE:
            try:
                logger.info(f"Sitemap: Provando USP parser su {base_url}")
                
                # WRAPPER con timeout per tree generation
                import time
                from concurrent.futures import ThreadPoolExecutor as TPE, TimeoutError as FTO
                
                def _get_tree():
                    return sitemap_tree_for_homepage(base_url)
                
                # Timeout 30s per generare tree
                with TPE(max_workers=1) as executor:
                    tree_future = executor.submit(_get_tree)
                    try:
                        tree = tree_future.result(timeout=30)  # 30s per tree
                    except FTO:
                        logger.warning(f"Sitemap: USP tree generation timeout (30s)")
                        tree = None
                
                if tree:
                    count_before = len(urls)
                    start_time = time.time()
                    max_usp_urls = 10000
                    max_usp_seconds = 60  # 60s per iterare (era 25s)
                    
                    for idx, page in enumerate(tree.all_pages()):
                        if idx >= max_usp_urls:
                            logger.info(f"Sitemap: Raggiunto limite {max_usp_urls} URLs, stop USP")
                            break
                        
                        if time.time() - start_time > max_usp_seconds:
                            logger.info(f"Sitemap: Timeout USP {max_usp_seconds}s, stop (trovati {idx} URLs)")
                            break
                        
                        url_str = str(page.url) if hasattr(page, 'url') else str(page)
                        if url_str and domain in url_str:
                            urls.add(url_str)
                    
                    count_after = len(urls)
                    logger.info(f"Sitemap: USP trovato {count_after - count_before} URLs (totale ora: {count_after})")
                    
            except Exception as e:
                logger.warning(f"Sitemap: USP error: {e}")
        
        # METODO 2: parsing DIRETTO XML (SOLO se USP ha trovato poco)
        if len(urls) < 100:
            sitemap_paths = [
                '/sitemap.xml',
                '/post-sitemap.xml',
                '/page-sitemap.xml',
            ]
            
            logger.info(f"Sitemap: USP trovato poco ({len(urls)} URLs), provo 3 XML diretti")
            
            for path in sitemap_paths:
                # STOP se hai già trovato abbastanza
                if len(urls) >= 1000:
                    logger.info(f"Sitemap: Trovati {len(urls)} URLs, STOP")
                    break
            sitemap_url = base_url.rstrip('/') + path
            try:
                logger.info(f"Sitemap: Provando {sitemap_url}")
                downloaded = fetch_url(sitemap_url)
                
                if downloaded:
                    logger.info(f"Sitemap: Scaricato {path} ({len(downloaded)} bytes)")
                    
                    # Cerca tutti i <loc>
                    import re
                    locs = re.findall(r'<loc>(.*?)</loc>', downloaded)
                    logger.info(f"Sitemap: Trovati {len(locs)} tag <loc> in {path}")
                    
                    # Se il sitemap contiene altri sitemap (sitemap index), scaricali
                    if 'sitemap' in downloaded.lower() and len(locs) < 100:
                        logger.info(f"Sitemap: {path} sembra un sitemap index, scarico sub-sitemap")
                        for sub_sitemap_url in locs[:10]:  # Max 10 sub-sitemap
                            if 'sitemap' in sub_sitemap_url and sub_sitemap_url.startswith('http'):
                                try:
                                    sub_downloaded = fetch_url(sub_sitemap_url)
                                    if sub_downloaded:
                                        sub_locs = re.findall(r'<loc>(.*?)</loc>', sub_downloaded)
                                        logger.info(f"Sitemap: Sub-sitemap {sub_sitemap_url} → {len(sub_locs)} URLs")
                                        locs.extend(sub_locs)
                                except Exception as e:
                                    logger.debug(f"Sub-sitemap {sub_sitemap_url} error: {e}")
                    
                    added = 0
                    for loc in locs:
                        if domain in loc:
                            urls.add(normalize_url(loc))
                            added += 1
                    
                    logger.info(f"Sitemap: {path} → +{added} URLs validi (totale ora: {len(urls)})")
                    
                    # Log primi 3
                    if added > 0:
                        sample = [loc for loc in locs if domain in loc][:3]
                        logger.info(f"Sitemap: Sample da {path}: {sample}")
                        
                    # Non fermarti mai, prova tutti i path!
                else:
                    logger.debug(f"Sitemap: {path} non disponibile (404 o altro)")
            except Exception as e:
                logger.debug(f"Sitemap: {path} error: {e}")
        
        # METODO 3: SCRAPING HOMEPAGE (se sitemap fallisce completamente)
        if len(urls) == 0:
            logger.info(f"Sitemap: Nessun URL trovato, provo scraping homepage")
            try:
                html = fetch_url(base_url)
                if html:
                    # Cerca tutti i link nella homepage
                    import re
                    links = re.findall(r'href=["\']([^"\']+)["\']', html)
                    
                    for link in links[:500]:  # Max 500 link
                        # Skip URL malformati
                        if not link or '//' in link[8:]:  # Skip // dopo http://
                            continue
                            
                        # Converti relativi in assoluti
                        if link.startswith('/'):
                            link = base_url.rstrip('/') + link
                        elif not link.startswith('http'):
                            continue
                        
                        # Solo dominio corretto, non homepage
                        if domain in link and link != base_url and link != base_url + '/':
                            normalized = normalize_url(link)
                            if normalized and len(normalized) > 15:  # Minimo 15 caratteri
                                urls.add(normalized)
                    
                    logger.info(f"Sitemap: Scraping homepage → {len(urls)} URLs")
            except Exception as e:
                logger.debug(f"Scraping homepage error: {e}")
        
        logger.info(f"Sitemap: TOTALE finale {len(urls)} URLs da tutti i metodi")
    
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_fetch)
        try:
            fut.result(timeout=100)  # 30s tree + 60s iter + 10s margine
        except FutureTimeoutError:
            logger.warning(f"Sitemap timeout (100s) - usando URLs trovati finora")
            # Continua comunque - ritorna quello che ha trovato
        except Exception as e:
            logger.error(f"Sitemap executor error: {e}")
    
    return urls


def discover_rss(base_url: str, logger: logging.Logger) -> Set[str]:
    """RSS con metodo MIGLIORATO (come vecchia versione) + DEBUG"""
    urls = set()
    domain = urlparse(base_url).netloc
    
    def _fetch():
        try:
            # METODO 1: find_feed_urls + extract_links (MIGLIORE!)
            logger.info(f"RSS: Cercando feed su {base_url}")
            feed_urls = feeds.find_feed_urls(base_url)
            logger.info(f"RSS: Trovati {len(feed_urls)} feed URLs: {feed_urls}")
            
            for idx, feed_url in enumerate(feed_urls[:5], 1):
                try:
                    logger.info(f"RSS: Elaborando feed {idx}/{len(feed_urls)}: {feed_url}")
                    # USA extract_links SENZA target_lang
                    entries = feeds.extract_links(feed_url)
                    logger.info(f"RSS: Feed {feed_url} ha ritornato {len(entries) if entries else 0} entries")
                    
                    if entries:
                        added = 0
                        for entry in entries:
                            if domain in entry:
                                urls.add(normalize_url(entry))
                                added += 1
                        logger.info(f"RSS: Aggiunti {added} URLs validi da {feed_url}")
                        
                        # Log primi 3 URL trovati
                        sample = list(urls)[:3]
                        logger.info(f"RSS: Sample URLs: {sample}")
                except Exception as e:
                    logger.warning(f"RSS: Feed extraction error {feed_url}: {e}")
            
            # METODO 2: RSS diretto (fallback)
            if len(urls) < 20:
                logger.info(f"RSS: Fallback - meno di 20 URLs, provo metodo diretto")
                for feed_url in feed_urls[:3]:
                    try:
                        downloaded = fetch_url(feed_url)
                        if downloaded:
                            logger.info(f"RSS: Scaricato feed {feed_url} ({len(downloaded)} bytes)")
                            links = feeds.extract_links(downloaded, feed_url)
                            logger.info(f"RSS: Estratti {len(links) if links else 0} links dal download")
                            for link in links[:100]:
                                if domain in urlparse(link).netloc:
                                    urls.add(normalize_url(link))
                    except Exception as e:
                        logger.warning(f"RSS: Fallback error {feed_url}: {e}")
            
            logger.info(f"RSS: TOTALE finale {len(urls)} URLs")
        except Exception as e:
            logger.error(f"RSS: Errore generale: {e}", exc_info=True)
    
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_fetch)
        try:
            fut.result(timeout=RSS_TIMEOUT)
        except FutureTimeoutError:
            logger.warning(f"RSS timeout ({RSS_TIMEOUT}s)")
        except Exception as e:
            logger.error(f"RSS executor error: {e}")
    
    return urls


def discover_crawl4ai(base_url: str, logger: logging.Logger) -> Set[str]:
    """Crawl4AI come backup discovery (opzionale)"""
    urls = set()
    
    if not CRAWL4AI_AVAILABLE:
        return urls
    
    domain = urlparse(base_url).netloc
    
    def _crawl():
        try:
            logger.info(f"Crawl4AI: Avvio crawler su {base_url}")
            crawler = WebCrawler()
            crawler.warmup()
            
            result = crawler.run(url=base_url)
            
            if result.success:
                # Estrai tutti i link dalla pagina
                import re
                links = re.findall(r'href=["\']([^"\']+)["\']', result.html)
                
                for link in links:
                    # Converti link relativi in assoluti
                    if link.startswith('/'):
                        link = base_url.rstrip('/') + link
                    elif not link.startswith('http'):
                        continue
                    
                    if domain in link:
                        urls.add(normalize_url(link))
                
                logger.info(f"Crawl4AI: Trovati {len(urls)} URLs")
            else:
                logger.warning(f"Crawl4AI: Fallito")
        except Exception as e:
            logger.debug(f"Crawl4AI error: {e}")
    
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_crawl)
        try:
            fut.result(timeout=30)
        except FutureTimeoutError:
            logger.warning("Crawl4AI timeout (30s)")
        except Exception:
            pass
    
    return urls


def discover_urls_robust(base_url: str, logger: logging.Logger, mode: str, max_urls: int = 10000) -> List[str]:
    """
    Discovery COMPLETA - USA DISCOVERY_V2 PULITA
    """
    from discovery_v2 import discover_all
    
    try:
        urls = discover_all(base_url, mode, logger, max_urls)
        logger.info(f"🔍 DEBUG CRAWLER: discover_all ritornato {len(urls)} URLs")
        logger.info(f"🔍 DEBUG CRAWLER: Primi 3 URLs: {urls[:3]}")
        return urls
    except Exception as e:
        logger.error(f"Discovery error: {e}")
        return []


def extract_brand(text: str) -> Optional[str]:
    brands = [
        'Rolex', 'Omega', 'Seiko', 'Breitling', 'Patek Philippe',
        'Audemars Piguet', 'Vacheron Constantin', 'IWC', 'Panerai',
        'Cartier', 'Tudor', 'Grand Seiko', 'Urwerk', 'Doxa',
        'Hamilton', 'Marathon', 'Heuer', 'TAG Heuer', 'Longines', 'Blancpain'
    ]
    
    text_lower = text.lower()
    for brand in brands:
        if brand.lower() in text_lower:
            return brand
    return None


def extract_article(url: str, logger: logging.Logger) -> Optional[Dict]:
    try:
        downloaded = fetch_url(url)
        if not downloaded:
            return None

        result = bare_extraction(
            downloaded,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            favor_precision=True,
            config=config,
            date_extraction_params={"extensive_search": True, "original_date": True}
        )

        if not result or not result.get('text'):
            return None

        text = result['text']
        if len(text.split()) < _MIN_WORDS:
            logger.debug(f"Skipped (too short {len(text.split())} words): {url}")
            return None

        date_val = result.get('date')
        logger.debug(f"{'Date found: ' + date_val if date_val else 'No date'} — {url}")

        return {
            'source_url':  url,
            'title':       result.get('title'),
            'text':        text,
            'date':        date_val,
            'authors':     result.get('author'),
            'sitename':    result.get('sitename'),
            'description': result.get('description'),
            'tags':        result.get('tags'),
        }
    except Exception as e:
        logger.debug(f"Extract error {url}: {e}")
        return None


def extract_article_playwright(url: str, logger: logging.Logger) -> Optional[Dict]:
    """Estrae contenuto con Playwright per pagine heavy-JS (Rolex, ecc.)"""
    if not PLAYWRIGHT_AVAILABLE:
        return None
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='it-IT',
                timezone_id='Europe/Rome',
            )
            context.set_extra_http_headers({
                'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8',
                'DNT': '1',
            })
            
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
            
            # CRITICAL: domcontentloaded per brand con lazy loading infinito
            page.goto(url, wait_until='domcontentloaded', timeout=45000)
            page.wait_for_timeout(4000)  # Aspetta JS iniziale
            
            # Scroll per triggerare lazy content
            try:
                page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
                page.wait_for_timeout(1000)
            except:
                pass
            
            title = page.title()
            text = page.evaluate('''
                () => {
                    const unwanted = document.querySelectorAll('script, style, nav, footer, header, .cookie, .modal');
                    unwanted.forEach(el => el.remove());
                    return document.body.innerText || '';
                }
            ''')
            
            browser.close()
            
            text = '\n'.join(line.strip() for line in text.split('\n') if line.strip())
            
            if not text or len(text) < 100:
                return None
            
            return {
                'source_url': url,
                'title': title,
                'text': text,
                'date': None,
                'authors': None
            }
            
    except Exception as e:
        logger.debug(f"Playwright extract error {url}: {e}")
        return None


def extract_article_smart(url: str, mode: str, logger: logging.Logger) -> Optional[Dict]:
    """
    Extraction intelligente con fallback automatico:
    1. Prova SEMPRE trafilatura prima (veloce)
    2. Se fallisce (text vuoto o <100 chars) → Playwright fallback
    """
    # STEP 1: Prova trafilatura (veloce per tutti)
    result = extract_article(url, logger)
    
    # Se successo con contenuto sufficiente → OK!
    if result and result.get('text') and len(result['text']) > 100:
        return result
    
    # STEP 2: Fallback Playwright (lento ma funziona per JS-heavy)
    if PLAYWRIGHT_AVAILABLE:
        logger.info(f"Trafilatura failed ({len(result.get('text', '')) if result else 0} chars), trying Playwright")
        return extract_article_playwright(url, logger)
    else:
        logger.warning(f"Extraction failed and Playwright not available")
        return None


# -------------- Process Site --------------

def process_site(
    site_url: str,
    out_dir: str,
    db: CrawlerDB,
    max_pages: int = 0,
    delay: float = 1.0,
    workers: int = 4,
    mode: str = 'blog',
    incremental: bool = False,
    site_timeout: int = 300
) -> Tuple[int, int]:
    
    domain = urlparse(site_url).netloc
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    out_path = Path(out_dir)
    out_path.mkdir(exist_ok=True)
    
    # Crea sottocartelle
    articles_dir = out_path / "articles"
    logs_dir = out_path / "logs"
    articles_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)
    
    clean_domain = domain.replace('www.', '').replace('.', '_')
    
    # Articles in articles/
    articles_file = articles_dir / f"{timestamp}_{clean_domain}.jsonl"
    
    # Log in logs/
    failed_file = logs_dir / f"{timestamp}_{clean_domain}_failed.txt"
    log_file = logs_dir / f"{timestamp}_{clean_domain}.log"
    
    logger = logging.getLogger(f"{domain}_{timestamp}")
    logger.setLevel(logging.DEBUG)  # Log TUTTO nel file
    logger.handlers.clear()
    
    # File handler - TUTTO (DEBUG)
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(fh)
    
    # Console handler - Più verboso per vedere avanzamento
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)  # INFO per vedere progressi
    ch.setFormatter(logging.Formatter(f'[{domain[:25]}] %(message)s'))
    logger.addHandler(ch)
    
    start_time = time.time()
    
    try:
        print(f"\n🚀 {site_url}")
        logger.info(f"🚀 {site_url} ({mode})")
        
        crawled_urls = db.get_crawled_urls(domain)
        logger.info(f"Already: {len(crawled_urls)} URLs")
        
        # Discovery con timeout globale (5 min per depth 5)
        print(f"🔍 Discovery...", end='', flush=True)
        discovery_max = max_pages if max_pages > 0 else 10000
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(discover_urls_robust, site_url, logger, mode, discovery_max)
            try:
                urls = fut.result(timeout=300)  # 5 minuti per discovery completa (depth 5)
            except FutureTimeoutError:
                logger.warning("Discovery timeout (300s)!")
                urls = []
            except Exception as e:
                logger.error(f"Discovery error: {e}")
                urls = []
        
        # Fix: confronta URL normalizzati (rimuovi trailing slash e frammenti)
        crawled_urls_normalized = {url.rstrip('/').split('#')[0] for url in crawled_urls}
        new_urls = []
        for u in urls:
            u_normalized = u.rstrip('/').split('#')[0]
            if u_normalized not in crawled_urls_normalized:
                new_urls.append(u)
        
        logger.info(f"🔍 DEBUG: URLs discovery={len(urls)}, già crawlati={len(crawled_urls_normalized)}, nuovi={len(new_urls)}")
        
        if max_pages > 0:
            new_urls = new_urls[:max_pages]
        
        print(f" ✅ {len(urls)} trovate → {len(new_urls)} da scaricare")
        logger.info(f"To crawl: {len(new_urls)}")
        
        if not new_urls:
            print(f"⚠️  Nessun URL nuovo")
            return (0, 0)
        
        # Log prime 3 nel file
        for u in new_urls[:3]:
            logger.info(f"  • {u}")
        
        ok_count = 0
        fail_count = 0
        lock = Lock()
        
        def worker(url: str):
            nonlocal ok_count, fail_count

            # Scarta URL spazzatura prima ancora di scaricare
            if is_junk_url(url):
                with lock:
                    fail_count += 1
                    db.mark_url_crawled(url, domain, 'skipped', 'junk_url')
                return

            try:
                art = extract_article_smart(url, mode, logger)
                
                if art and art.get('text'):
                    parsed_url = urlparse(url)
                    art['id'] = art['source_url']
                    art['site'] = domain
                    art['site_type'] = mode
                    art['source_domain'] = domain
                    art['source_path'] = parsed_url.path
                    art['crawled_at'] = datetime.utcnow().isoformat() + 'Z'
                    art['brand'] = extract_brand(art['text'][:1000])
                    
                    with lock:
                        with open(articles_file, 'a', encoding='utf-8') as f:
                            f.write(json.dumps(art, ensure_ascii=False) + '\n')
                    
                    # SALVA SOLO ARTICLES - chunking separato!
                    with lock:
                        ok_count += 1
                        db.mark_url_crawled(url, domain, 'success')
                else:
                    with lock:
                        with open(failed_file, 'a', encoding='utf-8') as f:
                            f.write(f"{url}\tno_content\n")
                        fail_count += 1
                        db.mark_url_crawled(url, domain, 'failed', 'no_content')
            
            except Exception as e:
                with lock:
                    with open(failed_file, 'a', encoding='utf-8') as f:
                        f.write(f"{url}\t{str(e)[:100]}\n")
                    fail_count += 1
                    db.mark_url_crawled(url, domain, 'error', str(e)[:100])
            
            finally:
                time.sleep(delay)
        
        logger.info(f"Crawling ({workers} workers)...")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(worker, url) for url in new_urls]
            
            if TQDM_AVAILABLE:
                pbar = tqdm(total=len(futures), desc=domain[:25], mininterval=1.0, maxinterval=5.0)
            
            completed = 0
            for future in as_completed(futures):  # NO timeout globale
                try:
                    future.result(timeout=30)  # 30s per singolo URL
                except TimeoutError:
                    logger.warning(f"URL timeout dopo 30s")
                    fail_count += 1
                except Exception as e:
                    logger.debug(f"URL error: {e}")
                finally:
                    completed += 1
                    if TQDM_AVAILABLE:
                        pbar.update(1)
                    # Stampa ogni 50 articoli anche senza tqdm
                    if completed % 50 == 0:
                        logger.info(f"Progress: {completed}/{len(futures)} ({ok_count} ok, {fail_count} fail)")
            
            if TQDM_AVAILABLE:
                pbar.close()
        
        duration = time.time() - start_time
        print(f"✅ {ok_count} articoli ({duration:.0f}s)\n")
        logger.info(f"✅ {ok_count} art, {fail_count} fail ({duration:.0f}s)")
        
        return ok_count, fail_count
    
    except Exception as e:
        duration = time.time() - start_time
        print(f"❌ ERRORE: {e}\n")
        logger.error(f"❌ FATAL: {e}")
        return (0, 0)
    
    finally:
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)


# -------------- Main --------------

def read_sites(file_path: str) -> List[Tuple[str, str]]:
    sites = []
    current_mode = 'blog'
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            if not line or line.startswith('#'):
                if line and 'brand' in line.lower():
                    current_mode = 'brand'
                elif line and ('blog' in line.lower() or 'magazine' in line.lower()):
                    current_mode = 'blog'
                continue
            
            parts = line.split()
            url = parts[0]
            mode = parts[1] if len(parts) > 1 and parts[1] in ('blog', 'brand') else current_mode
            
            if not url.startswith('http'):
                url = 'https://' + url
            
            sites.append((url, mode))
    
    return sites


def main():
    ap = argparse.ArgumentParser(description='RAG Crawler v11 - FINALE STABILE')
    
    ap.add_argument('--input', '-i', required=True)
    ap.add_argument('--out', '-o', default='./out')
    ap.add_argument('--workers', '-w', type=int, default=4)
    ap.add_argument('--delay', type=float, default=1.0)
    ap.add_argument('--max-pages', type=int, default=0)
    ap.add_argument('--chunk-size', type=int, default=300)
    ap.add_argument('--chunk-overlap', type=int, default=1)
    ap.add_argument('--no-chunking', action='store_true')
    ap.add_argument('--incremental', action='store_true')
    ap.add_argument('--site-timeout', type=int, default=300, help='Timeout per sito (s)')

    args = ap.parse_args()

    if not os.path.exists(args.input):
        print(f"❌ File {args.input} non trovato")
        sys.exit(1)

    os.makedirs(args.out, exist_ok=True)
    db = CrawlerDB(os.path.join(args.out, 'crawler.db'))

    sites = read_sites(args.input)
    if not sites:
        print('❌ Nessun sito')
        sys.exit(2)

    print(f"\n{'='*70}")
    print(f"🚀 RAG Crawler v13 - MAX Discovery")
    print(f"{'='*70}")
    print(f"Siti: {len(sites)}")
    print(f"Output: {args.out}/")
    print(f"{'='*70}")

    totals = [0, 0]  # [articoli, failed]
    
    for idx, (site_url, mode) in enumerate(sites, 1):
        print(f"\n[{idx}/{len(sites)}] ", end='')
        
        try:
            ok, fail = process_site(
                site_url, args.out, db, args.max_pages, args.delay,
                args.workers, mode, args.incremental, args.site_timeout
            )
            
            totals[0] += ok
            totals[1] += fail
        
        except Exception as e:
            print(f"❌ {e}")

    print(f"\n{'='*70}")
    print(f"📊 TOTALE: {totals[0]} articoli, {totals[1]} falliti")
    print(f"📁 Output: {args.out}/articles/ (JSONL)")
    print(f"📝 Logs: {args.out}/logs/")
    print(f"{'='*70}\n")
    
    # Salva statistiche globali
    stats_file = Path(args.out) / "crawl_stats.json"
    stats = {
        'timestamp': datetime.now().isoformat(),
        'sites_total': len(sites),
        'articles_total': totals[0],
        'failed_total': totals[1],
        'output_dir': str(args.out),
        'sites': []
    }
    
    # Leggi stats per sito dai log
    logs_dir = Path(args.out) / "logs"
    for log_file in logs_dir.glob("*.log"):
        with open(log_file, 'r', encoding='utf-8') as f:
            content = f.read()
            # Estrai stats dal log
            if '✅' in content:
                for line in content.split('\n'):
                    if line.startswith('INFO:') and '✅' in line:
                        stats['sites'].append({
                            'domain': log_file.stem.split('_', 2)[2],
                            'log_file': log_file.name,
                            'result': line.split('✅')[1].strip()
                        })
                        break
    
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    print(f"📈 Statistiche salvate: {stats_file}\n")
    
    db.close()


if __name__ == '__main__':
    main()
