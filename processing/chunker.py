#!/usr/bin/env python3
"""
CHUNKER INCREMENTALE - Processa solo nuovi articoli

Mantiene database SQLite di articoli già chunkati:
- article_id → chunk_file, processed_at

Usage:
    python chunker_incremental.py -i output/articles -o output/chunks
"""

import json
import argparse
import sqlite3
from pathlib import Path
from typing import List, Set
from datetime import datetime
from dataclasses import dataclass
import hashlib

@dataclass
class Chunk:
    id: str
    article_id: str
    chunk_index: int
    total_chunks: int
    text: str
    token_count: int
    metadata: dict


class SemanticChunker:
    """Chunker semantico - uguale a prima"""
    
    def __init__(self, target_size: int = 300, max_size: int = 450, 
                 min_size: int = 150, overlap: int = 1):
        self.target_size = target_size
        self.max_size = max_size
        self.min_size = min_size
        self.overlap = overlap
    
    def estimate_tokens(self, text: str) -> int:
        return len(text.split())
    
    def process_article(self, article: dict) -> List[Chunk]:
        text = article.get('text') or article.get('content', '')
        article_id = article['id']

        # Articolo senza testo o troppo corto → non generare chunk
        if not text or len(text.split()) < 50:
            return []
        
        sentences = self._split_sentences(text)
        chunks_data = []
        current_chunk = []
        current_tokens = 0
        
        for sentence in sentences:
            sentence_tokens = self.estimate_tokens(sentence)
            
            if current_tokens + sentence_tokens > self.max_size and current_chunk:
                chunks_data.append(' '.join(current_chunk))
                
                if self.overlap > 0:
                    current_chunk = current_chunk[-self.overlap:]
                    current_tokens = sum(self.estimate_tokens(s) for s in current_chunk)
                else:
                    current_chunk = []
                    current_tokens = 0
            
            current_chunk.append(sentence)
            current_tokens += sentence_tokens
        
        if current_chunk:
            chunks_data.append(' '.join(current_chunk))
        
        chunks = []
        for idx, chunk_text in enumerate(chunks_data):
            chunk_id = hashlib.md5(f"{article_id}_{idx}".encode()).hexdigest()[:16]
            
            chunk = Chunk(
                id=chunk_id,
                article_id=article_id,
                chunk_index=idx,
                total_chunks=len(chunks_data),
                text=chunk_text,
                token_count=self.estimate_tokens(chunk_text),
                metadata={
                    # Campi base
                    'url':         article.get('source_url') or article.get('url'),
                    'title':       article.get('title'),
                    'domain':      article.get('source_domain') or article.get('domain'),
                    'brand':       article.get('brand'),
                    'site_type':   article.get('site_type'),
                    # Campi temporali
                    'date':        article.get('date'),
                    'crawled_at':  article.get('crawled_at'),
                    # Autori e tag
                    'authors':     article.get('authors'),
                    'tags':        article.get('tags'),
                    'sitename':    article.get('sitename'),
                    'description': article.get('description'),
                }
            )
            chunks.append(chunk)
        
        return chunks
    
    def _split_sentences(self, text: str) -> List[str]:
        import re
        sentences = re.split(r'(?<=[.!?])\s+', text)
        return [s.strip() for s in sentences if s.strip()]


class ChunkerDB:
    """Database per tracking articoli processati"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._init_db()
    
    def _init_db(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS chunked_articles (
                article_id TEXT PRIMARY KEY,
                source_file TEXT,
                output_file TEXT,
                chunks_count INTEGER,
                processed_at TIMESTAMP
            )
        ''')
        self.conn.commit()
    
    def is_chunked(self, article_id: str) -> bool:
        cursor = self.conn.execute(
            'SELECT 1 FROM chunked_articles WHERE article_id = ?',
            (article_id,)
        )
        return cursor.fetchone() is not None
    
    def mark_chunked(self, article_id: str, source_file: str, output_file: str, chunks_count: int):
        self.conn.execute('''
            INSERT OR REPLACE INTO chunked_articles 
            (article_id, source_file, output_file, chunks_count, processed_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (article_id, source_file, output_file, chunks_count, datetime.now()))
        self.conn.commit()
    
    def get_stats(self) -> dict:
        cursor = self.conn.execute('''
            SELECT 
                COUNT(*) as total_articles,
                SUM(chunks_count) as total_chunks
            FROM chunked_articles
        ''')
        row = cursor.fetchone()
        return {
            'total_articles': row[0] or 0,
            'total_chunks': row[1] or 0
        }
    
    def close(self):
        self.conn.close()


def process_articles_file_incremental(
    input_file: Path,
    output_file: Path,
    chunker: SemanticChunker,
    db: ChunkerDB,
    force: bool = False
):
    """Processa solo articoli nuovi"""
    
    print(f"📄 {input_file.name}")
    
    # Carica articoli
    articles = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            articles.append(json.loads(line))
    
    # Filtra già processati
    if not force:
        new_articles = [a for a in articles if not db.is_chunked(a['id'])]
        skipped = len(articles) - len(new_articles)
        
        if skipped > 0:
            print(f"  ⏭️  Skipping {skipped} già processati")
        
        if not new_articles:
            print(f"  ✅ Tutti già processati")
            return 0, 0
        
        articles = new_articles
    
    print(f"  📊 {len(articles)} articoli da processare")

    total_chunks = 0
    skipped_quality = 0

    # Appendi ai chunks esistenti
    mode = 'a' if output_file.exists() and not force else 'w'

    with open(output_file, mode, encoding='utf-8') as f_out:
        for article in articles:
            chunks = chunker.process_article(article)

            # process_article ritorna [] se testo assente o < 50 parole
            if not chunks:
                skipped_quality += 1
                db.mark_chunked(article['id'], str(input_file), str(output_file), 0)
                continue

            for chunk in chunks:
                chunk_doc = {
                    'id': chunk.id,
                    'article_id': chunk.article_id,
                    'chunk_index': chunk.chunk_index,
                    'total_chunks': chunk.total_chunks,
                    'text': chunk.text,
                    'token_count': chunk.token_count,
                    'metadata': chunk.metadata
                }

                f_out.write(json.dumps(chunk_doc, ensure_ascii=False) + '\n')
                total_chunks += 1

            # Segna come processato
            db.mark_chunked(
                article['id'],
                str(input_file),
                str(output_file),
                len(chunks)
            )

    if skipped_quality:
        print(f"  🗑️  {skipped_quality} articoli scartati (testo assente o < 50 parole)")
    print(f"  ✅ {len(articles) - skipped_quality} articoli → {total_chunks} chunks\n")
    return len(articles), total_chunks


def main():
    parser = argparse.ArgumentParser(description='Chunker Incrementale')
    parser.add_argument('-i', '--input', required=True, help='Input dir (articles/)')
    parser.add_argument('-o', '--output', required=True, help='Output dir (chunks/)')
    parser.add_argument('--chunk-size', type=int, default=300, help='Target chunk size')
    parser.add_argument('--overlap', type=int, default=1, help='Sentence overlap')
    parser.add_argument('--force', action='store_true', help='Riprocessa tutto (ignora DB)')
    
    args = parser.parse_args()
    
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True)
    
    if not input_dir.exists():
        print(f"❌ Input dir not found: {input_dir}")
        return
    
    jsonl_files = list(input_dir.glob('*.jsonl'))
    
    if not jsonl_files:
        print(f"❌ No JSONL files in {input_dir}")
        return
    
    # Database
    db_path = output_dir / 'chunker.db'
    db = ChunkerDB(db_path)
    
    # Stats iniziali
    stats_before = db.get_stats()
    
    print(f"\n{'='*70}")
    print(f"🔨 CHUNKER INCREMENTALE")
    print(f"{'='*70}")
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")
    print(f"Files: {len(jsonl_files)}")
    print(f"Database: {db_path}")
    print(f"Già processati: {stats_before['total_articles']} articoli → {stats_before['total_chunks']} chunks")
    print(f"Force rebuild: {'Yes' if args.force else 'No'}")
    print(f"{'='*70}\n")
    
    chunker = SemanticChunker(
        target_size=args.chunk_size,
        max_size=int(args.chunk_size * 1.5),
        min_size=int(args.chunk_size * 0.5),
        overlap=args.overlap
    )
    
    total_articles = 0
    total_chunks = 0
    
    for jsonl_file in jsonl_files:
        output_file = output_dir / jsonl_file.name
        
        articles, chunks = process_articles_file_incremental(
            jsonl_file,
            output_file,
            chunker,
            db,
            force=args.force
        )
        
        total_articles += articles
        total_chunks += chunks
    
    # Stats finali
    stats_after = db.get_stats()
    
    print(f"\n{'='*70}")
    print(f"📊 NUOVI: {total_articles} articoli → {total_chunks} chunks")
    print(f"📊 TOTALI: {stats_after['total_articles']} articoli → {stats_after['total_chunks']} chunks")
    print(f"📁 Output: {output_dir}/")
    print(f"💾 Database: {db_path}")
    print(f"{'='*70}\n")
    
    db.close()


if __name__ == '__main__':
    main()
