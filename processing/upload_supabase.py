#!/usr/bin/env python3
"""
SUPABASE UPLOADER - GitHub Actions Version

Upload chunks a Supabase (SENZA embedding qui).
L'embedding viene generato da Supabase trigger via OpenAI API.

Usage:
    export SUPABASE_URL=xxx
    export SUPABASE_KEY=xxx
    python upload_supabase.py -i output/chunks
"""

import json
import os
import argparse
from pathlib import Path
from typing import List, Dict
from tqdm import tqdm

try:
    from supabase import create_client, Client
except ImportError:
    print("❌ pip install supabase")
    exit(1)


class SupabaseUploader:
    """Upload chunks a Supabase pgvector"""
    
    def __init__(self, url: str = None, key: str = None):
        url = url or os.getenv('SUPABASE_URL')
        key = key or os.getenv('SUPABASE_KEY')
        
        if not url or not key:
            raise ValueError("❌ SUPABASE_URL e SUPABASE_KEY richieste!")
        
        self.client: Client = create_client(url, key)
        print(f"✅ Connesso a Supabase: {url[:30]}...")
    
    def upload_chunks(self, chunks: List[Dict], batch_size: int = 100):
        """
        Upload chunks in batch
        
        IMPORTANTE: NON include 'embedding' field
        Il trigger Supabase lo genererà automaticamente via OpenAI API
        """
        
        total = len(chunks)
        print(f"📤 Uploading {total} chunks...")
        
        uploaded = 0
        failed = 0
        
        for i in tqdm(range(0, total, batch_size), desc="Batch"):
            batch = chunks[i:i + batch_size]
            
            # Prepara rows (SENZA embedding)
            rows = []
            for chunk in batch:
                row = {
                    'id': chunk['id'],
                    'content': chunk['text'],  # Supabase usa 'content'
                    # 'embedding': NULL → Trigger lo genera!
                    'metadata': {
                        'article_id': chunk.get('article_id'),
                        'chunk_index': chunk.get('chunk_index'),
                        'total_chunks': chunk.get('total_chunks'),
                        'url': chunk.get('metadata', {}).get('url'),
                        'title': chunk.get('metadata', {}).get('title'),
                        'domain': chunk.get('metadata', {}).get('domain'),
                        'brand': chunk.get('metadata', {}).get('brand'),
                    },
                    'url': chunk.get('metadata', {}).get('url'),
                    'title': chunk.get('metadata', {}).get('title'),
                    'domain': chunk.get('metadata', {}).get('domain'),
                    'brand': chunk.get('metadata', {}).get('brand'),
                }
                rows.append(row)
            
            try:
                # Upsert (insert or update)
                self.client.table('chunks').upsert(rows).execute()
                uploaded += len(rows)
            except Exception as e:
                print(f"\n⚠️  Batch {i//batch_size + 1} failed: {e}")
                failed += len(rows)
        
        print(f"\n✅ Upload completato:")
        print(f"   Uploaded: {uploaded}")
        print(f"   Failed: {failed}")
        
        return uploaded, failed


def main():
    parser = argparse.ArgumentParser(description='Upload chunks a Supabase')
    parser.add_argument('-i', '--input', required=True, help='Input dir (chunks/)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size upload')
    
    args = parser.parse_args()
    
    input_dir = Path(args.input)
    
    if not input_dir.exists():
        print(f"❌ Input dir not found: {input_dir}")
        return 1
    
    jsonl_files = list(input_dir.glob('*.jsonl'))
    
    if not jsonl_files:
        print(f"❌ No JSONL files in {input_dir}")
        return 1
    
    print(f"\n{'='*70}")
    print(f"📤 SUPABASE UPLOADER")
    print(f"{'='*70}")
    print(f"Input: {input_dir}")
    print(f"Files: {len(jsonl_files)}")
    print(f"{'='*70}\n")
    
    # Connetti Supabase
    try:
        uploader = SupabaseUploader()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return 1
    
    total_uploaded = 0
    total_failed = 0
    
    # Upload tutti i file
    for jsonl_file in jsonl_files:
        print(f"\n📄 {jsonl_file.name}")
        
        chunks = []
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    chunks.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        
        if not chunks:
            print(f"  ⏭️  Empty file, skipping")
            continue
        
        uploaded, failed = uploader.upload_chunks(chunks, batch_size=args.batch_size)
        total_uploaded += uploaded
        total_failed += failed
    
    print(f"\n{'='*70}")
    print(f"📊 SUMMARY")
    print(f"{'='*70}")
    print(f"Total uploaded: {total_uploaded}")
    print(f"Total failed: {total_failed}")
    print(f"{'='*70}\n")
    
    print("💡 NEXT STEPS:")
    print("   1. Check Supabase Dashboard → Table Editor → chunks")
    print("   2. Verify trigger generated embeddings (embedding column NOT NULL)")
    print("   3. Test similarity search with SQL:\n")
    print("      SELECT * FROM match_chunks(")
    print("        query_embedding := (SELECT embedding FROM chunks LIMIT 1),")
    print("        match_count := 5")
    print("      );\n")
    
    return 0 if total_failed == 0 else 1


if __name__ == '__main__':
    exit(main())
