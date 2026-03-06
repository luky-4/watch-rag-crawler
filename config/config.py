"""
Configuration - Environment Variables

Usage:
    from config.config import SUPABASE_URL, SUPABASE_KEY
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env if exists (per test locale)
load_dotenv()

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "chunks")

# OpenAI (opzionale - se usi embedding locale invece di Supabase trigger)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Paths
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
ARTICLES_DIR = OUTPUT_DIR / "articles"
CHUNKS_DIR = OUTPUT_DIR / "chunks"

# Crawler
MAX_PAGES = int(os.getenv("MAX_PAGES", "0"))  # 0 = unlimited
WORKERS = int(os.getenv("WORKERS", "4"))
DELAY = float(os.getenv("CRAWL_DELAY", "1.0"))

# Chunker
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "300"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "1"))

# Uploader
UPLOAD_BATCH_SIZE = int(os.getenv("UPLOAD_BATCH_SIZE", "100"))


def validate_env():
    """Valida che le env vars critiche siano settate"""
    required = {
        "SUPABASE_URL": SUPABASE_URL,
        "SUPABASE_KEY": SUPABASE_KEY,
    }
    
    missing = [k for k, v in required.items() if not v]
    
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")


if __name__ == "__main__":
    print("="*60)
    print("Configuration Check")
    print("="*60)
    
    try:
        validate_env()
        print("✅ All required environment variables set")
    except ValueError as e:
        print(f"❌ {e}")
    
    print(f"\nSupabase URL: {SUPABASE_URL[:30] if SUPABASE_URL else 'NOT SET'}...")
    print(f"Supabase Key: {'SET' if SUPABASE_KEY else 'NOT SET'}")
    print(f"OpenAI Key: {'SET' if OPENAI_API_KEY else 'NOT SET'}")
    print(f"\nOutput Dir: {OUTPUT_DIR}")
    print(f"Max Pages: {MAX_PAGES}")
    print(f"Workers: {WORKERS}")
