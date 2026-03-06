# 🕐 Watch RAG Crawler

Sistema RAG automatico per orologi con crawling notturno, chunking, e upload a Supabase con OpenAI embeddings.

## 🏗️ Architettura

```
GitHub Actions (cron notte)
    ↓
1. Crawler → Articles JSONL
2. Chunker → Chunks JSONL  
3. Upload a Supabase
    ↓
Supabase Trigger → OpenAI Embedding API
    ↓
Vector Database pronto per query
```

## 🚀 Setup

### 1. Configura Secrets GitHub

Settings → Secrets and variables → Actions → New secret:

```env
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=eyJxxx...
OPENAI_API_KEY=sk-xxx...
SITES_LIST=|
  hodinkee.com blog
  rolex.com brand
  ...
```

### 2. Configura Supabase

Esegui in Supabase SQL Editor:

```sql
-- Abilita vector
CREATE EXTENSION IF NOT EXISTS vector;

-- Tabella chunks
CREATE TABLE chunks (
  id TEXT PRIMARY KEY,
  content TEXT NOT NULL,
  embedding vector(1536),  -- OpenAI text-embedding-3-small
  metadata JSONB,
  url TEXT,
  title TEXT,
  domain TEXT,
  brand TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index vector
CREATE INDEX ON chunks 
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- Function embedding (OpenAI)
-- Vedi: processing/supabase_setup.sql
```

### 3. Deploy

```bash
git push origin main
```

GitHub Actions si attiva automaticamente ogni notte alle 2 AM UTC.

## 📁 Struttura

```
├── .github/workflows/
│   └── nightly-crawl.yml    # Cron GitHub Actions
├── crawlers/
│   ├── rag_site_crawler.py  # Crawler principale
│   ├── discovery_v2.py      # Discovery URLs
│   └── auction_crawler.py   # Aste (opzionale)
├── processing/
│   ├── chunker.py           # Chunking
│   ├── upload_supabase.py   # Upload (senza embedding)
│   └── supabase_setup.sql   # Setup DB
├── config/
│   ├── sites_example.txt    # Esempio siti
│   └── config.py            # Config
├── requirements.txt
└── README.md
```

## 🔧 Comandi Manuali

```bash
# Test locale (usa venv)
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Crawl
python3 crawlers/rag_site_crawler.py --input config/sites_example.txt

# Chunk
python3 processing/chunker.py -i output/articles -o output/chunks

# Upload
export SUPABASE_URL=xxx
export SUPABASE_KEY=xxx
python3 processing/upload_supabase.py -i output/chunks
```

## 💰 Costi

| Servizio | Piano | Costo/Mese |
|----------|-------|------------|
| **GitHub Actions** | Public repo | **$0** |
| **OpenAI Embeddings** | text-embedding-3-small | **$0.02** |
| **Supabase** | Free tier | **$0** |
| **TOTALE** | | **~$0.02/mese** 🎉 |

## 📊 Monitoraggio

- **Logs**: Actions → Nightly Crawl → Logs
- **Artifacts**: Download output files (30 giorni)
- **Supabase**: Dashboard → Table Editor → chunks

## 🔄 Workflow

**Automatico (ogni notte 2 AM):**
1. Crawler esegue su sites in `SITES_LIST` secret
2. Chunker processa articoli
3. Upload chunks a Supabase
4. Supabase trigger genera embeddings via OpenAI
5. Done! Vector DB aggiornato

**Manuale (on-demand):**
- Actions → Nightly Crawl → Run workflow

## ⚙️ Configurazione Avanzata

### Modifica Schedule

`.github/workflows/nightly-crawl.yml`:
```yaml
schedule:
  - cron: '0 14 * * *'  # 2 PM UTC (invece di 2 AM)
```

### Aggiungi Siti

Settings → Secrets → SITES_LIST → Edit:
```
hodinkee.com blog
nuovo-sito.com blog
```

## 🆘 Troubleshooting

**Workflow fallisce:**
- Controlla logs in Actions
- Verifica secrets configurati
- Test locale prima di push

**Timeout (6 ore max):**
- Riduci numero siti
- Split in job multipli

**Out of memory:**
- GitHub Actions: 7GB RAM
- Dovrebbe bastare per chunking

## 📝 License

MIT
