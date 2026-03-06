# 🚀 Setup Guide Completa

## 📋 Prerequisiti

- Account GitHub (gratuito)
- Account Supabase (free tier)
- Account OpenAI (API key)

---

## STEP 1: Setup Supabase

### 1.1 Crea Progetto

1. Vai su [supabase.com](https://supabase.com)
2. New Project → Nome: `watch-rag`
3. Password DB: (salvala!)
4. Region: scegline una vicina

### 1.2 Setup Database

Vai su SQL Editor e esegui tutto il contenuto di:
```
processing/supabase_setup.sql
```

Questo crea:
- ✅ Tabella `chunks` con pgvector
- ✅ Trigger per auto-embedding via OpenAI
- ✅ Function `match_chunks()` per similarity search

### 1.3 Configura OpenAI API Key

Settings → API → Custom Settings:

- **Name**: `app.openai_api_key`
- **Value**: `sk-xxx...` (tua OpenAI API key)

### 1.4 Ottieni Credentials

Settings → API:

- **Project URL**: `https://xxx.supabase.co` → copia
- **anon/public key**: `eyJxxx...` → copia

---

## STEP 2: Setup GitHub

### 2.1 Fork/Clone Repository

```bash
git clone https://github.com/TUO_USERNAME/watch-rag-crawler.git
cd watch-rag-crawler
```

Oppure:
- Download ZIP
- Estrai
- `git init && git add . && git commit -m "init"`

### 2.2 Crea Repository GitHub

**Opzione A - CLI:**
```bash
gh auth login
gh repo create watch-rag-crawler --private --source=. --push
```

**Opzione B - Web:**
1. GitHub.com → New Repository
2. Nome: `watch-rag-crawler`
3. Private ✅
4. Create

```bash
git remote add origin https://github.com/TUO_USER/watch-rag-crawler.git
git branch -M main
git push -u origin main
```

### 2.3 Configura Secrets

Settings → Secrets and variables → Actions → New secret

**Secret 1: SUPABASE_URL**
```
https://xxx.supabase.co
```

**Secret 2: SUPABASE_KEY**
```
eyJxxx...
```

**Secret 3: OPENAI_API_KEY**
```
sk-xxx...
```

**Secret 4: SITES_LIST**
```
hodinkee.com blog
monochrome-watches.com blog
rolex.com brand
patek.com brand
omegawatches.com brand
```

(Copia dalla lista in `config/sites_example.txt`, personalizza)

---

## STEP 3: Test

### 3.1 Test Manuale (prima del cron)

Actions → Nightly Crawl Pipeline → Run workflow:

- Max pages: `10` (test veloce)
- Run workflow

Guarda logs in tempo reale. Dovrebbe completare in ~5-10 min.

### 3.2 Verifica Output

**GitHub:**
- Actions → Run completato → Artifacts
- Download `crawl-logs-XXX.zip`

**Supabase:**
- Table Editor → `chunks`
- Dovresti vedere righe con:
  - `content` (testo)
  - `embedding` (NOT NULL se trigger funziona)
  - `url`, `title`, `domain`

### 3.3 Test Similarity Search

SQL Editor in Supabase:

```sql
-- 1. Verifica embeddings generati
SELECT 
    id, 
    title, 
    embedding IS NOT NULL as has_embedding,
    created_at
FROM chunks
LIMIT 10;

-- 2. Test similarity search
SELECT 
    title,
    domain,
    similarity
FROM match_chunks(
    query_embedding := (SELECT embedding FROM chunks LIMIT 1),
    match_count := 5
);
```

Se `has_embedding` = true → ✅ Trigger funziona!

---

## STEP 4: Attiva Cron

Il workflow è già configurato per eseguire ogni notte alle **2 AM UTC**.

Controlla schedule in:
```yaml
.github/workflows/nightly-crawl.yml
```

```yaml
schedule:
  - cron: '0 2 * * *'  # 2 AM UTC
```

Per cambiare orario, edita e fai push:
```bash
nano .github/workflows/nightly-crawl.yml
# Modifica cron
git add .github/workflows/nightly-crawl.yml
git commit -m "Change schedule"
git push
```

**Cron Generator:** [crontab.guru](https://crontab.guru/)

---

## STEP 5: Monitoraggio

### Logs

Actions → Nightly Crawl Pipeline → Latest run → Logs

### Notifiche

Se job fallisce, crea automaticamente Issue:
- Issues → Filtro: label `automation`

### Artifacts

Logs salvati per 30 giorni:
- Actions → Run → Artifacts

### Supabase Dashboard

Table Editor → `chunks` → vedi dati aggiornati

---

## 🔧 Manutenzione

### Aggiungere Siti

Settings → Secrets → SITES_LIST → Edit

Aggiungi riga:
```
nuovo-sito.com blog
```

Salva. Prossimo cron userà lista aggiornata!

### Modificare Config

Edita file, commit, push:
```bash
nano crawlers/rag_site_crawler.py
git add .
git commit -m "Update crawler"
git push
```

### Forzare Re-crawl

Actions → Run workflow → `Max pages: 0` (unlimited)

---

## 🐛 Troubleshooting

### Workflow fallisce

**Errore comune:** Secret non settato

**Fix:**
1. Settings → Secrets → verifica tutti presenti
2. Re-run workflow

### Trigger non genera embedding

**Sintomi:** `embedding` column = NULL

**Fix:**
1. Verifica `app.openai_api_key` settato in Supabase
2. SQL Editor:
   ```sql
   SELECT generate_openai_embedding('test');
   ```
   Dovrebbe ritornare array di 1536 numeri

**Se errore:**
- Check OpenAI API key valida
- Check credito OpenAI > $0
- Check quota OpenAI

### Timeout (>6 ore)

**Cause:** Troppi siti

**Fix:**
1. Riduci `MAX_PAGES` in workflow:
   ```yaml
   MAX_PAGES=${{ github.event.inputs.max_pages || '100' }}
   ```
2. Split sites in job multipli

### Out of Memory

**Raro** - GitHub Actions ha 7GB RAM

**Fix:** Riduci batch size in `upload_supabase.py`:
```python
--batch-size 50  # invece di 100
```

---

## 💰 Costi Stimati

| Servizio | Piano | Uso Mensile | Costo |
|----------|-------|-------------|-------|
| **GitHub Actions** | Free (public repo) | ~90 ore/mese | **$0** |
| **Supabase** | Free tier | <500MB DB | **$0** |
| **OpenAI Embeddings** | Pay-per-use | ~3M tokens/mese | **$0.06** |
| **TOTALE** | | | **~$0.06/mese** 🎉 |

**Se repo PRIVATO:**
- GitHub Actions: primi 2000 min gratis
- Eccedenza: ~$27/mese
- **Totale: ~$27/mese**

---

## 📊 Performance Attese

**Setup iniziale (10k chunks):**
- Crawl: ~2 ore
- Chunk: ~5 min
- Upload + Embedding: ~10 min
- **Totale: ~2.5 ore**

**Incrementale (100 nuovi chunks/giorno):**
- Crawl: ~10 min
- Chunk: ~10 sec
- Upload + Embedding: ~1 min
- **Totale: ~15 min**

---

## 🆘 Supporto

**Issue nel repo:**
Issues → New Issue → descrivi problema

**Check:**
1. Logs GitHub Actions
2. Supabase Logs (Dashboard → Logs)
3. OpenAI usage (platform.openai.com/usage)

---

## ✅ Checklist Post-Setup

- [ ] Supabase progetto creato
- [ ] SQL setup eseguito
- [ ] OpenAI API key configurata in Supabase
- [ ] GitHub repo creato
- [ ] 4 secrets GitHub configurati
- [ ] Test manuale completato con successo
- [ ] Chunks in Supabase con embedding NOT NULL
- [ ] Similarity search funziona
- [ ] Cron schedulato (Actions mostra "scheduled")

**Se tutto ✅ → Sistema pronto!** 🚀

Prossimo crawl: domani alle 2 AM UTC automaticamente.
