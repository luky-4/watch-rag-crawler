# ⚡ Quick Start - 5 Minuti

## 🎯 Setup Rapidissimo

### 1. Download & Estrai
```bash
# Estrai ZIP
unzip watch-rag-github-final.zip
cd github-repo-final
```

### 2. Supabase (2 min)
1. [supabase.com](https://supabase.com) → New Project
2. SQL Editor → Copia/incolla tutto da `processing/supabase_setup.sql` → Run
3. Settings → API → Custom Settings:
   - Nome: `app.openai_api_key`
   - Valore: tua OpenAI key `sk-xxx...`
4. Settings → API → Copia:
   - Project URL
   - anon key

### 3. GitHub (2 min)
```bash
# Inizializza repo
git init
git add .
git commit -m "Initial commit"

# Crea repo su GitHub.com (nome: watch-rag-crawler, PRIVATO)
# Poi:
git remote add origin https://github.com/TUO_USER/watch-rag-crawler.git
git push -u origin main
```

### 4. Secrets GitHub (1 min)
Settings → Secrets → New secret (crea 4 secrets):

```
SUPABASE_URL = https://xxx.supabase.co
SUPABASE_KEY = eyJxxx...
OPENAI_API_KEY = sk-xxx...
SITES_LIST = hodinkee.com blog
rolex.com brand
patek.com brand
```

### 5. Test! (10 min)
Actions → Nightly Crawl → Run workflow → Max pages: `10`

✅ Aspetta ~10 min → Verifica Supabase → Chunks con embeddings!

---

## 📋 Checklist

- [ ] ZIP estratto
- [ ] Supabase setup SQL eseguito
- [ ] OpenAI key configurata in Supabase
- [ ] GitHub repo creato
- [ ] 4 secrets configurati
- [ ] Test workflow completato
- [ ] Chunks in Supabase con embedding

**Tutto ✅ → Funziona!**

Prossimo crawl automatico: domani alle 2 AM UTC.

---

## 💡 Comandi Utili

```bash
# Aggiungere siti
# Settings → Secrets → SITES_LIST → Edit

# Cambiare orario cron
# Edita .github/workflows/nightly-crawl.yml
# cron: '0 14 * * *'  # 2 PM invece di 2 AM

# Re-run manuale
# Actions → Run workflow

# Vedere logs
# Actions → Latest run → Logs

# Verificare Supabase
# Table Editor → chunks
```

---

## 🆘 Problemi?

**Workflow fallisce:**
→ Controlla secrets tutti presenti

**Embedding NULL:**
→ Verifica OpenAI key in Supabase Settings

**Timeout:**
→ Riduci max_pages a 50

---

**Guida completa:** `SETUP_GUIDE.md`
