-- ============================================
-- SUPABASE SETUP per RAG con OpenAI Embeddings
-- ============================================
-- Esegui in: Supabase Dashboard → SQL Editor

-- 1. Abilita estensione vector
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Crea tabella chunks
CREATE TABLE IF NOT EXISTS chunks (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    embedding vector(1536),  -- OpenAI text-embedding-3-small = 1536D
    metadata JSONB,
    url TEXT,
    title TEXT,
    domain TEXT,
    brand TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Index per similarity search (IMPORTANTE!)
CREATE INDEX IF NOT EXISTS chunks_embedding_idx 
ON chunks USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- 4. Index metadata (per filtrare per brand/domain)
CREATE INDEX IF NOT EXISTS chunks_metadata_idx 
ON chunks USING gin (metadata);

-- 5. Index brand/domain
CREATE INDEX IF NOT EXISTS chunks_brand_idx ON chunks(brand);
CREATE INDEX IF NOT EXISTS chunks_domain_idx ON chunks(domain);

-- ============================================
-- FUNCTION: Generate Embedding via OpenAI API
-- ============================================

-- Richiede: http extension
CREATE EXTENSION IF NOT EXISTS http;

-- Function per chiamare OpenAI API
CREATE OR REPLACE FUNCTION generate_openai_embedding(content_text TEXT)
RETURNS vector(1536)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    result vector(1536);
    api_key TEXT;
    api_response JSON;
BEGIN
    -- Ottieni API key da vault
    api_key := current_setting('app.openai_api_key', true);
    
    IF api_key IS NULL THEN
        RAISE EXCEPTION 'OpenAI API key not set. Set it in Settings → API → Custom Settings';
    END IF;
    
    -- Chiama OpenAI API
    SELECT content INTO api_response
    FROM http((
        'POST',
        'https://api.openai.com/v1/embeddings',
        ARRAY[
            http_header('Authorization', 'Bearer ' || api_key),
            http_header('Content-Type', 'application/json')
        ],
        'application/json',
        json_build_object(
            'input', content_text,
            'model', 'text-embedding-3-small'
        )::text
    ));
    
    -- Estrai embedding da response
    result := ARRAY(
        SELECT json_array_elements_text(
            api_response->'data'->0->'embedding'
        )::float
    )::vector(1536);
    
    RETURN result;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'OpenAI API error: %', SQLERRM;
        RETURN NULL;
END;
$$;

-- ============================================
-- TRIGGER: Auto-generate embedding on INSERT
-- ============================================

CREATE OR REPLACE FUNCTION auto_generate_embedding()
RETURNS TRIGGER AS $$
BEGIN
    -- Se embedding è NULL, genera via OpenAI
    IF NEW.embedding IS NULL AND NEW.content IS NOT NULL THEN
        NEW.embedding := generate_openai_embedding(NEW.content);
        NEW.updated_at := NOW();
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Crea trigger
DROP TRIGGER IF EXISTS chunks_embedding_trigger ON chunks;
CREATE TRIGGER chunks_embedding_trigger
    BEFORE INSERT OR UPDATE ON chunks
    FOR EACH ROW
    WHEN (NEW.embedding IS NULL)
    EXECUTE FUNCTION auto_generate_embedding();

-- ============================================
-- FUNCTION: Similarity Search
-- ============================================

CREATE OR REPLACE FUNCTION match_chunks(
    query_embedding vector(1536),
    match_count INT DEFAULT 5,
    filter_brand TEXT DEFAULT NULL,
    filter_domain TEXT DEFAULT NULL
)
RETURNS TABLE (
    id TEXT,
    content TEXT,
    url TEXT,
    title TEXT,
    domain TEXT,
    brand TEXT,
    metadata JSONB,
    similarity FLOAT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        chunks.id,
        chunks.content,
        chunks.url,
        chunks.title,
        chunks.domain,
        chunks.brand,
        chunks.metadata,
        1 - (chunks.embedding <=> query_embedding) AS similarity
    FROM chunks
    WHERE 
        chunks.embedding IS NOT NULL
        AND (filter_brand IS NULL OR chunks.brand = filter_brand)
        AND (filter_domain IS NULL OR chunks.domain = filter_domain)
    ORDER BY chunks.embedding <=> query_embedding
    LIMIT match_count;
END;
$$;

-- ============================================
-- CONFIGURAZIONE API KEY
-- ============================================

-- Aggiungi OpenAI API key in Supabase:
-- Dashboard → Settings → API → Custom Settings

-- Nome: app.openai_api_key
-- Valore: sk-xxx...

-- Test la funzione:
-- SELECT generate_openai_embedding('test embedding');

-- ============================================
-- TEST SIMILARITY SEARCH
-- ============================================

-- 1. Inserisci chunk di test (trigger genererà embedding)
INSERT INTO chunks (id, content, title, url, domain)
VALUES (
    'test_chunk_1',
    'Rolex Submariner is a professional diving watch introduced in 1953.',
    'Rolex Submariner',
    'https://example.com/submariner',
    'example.com'
);

-- 2. Aspetta 2-3 secondi (OpenAI API call)

-- 3. Verifica embedding generato
SELECT id, content, embedding IS NOT NULL as has_embedding
FROM chunks 
WHERE id = 'test_chunk_1';

-- 4. Test similarity search
SELECT * FROM match_chunks(
    query_embedding := (SELECT embedding FROM chunks WHERE id = 'test_chunk_1'),
    match_count := 3
);

-- ============================================
-- PERFORMANCE TIPS
-- ============================================

-- Per dataset grandi (>100k chunks):
-- 1. Aumenta 'lists' nell'index:
--    DROP INDEX chunks_embedding_idx;
--    CREATE INDEX chunks_embedding_idx ON chunks 
--    USING ivfflat (embedding vector_cosine_ops)
--    WITH (lists = 1000);  -- ~sqrt(row_count)

-- 2. Usa VACUUM ANALYZE periodicamente:
--    VACUUM ANALYZE chunks;

-- ============================================
-- CLEANUP (se serve ricominciare)
-- ============================================

-- ATTENZIONE: Elimina TUTTI i dati!
-- DROP TABLE IF EXISTS chunks CASCADE;
-- DROP FUNCTION IF EXISTS generate_openai_embedding CASCADE;
-- DROP FUNCTION IF EXISTS auto_generate_embedding CASCADE;
-- DROP FUNCTION IF EXISTS match_chunks CASCADE;
