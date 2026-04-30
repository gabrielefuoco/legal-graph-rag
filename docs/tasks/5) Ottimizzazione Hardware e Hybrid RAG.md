# Fase 5: Ottimizzazione Hardware e Hardware-Accelerated Hybrid RAG

In questa fase, l'obiettivo è stato l'abbattimento dei tempi di latenza nella generazione degli embedding tramite l'integrazione dell'accelerazione hardware (GPU NVIDIA) e la validazione del sistema di recupero ibrido (Vector + Graph Search).

## Attività Svolte

### 1. Accelerazione Hardware (GPU Passthrough)
- **Containerizzazione GPU**: Configurazione del file `docker-compose.yml` per abilitare il passthrough dei driver NVIDIA all'interno del container `ollama`.
- **Integrazione CUDA**: Verifica del corretto funzionamento tramite l'esecuzione di `nvidia-smi` all'interno dell'ambiente Docker, confermando l'utilizzo della **NVIDIA GeForce RTX 3060**.
- **Prestazioni**: Riduzione drastica della latenza di inferenza per singolo embedding da ~4000ms (CPU) a **~50ms (GPU)**, portando a un incremento prestazionale di circa 80x.

### 2. Ottimizzazione della Pipeline di Ingestione
- **Tuning del Batching**: Incremento del `BATCH_SIZE` a **50 documenti** (circa 500-1000 nodi Expression per batch) nel file `transformers.py` per massimizzare il throughput sulla GPU senza saturare la memoria VRAM.
- **Payload Truncation**: Implementazione di una logica di troncamento aggressivo a **3500 caratteri** nel `VectorEngine` per garantire la compatibilità con il context window del modello `nomic-embed-text` (2048 token), prevenendo errori di overflow durante l'ingestion di testi legali molto lunghi.

### 3. Validazione Retrieval Ibrido (test_rag.py)
- **Ricerca Vettoriale**: Implementazione di query Cypher basate sull'indice vettoriale `expression_embedding_vector` per il recupero semantico delle norme.
- **Ricerca Graph-Based**: Integrazione di pattern di navigazione nel grafo TESEO per estrarre contesti normativi basati sui concetti semantici (Topics).
- **Hybrid Scoring**: Sviluppo di uno script di test (`test_rag.py` e `demo_rag.py`) per dimostrare la capacità del sistema di combinare risultati vettoriali (basati sul significato) e risultati grafici (basati sulla struttura e sull'ontologia).

### 4. Gestione Documenti Unknown (Hash ID)
- **Hash Deterministici**: Implementazione di un sistema di generazione ID basato su SHA256 dei metadati per i documenti privi di URN ufficiale (`urn:unknown`). Questo garantisce l'idempotenza e l'integrità del grafo anche in presenza di dati incompleti da fonti esterne (es. Camera API).

---

## Comandi per la Verifica

### 1. Verifica Stato GPU
```bash
docker compose exec ollama nvidia-smi
```

### 2. Esecuzione Test RAG Ibrido
```bash
docker compose run --rm data-app python demo_rag.py
```

### 3. Monitoring Embedding su Neo4j
```cypher
MATCH (e:Expression) 
WHERE e.embedding IS NOT NULL 
RETURN count(e) as embedded_nodes
```
