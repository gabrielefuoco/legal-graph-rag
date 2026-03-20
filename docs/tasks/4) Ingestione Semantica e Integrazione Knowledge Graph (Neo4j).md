# Fase 3: Ingestione Semantica e Integrazione Knowledge Graph (Neo4j)

In questa fase, l'obiettivo è stato lo sviluppo del motore di arricchimento semantico (matching con il Thesaurus TESEO) e vettoriale (embedding con Qwen3), unitamente alla pipeline di persistenza su Neo4j per la creazione del Knowledge Graph ibrido.

## Attività Svolte

### 1. Motore di Persistenza (Neo4j Loader)
- **Classe `AsyncNeo4jLoader`**: Sviluppo di un loader asincrono ottimizzato per il caricamento batch.
- **Configurazione Schema**:
  - Implementazione di vincoli di unicità (`UNIQUE constraint`) sugli ID globali (hash deterministici) e URN per garantire l'idempotenza.
  - Configurazione automatica del **Vector Index** (Cosine Similarity) per il retrieval semantico.
  - Creazione di **Full-Text Index** per la ricerca ibrida (BM25).
- **Atomicità**: Refactoring della pipeline per utilizzare transazioni atomiche singole (`execute_write`) per ogni batch, prevenendo dati parziali o orfani.

### 2. Arricchimento Semantico (TESEO Engine)
- **Algoritmo Fast Matching**: Integrazione della libreria `pyahocorasick` per il matching multi-pattern delle label TESEO in tempo lineare.
- **Word-Boundary Control**: Implementazione di logiche di controllo per garantire che i concetti TESEO siano matchati solo come parole intere, eliminando falsi positivi da sottostringhe.
- **Relazioni Semantiche**: Generazione automatica di archi `:HAS_TOPIC` tra i nodi normativi (`:Expression`) e i concetti del Thesaurus (`:TESEO_Concept`).

### 3. Motore Vettoriale (Vector Engine)
- **Integrazoine LangChain**: Utilizzo di modelli di embedding (via Qwen3 endpoint) per la vettorializzazione dei chunk.
- **Context Injection**: Prima della generazione del vettore, ogni nodo viene arricchito con la gerarchia dei padri (Atto > Libro > Articolo > Comma) per massimizzare la pertinenza del retrieval.
- **Embedding Batching**: Implementazione del batching per le chiamate di inferenza, riducendo drasticamente i tempi di latenza.

### 4. Pipeline di Orchestrazione (`transformers.py`)
- **Gestione Metadati Temporali**: Iniezione automatica delle date di vigenza (`vigenza_start`, `vigenza_end`) in tutti i nodi del grafo per abilitare il filtraggio temporale.
- **Resilienza (Dead Letter logic)**: Inserimento di blocchi `try-except` intorno alla generazione degli embedding per permettere alla pipeline di completare l'ingestion anche in caso di fallimenti temporanei del modello (i nodi vengono caricati senza vettore invece di mandare in crash il processo).
- **Sanitizzazione Input**: Correzione di `AttributeError` sistematici legati alla discrepanza tra i modelli Pydantic e i dizionari di transito.

---

## Comandi per l'Utilizzo

### 1. Ingestione Completa (Enrichment + Neo4j)
```bash
python manage.py enrich-and-load --input data/parsed/all_docs.json --teseo-rdf data/teseo.rdf
```


