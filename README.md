# Legal GraphRAG

**Legal GraphRAG** è un sistema di *Retrieval-Augmented Generation* per il dominio legale italiano che combina Knowledge Graph e Vector Search. Il progetto mira a superare i limiti dei sistemi RAG tradizionali integrando la struttura gerarchica delle norme (Akoma Ntoso) e la semantica del Thesaurus TESEO del Senato.

## 🏗️ Architettura

Il sistema adotta un approccio **Hybrid Knowledge Graph + Vector Search**, senza separazione tra database relazionale e vettoriale.

*   **Database:** [Neo4j](https://neo4j.com/) (Graph + Vector Index)
*   **Orchestration:** LangGraph (Gestione flussi ciclici e multi-hop)
*   **LLM:** Qwen3:4b (Fine-tuned per reasoning legale e output JSON)
*   **Ingestion:** Pipeline Asincrona Multi-Sorgente (Senato, Camera, EUR-Lex, Normattiva, Corte Cost, TESEO)
*   **Parsing:** Modulo Deterministico + Semantic Chunking (Context Injection)
*   **Semantica:** Integrazione Thesaurus TESEO (SKOS)

## 🚀 Per Iniziare

### Prerequisiti

*   Docker & Docker Compose
*   Python 3.10+

### Setup Infrastruttura

1.  **Avvio dei servizi (Neo4j e Python Environment):**
    ```bash
    docker compose up -d --build
    ```

2.  **Ingestione Dati (Pipeline Completa Asincrona):**
    ```bash
    # Esegue sequenzialmente (asincrono) il download da tutte le fonti
    docker compose exec data-app python manage.py ingest
    ```
    
    *Fonti supportate:* 
    *   **Senato della Repubblica** (Scraping atti legislativi)
    *   **Camera dei Deputati** (Open Data API)
    *   **EUR-Lex** (SPARQL endpoint, Regolamenti/Direttive > 2024)
    *   **Corte Costituzionale** (Sentenze)
    *   **Normattiva** (API asincrone)
    *   **TESEO** (Thesaurus SKOS)

3.  **Parsing Documenti (XML -> JSON Graph):**
    ```bash
    # Parsing intera directory
    python src/parsing/parser.py --input data/raw/ --output all_docs.json
    ```

4.  **Enrichment & Neo4j Load (Phase 3):**
    ```bash
    # Arricchisce i DocumentDTO con embedding (Qwen3) e topics (TESEO), poi carica in Neo4j
    python manage.py enrich-and-load --input all_docs.json --teseo-rdf data/teseo.rdf
    ```

## 📂 Struttura del Progetto

*   `src/ingestion`: Client asincroni (`aiohttp`) per Senato, Camera, EUR-Lex, Normattiva, Corte Costituzionale.
*   `src/parsing`: Parser Akoma Ntoso, Semantic Chunking e Context Injection.
*   `src/graph`: (WIP) Moduli per l'ingestion in Neo4j.
*   `data/`: Dati grezzi (XML, RDF) e processati.
*   `documentazione/`: Documentazione tecnica e di progetto.

## 📍 Stato del Progetto

Attualmente il progetto ha completato la **Fase 3 (Integrazione Neo4j & Arricchimento Semantico)**. Il sistema è in grado di:
*   Estrarre entità semantiche dal Thesaurus TESEO.
*   Generare embedding vettoriali per i chunk di testo (EXPRESSIONS) tramite Qwen3.
*   Persistere il grafo arricchito su Neo4j con schema ottimizzato (Constraints & Indices).


