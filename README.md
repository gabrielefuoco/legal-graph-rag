# Legal GraphRAG

**Legal GraphRAG** è un sistema di *Retrieval-Augmented Generation* per il dominio legale italiano che combina Knowledge Graph e Vector Search. Il progetto mira a superare i limiti dei sistemi RAG tradizionali integrando la struttura gerarchica delle norme (Akoma Ntoso) e la semantica del Thesaurus TESEO del Senato.

## 🏗️ Architettura

Il sistema adotta un approccio **Hybrid Knowledge Graph + Vector Search**, senza separazione tra database relazionale e vettoriale.

*   **Database:** [Neo4j](https://neo4j.com/) (Graph + Vector Index)
*   **Orchestration:** LangGraph (Gestione flussi ciclici e multi-hop)
*   **LLM:** Qwen3:4b (Fine-tuned per reasoning legale e output JSON)
*   **Ingestion:** Pipeline XML-Native (Akoma Ntoso / NIR)
*   **Semantica:** Integrazione Thesaurus TESEO (SKOS)

## 🚀 Per Iniziare

### Prerequisiti

*   Docker & Docker Compose

### Setup Infrastruttura

1.  **Avvio dei servizi (Neo4j e Python Environment):**
    ```bash
    docker compose up -d --build
    ```

2.  **Ingestione Dati (TESEO & Base Pipeline):**
    ```bash
    docker compose exec data-app python manage.py ingest
    ```

3.  **Download Normattiva (Opzionale/Bulk):**
    ```bash
    python src/ingestion/normattiva_api.py --year 2024 --email "tua@email.com"
    ```

## 📂 Struttura del Progetto

*   `src/`: Codice sorgente (Ingestion, Graph Logic, API).
*   `data/`: Dati grezzi (XML, RDF) e processati.
*   `documentazione/`: Documentazione tecnica e di progetto.
*   `docker-compose.yml`: Definizione dei servizi containerizzati.

## 📍 Stato del Progetto

Attualmente il progetto ha completato la **Fase 0 (Inizializzazione e Ingestione Dati)** ed è pronto per la **Fase 1 (Parsing e Costruzione del Grafo)**.


