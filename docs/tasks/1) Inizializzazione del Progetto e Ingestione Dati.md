# Fase 0: Inizializzazione del Progetto e Ingestione Dati

In questa fase iniziale, l'obiettivo primario è stato stabilire le fondamenta del progetto **Legal GraphRAG**. L'attenzione si è concentrata sulla predisposizione di un ambiente di sviluppo robusto e sull'acquisizione dei dati grezzi (Thesaurus TESEO e Atti Legislativi) necessari per le successive fasi di processamento.

## Attività Svolte

### 1. Setup Infrastrutturale

È stato creato un ambiente interamente containerizzato per garantire riproducibilità e isolamento:

- **Docker Compose:** Orchestrazione di due servizi:
    - `neo4j` (v5.26.9): Utilizzato per ospitare il grafo contenente ontologie e dati legislativi.
	    - Si è scelta di una versione recente per garantire supporto a lungo termine e accesso alle funzionalità di Graph Data Science (GDS).
    - `data-app` (Python 3.10): Configurato come ambiente di sviluppo per l'esecuzione degli script di ingestione e parsing.
- **Struttura del Progetto:** Organizzazione del codice secondo uno standard pulito (`src/`, `data/`, `libs/`), mantenendo una netta separazione tra la logica di ingestione (`src/ingestion`) e i dati grezzi (`data/raw`).

### 2. Strategia di Acquisizione Dati (Ingestion)

È stato adottato un approccio ibrido per ottimizzare l'efficienza del download:

- **Thesaurus TESEO:** Sviluppo di uno script Python (`teseo_downloader.py`) per interrogare direttamente l'endpoint SPARQL del Senato.
- **Dati Senato (Bulk):** Per la gestione dello storico ("Cold Start"), è stata preferita la clonazione della repository `AkomaNtosoBulkData`.
    - Link: https://github.com/SenatoDellaRepubblica/AkomaNtosoBulkData
    - Licenza: CC BY 3.0
- **Normattiva (Aggiornamenti):** Implementazione di un client API custom (`normattiva_api.py`) per la gestione del download puntuale di nuovi atti.
    - URL API: https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1
    - Licenza: CC BY 4.0

### 3. Sviluppo Client Normattiva

Questa fase ha presentato complessità dovute a discrepanze tra le specifiche reali e la documentazione online.

- Implementazione di un **flusso asincrono** per la gestione di grandi volumi di dati:
    1. Invio richiesta di nuova ricerca (`POST`).
    2. Conferma del token (`PUT`).
    3. Monitoraggio dello stato (`GET`).
    4. Download finale del pacchetto ZIP (`GET`). (Seguirà uno script di decompressione)
- Aggiunta di una funzionalità di **Bulk Download** per il download di intere annate tramite un flag (es. `--year 2024`).

---

## Comandi per l'Avvio del Setup

### 1. Avvio Infrastruttura
Avvio dei container (Neo4j e Python Env):
```bash
docker compose up -d --build
```

### 2. Ingestione Dati
Download del Thesaurus TESEO ed esecuzione della pipeline di base:
```bash
# Esecuzione dall'interno del container
docker compose exec data-app python manage.py ingest
```

### 3. Download Normattiva (Bulk)
Download di un intero anno (es. 2024) tramite il client asincrono:
```bash
# Esecuzione da locale o dal container
python src/ingestion/normattiva_api.py --year 2024 --email "tua@email.com"
```

## Stato Attuale
Il progetto è pronto per la **Fase 1: Parsing**. I dati sono disponibili in `data/raw` e l'infrastruttura è operativa.