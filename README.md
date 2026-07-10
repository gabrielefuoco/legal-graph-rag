# ⚖️ Legal GraphRAG

**Legal GraphRAG** è un framework avanzato di *Retrieval-Augmented Generation* specializzato nel dominio legislativo italiano. Il sistema combina la potenza dei **Knowledge Graph** con la flessibilità della **Vector Search**, sfruttando la struttura gerarchica delle norme (Akoma Ntoso) e l'ontologia semantica del **Thesaurus TESEO** del Senato.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Neo4j](https://img.shields.io/badge/Database-Neo4j-008CC1)
![LLM](https://img.shields.io/badge/LLM-Qwen3%20(Ollama)-orange)

---

## 🌟 Caratteristiche Principali

- **🔍 Retrieval Ibrido Multi-Canale**: Fusione di risultati tramite **Reciprocal Rank Fusion (RRF)** da tre sorgenti:
    - **Vector Search**: Embedding semantici per catturare il significato latente.
    - **BM25 (Full-Text)**: Match preciso di parole chiave e riferimenti normativi.
    - **Graph Traversal**: Navigazione delle relazioni semantiche fornite dal Thesaurus TESEO.
- **🏛️ Ingestione Multi-Sorgente Asincrona**: Pipeline ad alte prestazioni per il download da Senato, Camera, EUR-Lex, Normattiva, Corte Costituzionale e TESEO.
- **📄 Parsing Akoma Ntoso**: Parser deterministico che mantiene la gerarchia strutturale (Libri, Titoli, Articoli, Commi) e inietta il contesto globale in ogni chunk.
- **🧠 Semantica TESEO**: Integrazione profonda con il vocabolario controllato del Senato per l'identificazione automatica di concetti e argomenti.
- **⚡ Performance Ottimizzate**: Supporto per accelerazione hardware GPU (Ollama) e gestione intelligente dei batch di ingestion.

---

## 🏗️ Architettura Tecnica

Il sistema adotta un approccio **Native Graph-Vector Search** in Neo4j:

- **Orchestration**: `LangGraph` per la gestione di flussi ciclici, analisi della query e ragionamento multi-hop.
- **Models**: `Qwen3:4b` (via Ollama) ottimizzato per il reasoning legale e la generazione di output strutturato.
- **Schema Grafo**: Modello ottimizzato basato su entità `WORK`, `EXPRESSION` e `CONCEPT` (TESEO).

---

## 🚀 Per Iniziare

### 1. Prerequisiti
- Docker & Docker Compose
- Python 3.10+
- (Opzionale) NVIDIA GPU per accelerazione Ollama

### 2. Setup Infrastruttura
```bash
# Avvio dei servizi core (Neo4j e Ollama)
docker compose up -d --build
```

### 3. Ingestione Olistica e Caricamento (Holistic ETL)
Abbiamo unificato l'ingestione, il parsing e l'inserimento a grafo in un unico potentissimo comando orientato al *topic*.
Il comando scarica, parsa e collega: Normattiva, Senato (tramite Git Pull automatico dei Bulk Data e filtro SPARQL), Camera (Iter Legis) e Corte Costituzionale (Giurisprudenza).

```bash
# Sostituisci "appalti" con il tema di ricerca desiderato per la Tesi
python manage.py build-graph --topic "appalti"
```

---

## 🔍 Interazione con il Sistema

### CLI (Command Line Interface)
È possibile interrogare il sistema direttamente dal terminale con output formattato e pulito:
```bash
python manage.py retrieve --query "incentivi per la transizione energetica" --verbose
```
*Opzioni disponibili:*
- `--top-k`: Risultati estratti per canale.
- `--final-k`: Risultati mostrati dopo la fusione RRF.
- `--verbose`: Mostra i concetti TESEO identificati e i punteggi dei canali.
- `--full-text`: Visualizza l'intera espressione normativa.

### 🧪 Interactive Testing Sandbox
Per un'esperienza di test più visuale e dettagliata, utilizza il notebook interattivo:
- [`interactive_rag.ipynb`](file:///c:/Users/gabri/APP/Universit%C3%A0/Tesi/interactive_rag.ipynb)

Il notebook offre visualizzazioni Markdown, debug della fase di query analysis e confronto diretto tra i canali di ricerca.

---

## 📂 Struttura del Progetto

```text
├── src/
│   ├── ingestion/    # Client asincroni per le varie fonti legislative
│   ├── parsing/      # Parser Akoma Ntoso e trasformatori di dati
│   ├── rag/          # Engine di retrieval, fusion e query analyzer
│   ├── graph/        # Logica di interazione con Neo4j
│   └── utils/        # Utility comuni e configurazione
├── data/             # Cache dati grezzi e processati
├── tests/            # Suite di test unitari e di integrazione
└── manage.py         # Punto di ingresso unico per la gestione del sistema
```
