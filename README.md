# ⚖️ Legal GraphRAG

*Sistemi di Retrieval-Augmented Generation (RAG) per il dominio giuridico italiano: Knowledge Graph, Vector Search, orchestrazione agentica e generative AI*

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Neo4j](https://img.shields.io/badge/Database-Neo4j%205.26-008CC1)
![LLM](https://img.shields.io/badge/LLM-Qwen3.5%3A4b%20(Ollama)-orange)
![Embeddings](https://img.shields.io/badge/Embeddings-qwen3--embedding%3A0.6b%20(1024--d)-green)
![Architecture](https://img.shields.io/badge/Orchestration-LangGraph-purple)
![Deployment](https://img.shields.io/badge/Deploy-100%25%20On--Premise%20%2F%20Air--Gapped-success)

**Legal GraphRAG** è un framework avanzato di *Modular RAG* neuro-simbolico progettato specificamente per il dominio legislativo italiano (*Civil Law*). Il sistema supera i quattro limiti strutturali del *Naive RAG* in ambito forense (perdita del contesto gerarchico, divario lessicale, cecità ai rinvii normativi e amnesia vigenziale) unificando in un unico motore transazionale su **Neo4j** la rappresentazione formale degli atti (**Akoma Ntoso / NIR** e modello **FRBR**), il vocabolario controllato istituzionale (**Thesaurus TESEO** del Senato), la ricerca vettoriale densa (**HNSW**) e la ricerca lessicale (**BM25**), orchestrati da un ciclo agentico auto-riflessivo su **LangGraph**.

---

## 🌟 Caratteristiche Principali e Pilastri Architetturali

### 1. 🏛️ Pipeline ETL Asincrona e Parsing Deterministico
- **Ingestione Non-Bloccante Multi-Sorgente**: Acquisizione asincrona (`asyncio` / `aiohttp`) con *Exponential Backoff*, *Full Jitter* e *Circuit Breaker* dalle quattro fonti primarie dello Stato: **Normattiva** (macchina a 7 stati di polling), **Thesaurus TESEO** (estrazione SPARQL paginata SKOS/RDF), **Camera e Senato** (*Iter Legis* via Linked Open Data) e **Corte Costituzionale** (sindacato di legittimità).
- **DFS Context Injection (`AknParser`)**: Scansione deterministica dell'albero XML (*Akoma Ntoso 3.0* e *NIR 2.2*) tramite visita in profondità (DFS). Ogni comma (`:Expression`) viene arricchito con la catena ordinamentale dei nodi padre (`Libro > Titolo > Capo > Articolo + Rubrica`), azzerando il fenomeno del *Context Loss*.
- **Linearizzazione Duale delle Tabelle**: Le tabelle normative vengono gestite come nodi atomici con doppia rappresentazione: formattazione **GitHub Flavored Markdown** (`text_display`) per la leggibilità dell'utente a schermo e **serializzazione discorsiva soggetto-attributo-valore** (`text_vector`) per massimizzare la precisione dell'embedding vettoriale.
- **Fast Ontology Matching (Aho-Corasick)**: Ancoraggio dei commi agli oltre 40.000 descrittori del Thesaurus TESEO in tempo lineare $O(N + M)$ tramite automa a stati finiti, combinato con uno score ibrido lessicale-semantico ($\alpha = 0.3$, soglia $\tau = 0.45$) per istanziare archi pesati `:HAS_TOPIC`.
- **Validazione Pydantic V2 e Idempotenza**: Tipizzazione rigorosa tramite DTO e generazione deterministica degli ID primari con hashing crittografico SHA-256 sull'URN NIR (`urn:nir:stato:...`), garantendo caricamenti transazionali `MERGE` idempotenti.

### 2. 🔍 Motore di Retrieval Ibrido Parallelo e Grafo Additivo
- **Ricerca Parallela a Tre Canali su Neo4j**:
  1. **Vector Search (HNSW)**: Embedding densi a **1024 dimensioni** (`qwen3-embedding:0.6b`) con istruzione asimmetrica lato query.
  2. **BM25 Keyword Search (Lucene)**: Match esatto per codici alfanumerici, estremi di legge e tecnicismi giuridici.
  3. **Graph Semantic Search (TESEO)**: Navigazione topologica delle relazioni `:HAS_TOPIC` (calibrata con `TESEO_DENSE_THRESHOLD = 0.60` e `TESEO_MAX_CONCEPTS = 3`).
- **Weighted RRF con Deduplicazione Intra-Canale**: Unificazione delle graduatorie eterogenee tramite *Weighted Reciprocal Rank Fusion* ($k=60$, pesi $W_{\text{vector}}=1.0, W_{\text{bm25}}=1.0, W_{\text{graph}}=1.2$) previa deduplicazione per canale (`_deduplicate_channel`) per neutralizzare i percorsi multipli `:PART_OF`.
- **Principio del Grafo Additivo (`PRIORITY_BASE_CHUNKS = 6`)**: I migliori candidati estratti dai canali testuali costituiscono una quota protetta inviolabile che l'arricchimento relazionale non può scalzare, garantendo **zero regressioni** sulle query dirette.
- **Work-level Merging (Super-Chunking) e Topology Expansion**: Ricomposizione ordinata dei commi contigui dello stesso atto in *Super-Chunk* organici (abbattendo del **36.4%** i token di overhead e mitigando il *Lost in the Middle*) ed espansione orizzontale sui commi adiacenti (`:NEXT`) e sulle rubriche padre (`:PART_OF`) entro un budget di 6.000 caratteri.

### 3. ⏳ Temporal RAG e Gestione della Multivigenza
- **Principio *Tempus Regit Actum***: Tracciamento nativo degli intervalli di vigenza $I_d = [t_{\text{start}}, t_{\text{end}})$ sui nodi `:Expression` e modellazione dell'algebra delle novelle legislative tramite archi `:MODIFIES` (`SUBSTITUTION`, `INSERTION`, `REPEAL`).
- **Soft Semantic Tagging (Abrogation Marking)**: Anziché eliminare fisicamente le norme storiche (impedendo la consulenza retroattiva) o penalizzare testi risalenti ma vigenti (es. Codice Civile del 1942), il sistema inietta in testa al testo il marcatore esplicito `[ATTENZIONE: NORMA ABROGATA IL YYYY-MM-DD]`, preservando il diritto transitorio e azzerando le allucinazioni diacroniche.

### 4. 🤖 Orchestrazione Agentica LangGraph e Risolutore Simbolico
- **Compilatore Neuro-Simbolico di Rinvii (`resolver.py`)**: Intercetta in **12 ms** i riferimenti normativi espliciti nella query (es. *"art. 24 della legge 241/1990"*) tramite pattern regex certificati, sintetizza l'URN NIR canonico ed esegue il lookup diretto su Neo4j, superando la cecità lessicale degli indici tradizionali.
- **Espansione Citazionale Multi-Hop Guidata da Embedding (`CitationExpander`)**: Navigazione controllata lungo le oltre **21.415 relazioni `:CITES`** con vincolo rigido `MAX_CITATION_HOPS = 1` (per prevenire l'esplosione combinatoria delle reti *Scale-Free*). Per i rinvii a interi atti (`:Work`), ordina dinamicamente al volo gli articoli dell'atto citato per similarità coseno rispetto alla query (`CITATION_EXPANSION_PER_ACT = 3`).
- **Loop Auto-Riflessivo (Self-Reflective RAG)**: Un supervisore neurale strutturato Pydantic (**Retrieval Grader** a $T=0.0$) valuta la pertinenza dei frammenti estratti; in caso di insufficienza probatoria, attiva il **Query Rewriter** ($T=0.3$) con convergenza garantita entro $M \le 5$ iterazioni massime.
- **Sintesi Grounded e Deontologia Forense**: Generazione tramite `Qwen3.5:4b` in *Direct Stuffing* fino a **64.000 caratteri** (evitando la perdita informativa del Map-Reduce), con obbligo di citazione formale `[Atto, Articolo]`, *Citation Consistency Audit* e politica di **Negative Rejection** (fallback sicuro a zero allucinazioni in assenza di fonti).

---

## 🕸️ Schema del Knowledge Graph (Neo4j)

Il database unifica grafo di proprietà etichettato (LPG), indice vettoriale HNSW e indice full-text Lucene:

- **Nodi**:
  - `:Work`: Opera legislativa astratta FRBR identificata da URN NIR canonico.
  - `:StructuralUnit`: Partizioni gerarchiche intermedie (Libro, Parte, Titolo, Capo, Sezione, Articolo).
  - `:Expression`: Unità precettiva atomica (Comma, Lettera, Tabella) con `text_display`, `text_vector`, `embedding` (1024-d) ed estremi di vigenza.
  - `:TESEO_Concept`: Concetti del vocabolario controllato SKOS del Senato (`prefLabel`, `altLabels`).
  - `:Judgement` & `:IterLegisStep`: Pronunce della Corte Costituzionale e passaggi parlamentari dei DDL.
- **Relazioni**:
  - `(:Expression)-[:PART_OF]->(:StructuralUnit)-[:PART_OF]->(:Work)` (Gerarchia ordinamentale)
  - `(:Expression)-[:NEXT]->(:Expression)` (Continuità sequenziale dei commi)
  - `(:Expression)-[:CITES]->(:Work | :Expression)` (Rinvii normativi espliciti)
  - `(:Expression)-[:MODIFIES {type}]->(:Expression)` (Novelle legislative: sostituzioni, inserimenti, abrogazioni)
  - `(:Expression)-[:HAS_TOPIC {score}]->(:TESEO_Concept)` (Ancoraggio ontologico)
  - `(:Judgement)-[:INTERPRETS]->(:Work)` & `(:IterLegisStep)-[:ITER_STEP]->(:Work)`

---

## 📊 Validazione Sperimentale e Risultati (`graphrag-prod-v1`)

Il sistema è stato validato su un corpus di **6.824 atti normativi**, **30.362 disposizioni atomiche** e oltre **59.000 relazioni** (di cui **21.415 archi `:CITES`**), mediante un benchmark tripartito confrontato con una baseline ibrida convenzionale (Vector + BM25):

| Classe Benchmark | Configurazione | Recall@10 | MRR | Hit@1 | Sim. Semantica |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Classe A: Testo sufficiente** ($N=15$)<br>*(Verifica di non-regressione)* | Baseline (Vector + BM25) | 100.00% | 1.0000 | 100.00% | 0.7658 |
| | **Legal GraphRAG** | **100.00%** | **1.0000** | **100.00%** | **0.7667** |
| **Classe B: Rinvio nominato** ($N=12$)<br>*(Query puntuali `[Atto, Articolo]`)* | Baseline (Vector + BM25) | 0.00% | 0.0000 | 0.00% | 0.2591 |
| | **Legal GraphRAG** | **83.33%** | **0.6111** | **41.67%** | **0.6611** *(+155%)* |
| **Classe C: Multi-hop citazionale** ($N=20$)<br>*(Catene `:CITES` tra atti distinti)* | Baseline (Vector + BM25) | 15.00% | 0.1000 | 5.00% | 0.5115 |
| | **Legal GraphRAG** | **30.00%** *(2x)* | **0.2167** *(>2x)* | **15.00%** *(3x)* | **0.5160** |

### ⚡ Performance e Profilo Hardware (100% On-Premise)
Progettato per rispettare il segreto professionale (Art. 622 c.p.), il GDPR e l'EU AI Act in modalità **Air-Gapped**, l'intero stack gira su una singola GPU consumer **NVIDIA GeForce RTX 3060 (6 GB VRAM)** con KV-Cache quantizzata in `q8_0` (occupazione totale **~5.1 GB VRAM**):
- **Latenza Infrastruttura IR + Grafo** (TESEO + Embedding + 3 Canali Neo4j + RRF + Resolver + Citation & Topology Expander): **~331 ms**.
- **Latenza End-to-End Completa** (incluso Grading Pydantic e Generazione con `reasoning=False` a ~80 tok/s): **~2.25 s**.

---

## 🚀 Per Iniziare

### 1. Prerequisiti
- **Docker** & **Docker Compose**
- **Python 3.10+**
- GPU NVIDIA con supporto **CUDA 12.1** e *NVIDIA Container Toolkit* (fortemente raccomandato; 6 GB+ VRAM)

### 2. Deploy Infrastruttura Multi-Container
Avvia i tre container isolati (`neo4j` 5.26 con APOC e GDS, `ollama` con passthrough GPU e `legal-graph-app`):
```bash
cp .env.example .env    # Windows: copy .env.example .env
docker compose up -d --build
```
Lo stesso comando vale su **Windows** (Docker Desktop/WSL2) e su **Ubuntu** (Docker Engine). Tre differenze tra i due host:
- **GPU**: su Ubuntu, oltre a *NVIDIA Container Toolkit*, vanno decommentati i blocchi `deploy.resources` in `docker-compose.yml` (sono su `ollama`, `streamlit` e `jupyter`). Senza GPU il sistema gira comunque, con latenze maggiori.
- **PyTorch**: l'immagine installa per default le wheel CUDA (`cu121`, ~2,5 GB). Su un host senza NVIDIA conviene `TORCH_INDEX_URL=https://download.pytorch.org/whl/cpu` in `.env` prima del build: scarica ~10 volte meno.
- **Permessi**: le cartelle `neo4j/data`, `neo4j/logs` e `neo4j/plugins` non sono versionate e su Linux nascono di proprietà di `root`. Se Neo4j non parte per permessi: `sudo chown -R 7474:7474 neo4j/` (su Windows non serve).

Sequenza completa su Ubuntu (Docker Engine), host senza GPU:
```bash
cp .env.example .env
echo 'TORCH_INDEX_URL=https://download.pytorch.org/whl/cpu' >> .env
docker compose up -d --build
sudo chown -R 7474:7474 neo4j/    # solo se Neo4j non parte per permessi
docker compose ps                 # stato dei tre container
```
La dashboard è poi su `http://localhost:8501`, Neo4j Browser su `http://localhost:7474`.

### 3. Ingestione Olistica e Costruzione del Knowledge Graph (Holistic ETL)
Il comando unificato scarica in modo asincrono, valida con Pydantic V2, esegue il parsing Akoma Ntoso con *Context Injection*, linearizza le tabelle, calcola gli embedding a 1024-d e popola il grafo collegando Normattiva, TESEO, Camera, Senato e Corte Costituzionale:
```bash
# Sostituisci "appalti" con il dominio o tema normativo desiderato
python manage.py build-graph --topic "appalti"
```

---

## 🖥️ Interazione e Spiegabilità (XAI)

### Dashboard Web Interattiva (Streamlit)
Per avviare l'interfaccia grafica *White-Box* orientata all'ergonomia cognitiva forense (con bridge asincrono `nest_asyncio`):
```bash
python manage.py streamlit
```
La dashboard offre **tre livelli di spiegabilità (Explainable AI)** conformi all'art. 111 Cost. e all'EU AI Act:
1. **Livello Documentale**: Visualizzazione dei *Super-Chunk*, badge cromatici di canale (`[VECTOR]`, `[BM25]`, `[GRAPH]`, `[CITATION_HOP]`), link URN NIR cliccabili verso Normattiva, score RRF e avvisi di abrogazione.
2. **Livello Relazionale**: Ispezione delle query Cypher eseguite su Neo4j, archi `:CITES` attraversati e descrittori ontologici TESEO attivati.
3. **Livello Metacognitivo**: Tracciamento dello `StateGraph` LangGraph, motivazioni del *Retrieval Grader*, eventuali riscritture della query e controlli nella sidebar per studi di ablazione a runtime (`disable_graph`, `disable_bm25`, budget topologico).

### Command Line Interface (CLI)
È possibile interrogare direttamente il motore di retrieval e generazione da terminale:
```bash
python manage.py retrieve --query "Quali sono i limiti al subappalto a cascata nel codice dei contratti pubblici?" --verbose
```
*Opzioni principali:*
- `--top-k`: Candidati estratti per singolo canale (default: `15`).
- `--final-k`: Documenti finali dopo fusione RRF e reranking (default: `10`).
- `--verbose`: Mostra i concetti TESEO estratti, i pesi dei canali e le metriche di espansione.
- `--full-text`: Visualizza il testo integrale dei Super-Chunk normativi recuperati.

---

## 📂 Struttura del Progetto

```text
├── src/
│   ├── config.py                  # Configurazione centralizzata e iperparametri congelati (.env)
│   ├── logging_config.py          # Logging strutturato dell'applicazione
│   ├── ingestion/                 # Client asincroni (Normattiva 7-stati, SPARQL TESEO/Camera/Senato, Consulta)
│   │   ├── async_normattiva_client.py  # Macchina a stati di polling, backoff e circuit breaker
│   │   ├── async_teseo_client.py       # Estrazione SPARQL paginata SKOS/RDF del Thesaurus
│   │   ├── async_camera_client.py      # Iter Legis via Linked Open Data (Camera)
│   │   ├── async_senato_scraper.py     # Iter Legis via Linked Open Data (Senato)
│   │   ├── async_corte_cost_client.py  # Sindacato di legittimità della Corte Costituzionale
│   │   ├── async_eurlex_client.py      # Atti dell'Unione Europea (EUR-Lex)
│   │   └── neo4j_loader.py             # Caricamenti transazionali MERGE idempotenti
│   ├── parsing/                   # Pipeline deterministica XML Akoma Ntoso 3.0 / NIR 2.2
│   │   ├── parser.py              # Orchestratore AknParser con fallback di encoding
│   │   ├── namespaces.py          # Risoluzione dinamica dei dialetti XML (OASIS, CSD03, NIR)
│   │   ├── meta_parser.py         # Estrazione metadati FRBR, URN NIR, ELI e intervalli di vigenza
│   │   ├── body_parser.py         # Visita DFS, Context Injection e linearizzazione duale tabelle
│   │   ├── edge_extractor.py      # Graph discovery deterministico (:CITES, :MODIFIES)
│   │   ├── teseo_matcher.py       # Fast Ontology Matching con automa Aho-Corasick in tempo lineare
│   │   ├── vector_engine.py       # Embedding densi dei chunk e istruzione asimmetrica lato query
│   │   ├── transformers.py        # Mappatura dataset esterni in DTO e arricchimento in streaming
│   │   └── models.py              # DTO Pydantic V2 (GraphNodeDTO, FRBRMetadata, EdgeType)
│   ├── rag/                       # Motore di Retrieval Ibrido e Orchestrazione Agentica
│   │   ├── engine.py              # Macchina a stati finiti ciclica (StateGraph LangGraph)
│   │   ├── models.py              # Definizione tipizzata dello stato immutabile (RagState a 24 campi)
│   │   ├── query_analyzer.py      # Estrazione concetti TESEO e calcolo embedding asimmetrico
│   │   ├── retriever.py           # Fan-out asincrono su Neo4j (HNSW Vector, Lucene BM25, TESEO Graph)
│   │   ├── fusion.py              # Deduplicazione di canale, Weighted RRF (k=60), Abrogation Marking e Super-Chunking
│   │   ├── reranker.py            # Riordinamento candidati e protezione pavimento (PRIORITY_BASE_CHUNKS)
│   │   ├── resolver.py            # Compilatore neuro-simbolico regex -> URN NIR e lookup diretto
│   │   ├── expander.py            # CitationExpander multi-hop (:CITES, Hop=1) guidato da embedding
│   │   ├── topology_expander.py   # Espansione contestuale su commi contigui (:NEXT) e rubriche (:PART_OF)
│   │   ├── evaluator.py           # Retrieval Grader (T=0.0) e Query Rewriter (T=0.3) del loop agentico
│   │   ├── supervisor.py          # Agente ReAct LangGraph con i tool di retrieval
│   │   ├── think_filter.py        # Rimozione dei blocchi <think> dai completamenti del modello
│   │   ├── generator.py           # Sintesi grounded (Direct Stuffing 64k char, Map-Reduce fallback)
│   │   └── llm_factory.py         # Gestione istanze Ollama locali (Qwen3.5:4b, reasoning=False)
│   └── ui/
│       ├── app.py                 # Dashboard XAI a tre livelli con bridge nest_asyncio
│       ├── components.py          # Rendering di documenti recuperati, badge e tracce XAI
│       ├── rag_bridge.py          # Ponte asincrono/sincrono per lo streaming in Streamlit
│       └── style.css              # Foglio di stile della dashboard
├── scripts/                       # Suite di validazione, benchmark (Classi A, B, C) e diagnostica (diagnose_multihop.py)
├── data/                          # Cache locale pacchetti XML, ontologie RDF/Turtle (.ttl) e Golden Dataset
├── docker-compose.yml             # Orchestrazione container (Neo4j 5.26, Ollama CUDA, App)
└── manage.py                      # Entrypoint CLI unificato per ETL, retrieval e avvio UI
```