# Come è stato ottenuto il risultato — diario tecnico completo

Questo documento ricostruisce, nella versione **finale**, tutto ciò che è stato fatto per arrivare
ai numeri di `FINAL_RESULTS.md`. Ogni voce riporta: sintomo osservato → diagnosi (con i dati) →
correzione (file) → effetto misurato. Le voci in cui non c'è un effetto sono comunque elencate:
sono i tentativi che **non** hanno funzionato, e servono a non ripeterli.

Riferimenti: tag `graphrag-prod-v1`, branch `feat/retrieval-tuning-v2`.

---

## 1. Il risultato in una tabella

| Classe di domande | Baseline (denso+BM25) | GraphRAG (config unica) |
|---|---|---|
| **A. Testo sufficiente** (15) | 100,00% · MRR 1,0000 · sim 0,7658 | 100,00% · MRR 1,0000 · sim 0,7667 |
| **B. Rinvio nominato** (12) | 0,00% · MRR 0,0000 · sim 0,2591 | **83,33% · MRR 0,6111 · sim 0,6611** |
| **C. Multi-hop** (20 di 41) | 15,00% · MRR 0,1000 · sim 0,5115 | **30,00% · MRR 0,2167 · sim 0,5160** |

Tesi sostenuta: *GraphRAG copre le classi di domande in cui il recupero lessicale è
strutturalmente cieco — l'identità della fonte e la rete dei rinvii — senza degradare quelle in
cui il recupero funziona.*

---

## 2. Configurazione finale effettiva

**In `.env`** (nuova sezione "GraphRAG production configuration"):

```
TESEO_DENSE_THRESHOLD=0.60        # era 0.45
TESEO_MAX_CONCEPTS=3              # era 10
CITATION_EXPANSION_LIMIT=10       # era 10 fisso nel codice
CITATION_EXPANSION_PER_ACT=3      # nuovo
CITATION_HOP_SOURCE_TOP_K=5       # nuovo
PRIORITY_BASE_CHUNKS=6            # nuovo
RESOLVE_QUERY_CITATIONS=true      # nuovo
GRAPH_METADATA_IN_CONTEXT=false   # nuovo (misurato neutro/negativo)
GENERATOR_STUFF_THRESHOLD=64000
```

**Da `top_stage1_configs_v2.json`** (parametri di retrieval vincenti della griglia Stage 1):

```
weight_vector=1.5  weight_bm25=1.0  weight_graph=1.2  rrf_k=40
rerank_top_k=15    rerank_min_score=0.0  final_k=10  rag_top_k=30  vector_min_score=0.3
```

**Altro**: `RERANKER_BACKEND=llm` (OpenRouter), `MAX_CITATION_HOPS=1`, grader attivo.

---

## 3. Punto di partenza (cosa non andava)

1. **La griglia di ricerca non girava**: errori bloccanti che la interrompevano (import mancante,
   generazione doppia, parametri Stage-1 ignorati in Stage 2, chiave di cache incompleta).
2. **I pesi RRF erano inerti**: il canale vettoriale restituiva ~400 righe per 50 nodi, gonfiando
   il proprio peso nella fusione → cambiare `RRF_WEIGHT_GRAPH` non cambiava nulla.
3. **La generazione era lentissima**: 79 s per risposta e 31-54 s per il grading, con casi da
   235 s e risposta vuota (thinking mode del provider attivo).
4. **I rinvii erano stub**: 5.156 Work citati su 5.156 erano segnaposto ("Riferimento Esterno")
   → l'espansione citazioni non trovava nulla.
5. **L'espansione citazioni era strutturalmente sbagliata**: restituiva 10 articoli *arbitrari*
   dell'atto citato e partiva da tutti i chunk fusi (12-43 atti citati in gioco).
6. **Il grafo spiazzava la baseline**: i chunk iniettati (TESEO/citazioni) entravano nella stessa
   classifica e il reranker li metteva sopra la golden → recall 100% → 86,67%.
7. **La metrica di recall sottostimava**: una norma dentro un chunk fuso (`merged_*`) o restituita
   come chunk fratello (il corpus tiene articolo e commi come nodi distinti) era contata come
   "persa".

---

## 4. Correzioni, una per una

### 4.1 Pipeline di ricerca sbloccata
- **Sintomo**: le griglie si fermavano a metà; Stage 2 ignorava i parametri di Stage 1.
- **Correzione**: import `numpy` mancante, `settings.is_openrouter` → controllo su `LLM_PROVIDER`,
  `skip_generation=True` per evitare la doppia generazione, override *scoped* delle impostazioni
  Stage-1 dentro Stage 2, chiave di cache che include modalità stuff/map-reduce.
- **Effetto**: griglie completabili ed eseguibili in modo riproducibile.

### 4.2 Deduplicazione per canale prima della fusione (`src/rag/fusion.py`)
- **Sintomo**: i pesi RRF non avevano effetto (weight_graph 0.8 / 1.0 / 1.2 davano lo stesso
  punteggio).
- **Diagnosi**: il canale vettoriale restituiva ~8× le righe richieste (400 righe per 50 nodi),
  quindi il suo contributo nella fusione era gonfiato e dominava tutto.
- **Correzione**: `_deduplicate_channel()` — dedup per canale prima del RRF.
- **Effetto misurato**: i pesi tornano a contare (weight_graph 0.8 → 0.9052; 1.0 → 0.8877;
  1.2 → 0.8769, prima identici).

### 4.3 Prestazioni LLM (tempi e stabilità)
- **Sintomo**: 79 s per risposta, 31-54 s per il grading, un caso da 235 s con risposta vuota,
  stalli di rete.
- **Correzione**: `extra_body={"reasoning": {"enabled": False}}` per il provider, client resiliente
  (`ResilientChatOpenAI`, timeout 45 s, 3 tentativi), timeout HTTP 120 s, timeout interno del
  grader (180 s) con fallback "tieni tutti", `GENERATOR_STUFF_THRESHOLD=64000` per evitare il
  map-reduce.
- **Effetto**: generazione 79 s → **2,0 s**, grading 31-54 s → **2,1 s**, 43 stalli recuperati
  nella run della griglia, nessuna risposta vuota. Il map-reduce costa ~4 pp di similarità: a
  soglia 8000 lo colpiva il 63% delle query.

### 4.4 Riparazione dei rinvii nel grafo (`scripts/relink_citations.py`)
- **Sintomo**: l'espansione citazioni non trovava mai nulla ("nessuna citazione interna trovata").
- **Diagnosi**: tutti i Work citati erano segnaposto con URN fittizio (`...legge:1988;400`) mentre
  gli atti reali hanno URN datati (`...legge:2024-03-04;40`). 483 Work erano ricollegabili
  (21.415 archi = 36% dei 59.130).
- **Correzione**: script di relink (`--apply`), con marcatura pulita.
- **Effetto**: "Work citati con articoli percorribili: 0 → 483"; l'espansione citazioni inizia a
  funzionare ("Multi-hop 1: trovati 10 chunk citati").

### 4.5 Istruzione di embedding lato query (`src/config.py`, `src/parsing/vector_engine.py`)
- **Sintomo**: il modello di embedding (Qwen3) usa istruzioni diverse per documenti e query.
- **Correzione**: `EMBEDDING_QUERY_INSTRUCTION` + `compute_query_embedding()`, usata da
  `teseo_matcher` e `query_analyzer`. **Nessun re-indicizzazione** (l'indice resta valido).
- **Effetto**: allineamento domanda/documento senza ricostruire l'indice vettoriale.

### 4.6 Griglia Stage 1 (432 configurazioni) e Stage 2
- **Sintomo**: i parametri di retrieval non erano mai stati ottimizzati con profondità adeguata.
- **Correzione**: griglia estesa con `RAG_TOP_K` (15/30/50) e `RAG_VECTOR_MIN_SCORE` (0,3/0,2/0,1),
  pool di candidati come sovrainsieme, Stage 2 che rispetta i nuovi parametri; reranker LLM
  (`LLMReranker`, listwise) con factory `build_reranker()`.
- **Effetto**: Stage 1 recall **100%** (era 93,33%), MRR 0,8611 (era 0,8179); `RAG_TOP_K=30` è la
  leva principale; `RAG_VECTOR_MIN_SCORE` inerte.

### 4.7 Precisione TESEO (`src/config.py`, `.env`)
- **Sintomo**: con la configurazione di produzione il grafo *peggiorava* la risposta
  (similarità 0,7284 vs 0,7584 della baseline) e perdeva recall (86,67%).
- **Diagnosi**: 10 concetti TESEO a soglia 0,45 iniettavano chunk generici ("imprese", "merci")
  che il reranker promuoveva sopra la golden.
- **Correzione**: `TESEO_DENSE_THRESHOLD=0.60`, `TESEO_MAX_CONCEPTS=3`.
- **Effetto**: recall 86,67% → 93,33%, MRR 0,8667 → 0,9333, similarità 0,7284 → **0,7680**.

### 4.8 Espansione citazioni riscritta (`src/rag/expander.py`)
- **Sintomo**: l'hop scattava su tutte le domande ma la golden non entrava mai nel contesto
  (recall 0,00% sul set multi-hop).
- **Diagnosi** (misurata): l'hop restituiva 10 articoli **arbitrari** dell'atto citato — un codice
  ne ha 1.154 — mentre la golden era spesso **la più simile alla domanda dentro il suo atto**
  (rank 1 in 2 casi su 3). Inoltre partiva da 23 chunk, trascinando 12-43 atti citati.
- **Correzione**:
  1. si scaricano **tutti** gli articoli citati con il solo embedding (0,06-0,95 s; tutti i
     30.362 nodi hanno embedding), si ordinano per similarità con la domanda e si recupera il
     testo dei soli selezionati;
  2. **budget per atto** (`CITATION_EXPANSION_PER_ACT=3`) invece di un top-N globale;
  3. l'hop parte dai **migliori chunk fusi** (`CITATION_HOP_SOURCE_TOP_K=5`) e dai chunk risolti,
     non da tutti i chunk protetti.
- **Effetto**: la golden entra nel set iniettato quando l'atto è quello giusto; il multi-hop passa
  dal 5% al 30% con la config finale.

### 4.9 Risolutore di rinvii (`src/rag/resolver.py`, nodo nuovo `resolve_citations`)
- **Sintomo**: le domande che **nominano** una disposizione ("art. 18 della legge 145/2018")
  non erano risolvibili da nessun canale: il testo dell'articolo non contiene il nome né il numero
  del proprio atto.
- **Diagnosi**: le domande usano forme ufficiali numeriche ("decreto legislativo 18 aprile 2016,
  n. 50") che mappano **direttamente** sull'URN del grafo (`urn:nir:stato:decreto.legislativo:
  2016-04-18;50`); il numero di articolo citato, però, spesso appartiene all'atto *citante*,
  quindi da solo non basta.
- **Correzione**:
  1. parsing delle forme numeriche (giorno/mese/anno/numero, "legge N del YYYY", "d.lgs. N/YYYY")
     → costruzione dell'URN → match esatto dell'atto;
  2. abbinamento articolo↔atto: si prende il numero che **precede** il riferimento (come si cita
     in italiano);
  3. selezione degli articoli dell'atto **per rilevanza con la domanda** (embedding), non solo per
     numero;
  4. gli articoli risolti sono **fissati**: non possono essere tagliati dal reranker né scartati
     dal grader.
- **Effetto**: risolve qualcosa in **37/41** domande multi-hop (era 12/41); sulla classe "rinvii
  nominati" porta la recall da **0% a 83,33%**.
- **Nota tecnica**: un update `{}` di un nodo LangGraph diventa `None` e rompe il merge dello
  stato — il nodo restituisce sempre un dizionario non vuoto.

### 4.10 Il grafo non deve spiazzare la baseline (`src/rag/fusion.py`, `reranker.py`, `evaluator.py`)
- **Sintomo**: sul set principale il grafo perdeva 2 domande su 15 (recall 86,67% vs 100%).
- **Diagnosi**: la golden era **rank 1 nel canale vettoriale** (o BM25) ma usciva dal contesto
  finale; la fusione riscrive il campo `source` (`citation_resolve+vector`), quindi i controlli
  di sicurezza basati su uguaglianza non scattavano.
- **Correzione**:
  1. **pavimento sui chunk base** (`PRIORITY_BASE_CHUNKS=6`): i migliori chunk di vettoriale+BM25
     (per classifica dei canali, non della fusione) non possono essere rimossi dal cutoff RRF,
     dal reranker o dal grader;
  2. **chunk fusi tracciati** (`metadata['merged_ids']`): una norma dentro un chunk `merged_*`
     resta riconoscibile;
  3. controlli basati su **sottostringa** del campo `source`, non su uguaglianza.
- **Effetto**: recall sul set principale **86,67% → 100,00%**, MRR 1,0000, similarità 0,7667
  (baseline 0,7658). Il grafo diventa **additivo**: può aggiungere contesto, mai toglierlo.

### 4.11 Metrica di valutazione corretta (`scripts/ablation_graph.py`)
- **Sintomo**: numeri incoerenti tra run e sottostime sistematiche.
- **Diagnosi**: (a) una norma dentro un chunk fuso era contata come persa; (b) il corpus tiene
  articolo e commi come Expression distinte, quindi la norma può tornare come **chunk fratello**;
  (c) lo stesso testo è reso in modo leggermente diverso in campi diversi ("...senatori **e**
  venticinque..." vs "...senatori **e da** venticinque...").
- **Correzione**: la presenza della golden è verificata con **id** *oppure* **membro di chunk
  fuso** *oppure* **chunk fratello** (stessa unità strutturale) *oppure* **sovrapposizione di
  token ≥ 90%**.
- **Effetto**: il multi-hop passa da 10% a **30%** (non perché sia cambiato il sistema, ma perché
  prima veniva misurato male). È una correzione di **onestà della misura**, non di prestazione.

### 4.12 Verifica della metrica (`scripts/check_similarity_metric.py`)
- **Dubbio**: la similarità è troppo generosa?
- **Misura**: risposta identica 1,0000; **risposte diverse dello stesso dominio 0,2183**; testo
  fuori tema 0,1665; frase di fallback 0,2105. Le risposte corrette stanno a ~0,75-0,77.
- **Conclusione**: la metrica discrimina (divario ~0,5). Il rumore di una singola generazione LLM
  è però **±0,04** → le misure finali usano **2 generazioni per domanda** e le metriche di
  retrieval (recall/MRR/Hit@1) sono deterministiche.

### 4.13 Trasparenza su cosa usa la baseline (`scripts/check_baseline_graph_usage.py`)
- **Dubbio**: la "baseline" sfrutta il grafo?
- **Misura** (con `enable_graph_search=False`, multi-hop e topologia off): breadcrumb strutturale
  **6/6**, finestra di vigenza **6/6**, TESEO 0/6, citazioni 0/6, topologia 0/6.
- **Conclusione**: i *meccanismi* del grafo sono spenti, ma i chunk restano nodi del grafo con
  gerarchia e vigenza. Va dichiarato in tesi; per questo esiste `PURE_TEXT_BASELINE`, che rimuove
  anche breadcrumb, URN, vigenza e annotazioni (baseline "puro testo": similarità 0,7841 sul set
  principale).

---

## 5. Tentativi che NON hanno funzionato (documentati)

| Tentativo | Misura | Esito |
|---|---|---|
| Metadati del grafo nel prompt (istituti TESEO + "richiamata da") | 0,7683 → 0,7691 | **rumore**: `GRAPH_METADATA_IN_CONTEXT` resta `false` |
| Grader disattivato o `min_keep` 3/5/8 | recall identico in tutte le varianti | il grader non è la causa delle perdite; serve come compressore di contesto (13,5k vs 60k caratteri) |
| Due salti di citazione | similarità 0,582 | diluisce: `MAX_CITATION_HOPS=1` |
| Finestra reranker/finale enorme (40/20) | 0,7334 | nessun recupero |
| Peso del grafo 0,8 | 0,7408 | migliora ma non basta: la leva vera è la precisione TESEO |
| Rigenerare le domande multi-hop ancorate al contenuto della norma richiamata | 0/71 ammissibili | **tensione strutturale**: se la domanda assomiglia alla risposta la trova anche il RAG naive; se non le assomiglia, nessuno può trovarla perché gli archi citano l'atto, non l'articolo |
| Vigenza / abrogazioni | 0 `vigenza_end`, 0 archi `:MODIFIES` | non valutabile: le sorgenti Normattiva sono export **monovigente** |

---

## 6. Strumenti aggiunti

| Script | Cosa fa |
|---|---|
| `scripts/relink_citations.py` | ricollega i Work citati segnaposto agli atti reali (483 Work, 21.415 archi) |
| `scripts/ablation_graph.py` | ablation dei meccanismi (`--mode graph/grader/teseo/context/final/citation/metadata/puretext`), cache retrieval, `--repeats`, `--configs` |
| `scripts/build_multihop_benchmark.py` | costruisce il set multi-hop (campionamento + generazione + filtro automatico) |
| `scripts/build_reference_benchmark.py` | costruisce il set "rinvii nominati" (template meccanici dall'URN, nessun LLM) |
| `scripts/merge_multihop_sets.py` | unisce i set multi-hop |
| `scripts/eval_resolver.py` | valuta **solo** il risolutore di rinvii (nessun LLM) |
| `scripts/diagnose_multihop.py` | definisce la popolazione "rinvio risolvibile" e classifica i fallimenti |
| `scripts/check_similarity_metric.py` | verifica che la metrica di similarità discrimini |
| `scripts/check_baseline_graph_usage.py` | mostra cosa usa la baseline a grafo spento |
| `scripts/analyze_final_results.py` | genera `final_results_table.csv` + `FINAL_RESULTS.md` |
| `scripts/analyze_multihop_ablation.py` | analisi per-query dell'ablation multi-hop |
| `scripts/run_grid_search_v2.py` | runner delle griglie Stage 1 + Stage 2 |
| `scripts/analyze_grid_search_stage2.py` | analisi dei risultati Stage 2 |

---

## 7. I set di valutazione

| Set | N | Costruzione | Filtro automatico |
|---|---|---|---|
| `data/eval_dataset.json` | 15 | domande sul contenuto di disposizioni autocontenute | la baseline **trova** la golden in top-10 |
| `data/eval_reference.json` | 12 | template meccanici dall'URN: *"Cosa prevede l'articolo N della legge X, n. Y?"* — **nessun LLM** | la baseline **non** trova la golden |
| `data/eval_multihop_all.json` | 41 (20 misurate) | campione (articolo citante → atto citato → disposizione) via `:CITES`, domanda scritta da LLM | l'arco esiste; la baseline non trova la golden |

---

## 8. Cronologia dei commit (solo quelli che compongono lo stato finale)

```
ed0d848  data(eval): tabella finale con baseline omogenee (2 generazioni per domanda)
1083c21  chore(eval): tabella finale, diagnosi multi-hop, pulizia delle run superate
d9bae12  docs(benchmark): piano del benchmark finale + configurazione di produzione verificata
7650f57  docs(handoff): risultati finali e le tre affermazioni misurabili
8385036  fix(graph): contesto del grafo additivo + metrica di recall corretta
5515455  feat(graph): risoluzione dei rinvii nominati; il grafo non spiazza più la baseline
62fba38  docs(handoff): sessione pomeridiana sul benchmark multi-hop
5e49389  feat(graph): risoluzione del rinvio nominato nella domanda
38e9aaf  fix(graph): l'espansione citazioni raggiunge la disposizione richiamata; metriche oneste
a745520  docs: handoff del tuning del grafo
859ec63  data(eval): set multi-hop (41 domande) e tabelle di risultato
bb3f092  feat(graph-tuning): budget citazioni + metadati del grafo, builder del set multi-hop
db50b26  docs(grid-search): stime di durata della v2
f5a3a85  feat(grid-search): esperimento v2 (profondità di retrieval, istruzione query, reranker LLM)
2a6a9e1  chore(config): soglia di stuffing a 64000 per i provider cloud
fea8e76  fix(retrieval): deduplicazione per canale prima della fusione RRF
3c7769e  docs(grid-search): risultati Stage 1/2, configurazione vincente
4c60bd2  fix(grid-search): pipeline di tuning Stage 1+2 sbloccata e completata
```

---

## 9. Limiti da dichiarare

1. **Archi di citazione a livello di atto** (`:CITES` collega atti, non articoli): è la ragione del
   tetto della classe C. Diagnosi in `scripts/diagnose_multihop.py`: delle 41 domande multi-hop,
   22 nominano l'atto che contiene la risposta e in 20 di esse la disposizione sta al posto
   **13-587** fra gli articoli dell'atto per similarità.
2. **La classe B è un test di capacità**, non una media: misura una cosa che il recupero lessicale
   non può fare per costruzione. Va presentata come tale.
3. **Rumore di misura**: ±0,04 di similarità su una singola generazione LLM; le righe GraphRAG
   usano 2 generazioni per domanda. Le metriche di retrieval sono deterministiche.
4. **Metrica di similarità**: misura la copertura della risposta attesa, non la correttezza
   giuridica (serve un giudice umano o un LLM con rubric).
5. **Dimensioni**: classi A e C hanno 15 e 20 domande → distacchi ampi ma intervalli di confidenza
   larghi. Il piano per allargarle è in `BENCHMARK_PLAN.md`.
6. **Corpus**: 6.824 atti, 30.362 disposizioni, 59.130 archi di citazione. I numeri valgono per
   questo corpus.
7. **Embedding dei documenti**: l'indice vettoriale è costruito includendo il contesto gerarchico
   nel payload, quindi il canale vettoriale eredita la gerarchia del grafo anche nella baseline
   "puro testo" (dichiarato; ricostruire l'indice senza gerarchia è un lavoro a sé).

---

## 10. Riproduzione

```bash
# infrastruttura
docker compose up -d --pull never neo4j ollama

# tabella finale e diagnosi
py scripts/analyze_final_results.py
py scripts/diagnose_multihop.py

# le tre classi (baseline vs grafo)
RERANKER_BACKEND=llm py -u scripts/ablation_graph.py --dataset data/eval_dataset.json \
    --mode graph --configs 1 3 --with-generation --repeats 2 \
    --output ablation_main_results.csv --per-query-output ablation_main_per_query.csv
RERANKER_BACKEND=llm py -u scripts/ablation_graph.py --dataset data/eval_reference.json \
    --mode graph --configs 1 3 --with-generation --repeats 2 \
    --output ablation_ref_results.csv --per-query-output ablation_ref_per_query.csv
RERANKER_BACKEND=llm py -u scripts/ablation_graph.py --dataset data/eval_multihop_all.json \
    --queries $(seq 1 20) --mode graph --configs 1 3 --with-generation --repeats 2 \
    --output ablation_mh_results.csv --per-query-output ablation_mh_per_query.csv

# verifica della metrica e di cosa usa la baseline
py scripts/check_similarity_metric.py
py scripts/check_baseline_graph_usage.py
```
