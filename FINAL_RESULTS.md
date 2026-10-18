# Risultati finali — GraphRAG vs RAG naive

Configurazione:

```
TESEO_DENSE_THRESHOLD=0.60      TESEO_MAX_CONCEPTS=3
CITATION_EXPANSION_LIMIT=10     CITATION_EXPANSION_PER_ACT=3     CITATION_HOP_SOURCE_TOP_K=5
PRIORITY_BASE_CHUNKS=6          RESOLVE_QUERY_CITATIONS=true     GRAPH_METADATA_IN_CONTEXT=false
RERANKER_BACKEND=llm            RRF_WEIGHT_GRAPH=1.2             RERANK_TOP_K=15   FINAL_K=10
MAX_CITATION_HOPS=1             GENERATOR_STUFF_THRESHOLD=64000  grader attivo
```

I pesi e le profondità di retrieval dentro GraphRAG sono quelli vincenti della griglia a due stadi
(`top_stage1_configs_v2.json`):

```
weight_vector=1.5  weight_bm25=1.0  weight_graph=1.2  rrf_k=40
rerank_top_k=15    rerank_min_score=0.0  final_k=10  rag_top_k=30  vector_min_score=0.3
```

Tag della versione valutata: **`graphrag-prod-v1`**.
Tabella completa (tutti i valori calcolati): `final_results_table.csv` — generata da
`py scripts/analyze_final_results.py`.

## Le tre classi di domande

| Classe | Baseline (denso+BM25) | **GraphRAG (config unica)** |
|---|---|---|
| **A. Testo sufficiente** (15 domande) | recall 100,00% · MRR 1,0000 · Hit@1 100% · sim 0,7658 | **recall 100,00% · MRR 1,0000 · Hit@1 100% · sim 0,7667** |
| **B. Rinvio nominato** (12) | recall 0,00% · MRR 0,0000 · Hit@1 0% · sim 0,2591 | **recall 83,33% · MRR 0,6111 · Hit@1 41,67% · sim 0,6611** |
| **C. Multi-hop** (20 di 41) | recall 15,00% · MRR 0,1000 · Hit@1 5% · sim 0,5115 | **recall 30,00% · MRR 0,2167 · Hit@1 15% · sim 0,5160** |

Valori per classe anche su: chunk nel contesto (A: 2,00 vs 2,67 — B: 1,83 vs 3,83 — C: 1,90 vs
3,60), caratteri di contesto (A: 11.494 vs 13.988 — B: 4.534 vs 7.766 — C: 38.051 vs 59.762),
salti di citazione (0 vs 1 in tutte le classi).

## Come sono costruite le domande

| Classe | Costruzione | Filtro automatico |
|---|---|---|
| A | domande sul contenuto di disposizioni autocontenute | la baseline **trova** la golden in top-10 |
| B | template meccanico dall'URN: *"Cosa prevede l'articolo N della legge X, n. Y?"* (nessun LLM) | la baseline **non** trova la golden |
| C | campione (articolo citante → atto citato → disposizione) via `:CITES`, domanda scritta da LLM | l'arco esiste; la baseline non trova la golden |

## Lettura dei risultati

- **A — il grafo non degrada**: recall e MRR identici alla baseline (100% / 1,0000), similarità
  praticamente pari (0,7667 vs 0,7658, differenza dentro il rumore di misura). Il grafo aggiunge
  contesto senza togliere nulla: è la proprietà resa possibile dal *pavimento sui chunk base*.
- **B — il grafo domina**: 0% → 83,33%. Il testo dell'articolo **non contiene** né il nome né il
  numero del proprio atto: il recupero lessicale è cieco per costruzione, mentre il grafo risolve
  il riferimento (forma ufficiale → URN → atto → articolo).
- **C — il grafo raddoppia** (15% → 30%) ma non vince, e il motivo è nel **corpus**: gli archi
  `:CITES` collegano **atti**, non articoli. Se la domanda non nomina la disposizione, nessun
  meccanismo può sapere quale articolo sia richiamato; se la nomina (o ne condivide il lessico),
  allora la trova anche il recupero lessicale e il grafo non serve. È una **tensione strutturale**,
  documentata in `scripts/diagnose_multihop.py`: delle 41 domande multi-hop, 22 nominano l'atto
  che contiene la risposta e in 20 di queste la disposizione non è identificabile per similarità
  (posizione 13-587 fra gli articoli dell'atto).

## La metrica di similarità regge?

Verifica in `scripts/check_similarity_metric.py`: risposta identica **1,0000**; risposte diverse
dello **stesso dominio 0,2183**; testo **fuori tema 0,1665**; frase di **fallback 0,2105**. Le
risposte corrette stanno a ~0,75-0,77: il divario è ~0,5, la metrica discrimina.

Il rumore di una singola generazione LLM è però **±0,04** → le misure finali usano **2 generazioni
per domanda**; le metriche di retrieval (recall/MRR/Hit@1) sono deterministiche.

**Onestà della misura.** La golden è contata presente se compare per **id**, oppure **dentro un
chunk fuso** (`metadata['merged_ids']`), oppure come **chunk fratello** (il corpus tiene articolo e
commi come Expression distinte), oppure con **sovrapposizione di token ≥ 90%** (lo stesso testo è
reso in modo leggermente diverso in campi diversi). Con il criterio precedente il multi-hop
risultava al 10%: il passaggio a **30%** è una correzione di misura, non di prestazione.

## Cosa usa davvero la baseline

`scripts/check_baseline_graph_usage.py`, con `enable_graph_search=False` e topologia spenta:
breadcrumb strutturale **6/6**, finestra di vigenza **6/6**, TESEO **0/6**, citazioni **0/6**,
topologia **0/6**. I *meccanismi* del grafo sono spenti, ma i chunk restano nodi del grafo con
gerarchia e vigenza: va dichiarato in tesi. Per questo esiste `PURE_TEXT_BASELINE`, che rimuove
anche breadcrumb, URN, vigenza e annotazioni — la baseline "puro testo" misura **0,7841** di
similarità sul set principale. Resta un residuo dichiarato: l'indice vettoriale è costruito
includendo il contesto gerarchico nel payload, quindi il canale vettoriale eredita la gerarchia del
grafo **anche nella baseline "puro testo"** (ricostruire l'indice senza gerarchia è un lavoro a sé).

## Tentativi che NON hanno funzionato

| Tentativo | Misura | Esito |
|---|---|---|
| Metadati del grafo nel prompt (istituti TESEO + "richiamata da") | 0,7683 → 0,7691 | **rumore**: `GRAPH_METADATA_IN_CONTEXT` resta `false` |
| Grader disattivato o `min_keep` 3/5/8 | recall identico in tutte le varianti | il grader non è la causa delle perdite; serve come compressore di contesto (13,5k vs 60k caratteri) |
| Due salti di citazione | similarità 0,582 | diluisce: `MAX_CITATION_HOPS=1` |
| Finestra reranker/finale enorme (40/20) | 0,7334 | nessun recupero |
| Peso del grafo 0,8 | 0,7408 | migliora ma non basta: la leva vera è la precisione TESEO |
| Rigenerare le domande multi-hop ancorate al contenuto della norma richiamata | 0/71 ammissibili | **tensione strutturale**: se la domanda assomiglia alla risposta la trova anche il RAG naive; se non le assomiglia, nessuno può trovarla perché gli archi citano l'atto, non l'articolo |
| Vigenza / abrogazioni | 0 `vigenza_end`, 0 archi `:MODIFIES` | non valutabile: le sorgenti Normattiva sono export **monovigente** |

## Correzioni effettuate (cronologia che ha prodotto questi numeri)

1. **Pipeline di tuning sbloccata**: le griglie si interrompevano e Stage 2 ignorava i parametri di
   Stage 1. Con la pipeline riparata, Stage 1 passa a recall **100%** (era 93,33%) e MRR **0,8611**
   (era 0,8179): `RAG_TOP_K=30` è la leva principale, `RAG_VECTOR_MIN_SCORE` è inerte.
2. **Deduplicazione per canale prima della fusione RRF**: il canale vettoriale restituiva ~400
   righe per 50 richieste, gonfiando il proprio peso e rendendo **inerti** i pesi RRF. Dopo la
   correzione i pesi tornano a contare (weight_graph 0,8 → 0,9052; 1,0 → 0,8877; 1,2 → 0,8769).
3. **Rinvii del grafo ricollegati**: 5.156 Work citati erano tutti segnaposto con URN fittizio.
   `scripts/relink_citations.py` ne ricollega **483** (21.415 archi, 36% dei 59.130) e l'espansione
   citazioni comincia a trovare qualcosa.
4. **Espansione citazioni riscritta**: prima restituiva 10 articoli *arbitrari* dell'atto citato
   (un codice ne ha 1.154). Ora scarica tutti gli articoli citati con il solo embedding, li ordina
   per similarità con la domanda, applica un **budget per atto** (`CITATION_EXPANSION_PER_ACT=3`) e
   fa partire l'hop dai **5 migliori chunk fusi** (`CITATION_HOP_SOURCE_TOP_K=5`), non da tutti.
5. **Risolutore di rinvii** (`src/rag/resolver.py`, nodo `resolve_citations`): le forme ufficiali
   numeriche ("decreto legislativo 18 aprile 2016, n. 50") mappano direttamente sull'URN dell'atto;
   l'articolo si prende dal numero che **precede** il riferimento e si seleziona per rilevanza.
   Gli articoli risolti sono **fissati**: né il reranker né il grader possono scartarli. Risolve
   **37/41** domande multi-hop (erano 12/41) e porta la classe B da **0% a 83,33%**.
6. **Pavimento sui chunk base** (`PRIORITY_BASE_CHUNKS=6`): i migliori chunk di vettoriale+BM25 non
   possono essere spiazzati dal cutoff RRF, dal reranker o dal grader. Il grafo diventa
   **additivo**: può aggiungere contesto, mai toglierlo (recall sul set principale 86,67% → 100%).
7. **Metrica di valutazione corretta**: id / chunk fuso / chunk fratello / token ≥ 90% (§ sopra).
   È una correzione di onestà, non di prestazione.
8. **Infrastruttura LLM**: thinking mode del provider disattivato e client resiliente (timeout,
   retry) → generazione **79 s → 2 s**, grading **31-54 s → 2,1 s**, nessuna risposta vuota.

## Riproduzione

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

# griglie (opzionale, ~5 ore)
py scripts/run_grid_search_v2.py
py scripts/analyze_grid_search_stage2.py
```
