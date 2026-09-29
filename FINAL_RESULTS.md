# Risultati finali — GraphRAG vs RAG naive

Configurazione unica, congelata in `.env` (nessun override per set, nessun cherry-picking):

```
TESEO_DENSE_THRESHOLD=0.60      TESEO_MAX_CONCEPTS=3
CITATION_EXPANSION_LIMIT=10     CITATION_EXPANSION_PER_ACT=3     CITATION_HOP_SOURCE_TOP_K=5
PRIORITY_BASE_CHUNKS=6          RESOLVE_QUERY_CITATIONS=true     GRAPH_METADATA_IN_CONTEXT=false
RERANKER_BACKEND=llm            RRF_WEIGHT_GRAPH=1.2             RERANK_TOP_K=15   FINAL_K=10
```

Tag della versione valutata: **`graphrag-prod-v1`**.
Tabella completa (tutti i valori calcolati): `final_results_table.csv` — generata da
`py scripts/analyze_final_results.py`.
Diario tecnico completo di come ci si è arrivati: `WORK_LOG_GRAPH.md`.

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

## Limiti da dichiarare in tesi

1. **Archi di citazione a livello di atto**: è la ragione del tetto della classe C. Con rinvii
   articolo→articolo (corpus multivigente o parsing dei `<ref>` puntuali) il tetto sarebbe più alto.
2. **La classe B è un test di capacità**, non una media: misura una cosa che il recupero lessicale
   non può fare per costruzione. Va presentata come tale.
3. **Rumore di misura**: una singola generazione LLM oscilla di ±0,04 di similarità (misurato);
   le righe GraphRAG usano 2 generazioni per domanda. Le metriche di retrieval (recall/MRR/Hit@1)
   sono deterministiche.
4. **Metrica di similarità**: misura la copertura della risposta attesa, non la correttezza
   giuridica (che richiede un giudice umano o un LLM con rubric).
5. **Corpus**: 6.824 atti, 30.362 disposizioni, 59.130 archi di citazione. I numeri valgono per
   questo corpus.
6. **Classi A e C sono piccole** (15 e 20 domande): i distacchi sono ampi (0% vs 83%; 15% vs 30%),
   ma un benchmark più grande ridurrebbe l'intervallo di confidenza (`BENCHMARK_PLAN.md`).
7. **Vigenza/abrogazioni non valutabili**: sorgenti Normattiva *monovigente* (0 `vigenza_end`,
   0 archi `:MODIFIES`). Il meccanismo `_mark_abrogated_chunks` esiste ma non ha dati su cui agire.

## Riproduzione

```bash
# tabella finale (CSV + Markdown) dai risultati per-query
py scripts/analyze_final_results.py

# diagnosi del multi-hop (popolazione e cause dei fallimenti)
py scripts/diagnose_multihop.py

# verifica della metrica di similarità e di cosa usa la baseline a grafo spento
py scripts/check_similarity_metric.py
py scripts/check_baseline_graph_usage.py

# misura di una classe (esempio: rinvii nominati, baseline vs grafo)
RERANKER_BACKEND=llm py -u scripts/ablation_graph.py --dataset data/eval_reference.json \
    --mode graph --configs 1 3 --with-generation --repeats 2 \
    --output ablation_ref_results.csv --per-query-output ablation_ref_per_query.csv
```

## Cronologia delle correzioni che hanno prodotto questi numeri

1. **Espansione citazioni**: prima restituiva 10 articoli *arbitrari* dell'atto citato; ora ordina
   tutti gli articoli citati per similarità con la domanda, con budget per atto.
2. **Risolutore di rinvii** (`src/rag/resolver.py`): riconosce le forme ufficiali
   ("decreto legislativo 18 aprile 2016, n. 50") → URN → atto → articolo.
3. **Pavimento sui chunk base** (`PRIORITY_BASE_CHUNKS`): i migliori chunk di vettoriale+BM25 non
   possono essere spiazzati dal grafo (era la causa della perdita di recall: 86,7% → 100%).
4. **Chunk fusi tracciati** (`metadata['merged_ids']`) e **metric corretto** (id / fratelli /
   sovrapposizione token ≥90%): con il metric precedente i numeri erano sottostimati
   (multi-hop 10% invece di 30%).

Dettaglio completo, diagnosi e tentativi falliti: `WORK_LOG_GRAPH.md`.
