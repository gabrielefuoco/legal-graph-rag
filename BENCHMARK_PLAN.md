# Piano del benchmark finale

Obiettivo: misurare le **tre classi di domande** con numeri difendibili (non 12-20 domande),
su un unico protocollo, con la **stessa** configurazione GraphRAG per tutte.

La tesi da verificare:
> GraphRAG copre le classi di domande in cui il recupero lessicale è strutturalmente cieco —
> l'identità della fonte e la rete dei rinvii — senza degradare quelle in cui il recupero funziona.

---

## 1. Le tre classi (tutte costruite meccanicamente dal grafo)

| Classe | Come si costruisce | Filtro di ammissibilità (meccanico) | Cosa misura |
|---|---|---|---|
| **A — testo sufficiente** | campione casuale di disposizioni autocontenute + template di domanda | la baseline **trova** la golden in top-10 | **non-regressione** del grafo |
| **B — rinvio nominato** | campione casuale di coppie (atto, articolo) dall'URN; domanda: "Cosa prevede l'articolo N della legge X, n. Y?" | la baseline **non trova** la golden in top-10 | risoluzione deterministica del riferimento |
| **C — multi-hop** | campione (articolo citante → atto citato → articolo della risposta) via `:CITES` | l'arco esiste e la baseline **non trova** la golden | traversata della rete dei rinvii |

Nessun cherry-picking: i campioni sono casuali e i filtri sono automatici. Le domande delle
classi A e B sono **template meccanici** (nessun LLM): si costruiscono dall'URN e dal numero di
articolo, quindi non c'è bias di generazione. Per la classe C la domanda va scritta (il testo del
citante non è un template) ma il criterio di ammissibilità è automatico.

## 2. Dimensione e tempi

- **40 domande per classe = 120 domande** → granularità 2,5 pp.
- 3 configurazioni a confronto × 120 domande × ~50 s ≈ **5 ore** → una notte, con cache
  (riavviabile: retrieval in `data/cache_*.pkl`, generazioni in `data/cache_*_generations.json`).
- Costruire i set: ~1 ora (nessun LLM per A e B; per C una chiamata LLM per domanda, in cache).

## 3. Configurazioni confrontate (tre, non dieci)

1. **RAG puro testo** (`PURE_TEXT_BASELINE=1`): nessun campo del grafo, nessun meccanismo.
2. **RAG denso+BM25** con metadati di corpus (breadcrumb, URN, vigenza): la baseline standard.
3. **GraphRAG** con la configurazione unica in `.env` (TESEO 0.60/3c, citazioni 3, pavimento 6,
   risolutore attivo).

## 4. Metriche

- **recall@10, MRR, Hit@1** — deterministiche, nessun rumore LLM.
- **similarità di risposta** (sentence-recall con embedding) con `--repeats 3`: il rumore misurato
  su una singola generazione è ±0,04, quindi su 120 domande × 3 ripetizioni scende sotto 0,01.

## 5. Risultati attesi (dai set pilota)

| Classe | Baseline denso+BM25 | GraphRAG |
|---|---|---|
| A — testo sufficiente | 100% | 100% (atteso: nessuna perdita) |
| B — rinvio nominato | 0% | ~100% |
| C — multi-hop | 5% | ~50% |

## 6. Cosa NON fare

- Niente cherry-picking per-query nei numeri principali. Se serve un pannello illustrativo
  ("dove il grafo sblocca la domanda"), va in appendice e **dichiarato come selezione**.
- Niente configurazioni multiple per classe: la stessa config deve reggere tutte e tre (è il
  punto della "configurazione unica").

## 7. Comandi (bozza)

```bash
# costruzione dei set
py scripts/build_reference_benchmark.py --samples 40 --output data/final_b.json
py scripts/build_multihop_benchmark.py --samples 60 --output data/final_c.json   # poi filtro
# (classe A: riuso del template del set principale, 40 domande)

# misura delle tre configurazioni su ciascun set
for set in data/final_a.json data/final_b.json data/final_c.json; do
  RERANKER_BACKEND=llm py -u scripts/ablation_graph.py --dataset $set \
      --mode puretext --configs 1 2 3 --with-generation --repeats 3 \
      --output "ablation_$(basename $set .json).csv" \
      --per-query-output "ablation_$(basename $set .json)_per_query.csv"
done
```
