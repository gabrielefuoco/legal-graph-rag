# Report Grid Search Stadio 2 — Tuning Contesto e Generazione su OpenRouter

## 🎯 Risultati Globali e Vincitore Assoluto

- **Configurazioni valutate**: `135`
- **Miglior Setup Globale**: Score Globale **`0.8560`** | Recall: **`93.33%`** | Similarita Semantica: **`0.7787`**

### 🏆 Scheda Configurazione Vincente

| Parametro | Valore Ottimale | Descrizione |
|---|---|---|
| `LLM_PROVIDER` | `openrouter` | Provider LLM ad alta capacita |
| `OPENROUTER_MODEL_NAME` | `qwen/qwen3.5-9b` | Modello cloud di riferimento |
| `GENERATOR_NUM_CTX` | `8192` | Finestra di contesto generatore |
| `GENERATOR_STUFF_THRESHOLD` | `32000` | Soglia max caratteri per stuffing diretto |
| `TOPOLOGICAL_MAX_CHARS` | `6000` | Limite espansione topologica per super-chunk |
| `RRF_WEIGHT_VECTOR` | `0.8` | Peso RRF Vettoriale |
| `RRF_WEIGHT_BM25` | `0.8` | Peso RRF BM25 |
| `RRF_WEIGHT_GRAPH` | `0.8` | Peso RRF Grafo TESEO |
| `RRF_K` | `60` | Costante K di Rank Fusion |
| `RERANK_TOP_K` | `15` | Cutoff pre-reranking |
| `RERANK_MIN_SCORE` | `0.0` | Soglia minima Reranker |
| `FINAL_K` | `10` | Numero finale di chunk alimentati al generatore |

## 📊 Top 10 Configurazioni di Generazione

| Rank | Num Ctx | Stuff Thresh | Topo Chars | S1 Rank | Recall | Sim Score | Global Score |
|---|---|---|---|---|---|---|---|
| #1 | `8192` | `32000` | `6000` | #2 | 93.33% | 0.7787 | **0.8560** |
| #2 | `32768` | `64000` | `20000` | #5 | 93.33% | 0.7781 | **0.8557** |
| #3 | `8192` | `32000` | `6000` | #4 | 93.33% | 0.7774 | **0.8554** |
| #4 | `16384` | `64000` | `20000` | #4 | 93.33% | 0.7770 | **0.8552** |
| #5 | `32768` | `64000` | `20000` | #4 | 93.33% | 0.7767 | **0.8550** |
| #6 | `32768` | `64000` | `12000` | #2 | 93.33% | 0.7766 | **0.8550** |
| #7 | `32768` | `64000` | `12000` | #1 | 93.33% | 0.7761 | **0.8547** |
| #8 | `16384` | `64000` | `20000` | #5 | 93.33% | 0.7758 | **0.8546** |
| #9 | `8192` | `32000` | `6000` | #3 | 93.33% | 0.7757 | **0.8545** |
| #10 | `16384` | `32000` | `6000` | #3 | 93.33% | 0.7757 | **0.8545** |
