# Report Grid Search Stadio 2 — Tuning Contesto e Generazione su OpenRouter

## 🎯 Risultati Globali e Vincitore Assoluto

- **Configurazioni valutate**: `81`
- **Miglior Setup Globale**: Score Globale **`0.8546`** | Recall: **`93.33%`** | Similarita Semantica: **`0.7758`**

### 🏆 Scheda Configurazione Vincente

| Parametro | Valore Ottimale | Descrizione |
|---|---|---|
| `LLM_PROVIDER` | `openrouter` | Provider LLM ad alta capacita |
| `OPENROUTER_MODEL_NAME` | `qwen/qwen3.5-9b` | Modello cloud di riferimento |
| `GENERATOR_NUM_CTX` | `8192` | Finestra di contesto generatore |
| `GENERATOR_STUFF_THRESHOLD` | `16000` | Soglia max caratteri per stuffing diretto |
| `TOPOLOGICAL_MAX_CHARS` | `12000` | Limite espansione topologica per super-chunk |
| `RRF_WEIGHT_VECTOR` | `1.5` | Peso RRF Vettoriale |
| `RRF_WEIGHT_BM25` | `1.0` | Peso RRF BM25 |
| `RRF_WEIGHT_GRAPH` | `1.2` | Peso RRF Grafo TESEO |
| `RRF_K` | `40` | Costante K di Rank Fusion |
| `RERANK_TOP_K` | `15` | Cutoff pre-reranking |
| `RERANK_MIN_SCORE` | `0.0` | Soglia minima Reranker |
| `FINAL_K` | `10` | Numero finale di chunk alimentati al generatore |
| `RAG_TOP_K` | `30` | Candidati per canale pre-fusione |
| `RAG_VECTOR_MIN_SCORE` | `0.3` | Soglia minima similarità vettoriale |

## 📊 Top 10 Configurazioni di Generazione

| Rank | Num Ctx | Stuff Thresh | Topo Chars | S1 Rank | Recall | Sim Score | Global Score |
|---|---|---|---|---|---|---|---|
| #1 | `8192` | `16000` | `12000` | #1 | 93.33% | 0.7758 | **0.8546** |
| #2 | `8192` | `16000` | `12000` | #3 | 93.33% | 0.7758 | **0.8546** |
| #3 | `8192` | `16000` | `12000` | #2 | 93.33% | 0.7743 | **0.8538** |
| #4 | `32768` | `32000` | `20000` | #2 | 93.33% | 0.7743 | **0.8538** |
| #5 | `8192` | `32000` | `12000` | #2 | 93.33% | 0.7717 | **0.8525** |
| #6 | `32768` | `16000` | `12000` | #1 | 93.33% | 0.7710 | **0.8522** |
| #7 | `32768` | `16000` | `12000` | #3 | 93.33% | 0.7710 | **0.8522** |
| #8 | `8192` | `32000` | `12000` | #1 | 93.33% | 0.7708 | **0.8521** |
| #9 | `8192` | `32000` | `12000` | #3 | 93.33% | 0.7708 | **0.8521** |
| #10 | `32768` | `64000` | `20000` | #2 | 93.33% | 0.7704 | **0.8519** |
