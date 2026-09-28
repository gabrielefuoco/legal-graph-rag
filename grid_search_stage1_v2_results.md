# Report Grid Search Stadio 1 — Ottimizzazione Retrieval e Ranking

## 🎯 Executive Summary

- **Configurazioni valutate**: `432`
- **Baseline Ranking Vettoriale Puro**: Recall `80.00%` | MRR `0.7056`
- **Miglior Configurazione Trovata**: Recall **`100.00%`** (Δ `+20.00%`) | MRR **`0.8611`** | Score **`0.9383`**

### 🏆 Parametri Ottimali Stadio 1

| Parametro | Valore Ottimale | Descrizione |
|---|---|---|
| `RRF_WEIGHT_VECTOR` | `1.5` | Peso canale denso vettoriale |
| `RRF_WEIGHT_BM25` | `1.0` | Peso canale lessicale BM25 |
| `RRF_WEIGHT_GRAPH` | `1.2` | Peso navigazione concetti TESEO |
| `RRF_K` | `40` | Costante Reciprocal Rank Fusion |
| `RERANK_TOP_K` | `15` | Chunk inviati al CrossEncoder |
| `RERANK_MIN_SCORE` | `0.0` | Soglia minima post-sigmoide |
| `FINAL_K` | `10` | Chunk finali selezionati |
| `RAG_TOP_K` | `30` | Candidati per canale pre-fusione |
| `RAG_VECTOR_MIN_SCORE` | `0.3` | Soglia minima similarità vettoriale |

## 📊 Top 10 Configurazioni a Confronto

| Rank | V/B/G Weights | RRF K | Top-K | Vec.Min | Rerank Min | Final K | Recall | MRR | Hit@1 | Score | Δ vs Vector |
|---|---|---|---|---|---|---|---|---|---|---|---|
| #1 | `1.5/1.0/1.2` | `40` | `30` | `0.3` | `0.0` | `10` | **100.00%** | 0.8611 | 80.00% | **0.9383** | `+20.00%` |
| #2 | `1.5/1.0/1.2` | `40` | `30` | `0.2` | `0.0` | `10` | **100.00%** | 0.8611 | 80.00% | **0.9383** | `+20.00%` |
| #3 | `1.5/1.0/1.2` | `40` | `30` | `0.1` | `0.0` | `10` | **100.00%** | 0.8611 | 80.00% | **0.9383** | `+20.00%` |
| #4 | `1.5/1.0/1.2` | `60` | `30` | `0.3` | `0.0` | `10` | **100.00%** | 0.8611 | 80.00% | **0.9383** | `+20.00%` |
| #5 | `1.5/1.0/1.2` | `60` | `30` | `0.2` | `0.0` | `10` | **100.00%** | 0.8611 | 80.00% | **0.9383** | `+20.00%` |
| #6 | `1.5/1.0/1.2` | `60` | `30` | `0.1` | `0.0` | `10` | **100.00%** | 0.8611 | 80.00% | **0.9383** | `+20.00%` |
| #7 | `1.5/1.0/1.2` | `40` | `50` | `0.3` | `0.0` | `10` | **100.00%** | 0.8500 | 80.00% | **0.9350** | `+20.00%` |
| #8 | `1.5/1.0/1.2` | `40` | `50` | `0.2` | `0.0` | `10` | **100.00%** | 0.8500 | 80.00% | **0.9350** | `+20.00%` |
| #9 | `1.5/1.0/1.2` | `40` | `50` | `0.1` | `0.0` | `10` | **100.00%** | 0.8500 | 80.00% | **0.9350** | `+20.00%` |
| #10 | `1.5/1.0/1.2` | `60` | `50` | `0.3` | `0.0` | `10` | **100.00%** | 0.8500 | 80.00% | **0.9350** | `+20.00%` |
