# Report Grid Search Stadio 1 — Ottimizzazione Retrieval e Ranking

## 🎯 Executive Summary

- **Configurazioni valutate**: `1152`
- **Baseline Ranking Vettoriale Puro**: Recall `80.00%` | MRR `0.7000`
- **Miglior Configurazione Trovata**: Recall **`93.33%`** (Δ `+13.33%`) | MRR **`0.8179`** | Score **`0.8854`**

### 🏆 Parametri Ottimali Stadio 1

| Parametro | Valore Ottimale | Descrizione |
|---|---|---|
| `RRF_WEIGHT_VECTOR` | `0.8` | Peso canale denso vettoriale |
| `RRF_WEIGHT_BM25` | `0.8` | Peso canale lessicale BM25 |
| `RRF_WEIGHT_GRAPH` | `0.8` | Peso navigazione concetti TESEO |
| `RRF_K` | `40` | Costante Reciprocal Rank Fusion |
| `RERANK_TOP_K` | `15` | Chunk inviati al CrossEncoder |
| `RERANK_MIN_SCORE` | `0.0` | Soglia minima post-sigmoide |
| `FINAL_K` | `10` | Chunk finali selezionati |

## 📊 Top 10 Configurazioni a Confronto

| Rank | V/B/G Weights | RRF K | Rerank Min | Final K | Recall | MRR | Hit@1 | Score | Δ vs Vector |
|---|---|---|---|---|---|---|---|---|---|
| #1 | `0.8/0.8/0.8` | `40` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
| #2 | `0.8/0.8/0.8` | `60` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
| #3 | `0.8/1.0/0.8` | `40` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
| #4 | `0.8/1.0/0.8` | `60` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
| #5 | `0.8/1.2/0.8` | `40` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
| #6 | `0.8/1.2/0.8` | `60` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
| #7 | `1.0/0.8/0.8` | `40` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
| #8 | `1.0/0.8/1.2` | `60` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
| #9 | `1.0/1.0/0.8` | `40` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
| #10 | `1.0/1.0/1.2` | `60` | `0.0` | `10` | **93.33%** | 0.8179 | 80.00% | **0.8854** | `+13.33%` |
