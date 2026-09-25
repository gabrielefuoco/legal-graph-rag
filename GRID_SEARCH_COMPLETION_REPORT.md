# Report di completamento — Grid Search Legal GraphRAG (Stadio 1 + Stadio 2)

Data esecuzione: 25/09/2026 · Dataset: `data/eval_dataset.json` (15 query, 15 `expected_ids`)
Provider LLM: OpenRouter `qwen/qwen3.5-9b` · Embedding: Ollama `qwen3-embedding:0.6b` (indice Neo4j) · Reranker: `Qwen/Qwen3-Reranker-0.6B` (GPU locale)

| Fase | Comando | Esito | Durata |
|---|---|---|---|
| Stadio 1 | `py scripts/grid_search_stage1.py --dataset data/eval_dataset.json --cache-candidates data/cache_candidates.pkl` | ✅ exit 0 | ~12 min |
| Stadio 2 | `py scripts/grid_search_stage2.py --stage1-configs top_stage1_configs.json --dataset data/eval_dataset.json` | ✅ exit 0 | ~94 min |

Totale: **1152 + 135 = 1287 configurazioni valutate**, 2025 esecuzioni complete della pipeline, 225 retrieval distinti, 192 generazioni LLM distinte.

---

## 1. Risultati Stadio 1 — Retrieval e ranking

| Variante | Recall@10 | MRR | Hit@1 |
|---|---|---|---|
| Vettoriale puro (baseline) | 80.00% | 0.7000 | 66.67% |
| Ibrido (vettoriale+BM25+grafo TESEO), senza reranker | 93.33% | 0.8133 | 73.33% |
| Ibrido + reranker (**config vincente**) | **93.33%** | **0.8179** | **80.00%** |
| Δ vs baseline | **+13.33 pp** | +0.1179 | +13.33 pp |

**Configurazione vincente**: `RRF_WEIGHT_VECTOR=0.8`, `RRF_WEIGHT_BM25=0.8`, `RRF_WEIGHT_GRAPH=0.8`, `RRF_K=40`, `RERANK_TOP_K=15`, `RERANK_MIN_SCORE=0.0`, `FINAL_K=10`.

### Dove si guadagna
- **Recall**: interamente dai **canali ibridi** (BM25 + navigazione TESEO); il reranker non aggiunge recall. Due query recuperate rispetto al solo vettoriale (n.12 D.P.R. 560/1992, n.14 normativa sui controlli).
- **Ordinamento**: i canali ibridi portano MRR 0.70 → 0.8133; il **reranker** rifinisce la posizione (MRR → 0.8179, Hit@1 73.33% → 80%).
- **Query irrecuperabile**: 1/15 (n.2, composizione/nomina Commissione) resta a recall 0 in tutte le 1152 config → gap di retrieval (non di ranking).

### Sensibilità ai parametri (medie marginali su 1152 config)
| Parametro | Effetto | Valore migliore |
|---|---|---|
| Pesi RRF (0.8–2.0) | **nullo** (recall identica 0.875 in ogni combinazione) | indifferente |
| `RRF_K` (40 vs 60) | nullo (0.8481 vs 0.8481) | indifferente |
| `RERANK_TOP_K` (15 vs 20) | nullo (0.8482 vs 0.8479) | indifferente |
| `RERANK_MIN_SCORE` | **decisivo**: filtrare peggiora | **0.0** (0.8641 vs ~0.8427) |
| `FINAL_K` | **decisivo**: più contesto è meglio | **10** (0.8528 vs 0.8433 con 5) |

---

## 2. Risultati Stadio 2 — Contesto e generazione

135 configurazioni (5 config Stage-1 × 27 combinazioni di contesto) × 15 query.

- **Recall identica in tutte le 135 config**: 93.33% → i parametri Stage-1 (pesi/rrf_k) non cambiano i chunk recuperati.
- **Similarità semantica**: media 0.7547, dev.std 0.0212, range 0.7060–0.7787.
- **Global score** (0.5·recall + 0.5·similarità): 0.8197–0.8560.

### Configurazione vincente della griglia (score 0.8560)
`stage1_rank=2` (0.8/0.8/0.8, `RRF_K=60`) · `GENERATOR_NUM_CTX=8192` · `GENERATOR_STUFF_THRESHOLD=32000` · `TOPOLOGICAL_MAX_CHARS=6000` · Recall 93.33% · Similarità 0.7787.

Top 5: (2, 8192, 32000, 6000) 0.8560 · (5, 32768, 64000, 20000) 0.8557 · (4, 8192, 32000, 6000) 0.8554 · (4, 16384, 64000, 20000) 0.8552 · (4, 32768, 64000, 20000) 0.8550.

### Effetti marginali — una sola dimensione conta davvero
| Dimensione | Similarità media | Lettura |
|---|---|---|
| `GENERATOR_STUFF_THRESHOLD` 16000 → 32000 → 64000 | **0.7293 → 0.7637 → 0.7713** | **unico effetto sistematico** (+4.2 pp) |
| `GENERATOR_NUM_CTX` 8192/16384/32768 | 0.7552 / 0.7561 / 0.7529 | nessun effetto (OpenRouter gestisce il contesto) |
| `TOPOLOGICAL_MAX_CHARS` 6000/12000/20000 | 0.7538 / 0.7548 / 0.7556 | trascurabile (+0.2 pp) |
| `stage1_rank` 1/2/3/4/5 | 0.7542 / 0.7562 / 0.7582 / 0.7541 / 0.7512 | nessun effetto (conferma Stage 1) |

### Perché `GENERATOR_STUFF_THRESHOLD` domina
Sopra la soglia il generatore passa da **stuffing diretto** a **map-reduce** (1 chiamata per chunk + reduce): la fase MAP scarta i chunk che non contengono informazioni "utili" e perde il fatto della golden answer.

Contesti reali misurati (225 retrieval): media **15.016 char**, mediana 9.825. Percentuale di celle in map-reduce:
| soglia | celle in map-reduce |
|---|---|
| 8000 (valore attuale di `.env`) | **141/225 (63%)** |
| 16000 | 74/225 (33%) |
| 32000 | 34/225 (15%) |
| 64000 | 1/225 (0.4%) |

---

## 3. Configurazione finale consigliata

La griglia premia `stuff=32000` (score 0.8560) ma la differenza con le config a `stuff=64000` (miglior score 0.8557) è **0.0003**, cioè rumore (dev.std delle similarità = 0.0212). Con `64000` **nessuna cella** finisce in map-reduce, quindi la raccomandazione robusta è:

| Parametro | Valore consigliato | Note |
|---|---|---|
| `RRF_WEIGHT_VECTOR` / `BM25` / `GRAPH` | `0.8` / `0.8` / `0.8` | vincente Stage 1, indifferente vs alternative |
| `RRF_K` | `40` (o `60`, indifferente) | `60` è già il default di `.env` |
| `RERANK_TOP_K` | `15` | default `20` non cambia nulla |
| `RERANK_MIN_SCORE` | `0.0` | filtrare peggiora |
| `FINAL_K` | `10` | meglio di 5 |
| `GENERATOR_STUFF_THRESHOLD` | **`64000`** (minimo 32000) | **il parametro che conta**: evita il map-reduce |
| `TOPOLOGICAL_MAX_CHARS` | `20000` | miglior marginale, effetto piccolo |
| `GENERATOR_NUM_CTX` | invariato (profilo hardware) | ininfluente via OpenRouter |

Azione immediata suggerita: portare `GENERATOR_STUFF_THRESHOLD` da **8000 → 64000** in `.env` (in produzione il 63% delle query passava dal map-reduce).

---

## 4. Correzioni applicate al codice (con impatto misurato)

| File | Problema | Fix |
|---|---|---|
| `scripts/grid_search_stage1.py` | `import numpy` mancante → `NameError` bloccante | ripristinato |
| `scripts/grid_search_stage2.py` | `settings.is_openrouter` inesistente → `AttributeError` al primo task | check su `LLM_PROVIDER` |
| `scripts/grid_search_stage2.py` | `retrieve_with_trace` generava una risposta poi scartata (chiamata LLM sprecata per ogni cella) | `skip_generation=True` |
| `scripts/grid_search_stage2.py` | i parametri Stage-1 (pesi, `rrf_k`, `rerank_*`) non venivano applicati: la dimensione `stage1_rank` era finta | override temporanei di `settings` con `try/finally` |
| `scripts/grid_search_stage2.py` | chiave cache generazioni non distingueva stuffing vs map-reduce | modalità effettiva nella chiave |
| `scripts/grid_search_stage2.py` | 135 celle ripetevano gli stessi retrieval; similarità ricalcolata 2025 volte | cache retrieval (persistita su disco) + cache similarità |
| `src/rag/llm_factory.py` | `reasoning=False` ignorato per OpenRouter → thinking mode: 935+ token di ragionamento per chiamata (**generazione 79s, grader 31–54s, un caso di risposta VUOTA dopo 235s**) | `extra_body={"reasoning":{"enabled":false}}` → **generazione 2.0s, grader 2.1s** |
| `src/rag/llm_factory.py` | nessun timeout: risposte con header 200 e corpo mai consegnato bloccavano la pipeline (43 stall osservati) | `ResilientChatOpenAI`: **timeout 45s + 3 tentativi per ogni chiamata** + `timeout=120, max_retries=3` a livello client |
| `src/rag/evaluator.py` | un grader bloccato fermava il loop agentico | timeout 180s + fallback documentato "mantieni tutti i documenti" |
| `scripts/analyze_grid_search_stage2.py` | — | **nuovo**: analisi marginale dei risultati Stage 2 |

Riepilogo `git diff`: `run_grid_search.bat` +1, `scripts/grid_search_stage1.py`, `scripts/grid_search_stage2.py`, `src/rag/evaluator.py`, `src/rag/llm_factory.py` (+270/−88 complessivo). Nessuna riprogettazione: solo fix puntuali e cache.

---

## 5. Note metodologiche e limiti

1. **`avg_gen_time_s` = 0.0 per le config vincenti**: la latenza è misurata solo sulle generazioni realmente eseguite (le altre arrivano dalla cache). Valori utili: generazione single-shot 2.0s, grader 2.1s; tempi marginali per `num_ctx` 0.29 / 0.75 / 1.85s.
2. **Giudice di pertinenza (grader) LLM**: una cella ha perso un chunk per un timeout definitivo (3 tentativi) → comportamento previsto (`NESSUNA_INFO`); 43 stall recuperati via retry, 0 fallback del grader.
3. **Parametri Stage-1 equivalenti**: i top-5 config Stage-1 hanno metriche identiche in Stage 1 (0.8854). I 4 rank extra sono un check di robustezza (costo ~60 min) e confermano l'equivalenza.
4. **Recall satura al 93.33%**: la griglia non può migliorare il retrieval; le differenze di score sono guidate dalla fedeltà della generazione.
5. **Una query (n.2) resta irrecuperabile** in ogni configurazione: è il tetto di recall del dataset su questo indice.

## 6. Artefatti prodotti
- `grid_search_stage1_results.{csv,md}` (1152 righe), `top_stage1_configs.json`
- `grid_search_stage2_results.{csv,md}` (135 righe), `best_global_config.json`
- Cache riusabili: `data/cache_candidates.pkl`, `data/cache_stage2_generations.json`, `data/cache_stage2_retrievals.pkl`
- Analisi: `scripts/analyze_grid_search_stage2.py`
