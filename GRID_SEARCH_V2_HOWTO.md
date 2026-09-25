# Esperimento v2 — come lanciarlo (istruzioni per il lancio di domani)

Branch: `feat/retrieval-tuning-v2`

## Cosa è cambiato rispetto al run che abbiamo già in `main`

| Fase | Cosa | File toccati |
|---|---|---|
| 1 | **Fix fan-out RRF**: ogni canale viene deduplicato *prima* della fusione (il canale denso restituiva ~8 righe per espressione, gonfiando di 8× il suo peso RRF e rendendo inutili i pesi configurati) | `src/rag/fusion.py` |
| 2a | **`GENERATOR_STUFF_THRESHOLD=64000`** in `.env` (evita il map-reduce, che costava ~4 pp di similarità) | `.env`, `.env.example` |
| 3 | **Istruzione query-side sugli embedding** (nessuna re-indicizzazione: i documenti restano senza prefisso, come da specifica Qwen3-Embedding) | `src/config.py`, `src/parsing/vector_engine.py`, `src/parsing/teseo_matcher.py`, `src/rag/query_analyzer.py` |
| 4 | **Nuove dimensioni di griglia**: `RAG_TOP_K` (15/30/50) e `RAG_VECTOR_MIN_SCORE` (0.3/0.2/0.1), prima mai testate; i candidati vengono estratti come superset per poterle simulare | `scripts/grid_search_stage1.py`, `scripts/grid_search_stage2.py` |
| 5 | **Reranker opzionale `llm`** (listwise via OpenRouter: su OpenRouter non esistono modelli di rerank, quindi usa un LLM chat). Default: `local` | `src/rag/reranker.py`, `src/config.py`, `src/rag/engine.py` |
| — | Runner unico, CLI estese, report con le nuove colonne | `scripts/run_grid_search_v2.py`, `scripts/analyze_grid_search_stage2.py`, `run_grid_search.bat` |

## Come lanciarlo

**Opzione A — dal menu batch** (nuova voce `[6]`):

```
run_grid_search.bat   ->   6
```

**Opzione B — da terminale** (equivalente, con più controllo):

```bash
py scripts/run_grid_search_v2.py
```

**Opzione C — prima un mini-test (consigliata, ~5 minuti e pochissimi token):**

```bash
py scripts/run_grid_search_v2.py --queries 1 2 3 --top-n 2 --dry-run --tag smoke
```

Il runner stampa all'avvio il piano completo (griglia, cache usate, durata stimata) e scrive i log in
`grid_search_stage1_v2.log` e `grid_search_stage2_v2.log`.

### Flag utili

| Flag | Effetto |
|---|---|
| `--queries 1 2 3` | Limita il dataset (utile per smoke test) |
| `--top-n 2` | Quante config Stage-1 passano allo Stadio 2 (default 3) |
| `--embedding-instruction on\|off` | A/B dell'istruzione query-side (Fase 3) |
| `--reranker-backend local\|llm` | Reranker locale (CrossEncoder) o LLM listwise via OpenRouter |
| `--stuff-thresholds 64000` | Riduce la dimensione della griglia Stage-2 (il valore vincente noto) |
| `--num-ctx-options 8192 32768` / `--topo-options 6000 20000` | Griglia Stage-2 più piccola |
| `--dry-run` | Stadio 2 senza chiamate LLM (solo retrieval + similarità) |
| `--skip-stage1` / `--skip-stage2` | Esegue solo una delle due fasi |
| `--tag nome` | Cambia i nomi di output e le cache (per confronti A/B paralleli) |

## Durate attese (stimate sui tempi misurati del run precedente)

| Scenario | Comando | Tempo |
|---|---|---|
| Smoke test (3 query, dry-run) | `--queries 1 2 3 --top-n 2 --dry-run --tag smoke` | ~5-10 min |
| Stadio 1 esteso (15 query, 432 config) | `--skip-stage2` | ~25-40 min |
| Esperimento completo (default) | `py scripts/run_grid_search_v2.py` | ~1h 30m – 2h |
| Completo "rapido" (solo stuff 64000) | `--stuff-thresholds 64000` | ~45-60 min |

Nota: lo Stadio 2 costa soprattutto per i *retrieval* nuovi (i parametri Stage-1 sono cambiati) e per le
generazioni; le cache su disco (`data/cache_stage2_retrievals_v2.pkl`, `data/cache_stage2_generations_v2.json`)
permettono di riprendere da dove si era interrotto rilanciando lo stesso comando.

## Cosa guardare nei risultati

1. **`Dedup canale: 120 righe → 15 chunk distinti`** nei log: conferma che il fix della Fase 1 è attivo. Prima
   il canale denso pesava ~8× contro BM25/grafo.
2. **Stadio 1 (`grid_search_stage1_v2_results.md`)**: i pesi RRF ora sono significativi? `RAG_TOP_K` più alto
   migliora recall/MRR? Confronta con `grid_search_stage1_results.md` (recall 93.33%, MRR 0.8179).
3. **`RAG_VECTOR_MIN_SCORE`**: 0.3 era un filtro hard mai giustificato; verificare se 0.2/0.1 cambia qualcosa.
4. **Stadio 2 (`grid_search_stage2_v2_results.md` + `py scripts/analyze_grid_search_stage2.py --csv grid_search_stage2_v2_results.csv`)**:
   conferma che `GENERATOR_STUFF_THRESHOLD` resta l'unica dimensione con effetto sistematico.
5. **A/B istruzione embedding** (Fase 3): serve una seconda esecuzione con cache separate, es.

```bash
py scripts/run_grid_search_v2.py --embedding-instruction off --tag v2_noinstr --skip-stage2
py scripts/run_grid_search_v2.py --embedding-instruction on  --tag v2_instr   --skip-stage2
```

   poi confronto di recall/MRR nei due CSV.

## Note

- Il reranker locale costa ~1.2 GB di VRAM e ~2.8 s per query (p95 ~16 s); il backend `llm` sposta il costo
  sull'API (~1 chiamata per query con 15-30 chunk nel prompt) e libera la GPU.
- Lo Stadio 1 usa **sempre** il CrossEncoder locale (serve un punteggio pre-calcolabile per ogni candidato);
  `--reranker-backend llm` agisce solo sullo Stadio 2.
- L'istruzione di embedding **non** richiede re-indicizzazione: agisce solo sull'embedding della query.
