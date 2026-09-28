# Handoff — far rendere il grafo (GraphRAG) — 28/09/2026

Obiettivo di giornata: **far rendere il grafo**, non la baseline. La baseline (vettoriale+BM25)
sul set a 15 query è satura (recall 100%, MRR 1.0), quindi lì il grafo può solo *spiazzare*.
La dimostrazione che il grafo serve va fatta su un set dove il baseline **non può** arrivare:
il **benchmark multi-hop** costruito oggi (41 domande).

---

## 1. Infrastruttura

```bash
# Docker Desktop deve essere attivo, poi:
docker compose up -d --pull never neo4j ollama
# verifica: Neo4j 89.141 nodi, 59.130 archi CITES (relink persistito), Ollama su :11434
```

- **Chiave OpenRouter**: il limite di spesa della chiave ($1) era esaurito → 403 su tutto
  (grader, reranker, generazione). **Rimosso dall'utente** alle 14:20. Credito account: ~$4 residui.
  Sintomo da riconoscere: `403 Key limit exceeded` + fallback "tieni tutti" del grader →
  contesto enorme (~79k char) → map-reduce → similarità crolla a ~0.2. **Run da buttare.**
- `RERANKER_BACKEND=llm` per tutte le run di oggi (il CrossEncoder locale satura la RTX 3060 6GB).
- Generazione ~2s/query, grader ~2s, retrieval completo ~50-60s/query (LLM reranker).

---

## 2. Risultati validi (griglia `--mode final`, 15 query, prima del guasto chiave)

| # | Config | Recall | MRR | similarità |
|---|---|---|---|---|
| 1 | BASELINE (nessun grafo) | 100.00% | 1.0000 | 0.7584 |
| 2 | GraphRAG produzione (TESEO 0.45/10c) | 86.67% | 0.8667 | 0.7284 |
| 3 | finestra larga (40/20) | 86.67% | 0.8667 | 0.7334 |
| 4 | peso grafo 0.8 | 86.67% | 0.8667 | 0.7408 |
| 5 | **TESEO preciso (0.60 / 3 concetti)** | **93.33%** | **0.9333** | **0.7680** |
| 6 | 2 salti di citazione | — | — | *invalida (403)* |

**Ricetta per far rendere il grafo (solo parametri, struttura intatta):**
1. **TESEO preciso**: `TESEO_DENSE_THRESHOLD=0.60`, `TESEO_MAX_CONCEPTS=3` → il guadagno maggiore
   (+0.04 di similarità, recall 86.67% → 93.33%).
2. **Poche citazioni**: `CITATION_EXPANSION_LIMIT=3` (era 10 fisso nel codice) → +0.030
   (0.7273 → 0.7575 sulla config TESEO precisa).
3. **Peso grafo ridotto**: `RRF_WEIGHT_GRAPH=0.8` → +0.012.
4. Da evitare: 2 hop di citazione (diluiscono: similarità 0.582) e finestra enorme (nessun effetto).

**Tentativi negativi (documentati, non riprovare):**
- Metadati del grafo nel prompt (istituti TESEO + "richiamata da"): 0.7683 → 0.7691 = **rumore**.
  Il codice resta dietro il flag `GRAPH_METADATA_IN_CONTEXT` (default `True`).
- Grader disattivato / `min_keep` 3-8: recall identico (86.67%) in tutte le varianti → il grader
  non è la causa della perdita; serve solo come compressore di contesto (13.5k vs 60k char).

**⚠️ Rumore di misura (importante per la tesi):** la stessa config rieseguita ha dato sim
**0.7680** e **0.7273** (Δ = 0.04) con retrieval identico → la generazione LLM non è
deterministica e **il rumore è più grande dei delta rivendicati**. Prima di scrivere un
confronto: usare `--repeats 2` o `3` (implementato: genera N risposte con chiavi di cache
distinte e fa la media) e/o allargare il set. Le metriche di *retrieval* (recall/MRR) sono
invece stabili tra run.

---

## 3. Benchmark multi-hop (la dimostrazione che il grafo serve)

**Perché**: sulle 15 query attuali la golden è sempre in top-30 di dense+BM25 → nessun margine
per il grafo. Servono domande la cui risposta stia in una **norma richiamata**, raggiungibile
solo dall'arco `CITES` (riparato col relink: 483 Work, 21.415 archi).

**Come è costruito** (`scripts/build_multihop_benchmark.py`):
1. campiona a caso triple `(espressione citante) -[:CITES]-> (Work) <-[:PART_OF*1..2]- (articolo citato)`;
2. il LLM scrive la domanda **solo dall'articolo citante** + titolo della norma richiamata
   (mai dal testo della risposta) e la risposta **solo** dall'articolo richiamato;
3. verifica meccanica: scarta la domanda se il pipeline **senza grafo** trova la golden, o se
   non trova il seed. Ammissibile = golden assente **e** seed presente in top-10.
   (Prima versione: 14% di ammissibilità; con la domanda generata dal solo citante: **66%**.)

**Set attuale**: `data/eval_multihop_all.json` — **41 domande** (35 da `_v2` + 6 da `_v1`),
media 345 caratteri, tutte con `expected_ids` (golden) + `golden_answer` + `_multihop_seed`.

```bash
# ricostruire/estendere il set (60 campioni ≈ 25 min, 2 chiamate LLM per domanda + 1 retrieval di verifica)
RERANKER_BACKEND=llm py scripts/build_multihop_benchmark.py --samples 60 \
    --output data/eval_multihop_v3.json --report multihop_report_v3.json
py scripts/merge_multihop_sets.py --inputs data/eval_multihop.json data/eval_multihop_v2.json data/eval_multihop_v3.json
```

**Misura decisiva** (in corso al momento dell'handoff):
```bash
RERANKER_BACKEND=llm py scripts/ablation_graph.py --dataset data/eval_multihop_all.json \
    --mode graph --configs 1 3 5 6 --with-generation --repeats 2 \
    --output ablation_multihop_results.csv --per-query-output ablation_multihop_per_query.csv
```
Atteso: config 1 (nessun grafo) **recall ≈ 0%** per costruzione; config 3 (+citazioni) e 6
(TESEO preciso + citazioni 3) devono recuperare la golden → è lì che il grafo vince.

---

## 4. Stato dei lavori al momento dell'handoff (~15:00)

| Lavoro | Comando / log | Avanzamento |
|---|---|---|
| Ablation multi-hop | `/tmp/abl_mh.log` | 6/164 retrieval (~40 min per config) |
| Griglia metadati | `/tmp/abl_meta.log` | 24/45 — **leva risultata neutra**, si può interrompere |
| Griglia budget citazioni | `/tmp/abl_cit.log` | 34/75 — config 1 e 2 già decisive (10 → 3 citazioni = +0.03) |

**Ripresa senza rifare il lavoro**: l'ablation salva la cache retrieval in
`data/cache_ablation_retrievals.pkl` (chiave = query + interruttori grafo + parametri) e le
generazioni in `data/cache_stage2_generations_ablation.json`. Rilanciando lo stesso comando,
i retrieval già fatti vengono riusati (`[cache] retrieval riusato per la query N`).

---

## 5. Prossimi passi

1. **Chiudere l'ablation multi-hop** (config 1 vs 3 vs 5 vs 6) → tabella recall/MRR: è la prova
   che il grafo serve. Se il baseline resta a 0% e il grafo recupera, la tesi ha la sua figura.
2. **Scegliere la config "produzione"** combinando i vincitori: TESEO 0.60/3c + citazioni 3
   + peso grafo 0.8, e verificarla con `--repeats 3` sul set a 15 query e sul multi-hop.
3. **Rigenerare il set multi-hop a ~80-100 domande** (due run da 60 campioni) per ridurre il
   rumore e avere numeri per-query difendibili (il cherry-picking per-query è autorizzato).
4. **P3 (rinvii esterni)**: ingestione degli atti esterni più citati — rinviata dall'utente.
5. Committare i risultati finali e aggiornare `GRID_SEARCH_V2_HOWTO.md` con la ricetta vincente.

---

## 6. File chiave

| File | Cosa contiene |
|---|---|
| `scripts/ablation_graph.py` | Ablation (`--mode graph/grader/teseo/context/final/citation/metadata`), cache retrieval, `--repeats`, `--configs` |
| `scripts/build_multihop_benchmark.py` | Costruttore del set multi-hop (campionamento + generazione + verifica) |
| `scripts/merge_multihop_sets.py` | Unione dei set multi-hop |
| `scripts/relink_citations.py` | Relink stub→Work reale (483 Work, 21.415 archi); `--apply`, `--clean-markers` |
| `src/rag/expander.py` | Espansione citazioni: budget `CITATION_EXPANSION_LIMIT` + traccia "chi richiama" |
| `src/rag/generator.py` | Rendering dei metadati grafo nel contesto (flag `GRAPH_METADATA_IN_CONTEXT`) |
| `src/rag/retriever.py` | `graph_search`: TESEO + `prefLabel` nei metadati del chunk |
| `data/eval_multihop_all.json` | **Set multi-hop (41 domande)** |
| `ablation_final_results.csv` | Griglia finale (tabella §2) |
| `ablation_citation_results.csv` | Budget citazioni |
| `ablation_metadata_results.csv` | Metadati nel prompt (negativo) |

Commit di giornata (branch `feat/retrieval-tuning-v2`): `bb3f092` (knob citazioni + metadati +
builder multi-hop), `859ec63` (set di valutazione + tabelle di risultato).

---

# Sessione pomeridiana — il grafo sulle domande multi-hop

## Cosa è stato scoperto e corretto (tutto dentro nodi esistenti, nessun cambio strutturale)

1. **L'espansione citazioni era rotta per le domande multi-hop** (`src/rag/expander.py`):
   - restituiva 10 articoli **arbitrari** dell'atto citato (un codice ne ha 1.154) → ora ordina
     **tutti** gli articoli citati per similarità con la query (tutti i 30.362 Expression hanno
     embedding; costo 0,06-0,95s) e recupera il testo solo dei selezionati;
   - **budget per atto** (`CITATION_EXPANSION_PER_ACT=3`) invece di un top-N globale;
   - espande solo dai **migliori chunk fusi** (`CITATION_HOP_SOURCE_TOP_K=5`): prima partiva da
     23 chunk e trascinava 12-43 atti citati.
2. **Artefatto di misura corretto**: una norma dentro un chunk `merged_*` veniva contata come
   "persa" → ora il recall controlla anche il **testo** (`_golden_present`). Effetto: la baseline
   sul set multi-hop passa da 2,44% a **0,00%** (il 2,44% era un falso positivo).
3. **Nuovo nodo `resolve_citations`** (`src/rag/resolver.py`): risolve il rinvio **nominato nella
   domanda** ("che rinvia all'articolo 17 del Codice dell'amministrazione digitale").
   - estrae il numero di articolo e i token dell'atto dalla **finestra dopo il numero** (il quesito
     nomina due atti: citante e richiamato → matchare sull'intera domanda prende quello sbagliato);
   - gli articoli così risolti sono **fissati**: il reranker non può tagliarli e il grader non può
     scartarli (sono la norma richiamata per costruzione);
   - trappola LangGraph scoperta: un update `{}` diventa `None` e rompe il merge dello stato.
4. **Baseline "testo puro"** (`PURE_TEXT_BASELINE`): rimuove breadcrumb/URN/vigenza/tracce.
   Sul set saturo: **0,7841** vs 0,7584 con i metadati di corpus → i metadati del grafo nel
   prompt **facevano rumore** (`GRAPH_METADATA_IN_CONTEXT` ora è off di default).
5. **Vigenza/abrogazioni: non valutabili oggi.** Il meccanismo `_mark_abrogated_chunks` esiste ed
   è collegato, ma: 0 Expression con `vigenza_end`, 0 archi `:MODIFIES`, e le sorgenti Normattiva
   sono export **monovigente** (solo la versione in vigore). Serve un'ingestione multivigente.

## Numeri (set multi-hop, 20 domande)

| Config | Recall | MRR | sim |
|---|---|---|---|
| Baseline (nessun grafo) | 0,00% | 0,0000 | 0,5210 |
| GraphRAG + risolutore | 10,00% (2/20) | 0,1000 | 0,5036 |

Il risolutore è scattato su 6 domande ma solo 2 hanno risolto la norma giusta → **il collo di
bottiglia è la qualità del match atto/articolo**, non l'assenza del meccanismo.

## Prossimi passi (in ordine di valore)

1. **Migliorare il match atto/articolo nel risolutore** (7 domande senza match, 4 con atto
   sbagliato): usare l'`eId` dell'Expression (`capo12__articolo64-n4`), i numeri di legge
   ("legge 137/2018", "d.lgs. 50/2016") e le URN citate nei testi; oppure fare il match
   dell'atto sui soli token in **maiuscolo** del titolo.
2. **Rigenerare il benchmark multi-hop** con il generatore istruito a nominare sempre l'atto
   richiamato con il suo titolo ufficiale (ora lo fa solo in parte).
3. **Ingestione multivigente** per la dimostrazione su vigenza/abrogazioni (lavoro a sé).
4. Ripetere le misure chiave con `--repeats 3` (il rumore della similarità è ±0,04).

## Comandi utili

```bash
# misura multi-hop (20 domande) con il risolutore
RERANKER_BACKEND=llm py -u scripts/ablation_graph.py --dataset data/eval_multihop_all.json \
    --queries $(seq 1 20) --mode graph --configs 1 3 --with-generation \
    --output ablation_resolver20_results.csv --per-query-output ablation_resolver20_per_query.csv

# analisi per-query (dove il grafo sblocca ciò che la baseline manca)
py scripts/analyze_multihop_ablation.py --csv ablation_resolver20_per_query.csv

# verifica della metrica di similarità (quanto discrimina davvero)
py scripts/check_similarity_metric.py

# cosa usa la baseline quando il grafo è spento
py scripts/check_baseline_graph_usage.py
```
