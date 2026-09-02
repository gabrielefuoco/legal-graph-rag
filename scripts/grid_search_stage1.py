"""
Grid Search — Stadio 1: Ottimizzazione del Retrieval Ibrido e Ranking (Zero chiamate LLM).
Valuta e ottimizza:
  - Pesi RRF (Vector, BM25, Graph)
  - Costante RRF K
  - Cut-off di fusione (RAG_MIN_SCORE, RERANK_TOP_K)
  - Soglie e filtraggio del CrossEncoder Reranker (RERANK_MIN_SCORE, final_k)
  - Confronto matematico contro il ranking vettoriale puro e la configurazione di default.

Uso:
  py scripts/grid_search_stage1.py
  py scripts/grid_search_stage1.py --dataset data/eval_dataset.json --output results_stage1.csv
  py scripts/grid_search_stage1.py --cache-candidates data/cache_candidates.pkl
  py scripts/grid_search_stage1.py --load-candidates data/cache_candidates.pkl
"""
import asyncio
import argparse
import csv
import json
import logging
import math
import os
import pickle
import time
from copy import copy, deepcopy
from dataclasses import dataclass, asdict
from datetime import date
from itertools import product
from typing import List, Dict, Any, Tuple, Optional

import numpy as np

from src.config import settings
from src.logging_config import setup_logging
from src.rag.models import RagState, RetrievedChunk
from src.rag.fusion import _reciprocal_rank_fusion, _deduplicate_by_text, _mark_abrogated_chunks, _merge_chunks

setup_logging()
logger = logging.getLogger("grid_search_stage1")


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def calculate_recall(retrieved_chunks: List[RetrievedChunk], expected_ids: List[str]) -> float:
    if not expected_ids:
        return 1.0
    retrieved_ids = {c.expression_id for c in retrieved_chunks}
    hits = 0
    for exp_id in expected_ids:
        if exp_id in retrieved_ids or f"merged_{exp_id}" in retrieved_ids:
            hits += 1
    return hits / len(expected_ids)


def calculate_hit_at_k(retrieved_chunks: List[RetrievedChunk], expected_ids: List[str], k: int = 1) -> float:
    if not expected_ids:
        return 1.0
    top_chunks = retrieved_chunks[:k]
    retrieved_ids = {c.expression_id for c in top_chunks}
    for exp_id in expected_ids:
        if exp_id in retrieved_ids or f"merged_{exp_id}" in retrieved_ids:
            return 1.0
    return 0.0


def calculate_mrr(retrieved_chunks: List[RetrievedChunk], expected_ids: List[str]) -> float:
    if not expected_ids:
        return 1.0
    for rank_0, chunk in enumerate(retrieved_chunks):
        if chunk.expression_id in expected_ids or any(chunk.expression_id == f"merged_{e}" for e in expected_ids):
            return 1.0 / (rank_0 + 1)
    return 0.0


@dataclass
class Stage1Config:
    weight_vector: float
    weight_bm25: float
    weight_graph: float
    rrf_k: int
    rerank_top_k: int
    rerank_min_score: float
    final_k: int


@dataclass
class QueryCandidatePool:
    query: str
    expected_ids: List[str]
    vector_results: List[RetrievedChunk]
    bm25_results: List[RetrievedChunk]
    graph_results: List[RetrievedChunk]
    # Mappa: expression_id -> raw_score CrossEncoder
    reranker_raw_scores: Dict[str, float]
    vector_anchor_id: Optional[str]


def simulate_retrieval_and_rerank(
    pool: QueryCandidatePool,
    cfg: Stage1Config,
    use_reranker: bool = True
) -> List[RetrievedChunk]:
    """
    Esegue in-memory la fusione RRF, i filtri e il reranking usando i punteggi pre-calcolati.
    Complessità: O(N log N) in puro Python, ultra-rapido (microsecondi per query).
    """
    # 1. RRF
    channels = [
        (pool.vector_results, cfg.weight_vector),
        (pool.bm25_results, cfg.weight_bm25),
        (pool.graph_results, cfg.weight_graph),
    ]
    fused = _reciprocal_rank_fusion(channels, k=cfg.rrf_k)

    # 2. Cutoff min_score e posizionale
    min_score = settings.RAG_MIN_SCORE
    fused = [c for c in fused if c.score >= min_score]
    fused = fused[:cfg.rerank_top_k]

    # 3. Deduplicazione e filtri
    fused = _deduplicate_by_text(fused)
    fused = _mark_abrogated_chunks(fused, date.today())
    fused = _merge_chunks(fused)

    if not use_reranker or not pool.reranker_raw_scores:
        return fused[:cfg.final_k]

    # 4. Simulazione Reranking
    reranked = [copy(c) for c in fused]
    for c in reranked:
        base_id = c.expression_id.replace("merged_", "")
        raw_score = pool.reranker_raw_scores.get(base_id, -10.0)
        c.score = sigmoid(raw_score)

    reranked.sort(key=lambda x: x.score, reverse=True)

    # Filtro soglia Reranker con salvataggio minimo di 3
    filtered = [c for c in reranked if c.score >= cfg.rerank_min_score]
    min_keep = min(3, len(reranked))
    if len(filtered) < min_keep:
        filtered = reranked[:min_keep]

    final_chunks = filtered[:cfg.final_k]

    # Safety Net per Anchor Vettoriale
    if pool.vector_anchor_id:
        has_anchor = any(
            c.expression_id == pool.vector_anchor_id or c.expression_id == f"merged_{pool.vector_anchor_id}"
            for c in final_chunks
        )
        if not has_anchor:
            for c in fused:
                if c.expression_id == pool.vector_anchor_id or c.expression_id == f"merged_{pool.vector_anchor_id}":
                    final_chunks.append(c)
                    break

    return final_chunks


async def build_candidate_pools(
    dataset: List[dict],
    engine: Any,
    reranker: Any,
    top_k_candidates: int = 30
) -> List[QueryCandidatePool]:
    """
    Raccoglie i chunk grezzi da ciascun canale e pre-calcola i raw score del CrossEncoder.
    Questa fase viene eseguita una sola volta e memorizzata nella cache.
    """
    from src.rag.retriever import vector_search, bm25_search, graph_search

    pools = []
    total = len(dataset)
    for idx, item in enumerate(dataset, 1):
        query = item["query"]
        expected_ids = item.get("expected_ids", [])
        logger.info(f"[{idx}/{total}] Estrazione candidati per query: '{query[:60]}...'")

        # Prepariamo lo stato per l'analyzer
        state: RagState = {
            "query": query,
            "reference_date": None,
            "top_k": top_k_candidates,
            "final_k": top_k_candidates,
            "enable_graph_search": True,
            "enable_multi_hop": True,
            "max_citation_hops": 1,
            "enable_topological_expansion": False,
            "chat_history": [],
            "_driver": engine.analyzer.driver if hasattr(engine, "analyzer") else None,
            "_analyzer": engine.analyzer if hasattr(engine, "analyzer") else None,
            "_reranker": reranker,
        }

        # Step 1: Query Analysis (Estrazione concetti TESEO e embedding query)
        analyzed_state = await engine.analyzer.analyze(state)
        state.update(analyzed_state)

        # Step 2: Retrieval parallelo sui tre canali
        v_res, b_res, g_res = await asyncio.gather(
            vector_search(state),
            bm25_search(state),
            graph_search(state)
        )

        v_chunks = v_res.get("vector_results", [])
        b_chunks = b_res.get("bm25_results", [])
        g_chunks = g_res.get("graph_results", [])

        anchor_id = v_chunks[0].expression_id if v_chunks else None

        # Raccogli tutti i chunk unici per calcolare i punteggi CrossEncoder in batch
        unique_chunks_dict = {}
        for c in (v_chunks + b_chunks + g_chunks):
            if c.expression_id not in unique_chunks_dict:
                unique_chunks_dict[c.expression_id] = c

        all_unique_chunks = list(unique_chunks_dict.values())
        raw_scores = {}
        if reranker and reranker.model and all_unique_chunks:
            pairs = [[query, c.text] for c in all_unique_chunks]
            try:
                preds = reranker.model.predict(pairs, convert_to_numpy=True)
                for c, s in zip(all_unique_chunks, preds):
                    raw_scores[c.expression_id] = float(s)
            except Exception as e:
                logger.error(f"Errore nel calcolo del Reranker: {e}")

        pool = QueryCandidatePool(
            query=query,
            expected_ids=expected_ids,
            vector_results=v_chunks,
            bm25_results=b_chunks,
            graph_results=g_chunks,
            reranker_raw_scores=raw_scores,
            vector_anchor_id=anchor_id
        )
        pools.append(pool)

    return pools


def evaluate_grid(pools: List[QueryCandidatePool], grid_configs: List[Stage1Config]) -> List[dict]:
    """
    Valuta l'intera griglia di configurazioni sui candidate pool.
    """
    logger.info(f"Avvio valutazione di {len(grid_configs)} combinazioni su {len(pools)} query...")

    # 1. Calcola Baseline Vettoriale Pura (solo Vector, niente BM25, niente Graph, niente Reranker)
    vec_pure_cfg = Stage1Config(
        weight_vector=1.0,
        weight_bm25=0.0,
        weight_graph=0.0,
        rrf_k=60,
        rerank_top_k=20,
        rerank_min_score=0.0,
        final_k=10
    )
    vec_pure_recalls = []
    vec_pure_mrrs = []
    vec_pure_hits = []
    for p in pools:
        chunks = simulate_retrieval_and_rerank(p, vec_pure_cfg, use_reranker=False)
        vec_pure_recalls.append(calculate_recall(chunks, p.expected_ids))
        vec_pure_mrrs.append(calculate_mrr(chunks, p.expected_ids))
        vec_pure_hits.append(calculate_hit_at_k(chunks, p.expected_ids, k=1))

    base_vec_recall = float(np.mean(vec_pure_recalls)) if vec_pure_recalls else 0.0
    base_vec_mrr = float(np.mean(vec_pure_mrrs)) if vec_pure_mrrs else 0.0
    base_vec_hit1 = float(np.mean(vec_pure_hits)) if vec_pure_hits else 0.0

    logger.info("=" * 60)
    logger.info(f"BASELINE RANKING VETTORIALE PURO:")
    logger.info(f"  Recall@10: {base_vec_recall:.2%}")
    logger.info(f"  MRR:       {base_vec_mrr:.4f}")
    logger.info(f"  Hit@1:     {base_vec_hit1:.2%}")
    logger.info("=" * 60)

    # 2. Valutazione griglia
    results = []
    for cfg in grid_configs:
        recalls = []
        mrrs = []
        hits1 = []
        hits5 = []
        for p in pools:
            chunks = simulate_retrieval_and_rerank(p, cfg, use_reranker=True)
            recalls.append(calculate_recall(chunks, p.expected_ids))
            mrrs.append(calculate_mrr(chunks, p.expected_ids))
            hits1.append(calculate_hit_at_k(chunks, p.expected_ids, k=1))
            hits5.append(calculate_hit_at_k(chunks, p.expected_ids, k=5))

        avg_recall = float(np.mean(recalls)) if recalls else 0.0
        avg_mrr = float(np.mean(mrrs)) if mrrs else 0.0
        avg_hit1 = float(np.mean(hits1)) if hits1 else 0.0
        avg_hit5 = float(np.mean(hits5)) if hits5 else 0.0

        # Score composito prioritario: 60% Recall + 30% MRR + 10% Hit@1
        composite_score = (avg_recall * 0.60) + (avg_mrr * 0.30) + (avg_hit1 * 0.10)
        delta_recall_vs_pure = avg_recall - base_vec_recall

        res_dict = {
            **asdict(cfg),
            "recall": round(avg_recall, 4),
            "mrr": round(avg_mrr, 4),
            "hit_at_1": round(avg_hit1, 4),
            "hit_at_5": round(avg_hit5, 4),
            "composite_score": round(composite_score, 4),
            "delta_recall_vs_vector": round(delta_recall_vs_pure, 4),
            "baseline_vector_recall": round(base_vec_recall, 4),
            "baseline_vector_mrr": round(base_vec_mrr, 4),
        }
        results.append(res_dict)

    # Ordina per score composito decrescente
    results.sort(key=lambda x: (x["composite_score"], x["recall"]), reverse=True)
    return results


def generate_grid_search_report(results: List[dict], output_md: str):
    """Genera un report Markdown dettagliato per lo Stadio 1."""
    if not results:
        return

    best = results[0]
    base_vec_rec = best["baseline_vector_recall"]
    base_vec_mrr = best["baseline_vector_mrr"]

    with open(output_md, "w", encoding="utf-8") as f:
        f.write("# Report Grid Search Stadio 1 — Ottimizzazione Retrieval e Ranking\n\n")
        f.write("## 🎯 Executive Summary\n\n")
        f.write(f"- **Configurazioni valutate**: `{len(results)}`\n")
        f.write(f"- **Baseline Ranking Vettoriale Puro**: Recall `{base_vec_rec:.2%}` | MRR `{base_vec_mrr:.4f}`\n")
        f.write(
            f"- **Miglior Configurazione Trovata**: Recall **`{best['recall']:.2%}`** "
            f"(Δ `{best['delta_recall_vs_vector']:+.2%}`) | MRR **`{best['mrr']:.4f}`** | Score **`{best['composite_score']:.4f}`**\n\n"
        )

        f.write("### 🏆 Parametri Ottimali Stadio 1\n\n")
        f.write("| Parametro | Valore Ottimale | Descrizione |\n")
        f.write("|---|---|---|\n")
        f.write(f"| `RRF_WEIGHT_VECTOR` | `{best['weight_vector']}` | Peso canale denso vettoriale |\n")
        f.write(f"| `RRF_WEIGHT_BM25` | `{best['weight_bm25']}` | Peso canale lessicale BM25 |\n")
        f.write(f"| `RRF_WEIGHT_GRAPH` | `{best['weight_graph']}` | Peso navigazione concetti TESEO |\n")
        f.write(f"| `RRF_K` | `{best['rrf_k']}` | Costante Reciprocal Rank Fusion |\n")
        f.write(f"| `RERANK_TOP_K` | `{best['rerank_top_k']}` | Chunk inviati al CrossEncoder |\n")
        f.write(f"| `RERANK_MIN_SCORE` | `{best['rerank_min_score']}` | Soglia minima post-sigmoide |\n")
        f.write(f"| `FINAL_K` | `{best['final_k']}` | Chunk finali selezionati |\n\n")

        f.write("## 📊 Top 10 Configurazioni a Confronto\n\n")
        f.write("| Rank | V/B/G Weights | RRF K | Rerank Min | Final K | Recall | MRR | Hit@1 | Score | Δ vs Vector |\n")
        f.write("|---|---|---|---|---|---|---|---|---|---|\n")
        for rank, r in enumerate(results[:10], 1):
            w_str = f"{r['weight_vector']}/{r['weight_bm25']}/{r['weight_graph']}"
            delta_str = f"{r['delta_recall_vs_vector']:+.2%}"
            f.write(
                f"| #{rank} | `{w_str}` | `{r['rrf_k']}` | `{r['rerank_min_score']}` | `{r['final_k']}` | "
                f"**{r['recall']:.2%}** | {r['mrr']:.4f} | {r['hit_at_1']:.2%} | **{r['composite_score']:.4f}** | `{delta_str}` |\n"
            )

    logger.info(f"Report Markdown generato: {output_md}")


def build_default_grid() -> List[Stage1Config]:
    """Genera lo spazio di ricerca combinatorio per lo Stadio 1."""
    weight_vectors = [0.8, 1.0, 1.5]
    weight_bm25s = [0.8, 1.0, 1.2]
    weight_graphs = [0.8, 1.2, 1.5, 2.0]
    rrf_ks = [40, 60]
    rerank_top_ks = [15, 20]
    rerank_min_scores = [0.0, 0.03, 0.05, 0.10]
    final_ks = [5, 10]

    configs = []
    for wv, wb, wg, rk, rtk, rms, fk in product(
        weight_vectors, weight_bm25s, weight_graphs, rrf_ks, rerank_top_ks, rerank_min_scores, final_ks
    ):
        configs.append(Stage1Config(
            weight_vector=wv,
            weight_bm25=wb,
            weight_graph=wg,
            rrf_k=rk,
            rerank_top_k=rtk,
            rerank_min_score=rms,
            final_k=fk
        ))
    return configs


async def main():
    parser = argparse.ArgumentParser(description="Grid Search Stadio 1: Retrieval & Ranking Tuning")
    parser.add_argument("--dataset", type=str, default="data/eval_dataset.json", help="Path dataset di valutazione")
    parser.add_argument("--output", type=str, default="grid_search_stage1_results.csv", help="CSV di output")
    parser.add_argument("--top-configs-output", type=str, default="top_stage1_configs.json", help="JSON con le top configurazioni per lo Stadio 2")
    parser.add_argument("--cache-candidates", type=str, default=None, help="Salva i candidati estratti su file pickle")
    parser.add_argument("--load-candidates", type=str, default=None, help="Carica i candidati da file pickle (bypassa Neo4j/Ollama)")
    parser.add_argument("--queries", nargs="+", type=int, default=None, help="Indici query da valutare (1-indexed)")
    parser.add_argument("--top-n", type=int, default=5, help="Numero di migliori configurazioni da esportare per lo Stadio 2")
    args = parser.parse_args()

    pools: List[QueryCandidatePool] = []

    # Opzione 1: Caricamento da cache esistente
    if args.load_candidates and os.path.exists(args.load_candidates):
        logger.info(f"Caricamento candidati dalla cache: {args.load_candidates}")
        with open(args.load_candidates, "rb") as f:
            pools = pickle.load(f)
    else:
        # Opzione 2: Estrazione live da Neo4j
        if not os.path.exists(args.dataset):
            logger.error(f"Dataset {args.dataset} non trovato.")
            return

        with open(args.dataset, "r", encoding="utf-8") as f:
            full_dataset = json.load(f)

        if args.queries:
            dataset = [full_dataset[i - 1] for i in args.queries if 1 <= i <= len(full_dataset)]
        else:
            dataset = full_dataset

        from src.rag.engine import RagEngine
        from src.rag.reranker import Reranker

        logger.info("Inizializzazione RagEngine e Reranker per estrazione candidati...")
        engine = RagEngine()
        reranker = Reranker()

        pools = await build_candidate_pools(dataset, engine, reranker)
        await engine.close()

        if args.cache_candidates:
            os.makedirs(os.path.dirname(os.path.abspath(args.cache_candidates)) or ".", exist_ok=True)
            with open(args.cache_candidates, "wb") as f:
                pickle.dump(pools, f)
            logger.info(f"Candidati salvati nella cache: {args.cache_candidates}")

    if not pools:
        logger.error("Nessun pool di candidati disponibile per la valutazione.")
        return

    # Generazione e valutazione della griglia
    configs = build_default_grid()
    results = evaluate_grid(pools, configs)

    # Salvataggio risultati CSV
    if results:
        fieldnames = list(results[0].keys())
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        logger.info(f"Risultati CSV salvati in: {args.output}")

        # Salvataggio Report Markdown
        md_file = os.path.splitext(args.output)[0] + ".md"
        generate_grid_search_report(results, md_file)

        # Esportazione top-N per lo Stadio 2
        top_configs = results[:args.top_n]
        with open(args.top_configs_output, "w", encoding="utf-8") as f:
            json.dump(top_configs, f, indent=2)
        logger.info(f"Top {args.top_n} configurazioni esportate in: {args.top_configs_output}")


if __name__ == "__main__":
    asyncio.run(main())
