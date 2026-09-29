"""Ablation of the graph mechanisms on the live retrieval pipeline.

Separates the four things the graph does, which the grid search never isolated:

  1. dense + BM25 with the RAW query            -> `enable_graph_search=False` (also disables
                                                   the TESEO query expansion, so no graph at all)
  2. + TESEO matching and BM25 query expansion  -> graph_search on, multi-hop and topology off
  3. + citation expansion (multi-hop)           -> adds `expand_citations` (CITES hops)
  4. + topological expansion                    -> adds NEXT/PART_OF parent + adjacent chunks
  5. everything (production configuration)

Metrics: retrieval (recall / MRR / Hit@1) and, with --with-generation, answer-level semantic
similarity, because on this dataset the golden chunk is always inside the seed set and recall
is therefore saturated.

Usage:
    py scripts/ablation_graph.py                      # retrieval-only, 15 queries
    py scripts/ablation_graph.py --with-generation    # also generate answers and score them
    py scripts/ablation_graph.py --queries 1 2 3      # smaller scope
"""
import argparse
import asyncio
import hashlib
import json
import logging
import os
import pickle
import re
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings
from src.logging_config import setup_logging
from src.rag.models import RagState, RetrievedChunk

setup_logging()
logger = logging.getLogger("ablation_graph")

# Text snippets of the expected provisions, loaded once: ids alone cannot detect a provision
# that ended up inside a merged chunk.
_GOLDEN_SNIPPETS: Dict[str, str] = {}

# Provision id -> distinctive tokens of its wording (tolerant containment check)
_GOLDEN_TOKENS: Dict[str, set] = {}


async def load_golden_containers(dataset: List[dict]) -> int:
    """Map every expected provision to the sibling chunks of the same structural unit.

    The corpus stores an article and each of its commas as separate Expression nodes, so the
    provision can legitimately be returned under a sibling id (the article-level chunk).
    """
    ids = [e for item in dataset for e in (item.get("expected_ids") or [])]
    if not ids:
        return 0
    from neo4j import AsyncGraphDatabase
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI, auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )
    try:
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (e:Expression) WHERE e.id IN $ids
                MATCH (e)-[:PART_OF*1..2]->(u:StructuralUnit)
                MATCH (sib:Expression)-[:PART_OF*1..2]->(u)
                RETURN e.id AS id, collect(DISTINCT sib.id) AS siblings
                """,
                ids=list(dict.fromkeys(ids)),
            )
            async for record in result:
                siblings = {s for s in (record.get("siblings") or []) if s and s != record["id"]}
                if siblings:
                    _GOLDEN_CONTAINERS[record["id"]] = siblings
    finally:
        await driver.close()
    return len(_GOLDEN_CONTAINERS)

# (query, graph switches, knobs) -> (chunks, trace): lets configs that only differ in the
# grader policy or in prompt-side metadata reuse the same retrieval instead of repeating it.
_RETRIEVAL_CACHE: Dict[str, Any] = {}


async def load_golden_snippets(dataset: List[dict]) -> int:
    """Cache a short normalized text snippet for every expected provision."""
    ids = [e for item in dataset for e in (item.get("expected_ids") or [])]
    if not ids:
        return 0
    from neo4j import AsyncGraphDatabase
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI, auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )
    try:
        async with driver.session() as session:
            result = await session.run(
                "MATCH (e:Expression) WHERE e.id IN $ids RETURN e.id AS id, e.text_display AS text",
                ids=list(dict.fromkeys(ids)),
            )
            async for record in result:
                text = (record.get("text") or "").strip()
                if text:
                    snippet = " ".join(text[:160].split())
                    _GOLDEN_SNIPPETS[record["id"]] = snippet
                    _GOLDEN_TOKENS[record["id"]] = set(re.findall(r"[a-zà-ù]{4,}", snippet.lower()))
    finally:
        await driver.close()
    return len(_GOLDEN_SNIPPETS)


# Provision id -> ids of the sibling chunks sharing its structural unit
_GOLDEN_CONTAINERS: Dict[str, set] = {}


@dataclass
class GraphConfig:
    """One combination of the graph-related retrieval switches."""
    name: str
    graph_search: bool
    multi_hop: bool
    topological: bool
    grader_mode: str = "on"  # "on" | "off" (keep all) | "min_keep" (never below N chunks)
    min_keep: int = 0
    teseo_threshold: float = 0.0  # 0 = keep the configured default
    teseo_concepts: int = 0  # 0 = keep the configured default
    weight_graph: float = 0.0  # 0 = keep the configured default
    rerank_top_k: int = 0  # 0 = keep the configured default
    final_k: int = 0  # 0 = keep the configured default (10)
    citation_hops: int = 0  # 0 = keep the configured default
    citation_limit: int = 0  # 0 = keep the configured default (10)
    graph_metadata: int = -1  # -1 = keep default, 0 = off, 1 = on
    pure_text: int = -1  # -1 = keep default, 1 = strip every graph-derived field from chunks


CONFIGS = [
    GraphConfig("1. dense+BM25 (query nuda)", False, False, False),
    GraphConfig("2. + TESEO (match + query expansion)", True, False, False),
    GraphConfig("3. + espansione citazioni (multi-hop)", True, True, False),
    GraphConfig("4. + espansione topologica (NEXT/PART_OF)", True, False, True),
    GraphConfig("5. tutto (produzione)", True, True, True),
    GraphConfig("6. tutto + TESEO preciso (0.6/3c) + citazioni 3", True, True, True, "on", 0, teseo_threshold=0.60, teseo_concepts=3, citation_limit=3),
]

# Same GraphRAG (all graph mechanisms on), varying only how the relevance judge filters.
# Structure is untouched: this tests the filtering policy, i.e. the stage that drops chunks.
GRADER_CONFIGS = [
    GraphConfig("A. GraphRAG, grader attivo", True, True, True, "on", 0),
    GraphConfig("B. GraphRAG, grader off (tieni tutti)", True, True, True, "off", 0),
    GraphConfig("C. GraphRAG, grader min_keep=3", True, True, True, "min_keep", 3),
    GraphConfig("D. GraphRAG, grader min_keep=5", True, True, True, "min_keep", 5),
    GraphConfig("E. GraphRAG, grader min_keep=8", True, True, True, "min_keep", 8),
]

# Graph precision tuning: structure untouched, only how selective the TESEO concept matcher is.
# The channel previously matched generic concepts ("imprese", "merci") that pushed better
# candidates out of the fused top-k; a stricter threshold / fewer concepts keeps only
# specific matches, and weight_graph modulates how much they influence the fusion.
TESEO_CONFIGS = []
for _thr in (0.45, 0.60):
    for _concepts in (10, 5, 3):
        TESEO_CONFIGS.append(GraphConfig(
            f"TESEO thr={_thr} | concetti={_concepts}", True, True, True, "min_keep", 3,
            teseo_threshold=_thr, teseo_concepts=_concepts,
        ))
for _thr, _concepts, _wgraph in ((0.60, 3, 0.8), (0.60, 3, 2.0)):
    TESEO_CONFIGS.append(GraphConfig(
        f"TESEO thr={_thr} | concetti={_concepts} | w_graph={_wgraph}", True, True, True, "min_keep", 3,
        teseo_threshold=_thr, teseo_concepts=_concepts, weight_graph=_wgraph,
    ))

# Window tuning: the graph-injected chunks displace the golden beyond the final window.
# These are pure parameters (no structural change): how many fused candidates reach the
# reranker, how many chunks are handed to the judge/LLM, and how much the graph channel
# weighs in the fusion.
CONTEXT_CONFIGS = [
    GraphConfig("A. rerank_top_k=15 final_k=10 (produzione)", True, True, True, "on", 0, rerank_top_k=15, final_k=10),
    GraphConfig("B. rerank_top_k=30 final_k=10", True, True, True, "on", 0, rerank_top_k=30, final_k=10),
    GraphConfig("C. rerank_top_k=30 final_k=20", True, True, True, "on", 0, rerank_top_k=30, final_k=20),
    GraphConfig("D. rerank_top_k=40 final_k=20", True, True, True, "on", 0, rerank_top_k=40, final_k=20),
    GraphConfig("E. w_graph=0.8 rerank_top_k=30 final_k=10", True, True, True, "on", 0, rerank_top_k=30, final_k=10, weight_graph=0.8),
    GraphConfig("F. w_graph=0.8 rerank_top_k=30 final_k=20", True, True, True, "on", 0, rerank_top_k=30, final_k=20, weight_graph=0.8),
]

# Definitive comparison at IDENTICAL non-graph settings (same reranker, grader, window):
# arm 1 is graph-free by construction (no TESEO concepts, no TESEO query expansion in BM25,
# no citation hops, no topological enrichment), arms 2-5 give the graph its best shot.
FINAL_CONFIGS = [
    GraphConfig("1. BASELINE vettoriale+BM25 (nessun grafo)", False, False, False, "on", 0, rerank_top_k=30, final_k=10),
    GraphConfig("2. GraphRAG completo (stessa finestra)", True, True, True, "on", 0, rerank_top_k=30, final_k=10),
    GraphConfig("3. GraphRAG finestra larga (40/20)", True, True, True, "on", 0, rerank_top_k=40, final_k=20),
    GraphConfig("4. GraphRAG peso grafo 0.8", True, True, True, "on", 0, rerank_top_k=30, final_k=10, weight_graph=0.8),
    GraphConfig("5. GraphRAG TESEO preciso (0.6/3c)", True, True, True, "on", 0, rerank_top_k=30, final_k=10, teseo_threshold=0.60, teseo_concepts=3),
    GraphConfig("6. GraphRAG multi-hop 2 salti", True, True, True, "on", 0, rerank_top_k=30, final_k=10, citation_hops=2),
]

# Strict three-way comparison: is the graph itself (structure included) worth anything, once
# every graph-derived field is removed from the chunks the reranker and generator see?
PURETEXT_CONFIGS = [
    GraphConfig("1. Baseline TESTO-PURO (nessun campo del grafo)", False, False, False, "on", 0, pure_text=1),
    GraphConfig("2. Baseline con metadati di corpus (breadcrumb+vigenza)", False, False, False, "on", 0, pure_text=0),
    GraphConfig("3. GraphRAG completo (TESEO+citazioni+topologia)", True, True, True, "on", 0, pure_text=0),
    GraphConfig("4. GraphRAG preciso + citazioni 1", True, True, True, "on", 0, pure_text=0, teseo_threshold=0.60, teseo_concepts=3, citation_limit=1),
]
# Retrieval is identical across these configs: only the prompt/context annotation changes.
METADATA_CONFIGS = [
    GraphConfig("1. TESEO preciso (grafo silenzioso, no metadati)", True, True, True, "on", 0, rerank_top_k=30, final_k=10, teseo_threshold=0.60, teseo_concepts=3, graph_metadata=0),
    GraphConfig("2. TESEO preciso + metadati grafo nel contesto", True, True, True, "on", 0, rerank_top_k=30, final_k=10, teseo_threshold=0.60, teseo_concepts=3, graph_metadata=1),
    GraphConfig("3. TESEO preciso + metadati + citazioni 3", True, True, True, "on", 0, rerank_top_k=30, final_k=10, teseo_threshold=0.60, teseo_concepts=3, graph_metadata=1, citation_limit=3),
]
# promoted them above the golden. Varying the budget keeps the mechanism identical while
# controlling how much the graph is allowed to displace the base results.
CITATION_CONFIGS = [
    GraphConfig("1. TESEO preciso + citazioni 10 (attuale)", True, True, True, "on", 0, rerank_top_k=30, final_k=10, teseo_threshold=0.60, teseo_concepts=3, citation_limit=10),
    GraphConfig("2. TESEO preciso + citazioni 3", True, True, True, "on", 0, rerank_top_k=30, final_k=10, teseo_threshold=0.60, teseo_concepts=3, citation_limit=3),
    GraphConfig("3. TESEO preciso + citazioni 2", True, True, True, "on", 0, rerank_top_k=30, final_k=10, teseo_threshold=0.60, teseo_concepts=3, citation_limit=2),
    GraphConfig("4. TESEO preciso + citazioni 1", True, True, True, "on", 0, rerank_top_k=30, final_k=10, teseo_threshold=0.60, teseo_concepts=3, citation_limit=1),
    GraphConfig("5. TESEO preciso + citazioni 3 + w_graph 0.8", True, True, True, "on", 0, rerank_top_k=30, final_k=10, teseo_threshold=0.60, teseo_concepts=3, citation_limit=3, weight_graph=0.8),
]


def _golden_present(chunks: List[RetrievedChunk], expected_id: str) -> bool:
    """Is the expected provision in the context?

    Three ways to count as present, all deterministic:
      * a chunk *is* the provision (same id) or holds it as a member of a merged chunk;
      * a chunk is a sibling of the provision (the corpus stores an article and its commas as
        separate Expression nodes: returning the article does answer the question);
      * a chunk contains the provision's wording (token overlap >= 90%), which tolerates the
        small rendering differences between fields ("...senatori e venticinque..." vs
        "...senatori e da venticinque...").
    """
    wanted = {expected_id} | _GOLDEN_CONTAINERS.get(expected_id, set())
    snippet_tokens = _GOLDEN_TOKENS.get(expected_id) or set()
    for chunk in chunks:
        if chunk.expression_id in wanted:
            return True
        if wanted & set(chunk.metadata.get("merged_ids") or []):
            return True
        if snippet_tokens:
            text = getattr(chunk, "expanded_text", None) or chunk.text or ""
            tokens = set(re.findall(r"[a-zà-ù]{4,}", text.lower()))
            if tokens and len(snippet_tokens & tokens) / len(snippet_tokens) >= 0.9:
                return True
    return False


def calculate_recall(chunks: List[RetrievedChunk], expected_ids: List[str]) -> float:
    if not expected_ids:
        return 1.0
    hits = sum(1 for e in expected_ids if _golden_present(chunks, e))
    return hits / len(expected_ids)


def calculate_mrr(chunks: List[RetrievedChunk], expected_ids: List[str]) -> float:
    if not expected_ids:
        return 1.0
    for rank_0, chunk in enumerate(chunks):
        if _golden_present([chunk], expected_ids[0]):
            return 1.0 / (rank_0 + 1)
    return 0.0


def calculate_hit_at_k(chunks: List[RetrievedChunk], expected_ids: List[str], k: int = 1) -> float:
    if not expected_ids:
        return 1.0
    return 1.0 if any(_golden_present(chunks[:k], e) for e in expected_ids) else 0.0


async def main():
    global _RETRIEVAL_CACHE
    parser = argparse.ArgumentParser(description="Ablazione dei meccanismi del grafo")
    parser.add_argument("--dataset", default="data/eval_dataset.json")
    parser.add_argument("--queries", nargs="+", type=int, default=None, help="Indici query (1-indexed)")
    parser.add_argument("--output", default="ablation_graph_results.csv")
    parser.add_argument("--per-query-output", default=None, help="CSV per-query (config, query, metriche)")
    parser.add_argument("--repeats", type=int, default=1, help="Ripetizioni per query (media dei punteggi)")
    parser.add_argument("--mode", choices=["graph", "grader", "teseo", "context", "final", "citation", "metadata", "puretext"], default="graph", help="Quale ablazione eseguire")
    parser.add_argument("--with-generation", action="store_true", help="Genera le risposte e misura la similarità")
    parser.add_argument("--generations-cache", default="data/cache_stage2_generations_ablation.json")
    parser.add_argument("--retrieval-cache", default="data/cache_ablation_retrievals.pkl", help="Cache dei retrieval per (query, parametri)")
    parser.add_argument("--stage1-configs", default="top_stage1_configs_v2.json", help="Config Stage-1 da applicare")
    parser.add_argument("--configs", nargs="+", type=int, default=None, help="Indici (1-based) delle config da eseguire")
    args = parser.parse_args()

    with open(args.dataset, "r", encoding="utf-8") as f:
        dataset = json.load(f)
    if args.queries:
        dataset = [dataset[i - 1] for i in args.queries if 1 <= i <= len(dataset)]

    logger.info(f"Snippet delle golden caricate: {await load_golden_snippets(dataset)}")

    # Restore cached retrievals: configs sharing the same graph switches + knobs reuse them
    if os.path.exists(args.retrieval_cache):
        try:
            with open(args.retrieval_cache, "rb") as f:
                _RETRIEVAL_CACHE.update(pickle.load(f))
            logger.info(f"Ripristinati {len(_RETRIEVAL_CACHE)} retrieval dalla cache ({args.retrieval_cache}).")
        except Exception as e:
            logger.warning(f"Impossibile leggere la cache retrieval: {e}")

    # Apply the winning Stage 1 retrieval hyperparameters (same overrides as Stage 2)
    overrides = {}
    if os.path.exists(args.stage1_configs):
        with open(args.stage1_configs, "r", encoding="utf-8") as f:
            best = json.load(f)[0]
        overrides = {
            "RRF_WEIGHT_VECTOR": best.get("weight_vector", settings.RRF_WEIGHT_VECTOR),
            "RRF_WEIGHT_BM25": best.get("weight_bm25", settings.RRF_WEIGHT_BM25),
            "RRF_WEIGHT_GRAPH": best.get("weight_graph", settings.RRF_WEIGHT_GRAPH),
            "RRF_K": best.get("rrf_k", settings.RRF_K),
            "RAG_TOP_K": best.get("rag_top_k", settings.RAG_TOP_K),
            "RAG_VECTOR_MIN_SCORE": best.get("vector_min_score", settings.RAG_VECTOR_MIN_SCORE),
            "RERANK_TOP_K": best.get("rerank_top_k", settings.RERANK_TOP_K),
            "RERANK_MIN_SCORE": best.get("rerank_min_score", settings.RERANK_MIN_SCORE),
        }
        logger.info(f"Override Stage-1 applicati: {overrides}")
    previous = {k: getattr(settings, k) for k in overrides}
    for k, v in overrides.items():
        setattr(settings, k, v)

    from src.rag.engine import RagEngine

    engine = RagEngine()
    generator = None
    vector_engine = None
    cache = {}
    if args.with_generation:
        from src.parsing.vector_engine import VectorEngine
        from scripts.grid_search_stage2 import GenerationCache, compute_similarity

        generator = engine.generator
        vector_engine = VectorEngine()
        cache = GenerationCache(args.generations_cache)

    rows = []
    per_query_rows = []
    configs = {"graph": CONFIGS, "grader": GRADER_CONFIGS, "teseo": TESEO_CONFIGS, "context": CONTEXT_CONFIGS, "final": FINAL_CONFIGS, "citation": CITATION_CONFIGS, "metadata": METADATA_CONFIGS, "puretext": PURETEXT_CONFIGS}[args.mode]
    if args.configs:
        configs = [c for i, c in enumerate(configs, 1) if i in args.configs]
        logger.info(f"Config selezionate: {[c.name for c in configs]}")
    original_grade = engine.grader.grade_chunks_batch

    def make_graded(mode: str, min_keep: int):
        """Return a grading function implementing the requested filtering policy."""
        async def graded(query: str, chunks: List[RetrievedChunk]) -> List[bool]:
            if mode == "off":
                return [True] * len(chunks)
            verdicts = await original_grade(query, chunks)
            if mode == "min_keep" and min_keep > 0 and sum(verdicts) < min(min_keep, len(chunks)):
                # Safety net: keep every chunk the judge accepted AND top up with the best
                # remaining ones (chunks arrive sorted by reranker score) up to min_keep.
                keep = {i for i, v in enumerate(verdicts) if v}
                for i in range(len(chunks)):
                    if len(keep) >= min(min_keep, len(chunks)):
                        break
                    keep.add(i)
                verdicts = [i in keep for i in range(len(chunks))]
            return verdicts
        return graded

    try:
        for cfg in configs:
            engine.grader.grade_chunks_batch = make_graded(cfg.grader_mode, cfg.min_keep)
            # Per-config overrides of the graph precision knobs (structure untouched)
            tuned = {}
            if cfg.teseo_threshold:
                tuned["TESEO_DENSE_THRESHOLD"] = cfg.teseo_threshold
            if cfg.teseo_concepts:
                tuned["TESEO_MAX_CONCEPTS"] = cfg.teseo_concepts
            if cfg.weight_graph:
                tuned["RRF_WEIGHT_GRAPH"] = cfg.weight_graph
            if cfg.rerank_top_k:
                tuned["RERANK_TOP_K"] = cfg.rerank_top_k
            if cfg.citation_limit:
                tuned["CITATION_EXPANSION_LIMIT"] = cfg.citation_limit
            if cfg.graph_metadata >= 0:
                tuned["GRAPH_METADATA_IN_CONTEXT"] = bool(cfg.graph_metadata)
            if cfg.pure_text >= 0:
                tuned["PURE_TEXT_BASELINE"] = bool(cfg.pure_text)
            tuned_previous = {k: getattr(settings, k) for k in tuned}
            for k, v in tuned.items():
                setattr(settings, k, v)
            logger.info(
                f"--- {cfg.name} (grader={cfg.grader_mode}, min_keep={cfg.min_keep}, tuning={tuned}) ---"
            )
            recall, mrr, hit1, chunks_n, ctx_chars, hops, sims, gen_times = [], [], [], [], [], [], [], []
            for q_idx, item in enumerate(dataset, 1):
                query = item["query"]
                expected = item.get("expected_ids", [])
                # Retrieval depends only on the graph switches + knobs, not on grader mode or on
                # whether graph metadata is rendered into the prompt: cache identical requests.
                retr_key = (
                    f"{query}|{cfg.graph_search}|{cfg.multi_hop}|{cfg.topological}|{cfg.citation_hops or 1}"
                    f"|{settings.RAG_TOP_K}|{cfg.final_k or 10}|{settings.RRF_WEIGHT_GRAPH}"
                    f"|{settings.RERANK_TOP_K}|{settings.CITATION_EXPANSION_LIMIT}|{settings.TESEO_DENSE_THRESHOLD}"
                    f"|{settings.TESEO_MAX_CONCEPTS}|{settings.RERANKER_BACKEND}|{settings.PURE_TEXT_BASELINE}"
                    f"|{settings.RESOLVE_QUERY_CITATIONS}|{settings.CITATION_HOP_SOURCE_TOP_K}"
                    f"|{settings.CITATION_EXPANSION_PER_ACT}"
                    f"|{(settings.EMBEDDING_QUERY_INSTRUCTION or '')[:24]}"
                )
                cached = _RETRIEVAL_CACHE.get(retr_key)
                if cached is None:
                    chunks, trace, _ = await engine.retrieve_with_trace(
                        query=query,
                        top_k=settings.RAG_TOP_K,
                        final_k=cfg.final_k or 10,
                        enable_graph_search=cfg.graph_search,
                        enable_multi_hop=cfg.multi_hop,
                        max_citation_hops=cfg.citation_hops or 1,
                        enable_topological_expansion=cfg.topological,
                        skip_generation=not args.with_generation,
                    )
                    _RETRIEVAL_CACHE[retr_key] = (chunks, trace)
                else:
                    chunks, trace = cached
                    logger.info(f"  [cache] retrieval riusato per la query {q_idx}")
                recall.append(calculate_recall(chunks, expected))
                mrr.append(calculate_mrr(chunks, expected))
                hit1.append(calculate_hit_at_k(chunks, expected, 1))
                chunks_n.append(len(chunks))
                hops.append(trace.get("hop_count", 0))

                context = generator._format_context(chunks) if generator else ""
                ctx_chars.append(len(context))

                if args.with_generation:
                    from scripts.grid_search_stage2 import compute_similarity

                    model_name = (
                        settings.OPENROUTER_MODEL_NAME
                        if settings.LLM_PROVIDER.strip().lower() == "openrouter"
                        else settings.GENERATIVE_MODEL_NAME
                    )
                    # LLM answers are not deterministic even at temperature 0: with --repeats N we
                    # generate N independent answers per query (distinct cache keys) and average,
                    # so a single lucky draw cannot be mistaken for a real improvement.
                    reps = max(1, args.repeats)
                    rep_sims = []
                    for rep in range(reps):
                        mode_key = "ablation" if reps == 1 else f"ablation|rep{rep}"
                        key = cache.get_key(query, context, settings.GENERATOR_NUM_CTX, model_name, mode_key)
                        answer = cache.get(key)
                        if answer is None:
                            start = time.perf_counter()
                            res = await generator.generate(
                                {"query": query, "expanded_chunks": chunks, "generator_num_ctx": settings.GENERATOR_NUM_CTX}
                            )
                            answer = res.get("generation", "")
                            cache.set(key, answer)
                            gen_times.append(time.perf_counter() - start)
                        rep_sims.append(await compute_similarity(item.get("golden_answer", ""), answer, vector_engine))
                    sims.append(statistics.mean(rep_sims))

                per_query_rows.append({
                    "config": cfg.name,
                    "query_index": q_idx,
                    "query": query,
                    "recall": round(recall[-1], 4),
                    "mrr": round(mrr[-1], 4),
                    "hit_at_1": hit1[-1],
                    "n_chunks": chunks_n[-1],
                    "context_chars": ctx_chars[-1],
                    "hops": hops[-1],
                    "similarity": round(sims[-1], 4) if sims else "",
                })

            row = {
                "config": cfg.name,
                "graph_search": cfg.graph_search,
                "multi_hop": cfg.multi_hop,
                "topological": cfg.topological,
                "grader_mode": cfg.grader_mode,
                "min_keep": cfg.min_keep,
                "teseo_threshold": settings.TESEO_DENSE_THRESHOLD,
                "teseo_concepts": settings.TESEO_MAX_CONCEPTS,
                "weight_graph": settings.RRF_WEIGHT_GRAPH,
                "rerank_top_k": settings.RERANK_TOP_K,
                "final_k": cfg.final_k or 10,
                "citation_limit": settings.CITATION_EXPANSION_LIMIT,
                "queries": len(dataset),
                "avg_recall": round(statistics.mean(recall), 4),
                "avg_mrr": round(statistics.mean(mrr), 4),
                "avg_hit_at_1": round(statistics.mean(hit1), 4),
                "avg_chunks": round(statistics.mean(chunks_n), 2),
                "avg_context_chars": round(statistics.mean(ctx_chars)),
                "avg_hops": round(statistics.mean(hops), 2),
                "avg_similarity": round(statistics.mean(sims), 4) if sims else "",
                "avg_gen_time_s": round(statistics.mean(gen_times), 2) if gen_times else "",
            }
            rows.append(row)
            # Persist the retrieval cache after every config so an interrupted run resumes
            try:
                os.makedirs(os.path.dirname(os.path.abspath(args.retrieval_cache)) or ".", exist_ok=True)
                with open(args.retrieval_cache, "wb") as f:
                    pickle.dump(dict(_RETRIEVAL_CACHE), f)
            except Exception as e:
                logger.warning(f"Impossibile salvare la cache retrieval: {e}")
            for k, v in tuned_previous.items():
                setattr(settings, k, v)
            logger.info(
                f"{cfg.name:<44} recall {row['avg_recall']:.2%} | MRR {row['avg_mrr']:.4f} | "
                f"chunk {row['avg_chunks']} | ctx {row['avg_context_chars']} char"
                + (f" | sim {row['avg_similarity']}" if sims else "")
            )
    finally:
        engine.grader.grade_chunks_batch = original_grade
        await engine.close()
        for k, v in previous.items():
            setattr(settings, k, v)

    import csv

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Risultati salvati in {args.output}")

    if args.per_query_output:
        with open(args.per_query_output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(per_query_rows[0].keys()))
            writer.writeheader()
            writer.writerows(per_query_rows)
        logger.info(f"Per-query salvato in {args.per_query_output}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
