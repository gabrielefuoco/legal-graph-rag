"""Grid Search Stage 2: Context window and generation tuning via OpenRouter.

Evaluates candidates from top Stage 1 retrieval configurations against:
  - Context window size: GENERATOR_NUM_CTX (8k, 16k, 32k)
  - Stuffing vs map-reduce threshold: GENERATOR_STUFF_THRESHOLD (8k, 16k, 32k, 64k)
  - Topological expansion budget: TOPOLOGICAL_MAX_CHARS (6k, 12k, 20k)
  - Semantic similarity against golden answer, recall, and token efficiency.
  - Automatic response caching to handle rate limits and transient connection issues.

Usage:
    py scripts/grid_search_stage2.py --stage1-configs top_stage1_configs.json
    py scripts/grid_search_stage2.py --dataset data/eval_dataset.json --dry-run
    py scripts/grid_search_stage2.py --output results_stage2.csv
"""
import asyncio
import argparse
import csv
import hashlib
import json
import logging
import math
import os
import pickle
import re
import time
from dataclasses import dataclass, asdict
from itertools import product
from typing import List, Dict, Any, Optional

import sys
from pathlib import Path

import numpy as np

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings
from src.logging_config import setup_logging
from src.rag.llm_factory import get_llm
from src.rag.models import RagState, RetrievedChunk
from src.rag.generator import LegalGenerator

setup_logging()
logger = logging.getLogger("grid_search_stage2")


def split_into_sentences(text: str) -> List[str]:
    """Split raw text into clean sentence units."""
    if not text:
        return []
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    return [s.strip() for s in sentences if len(s.strip()) > 10]


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Compute cosine similarity between two numeric vectors."""
    v1 = np.array(vec1)
    v2 = np.array(vec2)
    norm = np.linalg.norm(v1) * np.linalg.norm(v2)
    if norm == 0:
        return 0.0
    return float(np.dot(v1, v2) / norm)


def fallback_text_similarity(text1: str, text2: str) -> float:
    """Compute token-level Jaccard overlap similarity as fallback metric."""
    if not text1 or not text2:
        return 0.0
    tokens1 = set(re.findall(r'\w+', text1.lower()))
    tokens2 = set(re.findall(r'\w+', text2.lower()))
    if not tokens1 or not tokens2:
        return 0.0
    intersection = tokens1.intersection(tokens2)
    union = tokens1.union(tokens2)
    return len(intersection) / len(union)


async def compute_similarity(golden_text: str, generated_text: str, vector_engine: Any = None) -> float:
    """Compute chunk-by-chunk semantic similarity using vector embeddings or token overlap fallback."""
    if not golden_text or not generated_text:
        return 0.0

    if vector_engine:
        try:
            golden_sentences = split_into_sentences(golden_text)
            gen_sentences = split_into_sentences(generated_text)
            if not golden_sentences or not gen_sentences:
                return 0.0

            g_embs = await vector_engine.compute_embeddings_batch(golden_sentences)
            gen_embs = await vector_engine.compute_embeddings_batch(gen_sentences)

            match_scores = []
            for g_emb in g_embs:
                best = max([cosine_similarity(g_emb, gen_emb) for gen_emb in gen_embs], default=0.0)
                match_scores.append(best)
            return float(np.mean(match_scores)) if match_scores else 0.0
        except Exception as e:
            logger.warning(f"Calcolo embedding fallito ({e}), fallback a text similarity.")

    return fallback_text_similarity(golden_text, generated_text)


@dataclass
class Stage2Config:
    """Configuration parameters for Stage 2 context and generation tuning."""
    stage1_rank: int
    weight_vector: float
    weight_bm25: float
    weight_graph: float
    rrf_k: int
    rerank_top_k: int
    rerank_min_score: float
    final_k: int
    generator_num_ctx: int
    generator_stuff_threshold: int
    topological_max_chars: int
    rag_top_k: int = 15
    vector_min_score: float = 0.3


# In-memory cache of retrieval outputs keyed by query + Stage 1 retrieval signature.
# Prevents re-running identical retrievals across Stage 2 context configurations.
_RETRIEVAL_CACHE: Dict[str, List[RetrievedChunk]] = {}

# Similarity scores are fully determined by (golden answer, generated answer), and cached
# generations produce identical pairs across configurations: memoize to avoid thousands of
# redundant embedding calls.
_SIMILARITY_CACHE: Dict[str, float] = {}

# Hard supervisor-level timeouts: the HTTP client does not always honour read timeouts on
# stalled OpenRouter responses, so every network operation is wrapped by asyncio.wait_for.
# These are last-resort guards: per-call retries are handled in src/rag/llm_factory.py.
RETRIEVAL_TIMEOUT_S = 240
GENERATION_TIMEOUT_S = 600
_CALL_ATTEMPTS = 2
_RETRIEVAL_CACHE_FILE = "data/cache_stage2_retrievals.pkl"


async def await_with_timeout(coro_factory, timeout_s: float, label: str):
    """Await a coroutine with a hard timeout, retrying once on stalls.

    Guards against OpenRouter responses that return headers but never deliver the body
    (observed stalls of 15+ minutes despite a 120s client timeout).
    """
    last_error = ""
    for attempt in range(1, _CALL_ATTEMPTS + 1):
        try:
            return await asyncio.wait_for(coro_factory(), timeout=timeout_s)
        except asyncio.TimeoutError:
            last_error = f"nessuna risposta entro {timeout_s:.0f}s"
            logger.warning(f"[TIMEOUT] {label}: {last_error} (tentativo {attempt}/{_CALL_ATTEMPTS}).")
    raise TimeoutError(f"{label}: {last_error} dopo {_CALL_ATTEMPTS} tentativi")


def load_retrieval_cache(path: str = None) -> int:
    """Restore persisted retrieval results so interrupted runs resume without redoing work."""
    path = path or _RETRIEVAL_CACHE_FILE
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "rb") as f:
            _RETRIEVAL_CACHE.update(pickle.load(f))
        return len(_RETRIEVAL_CACHE)
    except Exception as e:
        logger.warning(f"Impossibile leggere la cache retrieval ({path}): {e}")
        return 0


def save_retrieval_cache(path: str = None):
    """Persist retrieval results to disk (best effort)."""
    path = path or _RETRIEVAL_CACHE_FILE
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(dict(_RETRIEVAL_CACHE), f)
    except Exception as e:
        logger.warning(f"Impossibile salvare la cache retrieval ({path}): {e}")


class GenerationCache:
    """JSON-backed generation cache for resilience against network interruptions."""
    def __init__(self, cache_file: str):
        self.cache_file = cache_file
        self.cache: Dict[str, str] = {}
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    self.cache = json.load(f)
                logger.info(f"Caricate {len(self.cache)} risposte salvate dalla cache: {cache_file}")
            except Exception as e:
                logger.warning(f"Impossibile leggere cache: {e}")

    def get_key(self, query: str, context_text: str, num_ctx: int, model: str, gen_mode: str = "stuff") -> str:
        """Derive deterministic SHA256 cache key from query, context, and generator parameters.

        `gen_mode` distinguishes direct stuffing from map-reduce generation so that
        configurations sharing the same prompt but different generation pipelines
        never reuse each other's cached answer.
        """
        payload = f"{query}::{context_text}::{num_ctx}::{model}::{gen_mode}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def get(self, key: str) -> Optional[str]:
        """Retrieve cached generation if present."""
        return self.cache.get(key)

    def set(self, key: str, answer: str):
        """Store generated answer in cache and sync to file."""
        self.cache[key] = answer
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.cache_file)) or ".", exist_ok=True)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Errore scrittura cache: {e}")


def build_default_stage2_grid(
    stage1_top_configs: List[dict],
    num_ctx_options: List[int] = None,
    stuff_thresholds: List[int] = None,
    topo_max_chars_options: List[int] = None,
) -> List[Stage2Config]:
    """Combine top Stage 1 retrieval configurations with Stage 2 context parameters."""
    num_ctx_options = num_ctx_options or [8192, 16384, 32768]
    stuff_thresholds = stuff_thresholds or [16000, 32000, 64000]
    topo_max_chars_options = topo_max_chars_options or [6000, 12000, 20000]

    stage2_configs = []
    for rank, s1 in enumerate(stage1_top_configs, 1):
        for n_ctx, stuff_t, topo_c in product(num_ctx_options, stuff_thresholds, topo_max_chars_options):
            stage2_configs.append(Stage2Config(
                stage1_rank=rank,
                weight_vector=s1.get("weight_vector", 1.0),
                weight_bm25=s1.get("weight_bm25", 1.0),
                weight_graph=s1.get("weight_graph", 1.2),
                rrf_k=s1.get("rrf_k", 60),
                rerank_top_k=s1.get("rerank_top_k", 20),
                rerank_min_score=s1.get("rerank_min_score", 0.05),
                final_k=s1.get("final_k", 10),
                generator_num_ctx=n_ctx,
                generator_stuff_threshold=stuff_t,
                topological_max_chars=topo_c,
                rag_top_k=int(s1.get("rag_top_k", settings.RAG_TOP_K)),
                vector_min_score=float(s1.get("vector_min_score", 0.3)),
            ))
    return stage2_configs


async def evaluate_stage2_query(
    query_item: dict,
    cfg: Stage2Config,
    engine: Any,
    generator: LegalGenerator,
    cache: GenerationCache,
    vector_engine: Any = None,
    dry_run: bool = False
) -> dict:
    """Execute retrieval using Stage 1 settings and generation using Stage 2 parameters."""
    query = query_item["query"]
    expected_ids = query_item.get("expected_ids", [])
    golden_answer = query_item.get("golden_answer", "")

    # Retrieval depends only on query + Stage 1 parameters (not on num_ctx/stuff threshold),
    # so identical retrieval signatures are evaluated once and reused.
    retrieval_sig = (
        f"{query}|{cfg.weight_vector}|{cfg.weight_bm25}|{cfg.weight_graph}|{cfg.rrf_k}"
        f"|{cfg.rerank_top_k}|{cfg.rerank_min_score}|{cfg.final_k}|{cfg.topological_max_chars}"
        f"|{cfg.rag_top_k}|{cfg.vector_min_score}|{settings.RERANKER_BACKEND}"
        f"|{(settings.EMBEDDING_QUERY_INSTRUCTION or '')[:24]}"
    )
    chunks = _RETRIEVAL_CACHE.get(retrieval_sig)
    if chunks is None:
        # Temporarily apply Stage 1 retrieval hyperparameters via settings overrides
        overrides = {
            "RRF_WEIGHT_VECTOR": cfg.weight_vector,
            "RRF_WEIGHT_BM25": cfg.weight_bm25,
            "RRF_WEIGHT_GRAPH": cfg.weight_graph,
            "RRF_K": cfg.rrf_k,
            "RERANK_TOP_K": cfg.rerank_top_k,
            "RERANK_MIN_SCORE": cfg.rerank_min_score,
            "RAG_TOP_K": cfg.rag_top_k,
            "RAG_VECTOR_MIN_SCORE": cfg.vector_min_score,
        }
        previous_values = {key: getattr(settings, key) for key in overrides}
        try:
            for key, value in overrides.items():
                setattr(settings, key, value)
            # skip_generation avoids a discarded double generation: Stage 2 generates explicitly below
            chunks, _trace, _ = await await_with_timeout(
                lambda: engine.retrieve_with_trace(
                    query=query,
                    top_k=cfg.rerank_top_k,
                    final_k=cfg.final_k,
                    enable_graph_search=True,
                    enable_multi_hop=True,
                    enable_topological_expansion=True,
                    topo_max_chars=cfg.topological_max_chars,
                    generator_num_ctx=cfg.generator_num_ctx,
                    skip_generation=True,
                ),
                RETRIEVAL_TIMEOUT_S,
                f"retrieval '{query[:40]}...'",
            )
        finally:
            for key, value in previous_values.items():
                setattr(settings, key, value)
        _RETRIEVAL_CACHE[retrieval_sig] = chunks
        save_retrieval_cache()
    else:
        logger.info("⚡ Retrieval riutilizzato dalla cache (stessa firma Stage 1).")

    from scripts.grid_search_stage1 import calculate_recall
    recall = calculate_recall(chunks, expected_ids)

    # Build formatted context and derive cache key. The effective generation pipeline
    # (stuffing vs map-reduce) is part of the key to keep cached answers comparable.
    context_str = generator._format_context(chunks)
    total_chars = sum(len(getattr(c, "expanded_text", None) or c.text) for c in chunks)
    gen_mode = "stuff" if total_chars <= cfg.generator_stuff_threshold else "map_reduce"
    model_name = settings.OPENROUTER_MODEL_NAME if settings.LLM_PROVIDER.strip().lower() == "openrouter" else settings.GENERATIVE_MODEL_NAME
    cache_key = cache.get_key(query, context_str, cfg.generator_num_ctx, model_name, gen_mode)

    cached_gen = cache.get(cache_key)
    gen_time = 0.0

    if dry_run:
        answer = f"[DRY-RUN GENERATION] Query: {query[:30]}..."
    elif cached_gen is not None:
        logger.info("⚡ Risposta recuperata dalla cache locale.")
        answer = cached_gen
    else:
        start_t = time.perf_counter()
        state: RagState = {
            "query": query,
            "expanded_chunks": chunks,
            "generator_num_ctx": cfg.generator_num_ctx,
        }
        # Temporarily override generator stuffing threshold
        old_stuff = getattr(settings, "GENERATOR_STUFF_THRESHOLD", 8000)
        settings.GENERATOR_STUFF_THRESHOLD = cfg.generator_stuff_threshold
        try:
            res = await await_with_timeout(
                lambda: generator.generate(state),
                GENERATION_TIMEOUT_S,
                f"generazione '{query[:40]}...'",
            )
            answer = res.get("generation", "")
            cache.set(cache_key, answer)
        finally:
            settings.GENERATOR_STUFF_THRESHOLD = old_stuff
        gen_time = time.perf_counter() - start_t

    sim_key = hashlib.sha256(f"{golden_answer}::{answer}".encode("utf-8")).hexdigest()
    if sim_key in _SIMILARITY_CACHE:
        sim_score = _SIMILARITY_CACHE[sim_key]
    else:
        sim_score = await compute_similarity(golden_answer, answer, vector_engine)
        _SIMILARITY_CACHE[sim_key] = sim_score

    return {
        "recall": recall,
        "similarity": round(sim_score, 4),
        "gen_time_s": round(gen_time, 2),
        "context_chars": len(context_str),
        "chunks_count": len(chunks)
    }


def generate_stage2_report(results: List[dict], output_md: str):
    """Generate comprehensive Markdown report for Stage 2 evaluation."""
    if not results:
        return

    best = results[0]
    with open(output_md, "w", encoding="utf-8") as f:
        f.write("# Report Grid Search Stadio 2 — Tuning Contesto e Generazione su OpenRouter\n\n")
        f.write("## 🎯 Risultati Globali e Vincitore Assoluto\n\n")
        f.write(f"- **Configurazioni valutate**: `{len(results)}`\n")
        f.write(
            f"- **Miglior Setup Globale**: Score Globale **`{best['global_score']:.4f}`** | "
            f"Recall: **`{best['avg_recall']:.2%}`** | Similarita Semantica: **`{best['avg_similarity']:.4f}`**\n\n"
        )

        f.write("### 🏆 Scheda Configurazione Vincente\n\n")
        f.write("| Parametro | Valore Ottimale | Descrizione |\n")
        f.write("|---|---|---|\n")
        f.write(f"| `LLM_PROVIDER` | `openrouter` | Provider LLM ad alta capacita |\n")
        f.write(f"| `OPENROUTER_MODEL_NAME` | `{settings.OPENROUTER_MODEL_NAME}` | Modello cloud di riferimento |\n")
        f.write(f"| `GENERATOR_NUM_CTX` | `{best['generator_num_ctx']}` | Finestra di contesto generatore |\n")
        f.write(f"| `GENERATOR_STUFF_THRESHOLD` | `{best['generator_stuff_threshold']}` | Soglia max caratteri per stuffing diretto |\n")
        f.write(f"| `TOPOLOGICAL_MAX_CHARS` | `{best['topological_max_chars']}` | Limite espansione topologica per super-chunk |\n")
        f.write(f"| `RRF_WEIGHT_VECTOR` | `{best['weight_vector']}` | Peso RRF Vettoriale |\n")
        f.write(f"| `RRF_WEIGHT_BM25` | `{best['weight_bm25']}` | Peso RRF BM25 |\n")
        f.write(f"| `RRF_WEIGHT_GRAPH` | `{best['weight_graph']}` | Peso RRF Grafo TESEO |\n")
        f.write(f"| `RRF_K` | `{best['rrf_k']}` | Costante K di Rank Fusion |\n")
        f.write(f"| `RERANK_TOP_K` | `{best['rerank_top_k']}` | Cutoff pre-reranking |\n")
        f.write(f"| `RERANK_MIN_SCORE` | `{best['rerank_min_score']}` | Soglia minima Reranker |\n")
        f.write(f"| `FINAL_K` | `{best['final_k']}` | Numero finale di chunk alimentati al generatore |\n")
        f.write(f"| `RAG_TOP_K` | `{best.get('rag_top_k')}` | Candidati per canale pre-fusione |\n")
        f.write(f"| `RAG_VECTOR_MIN_SCORE` | `{best.get('vector_min_score')}` | Soglia minima similarità vettoriale |\n\n")

        f.write("## 📊 Top 10 Configurazioni di Generazione\n\n")
        f.write("| Rank | Num Ctx | Stuff Thresh | Topo Chars | S1 Rank | Recall | Sim Score | Global Score |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")
        for rank, r in enumerate(results[:10], 1):
            f.write(
                f"| #{rank} | `{r['generator_num_ctx']}` | `{r['generator_stuff_threshold']}` | `{r['topological_max_chars']}` | "
                f"#{r['stage1_rank']} | {r['avg_recall']:.2%} | {r['avg_similarity']:.4f} | **{r['global_score']:.4f}** |\n"
            )

    logger.info(f"Report Markdown Stadio 2 generato: {output_md}")


async def main():
    """Execute Stage 2 grid search workflow across context and model settings."""
    parser = argparse.ArgumentParser(description="Grid Search Stadio 2: Context & OpenRouter Tuning")
    parser.add_argument("--stage1-configs", type=str, default="top_stage1_configs.json", help="Path JSON con top configurazioni Stadio 1")
    parser.add_argument("--dataset", type=str, default="data/eval_dataset.json", help="Path dataset di valutazione")
    parser.add_argument("--output", type=str, default="grid_search_stage2_results.csv", help="CSV di output")
    parser.add_argument("--best-output", type=str, default="best_global_config.json", help="Configurazione globale ottimale")
    parser.add_argument("--cache-generations", type=str, default="data/cache_stage2_generations.json", help="Cache per generazioni OpenRouter")
    parser.add_argument("--retrieval-cache", type=str, default=None, help="Cache retrieval persistita (default: derivata da --cache-generations)")
    parser.add_argument(
        "--stuff-thresholds",
        nargs="+",
        type=int,
        default=None,
        help="Soglie GENERATOR_STUFF_THRESHOLD da valutare (default: 16000 32000 64000)",
    )
    parser.add_argument(
        "--num-ctx-options",
        nargs="+",
        type=int,
        default=None,
        help="Valori di GENERATOR_NUM_CTX da valutare (default: 8192 16384 32768)",
    )
    parser.add_argument(
        "--topo-options",
        nargs="+",
        type=int,
        default=None,
        help="Valori di TOPOLOGICAL_MAX_CHARS da valutare (default: 6000 12000 20000)",
    )
    parser.add_argument("--queries", nargs="+", type=int, default=None, help="Indici query da testare (1-indexed)")
    parser.add_argument("--dry-run", action="store_true", help="Esegui senza chiamare realmente le API LLM")
    args = parser.parse_args()

    # Load top Stage 1 configurations
    if not os.path.exists(args.stage1_configs):
        logger.warning(f"File {args.stage1_configs} non trovato. Uso configurazione di fallback da settings.")
        stage1_top = [{
            "weight_vector": settings.RRF_WEIGHT_VECTOR,
            "weight_bm25": settings.RRF_WEIGHT_BM25,
            "weight_graph": settings.RRF_WEIGHT_GRAPH,
            "rrf_k": settings.RRF_K,
            "rerank_top_k": settings.RERANK_TOP_K,
            "rerank_min_score": settings.RERANK_MIN_SCORE,
            "final_k": 10
        }]
    else:
        with open(args.stage1_configs, "r", encoding="utf-8") as f:
            stage1_top = json.load(f)

    # Load evaluation dataset
    if not os.path.exists(args.dataset):
        logger.error(f"Dataset {args.dataset} non trovato.")
        return

    with open(args.dataset, "r", encoding="utf-8") as f:
        full_dataset = json.load(f)

    dataset = [full_dataset[i - 1] for i in args.queries if 1 <= i <= len(full_dataset)] if args.queries else full_dataset

    configs = build_default_stage2_grid(
        stage1_top,
        num_ctx_options=args.num_ctx_options,
        stuff_thresholds=args.stuff_thresholds,
        topo_max_chars_options=args.topo_options,
    )
    logger.info(f"Generata griglia Stadio 2: {len(configs)} configurazioni su {len(dataset)} query.")

    cache = GenerationCache(args.cache_generations)

    global _RETRIEVAL_CACHE_FILE
    _RETRIEVAL_CACHE_FILE = args.retrieval_cache or args.cache_generations.replace("generations", "retrievals")
    restored = load_retrieval_cache()
    if restored:
        logger.info(f"Ripristinati {restored} retrieval dalla cache su disco ({_RETRIEVAL_CACHE_FILE}).")

    from src.rag.engine import RagEngine
    from src.parsing.vector_engine import VectorEngine

    engine = RagEngine()
    vector_engine = VectorEngine()
    generator = LegalGenerator()

    results = []
    for cfg_idx, cfg in enumerate(configs, 1):
        logger.info(f"--- Valutazione Config Stadio 2 [{cfg_idx}/{len(configs)}] (Num Ctx: {cfg.generator_num_ctx}, Stuff: {cfg.generator_stuff_threshold}) ---")
        q_recalls = []
        q_sims = []
        q_times = []

        for q_item in dataset:
            q_res = await evaluate_stage2_query(
                query_item=q_item,
                cfg=cfg,
                engine=engine,
                generator=generator,
                cache=cache,
                vector_engine=vector_engine,
                dry_run=args.dry_run
            )
            q_recalls.append(q_res["recall"])
            q_sims.append(q_res["similarity"])
            q_times.append(q_res["gen_time_s"])

        avg_rec = float(np.mean(q_recalls)) if q_recalls else 0.0
        avg_sim = float(np.mean(q_sims)) if q_sims else 0.0
        avg_t = float(np.mean(q_times)) if q_times else 0.0

        # Composite score: 50% Recall + 50% Similarity
        global_score = (avg_rec * 0.50) + (avg_sim * 0.50)

        res_row = {
            **asdict(cfg),
            "avg_recall": round(avg_rec, 4),
            "avg_similarity": round(avg_sim, 4),
            "avg_gen_time_s": round(avg_t, 2),
            "global_score": round(global_score, 4),
        }
        results.append(res_row)

    await engine.close()

    # Sort descending by composite score, breaking ties with similarity
    results.sort(key=lambda x: (x["global_score"], x["avg_similarity"]), reverse=True)

    # Export results to CSV
    if results:
        fieldnames = list(results[0].keys())
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        logger.info(f"Risultati salvati in: {args.output}")

        # Export Markdown report
        md_file = os.path.splitext(args.output)[0] + ".md"
        generate_stage2_report(results, md_file)

        # Export optimal global configuration to JSON
        best_cfg = results[0]
        with open(args.best_output, "w", encoding="utf-8") as f:
            json.dump(best_cfg, f, indent=2)
        logger.info(f"Miglior configurazione globale salvata in: {args.best_output}")


if __name__ == "__main__":
    asyncio.run(main())
