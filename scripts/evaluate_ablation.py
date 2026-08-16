"""
Ablation Study — Script parametrico per la valutazione del GraphRAG.
Tutti i parametri architetturali vengono letti da src.config.settings.

Uso:
  py scripts/evaluate_ablation.py                    # Esegue tutte le query del dataset
  py scripts/evaluate_ablation.py --queries 3 11 13  # Esegue solo le query indicate (1-indexed)
  py scripts/evaluate_ablation.py --output results.csv  # Salva su file custom
"""
import asyncio
import json
import os
import time
import csv
import argparse
import numpy as np
import re
import sys
from typing import List, Dict

sys.path.append(r"C:\Users\gabri\APP\Università\Tesi")

from src.rag.engine import RagEngine
from src.parsing.vector_engine import VectorEngine
from src.config import settings
from src.logging_config import setup_logging
import logging

setup_logging()
logger = logging.getLogger("ablation_study")


def split_into_sentences(text: str) -> List[str]:
    """Split testuale grezzo in frasi."""
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    return [s.strip() for s in sentences if len(s.strip()) > 10]


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    v1 = np.array(vec1)
    v2 = np.array(vec2)
    norm = np.linalg.norm(v1) * np.linalg.norm(v2)
    if norm == 0:
        return 0.0
    return float(np.dot(v1, v2) / norm)


async def calculate_sentence_similarity(golden_text: str, generated_text: str, vector_engine: VectorEngine) -> float:
    """
    Misura la similarità semantica chunk-by-chunk tra la golden answer e la risposta generata.
    Calcola l'embedding per ogni frase, e per ogni frase della golden answer trova il match migliore
    nella risposta generata (ordine indipendente). Restituisce la media dei best match.
    """
    if not golden_text or not generated_text:
        return 0.0
        
    golden_sentences = split_into_sentences(golden_text)
    gen_sentences = split_into_sentences(generated_text)
    
    if not golden_sentences or not gen_sentences:
        return 0.0

    logger.info(f"Calcolo similarità su {len(golden_sentences)} frasi attese e {len(gen_sentences)} generate.")
    
    golden_embeddings = await vector_engine.compute_embeddings_batch(golden_sentences)
    gen_embeddings = await vector_engine.compute_embeddings_batch(gen_sentences)
    
    match_scores = []
    for g_emb in golden_embeddings:
        best_score = -1.0
        for gen_emb in gen_embeddings:
            score = cosine_similarity(g_emb, gen_emb)
            if score > best_score:
                best_score = score
        match_scores.append(best_score)
        
    # Media delle similarità massime per ogni concetto golden
    return sum(match_scores) / len(match_scores) if match_scores else 0.0


def calculate_recall(retrieved_chunks: list, expected_ids: List[str]) -> float:
    if not expected_ids:
        return 1.0 # Nessun target atteso = 100%
    
    retrieved_ids = [chunk.expression_id for chunk in retrieved_chunks]
    hits = 0
    for exp_id in expected_ids:
        if exp_id in retrieved_ids or f"merged_{exp_id}" in retrieved_ids:
            hits += 1
    return hits / len(expected_ids)


async def evaluate_query(engine: RagEngine, vector_engine: VectorEngine, query_data: dict) -> dict:
    query = query_data["query"]
    expected_ids = query_data.get("expected_ids", [])
    golden_answer = query_data.get("golden_answer", "")
    
    logger.info(f"\n=== Valutazione Query: '{query}' ===")
    
    # RUN A: Baseline RAG
    logger.info("-> RUN A: Baseline RAG")
    start_time_a = time.perf_counter()
    chunks_a, trace_a, answer_a = await engine.retrieve_with_trace(
        query=query,
        enable_graph_search=False,
        enable_multi_hop=False,
        enable_topological_expansion=False
    )
    time_a = time.perf_counter() - start_time_a
    
    recall_a = calculate_recall(chunks_a, expected_ids)
    sim_a = await calculate_sentence_similarity(golden_answer, answer_a, vector_engine)
    ctx_len_a = sum(len(getattr(c, 'expanded_text', None) or c.text) for c in chunks_a)
    
    # RUN B: GraphRAG
    logger.info("-> RUN B: GraphRAG")
    start_time_b = time.perf_counter()
    chunks_b, trace_b, answer_b = await engine.retrieve_with_trace(
        query=query,
        enable_graph_search=True,
        enable_multi_hop=True,
        enable_topological_expansion=True
    )
    time_b = time.perf_counter() - start_time_b
    
    recall_b = calculate_recall(chunks_b, expected_ids)
    sim_b = await calculate_sentence_similarity(golden_answer, answer_b, vector_engine)
    ctx_len_b = sum(len(getattr(c, 'expanded_text', None) or c.text) for c in chunks_b)
    
    return {
        "query": query,
        "baseline_time_s": round(time_a, 2),
        "graphrag_time_s": round(time_b, 2),
        "baseline_recall": round(recall_a, 2),
        "graphrag_recall": round(recall_b, 2),
        "baseline_sim_score": round(sim_a, 3),
        "graphrag_sim_score": round(sim_b, 3),
        "baseline_ctx_chars": ctx_len_a,
        "graphrag_ctx_chars": ctx_len_b,
        "baseline_chunks": len(chunks_a),
        "graphrag_chunks": len(chunks_b)
    }


def log_config():
    """Logga la configurazione architetturale usata per questa run."""
    logger.info("=" * 60)
    logger.info("CONFIGURAZIONE ARCHITETTURALE")
    logger.info("=" * 60)
    logger.info(f"  LLM Generativo:        {settings.GENERATIVE_MODEL_NAME}")
    logger.info(f"  Embedding Model:       {settings.EMBEDDING_MODEL_NAME}")
    logger.info(f"  Reranker Model:        {settings.RERANKER_MODEL_NAME}")
    logger.info(f"  Generator num_ctx:     {settings.GENERATOR_NUM_CTX}")
    logger.info(f"  Supervisor num_ctx:    {settings.SUPERVISOR_NUM_CTX}")
    logger.info(f"  Stuff Threshold:       {settings.GENERATOR_STUFF_THRESHOLD}")
    logger.info(f"  Topo Max Chars:        {settings.TOPOLOGICAL_MAX_CHARS}")
    logger.info(f"  Topo Expand NEXT:      {settings.TOPOLOGICAL_EXPAND_NEXT}")
    logger.info(f"  Topo Expand PARENT:    {settings.TOPOLOGICAL_EXPAND_PARENT}")
    logger.info(f"  Topo Expand CITES:     {settings.TOPOLOGICAL_EXPAND_CITES}")
    logger.info(f"  RRF Weights (V/B/G):   {settings.RRF_WEIGHT_VECTOR}/{settings.RRF_WEIGHT_BM25}/{settings.RRF_WEIGHT_GRAPH}")
    logger.info(f"  Rerank Min Score:      {settings.RERANK_MIN_SCORE}")
    logger.info(f"  RAG Top K:             {settings.RAG_TOP_K}")
    logger.info(f"  Max Citation Hops:     {settings.MAX_CITATION_HOPS}")
    logger.info(f"  TESEO Dense Threshold: {settings.TESEO_DENSE_THRESHOLD}")
    logger.info(f"  Max Agentic Iterations:{settings.MAX_AGENTIC_ITERATIONS}")
    logger.info("=" * 60)


async def main():
    parser = argparse.ArgumentParser(description="Ablation Study parametrico per GraphRAG")
    parser.add_argument("--queries", nargs="+", type=int, default=None,
                        help="Indici delle query da testare (1-indexed). Es: --queries 3 11 13")
    parser.add_argument("--output", type=str, default="ablation_results.csv",
                        help="Path del CSV di output (default: ablation_results.csv)")
    parser.add_argument("--dataset", type=str, default="data/eval_dataset.json",
                        help="Path del dataset di valutazione")
    args = parser.parse_args()

    dataset_path = args.dataset
    if not os.path.exists(dataset_path):
        logger.error(f"Dataset {dataset_path} non trovato. Impossibile procedere.")
        return
        
    with open(dataset_path, "r", encoding="utf-8") as f:
        full_dataset = json.load(f)
    
    # Filtra le query se specificato
    if args.queries:
        dataset = [full_dataset[i - 1] for i in args.queries if 1 <= i <= len(full_dataset)]
        logger.info(f"MINI-TEST: eseguendo {len(dataset)} query su {len(full_dataset)} totali (indici: {args.queries})")
    else:
        dataset = full_dataset
        logger.info(f"FULL-TEST: eseguendo tutte le {len(dataset)} query")
    
    # Logga la configurazione
    log_config()
        
    engine = RagEngine()
    vector_engine = VectorEngine()
    
    csv_path = args.output
    file_initialized = False
    
    results = []
    for i, data in enumerate(dataset, 1):
        logger.info(f"\n--- Query {i}/{len(dataset)} ---")
        res = await evaluate_query(engine, vector_engine, data)
        results.append(res)
        
        # Salvataggio incrementale ad ogni iterazione
        mode = "a" if file_initialized else "w"
        with open(csv_path, mode, newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=res.keys())
            if not file_initialized:
                writer.writeheader()
                file_initialized = True
            writer.writerow(res)
        
    await engine.close()
    
    if results:
        # Metriche aggregate
        b_recalls = [r["baseline_recall"] for r in results]
        g_recalls = [r["graphrag_recall"] for r in results]
        b_sims = [r["baseline_sim_score"] for r in results]
        g_sims = [r["graphrag_sim_score"] for r in results]
        
        avg_b_rec = sum(b_recalls)/len(b_recalls)
        avg_g_rec = sum(g_recalls)/len(g_recalls)
        avg_b_sim = sum(b_sims)/len(b_sims)
        avg_g_sim = sum(g_sims)/len(g_sims)

        logger.info("\n" + "=" * 60)
        logger.info("RIEPILOGO ABLATION STUDY")
        logger.info("=" * 60)
        logger.info(f"  Query valutate:        {len(results)}")
        logger.info(f"  Baseline Recall:       {avg_b_rec:.2%}")
        logger.info(f"  GraphRAG Recall:       {avg_g_rec:.2%}")
        logger.info(f"  Baseline Sim media:    {avg_b_sim:.4f}")
        logger.info(f"  GraphRAG Sim media:    {avg_g_sim:.4f}")
        logger.info(f"  Risultati salvati in:  {csv_path}")
        logger.info("=" * 60)

        # Generazione report Markdown leggibile
        md_path = os.path.splitext(csv_path)[0] + ".md"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("# Report Studio di Ablazione — GraphRAG vs Baseline\n\n")
            f.write("## Riepilogo Aggregato\n")
            f.write(f"- **Query valutate**: {len(results)}\n")
            f.write(f"- **Recall media**: Baseline **{avg_b_rec:.2%}** | GraphRAG **{avg_g_rec:.2%}**\n")
            f.write(f"- **Similarità Semantica media**: Baseline **{avg_b_sim:.4f}** | GraphRAG **{avg_g_sim:.4f}**\n\n")
            f.write("## Dettaglio Risultati per Query\n\n")
            f.write("| # | Query | Sim Baseline | Sim GraphRAG | Delta | Esito |\n")
            f.write("|---|---|---|---|---|---|\n")
            for idx, r in enumerate(results, 1):
                q_short = r["query"] if len(r["query"]) <= 50 else (r["query"][:47] + "...")
                b_s = r["baseline_sim_score"]
                g_s = r["graphrag_sim_score"]
                delta = round(g_s - b_s, 3)
                if delta > 0.01:
                    esito = "✅ Migliore"
                elif delta < -0.01:
                    esito = "⚠️ Peggiore"
                else:
                    esito = "➖ Parità"
                f.write(f"| {idx} | {q_short} | {b_s:.3f} | {g_s:.3f} | {delta:+.3f} | {esito} |\n")
        logger.info(f"  Report Markdown in:    {md_path}")


if __name__ == "__main__":
    asyncio.run(main())
