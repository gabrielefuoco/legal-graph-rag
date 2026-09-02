import pytest
from src.rag.models import RetrievedChunk
from scripts.grid_search_stage1 import (
    calculate_recall,
    calculate_hit_at_k,
    calculate_mrr,
    simulate_retrieval_and_rerank,
    evaluate_grid,
    Stage1Config,
    QueryCandidatePool
)

def test_metrics_calculation():
    chunks = [
        RetrievedChunk(text="t1", expression_id="id_1", score=0.9),
        RetrievedChunk(text="t2", expression_id="id_2", score=0.8),
        RetrievedChunk(text="t3", expression_id="id_3", score=0.7),
    ]
    expected = ["id_2", "id_3"]
    assert calculate_recall(chunks, expected) == 1.0
    assert calculate_hit_at_k(chunks, expected, k=1) == 0.0
    assert calculate_hit_at_k(chunks, expected, k=2) == 1.0
    assert calculate_mrr(chunks, expected) == 0.5  # id_2 is at rank 2 -> 1/2

def test_simulate_retrieval_and_rerank():
    pool = QueryCandidatePool(
        query="test query",
        expected_ids=["chunk_b"],
        vector_results=[
            RetrievedChunk(text="doc A", expression_id="chunk_a", score=0.9, source="vector"),
            RetrievedChunk(text="doc B", expression_id="chunk_b", score=0.7, source="vector"),
        ],
        bm25_results=[
            RetrievedChunk(text="doc B", expression_id="chunk_b", score=10.0, source="bm25"),
        ],
        graph_results=[],
        reranker_raw_scores={"chunk_a": 1.0, "chunk_b": 3.0},
        vector_anchor_id="chunk_a"
    )

    cfg = Stage1Config(
        weight_vector=1.0,
        weight_bm25=1.0,
        weight_graph=1.0,
        rrf_k=60,
        rerank_top_k=10,
        rerank_min_score=0.1,
        final_k=5
    )

    final_chunks = simulate_retrieval_and_rerank(pool, cfg, use_reranker=True)
    assert len(final_chunks) == 2
    # chunk_b has higher reranker raw score (3.0 vs 1.0), so should be rank 1
    assert final_chunks[0].expression_id == "chunk_b"

def test_evaluate_grid():
    pool = QueryCandidatePool(
        query="query 1",
        expected_ids=["target_1"],
        vector_results=[
            RetrievedChunk(text="other", expression_id="other", score=0.9, source="vector"),
            RetrievedChunk(text="target", expression_id="target_1", score=0.5, source="vector"),
        ],
        bm25_results=[
            RetrievedChunk(text="target", expression_id="target_1", score=12.0, source="bm25"),
        ],
        graph_results=[],
        reranker_raw_scores={"target_1": 4.0, "other": -1.0},
        vector_anchor_id="other"
    )

    configs = [
        Stage1Config(weight_vector=1.0, weight_bm25=1.0, weight_graph=1.0, rrf_k=60, rerank_top_k=5, rerank_min_score=0.1, final_k=2),
        Stage1Config(weight_vector=0.5, weight_bm25=1.5, weight_graph=1.0, rrf_k=60, rerank_top_k=5, rerank_min_score=0.1, final_k=2),
    ]

    results = evaluate_grid([pool], configs)
    assert len(results) == 2
    assert "composite_score" in results[0]
    assert "delta_recall_vs_vector" in results[0]
