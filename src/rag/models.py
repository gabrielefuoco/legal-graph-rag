"""Data models for the Retrieval Engine.

Defines the LangGraph State (RagState) and output DTOs (RetrievedChunk, AnalyzedQuery).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import TypedDict, Any


@dataclass
class RetrievedChunk:
    """Data transfer object representing a single chunk retrieved from the knowledge graph."""

    text: str                                  # Expression text
    expression_id: str                         # Expression node ID in Neo4j
    work_urn: str | None = None                # Parent Work URN
    structural_context: str = ""               # Structural breadcrumb, e.g. "articolo - Art. 5"
    score: float = 0.0                         # RRF post-fusion or rerank score
    source: str = ""                           # "vector" | "bm25" | "graph" | "citation_hop"
    vigenza_start: date | None = None
    vigenza_end: date | None = None
    metadata: dict[str, Any] = field(default_factory=dict)  # Additional metadata (e.g., retrieval reasoning)
    expansion_trace: dict | None = None        # Traversed graph edges for XAI trace
    expanded_text: str | None = None           # Expanded text (original + topological context)


@dataclass
class AnalyzedQuery:
    """Analysis result for a user query including ontology concepts and expansions."""

    original_query: str                                       # Original query text
    teseo_concept_ids: list[str] = field(default_factory=list) # Matched TESEO concept IDs
    expanded_labels: list[str] = field(default_factory=list)   # Expanded narrower/broader taxonomy labels
    expanded_query_text: str = ""                              # Enriched query for BM25 search
    reranker_instruction: str = ""                             # Task-aware reranker instruction


class RagState(TypedDict):
    """Shared state across all nodes of the LangGraph retrieval graph.

    Each node reads its required inputs and writes its outputs.
    Channel fields are segregated to prevent conflicts during parallel execution.
    """

    # Input parameters
    query: str
    iterations: int                          # Agentic loop iteration count
    rewritten_queries: list[str]             # History of reformulated queries
    reference_date: str | None               # Optional ISO date (YYYY-MM-DD) for validity filtering
    top_k: int                               # Number of candidate results extracted per channel
    final_k: int                             # Target number of final results after reranking
    enable_graph_search: bool                # Flag to toggle TESEO-based graph search
    enable_multi_hop: bool                   # Flag to toggle multi-hop citation traversal
    max_citation_hops: int                   # Maximum citation hops allowed
    enable_topological_expansion: bool       # Flag to toggle topological context expansion
    topo_max_chars: int                      # Maximum character budget for topological expansion
    generator_num_ctx: int                   # Context window size for generation LLM
    skip_generation: bool                    # Flag to bypass the final generation node
    chat_history: list[dict[str, str]] | None # Conversation history for multi-turn chat

    # Step 1: Query Analysis
    analyzed_query: AnalyzedQuery | None
    query_embedding: list[float] | None

    # Step 2: Retrieval channels
    vector_results: list[RetrievedChunk]
    bm25_results: list[RetrievedChunk]
    graph_results: list[RetrievedChunk]

    # Step 3: Fusion
    fused_chunks: list[RetrievedChunk]
    vector_anchor_id: str | None

    # Step 4: Expansion and grading
    hop_count: int
    final_chunks: list[RetrievedChunk]
    expanded_chunks: list[RetrievedChunk]    # Chunks enriched by topological expansion

    # Step 5: Generation
    generation: str | None

    # Injected internal dependencies
    _driver: Any      # AsyncDriver (Neo4j)
    _analyzer: Any    # QueryAnalyzer
    _reranker: Any    # Reranker
    _llm: Any         # LegalGenerator
    _grader: Any      # RetrievalGrader
    _rewriter: Any    # QueryRewriter

