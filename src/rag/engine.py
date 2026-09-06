"""RAG Engine — LangGraph orchestration for hybrid retrieval and generation.

Builds the StateGraph and provides the public interface `RagEngine.retrieve()`.

Graph flow:
    START -> contextualize_query -> analyze_query -> retrieve_all -> fuse_and_filter -> rerank
    rerank -> should_expand? -> expand_citations (loop) -> grade_documents
    grade_documents -> [topological_expand -> generate, rewrite_query -> analyze_query, fallback_generation] -> END
"""
import logging

from langgraph.graph import StateGraph, START, END
from neo4j import AsyncGraphDatabase

from src.config import settings
from src.parsing.teseo_matcher import TESEOMatcher
from src.parsing.vector_engine import VectorEngine
from src.rag.models import RagState, RetrievedChunk, AnalyzedQuery
from src.rag.query_analyzer import QueryAnalyzer, analyze_query, contextualize_query
from src.rag.retriever import vector_search, bm25_search, graph_search
from src.rag.fusion import fuse_and_filter
from src.rag.reranker import Reranker, rerank_node
from src.rag.expander import should_expand, expand_citations
from src.rag.generator import LegalGenerator, generation_node
from src.rag.topology_expander import topological_expand_node
from src.rag.evaluator import (
    RetrievalGrader,
    QueryRewriter,
    grade_documents_node,
    rewrite_query_node
)

logger = logging.getLogger(__name__)


class RAGResult(list):
    """Backward-compatible list wrapper of RetrievedChunk with an answer attribute."""
    def __init__(self, chunks, answer: str | None = None):
        super().__init__(chunks)
        self.answer = answer


def fallback_generation(state: RagState) -> dict:
    """Return a default message when no retrieved documents pass the grader."""
    logger.info("Esecuzione fallback_generation: nessun documento disponibile.")
    return {
        "generation": "Non dispongo di informazioni sufficienti per rispondere a questa domanda."
    }


def post_grading_router(state: RagState) -> str:
    """Route based on document grading relevance and agentic iteration limits."""
    chunks = state.get("final_chunks")
    
    if chunks:
        return "topological_expand"
    
    iterations = state.get("iterations", 0)
    if iterations < settings.MAX_AGENTIC_ITERATIONS:
        return "rewrite_query"
        
    if state.get("skip_generation"):
        return "__end__"
    return "fallback_generation"


def post_expansion_router(state: RagState) -> str:
    """Route after topological expansion to generation or graph termination."""
    if state.get("skip_generation"):
        return "__end__"
    return "generate"


async def retrieve_all(state: RagState) -> dict:
    """Execute vector, BM25, and graph retrieval channels concurrently."""
    import asyncio
    
    # Run all three retrieval branches in parallel
    results = await asyncio.gather(
        vector_search(state),
        bm25_search(state),
        graph_search(state)
    )
    
    # Combine output dictionaries
    combined = {}
    for r in results:
        combined.update(r)
    return combined


def _build_graph() -> StateGraph:
    """Build the LangGraph StateGraph for hybrid retrieval, generation, and agentic loop."""
    builder = StateGraph(RagState)

    # Node registrations
    builder.add_node("contextualize_query", contextualize_query)
    builder.add_node("analyze_query", analyze_query)
    builder.add_node("retrieve_all", retrieve_all)
    builder.add_node("fuse_and_filter", fuse_and_filter)
    builder.add_node("rerank", rerank_node)
    builder.add_node("expand_citations", expand_citations)
    builder.add_node("grade_documents", grade_documents_node)
    builder.add_node("topological_expand", topological_expand_node)
    builder.add_node("rewrite_query", rewrite_query_node)
    builder.add_node("generate", generation_node)
    builder.add_node("fallback_generation", fallback_generation)

    # Initial pipeline flow
    builder.add_edge(START, "contextualize_query")
    builder.add_edge("contextualize_query", "analyze_query")
    builder.add_edge("analyze_query", "retrieve_all")
    builder.add_edge("retrieve_all", "fuse_and_filter")
    builder.add_edge("fuse_and_filter", "rerank")

    # Conditional citation expansion loop
    builder.add_conditional_edges(
        "rerank",
        should_expand,
        {
            "expand_citations": "expand_citations",
            "__end__": "grade_documents",
        },
    )
    builder.add_edge("expand_citations", "fuse_and_filter")

    # Self-reflective grading router
    builder.add_conditional_edges(
        "grade_documents",
        post_grading_router,
        {
            "topological_expand": "topological_expand",
            "rewrite_query": "rewrite_query",
            "fallback_generation": "fallback_generation",
            "__end__": END,
        }
    )
    
    # Post-expansion routing
    builder.add_conditional_edges(
        "topological_expand",
        post_expansion_router,
        {
            "generate": "generate",
            "__end__": END,
        }
    )
    
    # Loop rewritten query back to analyzer
    builder.add_edge("rewrite_query", "analyze_query")

    # Terminal edges
    builder.add_edge("generate", END)
    builder.add_edge("fallback_generation", END)

    return builder


class RagEngine:
    """Main facade for the RAG retrieval and generation engine."""

    def __init__(self):
        # Dependencies
        self.vector_engine = VectorEngine()
        self.teseo_matcher = TESEOMatcher(settings.TESEO_RDF_PATH)
        self.analyzer = QueryAnalyzer(
            teseo_matcher=self.teseo_matcher,
            vector_engine=self.vector_engine,
        )
        self.reranker = Reranker()
        self.generator = LegalGenerator()
        self.grader = RetrievalGrader()
        self.rewriter = QueryRewriter()

        # Compile LangGraph graph
        builder = _build_graph()
        self.graph = builder.compile()
        logger.info("RagEngine inizializzato: grafo LangGraph compilato.")
        self._warmed_up = False

    async def _warmup(self):
        """Run a dummy embedding to warm up the embedding model and TESEO matcher."""
        if self._warmed_up:
            return
        logger.info("[WARMUP] Avvio warmup del modello di embedding...")
        try:
            await self.analyzer.vector_engine.compute_embeddings_batch(["warmup"])
            logger.info("[WARMUP] Modello di embedding caricato in VRAM.")
            
            # Precompute semantic matching matrix for TESEO
            await self.teseo_matcher.precompute_embeddings(self.vector_engine)
        except Exception as e:
            logger.warning(f"[WARMUP] Fallito: {e}")
        finally:
            self._warmed_up = True

    async def retrieve(
        self,
        query: str,
        reference_date: str | None = None,
        top_k: int = 15,
        final_k: int = 5,
        enable_graph_search: bool = True,
        enable_multi_hop: bool = True,
        max_citation_hops: int = 1,
        enable_topological_expansion: bool = True,
        topo_max_chars: int | None = None,
        generator_num_ctx: int | None = None,
        chat_history: list[dict[str, str]] | None = None,
    ) -> RAGResult:
        """Execute hybrid retrieval and generation for a given query."""
        if not self._warmed_up:
            await self._warmup()
            
        _topo_max_chars = topo_max_chars if topo_max_chars is not None else settings.TOPOLOGICAL_MAX_CHARS
        _generator_num_ctx = generator_num_ctx if generator_num_ctx is not None else settings.GENERATOR_NUM_CTX

        # Initial state
        initial_state: RagState = {
            "query": query,
            "reference_date": reference_date,
            "top_k": top_k,
            "final_k": final_k,
            "enable_graph_search": enable_graph_search,
            "enable_multi_hop": enable_multi_hop,
            "max_citation_hops": max_citation_hops,
            "enable_topological_expansion": enable_topological_expansion,
            "topo_max_chars": _topo_max_chars,
            "generator_num_ctx": _generator_num_ctx,
            "skip_generation": False,
            "chat_history": chat_history or [],
            "iterations": 0,
            "rewritten_queries": [],
            "analyzed_query": None,
            "query_embedding": None,
            "vector_results": [],
            "bm25_results": [],
            "graph_results": [],
            "fused_chunks": [],
            "hop_count": 0,
            "final_chunks": [],
            "expanded_chunks": [],
            "generation": None,
            # Injected dependencies
            "_analyzer": self.analyzer,
            "_reranker": self.reranker,
            "_llm": self.generator,
            "_grader": self.grader,
            "_rewriter": self.rewriter,
        }

        # Execute graph with session-scoped async driver
        import time
        start = time.perf_counter()
        
        async with AsyncGraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
        ) as driver:
            initial_state["_driver"] = driver
            result = await self.graph.ainvoke(initial_state)
            
        elapsed = time.perf_counter() - start
        logger.info(f"[PIPELINE] Esecuzione completa in {elapsed:.1f}s | Iterazioni: {result.get('iterations', 0)}")

        # Return chunks and generated answer wrapped in RAGResult
        final = result.get("expanded_chunks") or result.get("final_chunks") or result.get("fused_chunks") or []
        generation = result.get("generation")
        return RAGResult(final, answer=generation)

    async def retrieve_with_trace(
        self,
        query: str,
        reference_date: str | None = None,
        top_k: int = 15,
        final_k: int = 5,
        enable_graph_search: bool = True,
        enable_multi_hop: bool = True,
        max_citation_hops: int = 1,
        enable_topological_expansion: bool = True,
        topo_max_chars: int | None = None,
        generator_num_ctx: int | None = None,
        chat_history: list[dict[str, str]] | None = None,
        skip_generation: bool = False,
        status_callback=None
    ) -> tuple[list[RetrievedChunk], dict, str | None]:
        """Execute hybrid retrieval returning chunks, execution trace, and optional generation.
        
        If skip_generation is True, generation node is bypassed to allow external streaming.
        """
        if not self._warmed_up:
            await self._warmup()
            
        _topo_max_chars = topo_max_chars if topo_max_chars is not None else settings.TOPOLOGICAL_MAX_CHARS
        _generator_num_ctx = generator_num_ctx if generator_num_ctx is not None else settings.GENERATOR_NUM_CTX

        initial_state: RagState = {
            "query": query,
            "reference_date": reference_date,
            "top_k": top_k,
            "final_k": final_k,
            "enable_graph_search": enable_graph_search,
            "enable_multi_hop": enable_multi_hop,
            "max_citation_hops": max_citation_hops,
            "enable_topological_expansion": enable_topological_expansion,
            "topo_max_chars": _topo_max_chars,
            "generator_num_ctx": _generator_num_ctx,
            "skip_generation": skip_generation,
            "chat_history": chat_history or [],
            "iterations": 0,
            "rewritten_queries": [],
            "analyzed_query": None,
            "query_embedding": None,
            "vector_results": [],
            "bm25_results": [],
            "graph_results": [],
            "fused_chunks": [],
            "hop_count": 0,
            "final_chunks": [],
            "generation": None,
            "_analyzer": self.analyzer,
            "_reranker": self.reranker,
            "_llm": self.generator,
            "_grader": self.grader,
            "_rewriter": self.rewriter,
        }

        # Execute graph with event streaming
        import time
        start = time.perf_counter()
        
        result = dict(initial_state)
        
        async with AsyncGraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
        ) as driver:
            initial_state["_driver"] = driver
            async for output in self.graph.astream(initial_state):
                for node_name, state_update in output.items():
                    if status_callback:
                        status_callback(node_name, state_update)
                    result.update(state_update)
                
        elapsed = time.perf_counter() - start
        logger.info(f"[PIPELINE] Esecuzione completa in {elapsed:.1f}s | Iterazioni: {result.get('iterations', 0)}")
        
        final_chunks = result.get("expanded_chunks") or result.get("final_chunks") or result.get("fused_chunks") or []
        generation = result.get("generation")
        
        trace = {
            "hop_count": result.get("hop_count", 0),
            "iterations": result.get("iterations", 0),
            "rewritten_queries": result.get("rewritten_queries", []),
            "analyzed_query": result.get("analyzed_query"),
            "enable_graph_search": result.get("enable_graph_search"),
            "enable_multi_hop": result.get("enable_multi_hop"),
            "vector_results_count": len(result.get("vector_results") or []),
            "bm25_results_count": len(result.get("bm25_results") or []),
            "graph_results_count": len(result.get("graph_results") or []),
            "query": result.get("query", query)
        }

        return final_chunks, trace, generation

    async def close(self):
        """Release allocated engine resources."""
        logger.info("RagEngine chiuso.")
