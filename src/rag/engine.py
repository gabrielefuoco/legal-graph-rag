"""
RAG Engine — Orchestrazione del Retrieval Engine con LangGraph.

Costruisce il StateGraph e fornisce la facciata pubblica `RagEngine.retrieve()`.

Flusso del grafo:
    START → analyze_query → [vector_search, bm25_search, graph_search] (fan-out)
         → fuse_and_filter → should_expand? → expand_citations ↺ → END
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
from src.rag.evaluator import (
    RetrievalGrader,
    QueryRewriter,
    grade_documents_node,
    rewrite_query_node
)

logger = logging.getLogger(__name__)


class RAGResult(list):
    """
    Wrapper retrocompatibile che si comporta come una lista di RetrievedChunk,
    ma espone l'attributo .answer per contenere la risposta generata dall'LLM.
    """
    def __init__(self, chunks, answer: str | None = None):
        super().__init__(chunks)
        self.answer = answer



def fallback_generation(state: RagState) -> dict:
    """Nodo che ritorna un messaggio predefinito se nessun documento supera il filtro."""
    logger.info("Esecuzione fallback_generation: nessun documento disponibile.")
    return {
        "generation": "Non dispongo di informazioni sufficienti per rispondere a questa domanda."
    }


def post_grading_router(state: RagState) -> str:
    """Controlla la pertinenza dei documenti e, in caso negativo, il limite di iterazioni."""
    chunks = state.get("final_chunks")
    
    if chunks:
        if state.get("skip_generation"):
            return "__end__"
        return "generate"
    
    iterations = state.get("iterations", 0)
    if iterations < settings.MAX_AGENTIC_ITERATIONS:
        return "rewrite_query"
        
    if state.get("skip_generation"):
        return "__end__"
    return "fallback_generation"


async def retrieve_all(state: RagState) -> dict:
    """
    Nodo LangGraph che esegue i tre canali di retrieval in parallelo.
    Sostituisce il fan-out/fan-in manuale per evitare problemi di sincronizzazione.
    """
    import asyncio
    
    # Esegue i tre nodi in parallelo
    results = await asyncio.gather(
        vector_search(state),
        bm25_search(state),
        graph_search(state)
    )
    
    # Unisce i dizionari di output
    combined = {}
    for r in results:
        combined.update(r)
    return combined


def _build_graph() -> StateGraph:
    """
    Costruisce il StateGraph LangGraph per il retrieval ibrido + generazione + loop agentico.

    Topologia aggiornata Fase 9:
        START → contextualize_query → analyze_query → retrieve_all → fuse_and_filter → rerank
        rerank → should_expand (condizionale)
            → expand_citations → fuse_and_filter (ciclo)
            → grade_documents (evaluator chunk-by-chunk)
                 → check_relevance (router)
                     → SI: generate → END
                     → NO: check_iterations (router)
                         → SI (iterations < max): rewrite_query → analyze_query (ciclo)
                         → NO (iterations >= max): fallback_generation → END
    """
    builder = StateGraph(RagState)

    # Registrazione nodi
    builder.add_node("contextualize_query", contextualize_query)
    builder.add_node("analyze_query", analyze_query)
    builder.add_node("retrieve_all", retrieve_all)
    builder.add_node("fuse_and_filter", fuse_and_filter)
    builder.add_node("rerank", rerank_node)
    builder.add_node("expand_citations", expand_citations)
    builder.add_node("grade_documents", grade_documents_node)
    builder.add_node("rewrite_query", rewrite_query_node)
    builder.add_node("generate", generation_node)
    builder.add_node("fallback_generation", fallback_generation)

    # Flusso iniziale
    builder.add_edge(START, "contextualize_query")
    builder.add_edge("contextualize_query", "analyze_query")
    builder.add_edge("analyze_query", "retrieve_all")
    builder.add_edge("retrieve_all", "fuse_and_filter")
    builder.add_edge("fuse_and_filter", "rerank")

    # Ciclo condizionale espansione citazioni
    builder.add_conditional_edges(
        "rerank",
        should_expand,
        {
            "expand_citations": "expand_citations",
            "__end__": "grade_documents",
        },
    )
    builder.add_edge("expand_citations", "fuse_and_filter")

    # Routing post-grading (Self-Reflective Loop)
    builder.add_conditional_edges(
        "grade_documents",
        post_grading_router,
        {
            "generate": "generate",
            "rewrite_query": "rewrite_query",
            "fallback_generation": "fallback_generation",
            "__end__": END,
        }
    )
    
    # Rewriter riporta ad analyze_query
    builder.add_edge("rewrite_query", "analyze_query")

    # Nodi finali
    builder.add_edge("generate", END)
    builder.add_edge("fallback_generation", END)

    return builder


class RagEngine:
    """
    Facciata principale del RAG Engine.
    """

    def __init__(self):
        # Dipendenze
        self.driver = AsyncGraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
        )
        self.vector_engine = VectorEngine()
        self.teseo_matcher = TESEOMatcher(settings.TESEO_RDF_PATH)
        self.analyzer = QueryAnalyzer(
            teseo_matcher=self.teseo_matcher,
            vector_engine=self.vector_engine,
            driver=self.driver,
        )
        self.reranker = Reranker()
        self.generator = LegalGenerator()
        self.grader = RetrievalGrader()
        self.rewriter = QueryRewriter()

        # Compila il grafo
        builder = _build_graph()
        self.graph = builder.compile()
        logger.info("RagEngine inizializzato: grafo LangGraph compilato.")

    async def retrieve(
        self,
        query: str,
        reference_date: str | None = None,
        top_k: int = 15,
        final_k: int = 5,
        enable_graph_search: bool = True,
        enable_multi_hop: bool = True,
        chat_history: list[dict[str, str]] | None = None,
    ) -> RAGResult:
        """
        Esegue il retrieval ibrido e la generazione per una data query.
        """
        # Stato iniziale
        initial_state: RagState = {
            "query": query,
            "reference_date": reference_date,
            "top_k": top_k,
            "final_k": final_k,
            "enable_graph_search": enable_graph_search,
            "enable_multi_hop": enable_multi_hop,
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
            "generation": None,
            # Dipendenze iniettate
            "_driver": self.driver,
            "_analyzer": self.analyzer,
            "_reranker": self.reranker,
            "_llm": self.generator,
            "_grader": self.grader,
            "_rewriter": self.rewriter,
        }

        # Esecuzione del grafo
        result = await self.graph.ainvoke(initial_state)

        # Ritorna i chunk e l'answer tramite RAGResult
        final = result.get("final_chunks") or result.get("fused_chunks") or []
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
        chat_history: list[dict[str, str]] | None = None,
        skip_generation: bool = False
    ) -> tuple[list[RetrievedChunk], dict, str | None]:
        """
        Esegue il retrieval ibrido e ritorna i chunk, la traccia XAI e opzionalmente la generazione.
        Se skip_generation=True, bypassa il nodo 'generate' per permettere lo streaming esterno.
        """
        initial_state: RagState = {
            "query": query,
            "reference_date": reference_date,
            "top_k": top_k,
            "final_k": final_k,
            "enable_graph_search": enable_graph_search,
            "enable_multi_hop": enable_multi_hop,
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
            "_driver": self.driver,
            "_analyzer": self.analyzer,
            "_reranker": self.reranker,
            "_llm": self.generator,
            "_grader": self.grader,
            "_rewriter": self.rewriter,
        }

        # Esecuzione del grafo. Se skip_generation=True, si fermerà prima della generazione.
        result = await self.graph.ainvoke(initial_state)
        
        final_chunks = result.get("final_chunks") or result.get("fused_chunks") or []
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
            "query": result.get("query", query) # La query finale riscritta
        }

        return final_chunks, trace, generation

    async def close(self):
        """Chiude le risorse (driver Neo4j)."""
        await self.driver.close()
        logger.info("RagEngine chiuso.")
