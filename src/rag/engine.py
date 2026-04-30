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
from src.rag.query_analyzer import QueryAnalyzer, analyze_query
from src.rag.retriever import vector_search, bm25_search, graph_search
from src.rag.fusion import fuse_and_filter
from src.rag.expander import should_expand, expand_citations

logger = logging.getLogger(__name__)


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
    Costruisce il StateGraph LangGraph per il retrieval ibrido.

    Topologia:
        START → analyze_query → retrieve_all → fuse_and_filter
        fuse_and_filter → should_expand (condizionale)
            → expand_citations → fuse_and_filter  (ciclo)
            → END
    """
    builder = StateGraph(RagState)

    # Registrazione nodi
    builder.add_node("analyze_query", analyze_query)
    builder.add_node("retrieve_all", retrieve_all)
    builder.add_node("fuse_and_filter", fuse_and_filter)
    builder.add_node("expand_citations", expand_citations)

    # START → analyze_query
    builder.add_edge(START, "analyze_query")

    # Pipeline lineare: analyze_query → retrieve_all → fuse_and_filter
    builder.add_edge("analyze_query", "retrieve_all")
    builder.add_edge("retrieve_all", "fuse_and_filter")

    # Ciclo condizionale: fuse_and_filter → should_expand → expand o END
    builder.add_conditional_edges(
        "fuse_and_filter",
        should_expand,
        {
            "expand_citations": "expand_citations",
            "__end__": END,
        },
    )
    builder.add_edge("expand_citations", "fuse_and_filter")

    return builder


class RagEngine:
    """
    Facciata principale del Retrieval Engine.

    Inizializza le dipendenze (Neo4j, TESEOMatcher, VectorEngine),
    compila il grafo LangGraph e espone il metodo `retrieve()`.

    Esempio di utilizzo:
        engine = RagEngine()
        chunks = await engine.retrieve("Zone Logistiche Semplificate")
        for c in chunks:
            print(f"[{c.score:.3f}] ({c.source}) {c.text[:100]}")
        await engine.close()
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

        # Compila il grafo
        builder = _build_graph()
        self.graph = builder.compile()
        logger.info("RagEngine inizializzato: grafo LangGraph compilato.")

    async def retrieve(
        self,
        query: str,
        reference_date: str | None = None,
        top_k: int = 10,
        final_k: int = 5,
    ) -> list[RetrievedChunk]:
        """
        Esegue il retrieval ibrido per una data query.

        Args:
            query: Domanda dell'utente in linguaggio naturale.
            reference_date: Data di riferimento ISO (YYYY-MM-DD) per il filtro temporale.
                            Se None, non applica filtri temporali.
            top_k: Quanti risultati estrarre per ogni canale (es. 10 da vector, 10 da bm25).
            final_k: Quanti risultati finali mantenere dopo il merging RRF.

        Returns:
            Lista di RetrievedChunk ordinata per score RRF decrescente.
        """
        # Stato iniziale
        initial_state: RagState = {
            "query": query,
            "reference_date": reference_date,
            "top_k": top_k,
            "final_k": final_k,
            "analyzed_query": None,
            "query_embedding": None,
            "vector_results": [],
            "bm25_results": [],
            "graph_results": [],
            "fused_chunks": [],
            "hop_count": 0,
            "final_chunks": [],
            # Dipendenze iniettate nello state (prefisso _ = privato)
            "_driver": self.driver,
            "_analyzer": self.analyzer,
        }

        # Invocazione del grafo
        result = await self.graph.ainvoke(initial_state)

        # Ritorna final_chunks se popolato, altrimenti fused_chunks
        final = result.get("final_chunks") or result.get("fused_chunks") or []
        return final

    async def close(self):
        """Chiude le risorse (driver Neo4j)."""
        await self.driver.close()
        logger.info("RagEngine chiuso.")
