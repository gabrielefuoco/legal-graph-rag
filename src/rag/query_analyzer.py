"""
Query Analyzer — Step 1 del Retrieval Engine.

Analizza la query dell'utente:
1. Matching TESEO (Aho-Corasick) per estrarre concetti semantici
2. Espansione BROADER/NARROWER via Neo4j (se disponibili)
3. Calcolo dell'embedding della query per la Vector Search
"""
import logging
from typing import List

from neo4j import AsyncDriver

from src.config import settings
from src.parsing.teseo_matcher import TESEOMatcher
from src.parsing.vector_engine import VectorEngine
from src.rag.models import RagState, AnalyzedQuery

logger = logging.getLogger(__name__)


class QueryAnalyzer:
    """
    Analizza e arricchisce la query dell'utente prima del retrieval.

    Utilizza il TESEOMatcher (Aho-Corasick) per estrarre concetti
    dal thesaurus del Senato e il VectorEngine per calcolare l'embedding.
    """

    def __init__(self, teseo_matcher: TESEOMatcher, vector_engine: VectorEngine, driver: AsyncDriver):
        self.teseo_matcher = teseo_matcher
        self.vector_engine = vector_engine
        self.driver = driver

    async def _expand_teseo_concepts(self, concept_ids: List[str]) -> List[str]:
        """
        Naviga le relazioni BROADER/NARROWER nel grafo Neo4j
        per espandere i concetti TESEO trovati.

        Se le relazioni non esistono, ritorna lista vuota (graceful).
        """
        if not concept_ids:
            return []

        expanded_labels = []

        try:
            async with self.driver.session() as session:
                # Cerca i figli (narrower) dei concetti trovati
                result = await session.run(
                    """
                    MATCH (child:TESEO_Concept)-[:BROADER]->(parent:TESEO_Concept)
                    WHERE parent.id IN $concept_ids
                    RETURN DISTINCT child.id AS id
                    """,
                    concept_ids=concept_ids,
                )
                async for record in result:
                    expanded_labels.append(record["id"])
        except Exception as e:
            # Le relazioni BROADER potrebbero non esistere nel grafo
            logger.debug(f"Espansione TESEO fallita (probabilmente assente): {e}")

        return expanded_labels


async def analyze_query(state: RagState) -> dict:
    """
    Nodo LangGraph: analizza la query e produce AnalyzedQuery + embedding.

    Legge: state["query"]
    Scrive: state["analyzed_query"], state["query_embedding"]
    """
    query = state["query"]
    analyzer: QueryAnalyzer = state["_analyzer"]  # Iniettato dall'engine

    # 1. Matching TESEO (Aho-Corasick sulla query)
    topics = analyzer.teseo_matcher.extract_topics(query)
    concept_ids = [t["teseo_id"] for t in topics]
    matched_labels = [t["label"] for t in topics]

    logger.info(f"TESEO match sulla query: {len(topics)} concetti trovati: {matched_labels}")

    # 2. Espansione BROADER/NARROWER
    expanded_ids = await analyzer._expand_teseo_concepts(concept_ids)
    all_concept_ids = list(set(concept_ids + expanded_ids))

    if expanded_ids:
        logger.info(f"Espansione TESEO: +{len(expanded_ids)} concetti (narrower terms)")

    # 3. Composizione expanded_query_text (per BM25)
    all_labels = matched_labels  # Le label espanse le usiamo come concept IDs per la graph search
    expanded_query_text = query
    if all_labels:
        expanded_query_text = f"{query} {' '.join(all_labels)}"

    analyzed = AnalyzedQuery(
        original_query=query,
        teseo_concept_ids=all_concept_ids,
        expanded_labels=all_labels,
        expanded_query_text=expanded_query_text,
    )

    # 4. Calcolo embedding della query
    try:
        embeddings = await analyzer.vector_engine.compute_embeddings_batch([query])
        query_embedding = embeddings[0]
    except Exception as e:
        logger.error(f"Errore nel calcolo embedding della query: {e}")
        query_embedding = None

    return {
        "analyzed_query": analyzed,
        "query_embedding": query_embedding,
    }
