"""Retriever module for multi-channel legal information retrieval.

Provides three independent retrieval nodes executed concurrently:
1. vector_search: Cosine similarity over Neo4j vector index
2. bm25_search: Full-text Lucene search over Neo4j text index
3. graph_search: Topic-based graph traversal over TESEO ontology nodes
"""
import logging
from datetime import date
from typing import Optional

from neo4j import AsyncDriver

from src.config import settings
from src.rag.models import RagState, RetrievedChunk

logger = logging.getLogger(__name__)


def _parse_date(value) -> date | None:
    """Convert a Neo4j date value to a standard Python date object."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date(value.year, value.month, value.day)
    except Exception:
        return None


def _build_breadcrumb(record) -> str:
    """Build a structural breadcrumb trail from a Cypher record (e.g. 'Legge 25 gen 1904 > Titolo II > Art. 38')."""
    parts = []
    work_title = record.get("work_title")
    if work_title:
        # Truncate overly long act titles
        if len(work_title) > 60:
            work_title = work_title[:57] + "..."
        parts.append(work_title)
    
    # Breadcrumb path returned by Cypher query
    breadcrumb_path = record.get("breadcrumb_path")
    if breadcrumb_path:
        parts.extend(breadcrumb_path)
    
    structural_tag = record.get("structural_tag")
    if structural_tag and structural_tag not in parts:
        parts.append(structural_tag)
    
    return " > ".join(parts) if parts else "articolo"


def _record_to_chunk(record, source: str) -> RetrievedChunk:
    """Convert a Cypher record into a RetrievedChunk object with URN fallback heuristics."""
    metadata = {}
    if "matched_concept" in record:
        metadata["matched_concepts"] = [record["matched_concept"]]
    if "matched_concepts" in record:
        metadata["matched_concepts"] = record["matched_concepts"]
    
    work_urn = record.get("work_urn")
    expression_id = record.get("expression_id") or ""
    
    # Fallback heuristic: search for Italian URN NIR pattern in chunk text if missing
    if not work_urn or work_urn == "urn:unknown":
        import re
        urn_match = re.search(r"urn:nir:[a-z0-9\.\-;~:]+", record.get("text") or "", re.IGNORECASE)
        if urn_match:
            work_urn = urn_match.group(0)
    
    # Store act title in metadata if available
    if record.get("work_title"):
        metadata["work_title"] = record.get("work_title")

    # Construct structural breadcrumb
    breadcrumb = _build_breadcrumb(record)

    return RetrievedChunk(
        text=record.get("text") or "",
        expression_id=expression_id,
        work_urn=work_urn or "urn:unknown",
        structural_context=breadcrumb,
        score=float(record.get("score") or 0.0),
        source=source,
        vigenza_start=_parse_date(record.get("vigenza_start")),
        vigenza_end=_parse_date(record.get("vigenza_end")),
        metadata=metadata
    )


async def vector_search(state: RagState) -> dict:
    """LangGraph node: execute cosine similarity search over Neo4j vector index."""
    import time
    start = time.perf_counter()
    driver: AsyncDriver = state["_driver"]
    query_embedding = state.get("query_embedding")
    top_k = settings.RAG_TOP_K

    if not query_embedding:
        logger.warning("Nessun embedding calcolato, skip vector search.")
        return {"vector_results": []}

    chunks = []
    min_score = settings.RAG_VECTOR_MIN_SCORE

    try:
        async with driver.session() as session:
            result = await session.run(
                """
                CALL db.index.vector.queryNodes('expression_embedding_vector', $top_k, $query_vector)
                YIELD node AS e, score
                WHERE score >= $min_score
                OPTIONAL MATCH (e)-[:PART_OF*1..2]->(w:Work)
                OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
                // Breadcrumb: traverse PART_OF hierarchy for structural context
                OPTIONAL MATCH path = (e)-[:PART_OF*1..4]->(w)
                WITH e, w, u, score,
                     [n IN nodes(path) WHERE n:StructuralUnit | n.tag_name] AS breadcrumb_path
                RETURN e.text_display AS text,
                       e.id AS expression_id,
                       w.urn AS work_urn,
                       w.title AS work_title,
                       u.tag_name AS structural_tag,
                       e.vigenza_start AS vigenza_start,
                       e.vigenza_end AS vigenza_end,
                       breadcrumb_path,
                       score
                """,
                top_k=top_k,
                query_vector=query_embedding,
                min_score=min_score,
            )
            async for record in result:
                chunks.append(_record_to_chunk(record, source="vector"))
    except Exception as e:
        logger.error(f"Errore nella Vector Search: {e}")

    elapsed = time.perf_counter() - start
    logger.info(f"  └─ Vector: {len(chunks)} risultati in {elapsed:.2f}s (soglia ≥ {min_score})")
    return {"vector_results": chunks}


async def bm25_search(state: RagState) -> dict:
    """LangGraph node: execute BM25 full-text keyword search over Neo4j index."""
    import time
    start = time.perf_counter()
    driver: AsyncDriver = state["_driver"]
    analyzed = state.get("analyzed_query")
    top_k = settings.RAG_TOP_K

    if not analyzed:
        logger.warning("Query non analizzata, skip BM25 search.")
        return {"bm25_results": []}

    # Use expanded query for BM25 matching
    query_text = analyzed.expanded_query_text or analyzed.original_query

    # Escape Lucene special syntax characters
    query_text_escaped = _escape_lucene(query_text)

    if not query_text_escaped.strip():
        return {"bm25_results": []}

    chunks = []

    try:
        async with driver.session() as session:
            result = await session.run(
                """
                CALL db.index.fulltext.queryNodes('expression_text_fulltext', $query_text)
                YIELD node AS e, score
                OPTIONAL MATCH (e)-[:PART_OF*1..2]->(w:Work)
                OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
                // Breadcrumb: traverse PART_OF hierarchy for structural context
                OPTIONAL MATCH path = (e)-[:PART_OF*1..4]->(w)
                WITH e, w, u, score,
                     [n IN nodes(path) WHERE n:StructuralUnit | n.tag_name] AS breadcrumb_path
                RETURN e.text_display AS text,
                       e.id AS expression_id,
                       w.urn AS work_urn,
                       w.title AS work_title,
                       u.tag_name AS structural_tag,
                       e.vigenza_start AS vigenza_start,
                       e.vigenza_end AS vigenza_end,
                       breadcrumb_path,
                       score
                LIMIT $top_k
                """,
                query_text=query_text_escaped,
                top_k=top_k,
            )
            async for record in result:
                chunks.append(_record_to_chunk(record, source="bm25"))
    except Exception as e:
        logger.error(f"Errore nella BM25 Search: {e}")

    elapsed = time.perf_counter() - start
    logger.info(f"  └─ BM25: {len(chunks)} risultati in {elapsed:.2f}s")
    return {"bm25_results": chunks}


def _escape_lucene(text: str) -> str:
    """Escape special Lucene syntax characters to prevent query parsing errors."""
    special_chars = r'+-&|!(){}[]^"~*?:\/'
    escaped = []
    for char in text:
        if char in special_chars:
            escaped.append(f"\\{char}")
        else:
            escaped.append(char)
    return "".join(escaped)


async def graph_search(state: RagState) -> dict:
    """LangGraph node: retrieve provisions linked to matched TESEO ontology concepts."""
    import time
    start = time.perf_counter()
    driver: AsyncDriver = state["_driver"]
    analyzed = state.get("analyzed_query")
    top_k = settings.RAG_TOP_K

    # Skip graph retrieval if disabled
    if not state.get("enable_graph_search", True):
        logger.info("Ricerca su grafo disabilitata tramite flag (enable_graph_search=False). Skip graph_search.")
        return {"graph_results": []}

    if not analyzed or not analyzed.teseo_concept_ids:
        logger.info("Nessun concetto TESEO trovato nella query, skip graph search.")
        return {"graph_results": []}

    logger.info(f"  └─ Graph: cercando {len(analyzed.teseo_concept_ids)} concetti TESEO: {analyzed.teseo_concept_ids[:5]}")
    chunks = []

    try:
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (e:Expression)-[r:HAS_TOPIC]->(t:TESEO_Concept)
                WHERE t.id IN $teseo_ids
                OPTIONAL MATCH (e)-[:PART_OF*1..2]->(w:Work)
                OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
                // Structural breadcrumb
                OPTIONAL MATCH path = (e)-[:PART_OF*1..4]->(w)
                WITH e, w, u,
                     MAX(r.score) AS score,
                     COLLECT(DISTINCT t.id) AS matched_concepts,
                     [n IN nodes(path) WHERE n:StructuralUnit | n.tag_name] AS breadcrumb_path
                RETURN e.text_display AS text,
                       e.id AS expression_id,
                       w.urn AS work_urn,
                       w.title AS work_title,
                       u.tag_name AS structural_tag,
                       e.vigenza_start AS vigenza_start,
                       e.vigenza_end AS vigenza_end,
                       breadcrumb_path,
                       score,
                       matched_concepts
                ORDER BY score DESC
                LIMIT $top_k
                """,
                teseo_ids=analyzed.teseo_concept_ids,
                top_k=top_k,
            )
            async for record in result:
                chunk = _record_to_chunk(record, source="graph")
                # Fallback: assign matched query labels if matched_concepts list is empty
                if not chunk.metadata.get("matched_concepts"):
                    chunk.metadata["matched_concepts"] = analyzed.expanded_labels
                chunks.append(chunk)
    except Exception as e:
        logger.error(f"Errore nella Graph Search: {e}")

    elapsed = time.perf_counter() - start
    logger.info(f"  └─ Graph: {len(chunks)} risultati in {elapsed:.2f}s")
    return {"graph_results": chunks}
