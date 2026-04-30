"""
Retriever — Step 2 del Retrieval Engine.

Tre nodi LangGraph indipendenti che eseguono retrieval su canali diversi:
1. vector_search  — Cosine similarity sull'indice vettoriale Neo4j
2. bm25_search    — Full-text search sull'indice BM25 Neo4j
3. graph_search   — Navigazione dei topic TESEO nel grafo
"""
import logging
from datetime import date

from neo4j import AsyncDriver

from src.config import settings
from src.rag.models import RagState, RetrievedChunk

logger = logging.getLogger(__name__)


def _parse_date(value) -> date | None:
    """Converte un valore Neo4j date in Python date (gestisce None e neo4j.time.Date)."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    # neo4j.time.Date ha .year, .month, .day
    try:
        return date(value.year, value.month, value.day)
    except Exception:
        return None


def _record_to_chunk(record, source: str) -> RetrievedChunk:
    """Converte un record Cypher in un RetrievedChunk con euristiche per URN."""
    metadata = {}
    if "matched_concept" in record:
        metadata["matched_concepts"] = [record["matched_concept"]]
    if "matched_concepts" in record:
        metadata["matched_concepts"] = record["matched_concepts"]
    
    work_urn = record.get("work_urn")
    expression_id = record.get("expression_id") or ""
    
    # Eueristica: se l'URN è mancante, proviamo a cercarlo nel testo del chunk
    if not work_urn or work_urn == "urn:unknown":
        import re
        # Cerca pattern urn:nir: (standard italiano)
        urn_match = re.search(r"urn:nir:[a-z0-9\.\-;~:]+", record.get("text") or "", re.IGNORECASE)
        if urn_match:
            work_urn = urn_match.group(0)
    
    # Se abbiamo un titolo del Work, lo mettiamo nei metadati
    if record.get("work_title"):
        metadata["work_title"] = record.get("work_title")

    return RetrievedChunk(
        text=record.get("text") or "",
        expression_id=expression_id,
        work_urn=work_urn or "urn:unknown",
        structural_context=record.get("structural_tag") or "articolo",
        score=float(record.get("score") or 0.0),
        source=source,
        vigenza_start=_parse_date(record.get("vigenza_start")),
        vigenza_end=_parse_date(record.get("vigenza_end")),
        metadata=metadata
    )


# ---------------------------------------------------------------------------
# Nodo 1: Vector Search
# ---------------------------------------------------------------------------

async def vector_search(state: RagState) -> dict:
    """
    Nodo LangGraph: ricerca per similarità coseno sull'indice vettoriale.

    Legge: state["query_embedding"]
    Scrive: state["vector_results"]
    """
    driver: AsyncDriver = state["_driver"]
    query_embedding = state.get("query_embedding")
    top_k = settings.RAG_TOP_K

    if not query_embedding:
        logger.warning("Nessun embedding calcolato, skip vector search.")
        return {"vector_results": []}

    chunks = []

    try:
        async with driver.session() as session:
            result = await session.run(
                """
                CALL db.index.vector.queryNodes('expression_embedding_vector', $top_k, $query_vector)
                YIELD node AS e, score
                OPTIONAL MATCH (e)-[:PART_OF*1..2]->(w:Work)
                OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
                RETURN e.text_display AS text,
                       e.id AS expression_id,
                       w.urn AS work_urn,
                       w.title AS work_title,
                       u.tag_name AS structural_tag,
                       e.vigenza_start AS vigenza_start,
                       e.vigenza_end AS vigenza_end,
                       score
                """,
                top_k=top_k,
                query_vector=query_embedding,
            )
            async for record in result:
                chunks.append(_record_to_chunk(record, source="vector"))
    except Exception as e:
        logger.error(f"Errore nella Vector Search: {e}")

    logger.info(f"Vector Search: {len(chunks)} risultati")
    return {"vector_results": chunks}


# ---------------------------------------------------------------------------
# Nodo 2: BM25 Full-Text Search
# ---------------------------------------------------------------------------

async def bm25_search(state: RagState) -> dict:
    """
    Nodo LangGraph: ricerca keyword BM25 sull'indice full-text Neo4j.

    Usa l'indice 'expression_text_fulltext' già creato in neo4j_loader.
    La query è l'expanded_query_text (query originale + termini TESEO).

    Legge: state["analyzed_query"]
    Scrive: state["bm25_results"]
    """
    driver: AsyncDriver = state["_driver"]
    analyzed = state.get("analyzed_query")
    top_k = settings.RAG_TOP_K

    if not analyzed:
        logger.warning("Query non analizzata, skip BM25 search.")
        return {"bm25_results": []}

    # Usa la query espansa per il BM25
    query_text = analyzed.expanded_query_text or analyzed.original_query

    # Lucene richiede escape di caratteri speciali
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
                RETURN e.text_display AS text,
                       e.id AS expression_id,
                       w.urn AS work_urn,
                       w.title AS work_title,
                       u.tag_name AS structural_tag,
                       e.vigenza_start AS vigenza_start,
                       e.vigenza_end AS vigenza_end,
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

    logger.info(f"BM25 Search: {len(chunks)} risultati")
    return {"bm25_results": chunks}


def _escape_lucene(text: str) -> str:
    """Escape dei caratteri speciali Lucene per evitare errori di sintassi."""
    special_chars = r'+-&|!(){}[]^"~*?:\/'
    escaped = []
    for char in text:
        if char in special_chars:
            escaped.append(f"\\{char}")
        else:
            escaped.append(char)
    return "".join(escaped)


# ---------------------------------------------------------------------------
# Nodo 3: Graph Search (TESEO Topics)
# ---------------------------------------------------------------------------

async def graph_search(state: RagState) -> dict:
    """
    Nodo LangGraph: ricerca basata sulla navigazione dei topic TESEO.

    Trova le Expression collegate ai concetti TESEO estratti dalla query.

    Legge: state["analyzed_query"]
    Scrive: state["graph_results"]
    """
    driver: AsyncDriver = state["_driver"]
    analyzed = state.get("analyzed_query")
    top_k = settings.RAG_TOP_K

    if not analyzed or not analyzed.teseo_concept_ids:
        logger.info("Nessun concetto TESEO trovato nella query, skip graph search.")
        return {"graph_results": []}

    chunks = []

    try:
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (e:Expression)-[r:HAS_TOPIC]->(t:TESEO_Concept)
                WHERE t.id IN $teseo_ids
                OPTIONAL MATCH (e)-[:PART_OF*1..2]->(w:Work)
                OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
                RETURN e.text_display AS text,
                       e.id AS expression_id,
                       w.urn AS work_urn,
                       w.title AS work_title,
                       u.tag_name AS structural_tag,
                       e.vigenza_start AS vigenza_start,
                       e.vigenza_end AS vigenza_end,
                       MAX(r.score) AS score,
                       COLLECT(t.id) AS matched_concepts
                ORDER BY score DESC
                LIMIT $top_k
                """,
                teseo_ids=analyzed.teseo_concept_ids,
                top_k=top_k,
            )
            async for record in result:
                chunk = _record_to_chunk(record, source="graph")
                # Fallback: se matched_concepts è vuoto ma sappiamo che è un match GRAPH,
                # usiamo le label trovate nella query come spiegazione
                if not chunk.metadata.get("matched_concepts"):
                    chunk.metadata["matched_concepts"] = analyzed.expanded_labels
                chunks.append(chunk)
    except Exception as e:
        logger.error(f"Errore nella Graph Search: {e}")

    logger.info(f"Graph Search: {len(chunks)} risultati")
    return {"graph_results": chunks}
