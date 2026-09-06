"""Citation Expander — Recursive multi-hop retrieval over graph citations.

Traverses incoming and outgoing :CITES edges for retrieved chunks to incorporate
referenced legal provisions into the retrieval context. Controlled by max_citation_hops.
"""
import logging

from neo4j import AsyncDriver

from src.config import settings
from src.rag.models import RagState, RetrievedChunk
from src.rag.retriever import _parse_date

logger = logging.getLogger(__name__)


def should_expand(state: RagState) -> str:
    """Conditional router determining whether to expand citations or proceed to grading.

    Returns:
        "expand_citations" if hop_count < max_citation_hops and source chunks exist,
        "__end__" otherwise.
    """
    hop_count = state.get("hop_count", 0)
    fused_chunks = state.get("fused_chunks") or []

    # Skip multi-hop expansion if disabled
    if not state.get("enable_multi_hop", True):
        logger.info("Multi-hop disabilitato tramite flag (enable_multi_hop=False). Skip espansione.")
        return "__end__"

    max_hops = state.get("max_citation_hops", settings.MAX_CITATION_HOPS)

    if hop_count >= max_hops:
        logger.info(f"Multi-hop: raggiunto limite ({hop_count}/{max_hops}), terminazione.")
        return "__end__"

    if not fused_chunks:
        logger.info("Multi-hop: nessun chunk da espandere, terminazione.")
        return "__end__"

    return "expand_citations"


async def expand_citations(state: RagState) -> dict:
    """Expand retrieved chunks by querying outbound :CITES edges in Neo4j."""
    driver: AsyncDriver = state["_driver"]
    # Start from chunks surviving the reranker to avoid expanding irrelevant citations
    source_chunks = state.get("final_chunks") or state.get("fused_chunks") or []
    hop_count = state.get("hop_count", 0)

    # Collect expression IDs from source chunks
    chunk_ids = [c.expression_id for c in source_chunks if c.expression_id]
    
    # Prevent duplicates against existing fused chunks
    existing_ids = {c.expression_id for c in (state.get("fused_chunks") or [])}

    if not chunk_ids:
        return {
            "hop_count": hop_count + 1,
        }

    new_chunks = []

    try:
        async with driver.session() as session:
            # Query direct citations originating from current chunks
            result = await session.run(
                """
                UNWIND $chunk_ids AS src_id
                MATCH (src {id: src_id})-[:CITES]->(target)
                WHERE (target:Expression OR target:Work)
                
                // If target is Work, retrieve child Expression nodes
                WITH target
                OPTIONAL MATCH (child:Expression)-[:PART_OF*1..2]->(target)
                WHERE target:Work

                // Unify: use target directly if Expression, otherwise child
                WITH coalesce(
                    CASE WHEN target:Expression THEN target ELSE null END,
                    child
                ) AS cited
                WHERE cited IS NOT NULL

                OPTIONAL MATCH (cited)-[:PART_OF]->(u:StructuralUnit)
                OPTIONAL MATCH (u)-[:PART_OF]->(w:Work)
                
                RETURN DISTINCT
                       cited.text_display AS text,
                       cited.id AS expression_id,
                       w.urn AS work_urn,
                       u.tag_name AS structural_tag,
                       cited.vigenza_start AS vigenza_start,
                       cited.vigenza_end AS vigenza_end
                LIMIT 10
                """,
                chunk_ids=chunk_ids,
            )
            async for record in result:
                expr_id = record.get("expression_id") or ""
                # Avoid duplicates with already present chunks
                if expr_id and expr_id not in existing_ids:
                    new_chunks.append(RetrievedChunk(
                        text=record.get("text") or "",
                        expression_id=expr_id,
                        work_urn=record.get("work_urn"),
                        structural_context=record.get("structural_tag") or "",
                        score=0.0,  # Base score for expansion chunks
                        source="citation_hop",
                        vigenza_start=_parse_date(record.get("vigenza_start")),
                        vigenza_end=_parse_date(record.get("vigenza_end")),
                    ))
                    existing_ids.add(expr_id)

    except Exception as e:
        logger.error(f"Errore nell'espansione citazioni: {e}")

    if new_chunks:
        logger.info(f"Multi-hop {hop_count + 1}: trovati {len(new_chunks)} chunk citati")
    else:
        logger.info(f"Multi-hop {hop_count + 1}: nessuna citazione interna trovata")

    # Append newly discovered chunks to graph_results for subsequent fusion and RRF ranking
    current_graph_results = state.get("graph_results") or []
    updated_graph_results = current_graph_results + new_chunks

    return {
        "graph_results": updated_graph_results,
        "hop_count": hop_count + 1,
    }
