"""
Citation Expander — Step 4 del Retrieval Engine.

Implementa il multi-hop ricorsivo:
- Per ogni chunk fuso, cerca archi :CITES verso nodi interni al grafo
- Se trova norme citate, le aggiunge al contesto
- Il ciclo è controllato da MAX_CITATION_HOPS (default 1)
"""
import logging

from neo4j import AsyncDriver

from src.config import settings
from src.rag.models import RagState, RetrievedChunk
from src.rag.retriever import _parse_date

logger = logging.getLogger(__name__)


def should_expand(state: RagState) -> str:
    """
    Routing condizionale LangGraph: decide se espandere le citazioni o terminare.

    Ritorna:
        - "expand_citations" se hop_count < MAX_CITATION_HOPS e ci sono chunk
        - "__end__" altrimenti
    """
    hop_count = state.get("hop_count", 0)
    fused_chunks = state.get("fused_chunks") or []

    # Salta il multi-hop se disabilitato
    if not state.get("enable_multi_hop", True):
        logger.info("Multi-hop disabilitato tramite flag (enable_multi_hop=False). Skip espansione.")
        return "__end__"

    max_hops = state.get("max_citation_hops", settings.MAX_CITATION_HOPS)

    if hop_count >= max_hops:
        logger.info(f"Multi-hop: raggiunto limite ({hop_count}/{max_hops}), terminazione.")
        # Copia i chunk fusi nei final_chunks prima di terminare
        return "__end__"

    if not fused_chunks:
        logger.info("Multi-hop: nessun chunk da espandere, terminazione.")
        return "__end__"

    return "expand_citations"


async def expand_citations(state: RagState) -> dict:
    """
    Nodo LangGraph: espande i chunk trovati navigando gli archi :CITES.

    Per ogni chunk in fused_chunks, cerca citazioni verso nodi Expression o Work
    interni al grafo. Se trova testo citato, lo aggiunge come nuovo chunk.

    Legge: state["fused_chunks"], state["hop_count"], state["_driver"]
    Scrive: state["fused_chunks"] (aggiornato), state["hop_count"] (+1),
            state["final_chunks"]
    """
    driver: AsyncDriver = state["_driver"]
    # Partiamo dai chunk che sono sopravvissuti al reranker (final_chunks)
    # per evitare di espandere citazioni di documenti giudicati irrilevanti
    source_chunks = state.get("final_chunks") or state.get("fused_chunks") or []
    hop_count = state.get("hop_count", 0)

    # Raccogli tutti gli expression_id dei chunk attuali
    chunk_ids = [c.expression_id for c in source_chunks if c.expression_id]
    
    # Per evitare duplicati finali, controlliamo contro i fused_chunks originali
    existing_ids = {c.expression_id for c in (state.get("fused_chunks") or [])}

    if not chunk_ids:
        return {
            "hop_count": hop_count + 1,
        }

    new_chunks = []

    try:
        async with driver.session() as session:
            # Cerca citazioni dirette dai chunk attuali
            result = await session.run(
                """
                UNWIND $chunk_ids AS src_id
                MATCH (src {id: src_id})-[:CITES]->(target)
                WHERE (target:Expression OR target:Work)
                
                // Se il target è un Work, prendi le Expression figlie
                WITH target
                OPTIONAL MATCH (child:Expression)-[:PART_OF*1..2]->(target)
                WHERE target:Work

                // Unifica: se target è Expression usiamo target, altrimenti child
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
                # Evita duplicati con i chunk già presenti
                if expr_id and expr_id not in existing_ids:
                    new_chunks.append(RetrievedChunk(
                        text=record.get("text") or "",
                        expression_id=expr_id,
                        work_urn=record.get("work_urn"),
                        structural_context=record.get("structural_tag") or "",
                        score=0.0,  # Score base per i chunk di espansione
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

    # FIX: Aggiungiamo i nuovi chunk trovati al pool di graph_results
    # così il successivo nodo fuse_and_filter li integrerà e li valuterà col RRF
    current_graph_results = state.get("graph_results") or []
    updated_graph_results = current_graph_results + new_chunks

    return {
        "graph_results": updated_graph_results,
        "hop_count": hop_count + 1,
    }
