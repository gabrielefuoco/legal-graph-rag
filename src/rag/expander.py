"""Citation Expander — Recursive multi-hop retrieval over graph citations.

Traverses incoming and outgoing :CITES edges for retrieved chunks to incorporate
referenced legal provisions into the retrieval context. Controlled by max_citation_hops.
"""
import logging
import re

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
    """Expand retrieved chunks by querying outbound :CITES edges in Neo4j.

    The edge says *which act* is referenced; the embedding of the cited articles decides
    *which article* of that act is worth keeping. Without that ranking the hop returned an
    arbitrary sample of a possibly huge act (a code can hold hundreds of articles), which is
    why the correct provision was almost never among the injected chunks.
    """
    driver: AsyncDriver = state["_driver"]
    # Seeds = chunks that survived the reranker *plus* the top of the wider fused set: the act that
    # performs the citation is often pushed out of the final window, but expanding every fused
    # chunk drags in dozens of cited acts, so the one the question is about gets lost.
    source_chunks = list(state.get("final_chunks") or [])
    fused = state.get("fused_chunks") or []
    hop_sources = getattr(settings, "CITATION_HOP_SOURCE_TOP_K", 5)
    for extra in fused[:hop_sources]:
        if all(extra.expression_id != c.expression_id for c in source_chunks):
            source_chunks.append(extra)
    hop_count = state.get("hop_count", 0)

    # Collect expression IDs from source chunks. Chunks merged by _merge_chunks carry a
    # synthetic "merged_<id>" identifier that does not exist as a graph node: strip the
    # prefix so their citations can still be traversed.
    chunk_ids = [
        c.expression_id[len("merged_"):] if c.expression_id.startswith("merged_") else c.expression_id
        for c in source_chunks
        if c.expression_id
    ]
    chunk_ids = list(dict.fromkeys(chunk_ids))  # preserve order, drop duplicates
    
    # Prevent duplicates against existing fused chunks
    existing_ids = {c.expression_id for c in (state.get("fused_chunks") or [])}

    if not chunk_ids:
        return {
            "hop_count": hop_count + 1,
        }

    new_chunks = []

    # Query embedding used to rank the cited articles by relevance
    query_embedding = state.get("query_embedding")
    if query_embedding is None:
        analyzer = state.get("_analyzer")
        engine = getattr(analyzer, "vector_engine", None)
        if engine is not None:
            try:
                embeddings = await engine.compute_embeddings_batch([state.get("query", "")])
                query_embedding = embeddings[0] if embeddings else None
            except Exception as e:
                logger.warning(f"Embedding della query non disponibile per l'espansione: {e}")

    budget = getattr(settings, "CITATION_EXPANSION_LIMIT", 10)

    try:
        async with driver.session() as session:
            # Step 1: every article of every cited act, with its embedding only. A code can hold
            # over a thousand articles, so an arbitrary LIMIT would silently drop the relevant
            # provision before it is ever scored.
            result = await session.run(
                """
                UNWIND $chunk_ids AS src_id
                MATCH (src {id: src_id})-[cit:CITES]->(target)
                WHERE (target:Expression OR target:Work)

                // If target is Work, retrieve child Expression nodes
                WITH src, cit, target
                OPTIONAL MATCH (child:Expression)-[:PART_OF*1..2]->(target)
                WHERE target:Work

                // Unify: use target directly if Expression, otherwise child
                WITH src, cit, coalesce(
                    CASE WHEN target:Expression THEN target ELSE null END,
                    child
                ) AS cited
                WHERE cited IS NOT NULL

                // Which act performs the citation: metadata for the generator
                OPTIONAL MATCH (src)-[:PART_OF*1..2]->(sw:Work)
                // Which act is cited: needed to give every cited act its own budget
                OPTIONAL MATCH (cited)-[:PART_OF*1..2]->(cw:Work)

                RETURN DISTINCT
                       cited.id AS expression_id,
                       cited.embedding AS embedding,
                       cw.urn AS cited_act,
                       sw.title AS citing_title,
                       left(coalesce(cit.text, ''), 200) AS citation_text
                """,
                chunk_ids=chunk_ids,
            )
            candidates = [record async for record in result]

            if query_embedding is not None:
                def _score(record) -> float:
                    embedding = record.get("embedding")
                    if not embedding:
                        return -1.0
                    dot = sum(a * b for a, b in zip(query_embedding, embedding))
                    na = sum(a * a for a in query_embedding) ** 0.5
                    nb = sum(b * b for b in embedding) ** 0.5
                    return dot / (na * nb) if na and nb else -1.0

                # The citation text ("ai sensi dell'articolo 17 del Codice dell'amministrazione
                # digitale") names the referenced provision: it is the strongest signal for
                # picking the right cited act when a law cites dozens of them.
                query_tokens = set(re.findall(r"[a-zà-ù]{4,}", (state.get("query") or "").lower()))

                def _citation_overlap(record) -> float:
                    text = (record.get("citation_text") or "").lower()
                    tokens = set(re.findall(r"[a-zà-ù]{4,}", text))
                    if not tokens or not query_tokens:
                        return 0.0
                    return len(tokens & query_tokens) / len(tokens)

                def _combined(record) -> float:
                    return _score(record)

                candidates.sort(key=_combined, reverse=True)
                best_scores = [_score(c) for c in candidates[:3]]
                best_overlaps = [_citation_overlap(c) for c in candidates[:3]]
                logger.info(
                    f"Multi-hop {hop_count + 1}: {len(candidates)} articoli citati ordinati per "
                    f"similarità+rinvio (top sim: {', '.join(f'{s:.3f}' for s in best_scores)} | "
                    f"top rinvio: {', '.join(f'{o:.2f}' for o in best_overlaps)})"
                )

            selected = []
            citing_meta = {}
            per_act = getattr(settings, "CITATION_EXPANSION_PER_ACT", budget)

            # Group by cited act and keep its best articles: a citation hop should surface the
            # relevant provisions of *every* referenced act, not only of the one that happens to
            # contain the globally most similar article.
            groups: dict[str, list] = {}
            for record in candidates:
                act = record.get("cited_act") or record.get("expression_id") or "?"
                groups.setdefault(act, []).append(record)

            ordered = sorted(
                groups.values(),
                key=lambda recs: _combined(recs[0]) if query_embedding is not None else 0.0,
                reverse=True,
            )
            for recs in ordered:
                for record in recs[:per_act]:
                    expr_id = record.get("expression_id") or ""
                    if not expr_id or expr_id in existing_ids or expr_id in citing_meta:
                        continue
                    citing_meta[expr_id] = record
                    selected.append(expr_id)
                    if len(selected) >= budget:
                        break
                if len(selected) >= budget:
                    break
            logger.info(
                f"Multi-hop {hop_count + 1}: {len(groups)} atti citati, selezionati {len(selected)} "
                f"articoli (max {per_act} per atto, tetto {budget})"
            )

            # Step 2: fetch the full records of the selected articles only
            details = []
            if selected:
                detail_result = await session.run(
                    """
                    UNWIND $ids AS cid
                    MATCH (cited:Expression {id: cid})
                    OPTIONAL MATCH (cited)-[:PART_OF]->(u:StructuralUnit)
                    OPTIONAL MATCH (u)-[:PART_OF]->(w:Work)
                    RETURN cited.id AS expression_id,
                           cited.text_display AS text,
                           w.urn AS work_urn,
                           u.tag_name AS structural_tag,
                           cited.vigenza_start AS vigenza_start,
                           cited.vigenza_end AS vigenza_end
                    """,
                    ids=selected,
                )
                details = [record async for record in detail_result]

            for record in details:
                expr_id = record.get("expression_id") or ""
                # Avoid duplicates with already present chunks
                if expr_id and expr_id not in existing_ids:
                    meta = citing_meta.get(expr_id) or {}
                    new_chunks.append(RetrievedChunk(
                        text=record.get("text") or "",
                        expression_id=expr_id,
                        work_urn=record.get("work_urn"),
                        structural_context=record.get("structural_tag") or "",
                        score=0.0,  # Base score for expansion chunks
                        source="citation_hop",
                        vigenza_start=_parse_date(record.get("vigenza_start")),
                        vigenza_end=_parse_date(record.get("vigenza_end")),
                        expansion_trace={
                            "relation": "CITES",
                            "citing_title": meta.get("citing_title"),
                            "citation_text": meta.get("citation_text"),
                        },
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
