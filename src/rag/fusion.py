"""Fusion and filtering module for hybrid retrieval channels.

Implements weighted Reciprocal Rank Fusion (RRF), text-overlap deduplication,
temporal validity checking, and structural chunk merging.
"""
import logging
from collections import defaultdict
from datetime import date

from src.config import settings
from src.rag.models import RagState, RetrievedChunk

logger = logging.getLogger(__name__)


def _deduplicate_channel(results: list[RetrievedChunk]) -> list[RetrievedChunk]:
    """Collapse duplicated rows of a single retrieval channel into one entry per expression.

    The dense channel fans out over `PART_OF` paths, so the same expression can be returned
    several times (measured: ~8 rows per expression). Left as-is, those duplicates accumulate
    extra RRF score for the same expression, silently multiplying that channel's weight by the
    fan-out factor and making the configured weights meaningless.

    The first occurrence keeps its rank; metadata is merged and the longest text wins.
    """
    if len(results) <= 1:
        return results

    deduped: list[RetrievedChunk] = []
    seen: dict[str, RetrievedChunk] = {}
    for chunk in results:
        existing = seen.get(chunk.expression_id)
        if existing is None:
            seen[chunk.expression_id] = chunk
            deduped.append(chunk)
            continue

        # Merge metadata (union of matched concepts) without losing information
        if chunk.metadata:
            for key, value in chunk.metadata.items():
                if key == "matched_concepts" and key in existing.metadata:
                    existing.metadata[key] = list(set(existing.metadata[key] + value))
                elif key not in existing.metadata:
                    existing.metadata[key] = value

        # Keep the most informative text representation
        if len(chunk.text) > len(existing.text):
            existing.text = chunk.text
            existing.structural_context = chunk.structural_context or existing.structural_context

    if len(deduped) < len(results):
        logger.info(f"Dedup canale: {len(results)} righe → {len(deduped)} chunk distinti")
    return deduped


def _reciprocal_rank_fusion(
    channels: list[tuple[list[RetrievedChunk], float]],
    k: int = 60,
) -> list[RetrievedChunk]:
    """Weighted Reciprocal Rank Fusion (RRF).

    For each chunk, the combined score is:
        RRF_score = sum(weight / (k + rank))
    where rank is the 1-indexed position in the channel and weight is the channel weight.
    Chunks are identified uniquely by `expression_id`.

    Each channel is deduplicated first: RRF must rank *distinct* items, otherwise duplicated
    rows inflate the score of the channel that produced them.

    Args:
        channels: List of (results, weight) tuples for each retrieval channel.
        k: RRF constant (default 60).

    Returns:
        List of RetrievedChunk instances sorted by descending RRF score.
    """
    # Accumulate scores per expression_id
    scores: dict[str, float] = defaultdict(float)
    # Keep the chunk with the most complete text for each ID
    best_chunk: dict[str, RetrievedChunk] = {}
    # Track retrieval sources
    sources: dict[str, set[str]] = defaultdict(set)

    for results, weight in channels:
        for rank_0, chunk in enumerate(_deduplicate_channel(results)):
            rank = rank_0 + 1  # 1-indexed
            rrf_score = weight / (k + rank)
            scores[chunk.expression_id] += rrf_score
            sources[chunk.expression_id].add(chunk.source)

            # Keep the chunk with the longest text as representative
            existing = best_chunk.get(chunk.expression_id)
            if existing is None:
                best_chunk[chunk.expression_id] = chunk
            else:
                # Merge metadata intelligently (union of lists for matched_concepts)
                if chunk.metadata:
                    for key, val in chunk.metadata.items():
                        if key == "matched_concepts" and key in existing.metadata:
                            combined = list(set(existing.metadata[key] + val))
                            existing.metadata[key] = combined
                        else:
                            existing.metadata[key] = val

                if len(chunk.text) > len(existing.text):
                    # If the new chunk has longer text, use it while preserving accumulated metadata
                    old_meta = existing.metadata
                    best_chunk[chunk.expression_id] = chunk
                    best_chunk[chunk.expression_id].metadata = old_meta

    # Build fused results sorted by score
    fused = []
    for expr_id, rrf_score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
        chunk = best_chunk[expr_id]
        fused.append(RetrievedChunk(
            text=chunk.text,
            expression_id=chunk.expression_id,
            work_urn=chunk.work_urn,
            structural_context=chunk.structural_context,
            score=rrf_score,
            source="+".join(sorted(sources[expr_id])),
            vigenza_start=chunk.vigenza_start,
            vigenza_end=chunk.vigenza_end,
            metadata=chunk.metadata
        ))

    return fused


def _deduplicate_by_text(chunks: list[RetrievedChunk], overlap_threshold: float = 0.8) -> list[RetrievedChunk]:
    """Remove chunks whose text substantially overlaps with another chunk, keeping the higher-scoring one."""
    if len(chunks) <= 1:
        return chunks
    
    # Sort by score descending to retain top-scoring chunks
    sorted_chunks = sorted(chunks, key=lambda c: c.score, reverse=True)
    kept = []
    
    for candidate in sorted_chunks:
        is_duplicate = False
        candidate_words = set(candidate.text.lower().split())
        
        if not candidate_words:
            continue
            
        for existing in kept:
            existing_words = set(existing.text.lower().split())
            if not existing_words:
                continue
            
            # Compute bidirectional overlap
            intersection = candidate_words & existing_words
            smaller_set = min(len(candidate_words), len(existing_words))
            
            if smaller_set > 0 and len(intersection) / smaller_set >= overlap_threshold:
                is_duplicate = True
                break
        
        if not is_duplicate:
            kept.append(candidate)
    
    if len(kept) < len(chunks):
        logger.info(f"Dedup: rimossi {len(chunks) - len(kept)} chunk duplicati")
    
    return kept


def _mark_abrogated_chunks(
    chunks: list[RetrievedChunk],
    reference_date: date,
) -> list[RetrievedChunk]:
    """Check temporal validity and prepend a warning banner to abrogated provisions."""
    for chunk in chunks:
        is_abrogato = False
        if chunk.vigenza_end and chunk.vigenza_end < reference_date:
            is_abrogato = True

        if is_abrogato and not chunk.text.startswith("[ATTENZIONE: NORMA ABROGATA]"):
            chunk.text = f"[ATTENZIONE: NORMA ABROGATA]\n{chunk.text}"
            logger.info(f"Marcata norma abrogata: {chunk.expression_id}")

    return chunks


def _strip_graph_fields(chunks: list[RetrievedChunk]) -> list[RetrievedChunk]:
    """Remove every graph-derived field so the pipeline sees plain text only.

    Used by the strict text-only baseline: the structural breadcrumb (PART_OF), the act URN,
    the vigenza window and the graph annotations (TESEO concepts, citation traces, topological
    expansions) all come from the knowledge graph and must not reach the reranker or the
    generator if the claim is "text retrieval without the graph".
    """
    for chunk in chunks:
        chunk.structural_context = ""
        chunk.work_urn = None
        chunk.vigenza_start = None
        chunk.vigenza_end = None
        chunk.expansion_trace = None
        chunk.expanded_text = None
        chunk.metadata = {}
    return chunks


def _merge_chunks(chunks: list[RetrievedChunk]) -> list[RetrievedChunk]:
    """Merge structurally adjacent chunks belonging to the same legal act (Work)."""
    if not chunks:
        return []

    # Group chunks by Work
    grouped: dict[str, list[RetrievedChunk]] = defaultdict(list)
    for c in chunks:
        key = c.metadata.get("work_title") or c.work_urn or c.expression_id
        grouped[key].append(c)

    merged_chunks = []
    for key, group in grouped.items():
        if len(group) == 1:
            merged_chunks.append(group[0])
            continue
        
        # Sort by structural_context
        group_sorted = sorted(group, key=lambda x: x.structural_context or "")
        
        # Partition into adjacent subgroups sharing the same parent structure
        sub_groups = []
        current_sub = [group_sorted[0]]
        
        for i in range(1, len(group_sorted)):
            prev_ctx = current_sub[-1].structural_context or ""
            curr_ctx = group_sorted[i].structural_context or ""
            
            prev_parts = prev_ctx.rsplit(" > ", 1)
            curr_parts = curr_ctx.rsplit(" > ", 1)
            
            same_parent = (len(prev_parts) > 1 and len(curr_parts) > 1 
                          and prev_parts[0] == curr_parts[0])
            
            if same_parent:
                current_sub.append(group_sorted[i])
            else:
                sub_groups.append(current_sub)
                current_sub = [group_sorted[i]]
        sub_groups.append(current_sub)
        
        # Merge only subgroups containing multiple chunks
        for sub in sub_groups:
            if len(sub) == 1:
                merged_chunks.append(sub[0])
                continue
            
            base_chunk = sub[0]
            max_score = max(c.score for c in sub)
            
            texts_to_join = []
            for c in sub:
                if c.structural_context:
                    texts_to_join.append(f"--- {c.structural_context} ---\n{c.text}")
                else:
                    texts_to_join.append(c.text)
                    
            merged_text = "\n\n".join(texts_to_join)
            
            all_sources = set()
            for c in sub:
                all_sources.update(c.source.split("+"))
            
            merged_chunk = RetrievedChunk(
                text=merged_text,
                expression_id="merged_" + base_chunk.expression_id,
                work_urn=base_chunk.work_urn,
                structural_context="MULTIPLE_CHUNKS",
                score=max_score,
                source="+".join(sorted(all_sources)),
                vigenza_start=base_chunk.vigenza_start,
                vigenza_end=base_chunk.vigenza_end,
                metadata=base_chunk.metadata.copy()
            )
            merged_chunks.append(merged_chunk)
        
    merged_chunks.sort(key=lambda x: x.score, reverse=True)
    return merged_chunks


def fuse_and_filter(state: RagState) -> dict:
    """LangGraph node to fuse retrieval channels via RRF and apply filtering and merging."""
    import time
    start = time.perf_counter()
    vector_results = state.get("vector_results") or []
    bm25_results = state.get("bm25_results") or []
    graph_results = state.get("graph_results") or []

    # Weighted RRF
    channels = [
        (vector_results, settings.RRF_WEIGHT_VECTOR),
        (bm25_results, settings.RRF_WEIGHT_BM25),
        (graph_results, settings.RRF_WEIGHT_GRAPH),
    ]
    fused = _reciprocal_rank_fusion(channels, k=settings.RRF_K)

    # Score cutoff: remove low-scoring chunks
    min_score = settings.RAG_MIN_SCORE
    before_cutoff = len(fused)
    fused = [c for c in fused if c.score >= min_score]
    if len(fused) < before_cutoff:
        logger.info(f"Score cutoff (>= {min_score}): rimossi {before_cutoff - len(fused)} chunk sotto soglia")

    # Positional cutoff for reranker candidate pool
    rerank_top_k = settings.RERANK_TOP_K
    fused = fused[:rerank_top_k]
    
    elapsed = time.perf_counter() - start
    logger.info(
        f"[3/6] FUSION — V:{len(vector_results)} + B:{len(bm25_results)} + G:{len(graph_results)} "
        f"→ {before_cutoff} unici → {len(fused)} per reranker | {elapsed:.2f}s"
    )

    # Text overlap deduplication
    fused = _deduplicate_by_text(fused)

    # Strict text-only baseline: drop graph fields *before* merging/marking so those stages
    # cannot silently rely on the graph either.
    if getattr(settings, "PURE_TEXT_BASELINE", False):
        fused = _strip_graph_fields(fused)
        logger.info("Baseline testo-puro: rimossi breadcrumb, URN, vigenza e annotazioni del grafo")

    # Mark abrogated provisions relative to reference date
    ref_date_str = state.get("reference_date")
    ref_date = date.today()  # Default to today if omitted
    if ref_date_str:
        try:
            ref_date = date.fromisoformat(ref_date_str)
        except ValueError:
            logger.warning(f"Data di riferimento non valida: {ref_date_str}, uso oggi")
            
    fused = _mark_abrogated_chunks(fused, ref_date)
    
    # Merge adjacent chunks from the same act
    fused = _merge_chunks(fused)

    vector_anchor_id = vector_results[0].expression_id if vector_results else None

    return {
        "fused_chunks": fused,
        "vector_anchor_id": vector_anchor_id,
    }
