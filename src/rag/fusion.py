"""
Fusion & Filtering — Step 3 del Retrieval Engine.

Implementa:
- Reciprocal Rank Fusion (RRF) pesato per combinare i risultati dei 3 canali
- Filtro temporale rigido basato su vigenza_start / vigenza_end
"""
import logging
from collections import defaultdict
from datetime import date

from src.config import settings
from src.rag.models import RagState, RetrievedChunk

logger = logging.getLogger(__name__)


def _reciprocal_rank_fusion(
    channels: list[tuple[list[RetrievedChunk], float]],
    k: int = 60,
) -> list[RetrievedChunk]:
    """
    Reciprocal Rank Fusion (RRF) pesato.

    Per ogni chunk, lo score combinato è:
        RRF_score = Σ (weight / (k + rank))
    dove rank è la posizione nel canale (1-indexed) e weight è il peso del canale.

    I chunk sono identificati univocamente per `expression_id`.
    Se lo stesso nodo appare in più canali, gli score si sommano.

    Args:
        channels: Lista di (risultati, peso) per ogni canale.
        k: Costante RRF (default 60, standard nella letteratura).

    Returns:
        Lista di RetrievedChunk ordinata per score RRF decrescente.
    """
    # Accumula score per expression_id
    scores: dict[str, float] = defaultdict(float)
    # Mantieni il chunk con il testo più completo per ogni ID
    best_chunk: dict[str, RetrievedChunk] = {}
    # Traccia le fonti
    sources: dict[str, set[str]] = defaultdict(set)

    for results, weight in channels:
        for rank_0, chunk in enumerate(results):
            rank = rank_0 + 1  # 1-indexed
            rrf_score = weight / (k + rank)
            scores[chunk.expression_id] += rrf_score
            sources[chunk.expression_id].add(chunk.source)

            # Conserva il chunk con il testo più lungo come rappresentante
            existing = best_chunk.get(chunk.expression_id)
            if existing is None:
                best_chunk[chunk.expression_id] = chunk
            else:
                # Uniamo i metadati in modo intelligente (unione di liste per matched_concepts)
                if chunk.metadata:
                    for key, val in chunk.metadata.items():
                        if key == "matched_concepts" and key in existing.metadata:
                            # Unione di liste senza duplicati
                            combined = list(set(existing.metadata[key] + val))
                            existing.metadata[key] = combined
                        else:
                            existing.metadata[key] = val

                if len(chunk.text) > len(existing.text):
                    # Se il nuovo chunk ha testo più lungo, lo usiamo come base
                    # ma preserviamo i metadati accumulati finora
                    old_meta = existing.metadata
                    best_chunk[chunk.expression_id] = chunk
                    best_chunk[chunk.expression_id].metadata = old_meta

    # Costruisci risultato finale
    fused = []
    for expr_id, rrf_score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
        chunk = best_chunk[expr_id]
        fused.append(RetrievedChunk(
            text=chunk.text,
            expression_id=chunk.expression_id,
            work_urn=chunk.work_urn,
            structural_context=chunk.structural_context,
            score=rrf_score,
            source="+".join(sorted(sources[expr_id])),  # es. "bm25+vector"
            vigenza_start=chunk.vigenza_start,
            vigenza_end=chunk.vigenza_end,
            metadata=chunk.metadata
        ))

    return fused


def _mark_abrogated_chunks(
    chunks: list[RetrievedChunk],
    reference_date: date,
) -> list[RetrievedChunk]:
    """
    Controlla la vigenza e marca esplicitamente i testi abrogati anteponendo un avviso.
    Non rimuove i chunk, in modo che l'LLM sappia che la norma esiste ma non è più in vigore.
    """
    for chunk in chunks:
        is_abrogato = False
        if chunk.vigenza_end and chunk.vigenza_end < reference_date:
            is_abrogato = True

        if is_abrogato and not chunk.text.startswith("[ATTENZIONE: NORMA ABROGATA]"):
            chunk.text = f"[ATTENZIONE: NORMA ABROGATA]\n{chunk.text}"
            logger.info(f"Marcata norma abrogata: {chunk.expression_id}")

    return chunks


def _merge_chunks(chunks: list[RetrievedChunk]) -> list[RetrievedChunk]:
    """
    Fonde i chunk restituiti che appartengono allo stesso Atto (Work).
    Usa `work_title` dai metadati (o `work_urn`) per raggrupparli.
    Mantiene lo score massimo del gruppo e concatena i testi (in ordine di `structural_context`).
    """
    if not chunks:
        return []

    # Raggruppa i chunk per Work
    grouped: dict[str, list[RetrievedChunk]] = defaultdict(list)
    for c in chunks:
        key = c.metadata.get("work_title") or c.work_urn or c.expression_id
        grouped[key].append(c)

    merged_chunks = []
    for key, group in grouped.items():
        if len(group) == 1:
            merged_chunks.append(group[0])
            continue
            
        # Ordiniamo per structural_context (es. "articolo - Art. 1" prima di "articolo - Art. 2")
        group_sorted = sorted(group, key=lambda x: x.structural_context or "")
        
        base_chunk = group_sorted[0]
        max_score = max(c.score for c in group_sorted)
        
        # Concateniamo i testi con un separatore chiaro
        texts_to_join = []
        for c in group_sorted:
            if c.structural_context:
                texts_to_join.append(f"--- {c.structural_context} ---\n{c.text}")
            else:
                texts_to_join.append(c.text)
                
        merged_text = "\n\n".join(texts_to_join)
        
        # Raccogliamo tutte le fonti
        all_sources = set()
        for c in group_sorted:
            all_sources.update(c.source.split("+"))
            
        # Creiamo un nuovo chunk fuso
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
        
    # Riordiniamo la lista finale per score discendente
    merged_chunks.sort(key=lambda x: x.score, reverse=True)
    return merged_chunks


def fuse_and_filter(state: RagState) -> dict:
    """
    Nodo LangGraph: fonde i risultati dei 3 canali con RRF e applica il filtro temporale.

    Legge: state["vector_results"], state["bm25_results"], state["graph_results"],
           state["reference_date"]
    Scrive: state["fused_chunks"], state["hop_count"]
    """
    vector_results = state.get("vector_results") or []
    bm25_results = state.get("bm25_results") or []
    graph_results = state.get("graph_results") or []

    # RRF pesato
    channels = [
        (vector_results, settings.RRF_WEIGHT_VECTOR),
        (bm25_results, settings.RRF_WEIGHT_BM25),
        (graph_results, settings.RRF_WEIGHT_GRAPH),
    ]
    fused = _reciprocal_rank_fusion(channels, k=settings.RRF_K)

    logger.info(
        f"RRF Fusion: {len(vector_results)} vector + {len(bm25_results)} bm25 + "
        f"{len(graph_results)} graph → {len(fused)} chunk unici"
    )

    # Cutoff (Top-K finale)
    final_k = state.get("final_k", 5)
    fused = fused[:final_k]
    
    logger.info(f"Cutoff applicato: estratti i top {final_k} chunk finali")

    # Marcatura temporale per norme abrogate
    ref_date_str = state.get("reference_date")
    ref_date = date.today()  # Default a oggi se non specificata
    if ref_date_str:
        try:
            ref_date = date.fromisoformat(ref_date_str)
        except ValueError:
            logger.warning(f"Data di riferimento non valida: {ref_date_str}, uso oggi")
            
    fused = _mark_abrogated_chunks(fused, ref_date)
    
    # Merging dei chunk dello stesso Atto
    fused = _merge_chunks(fused)

    return {
        "fused_chunks": fused,
    }
