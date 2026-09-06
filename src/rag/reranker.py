"""Cross-Encoder reranking module with score thresholding and safety netting."""

import logging
import os
import torch
from typing import List
from sentence_transformers import CrossEncoder
from src.config import settings
from src.rag.models import RetrievedChunk, RagState

# Silence non-critical Hugging Face tokenizer warnings
os.environ["TOKENIZERS_PARALLELISM"] = "false"

logger = logging.getLogger(__name__)

class Reranker:
    """Cross-Encoder reranker supporting GPU acceleration and threshold-based filtering."""

    def __init__(self, model_name: str = None):
        model_name = model_name or settings.RERANKER_MODEL_NAME
        device = "cuda" if torch.cuda.is_available() else "cpu"
        # Support Apple Silicon MPS
        if device == "cpu" and hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            device = "mps"
            
        logger.info(f"Caricamento Reranker ({model_name}) su device: {device}")
        
        try:
            kwargs = {}
            if device == "cuda":
                kwargs["model_kwargs"] = {"torch_dtype": torch.float16}
                logger.info("Caricamento CrossEncoder in FP16 per ottimizzazione VRAM.")
                
            self.model = CrossEncoder(model_name, max_length=2048, device=device, **kwargs)
            logger.info("Reranker caricato con successo.")
        except Exception as e:
            logger.error(f"Errore nel caricamento del Reranker: {e}")
            self.model = None

    def rerank(self, query: str, chunks: List[RetrievedChunk], instruction: str = None) -> List[RetrievedChunk]:
        """Rerank retrieved chunks and filter low-confidence results below a threshold.

        Supports instruction prefixes for task-aware cross-encoder models.
        """
        if not self.model or not chunks:
            return chunks
        from copy import copy
        chunks = [copy(c) for c in chunks]

        # Format instruction-aware prompt if provided
        if instruction:
            prompted_query = f"Istruzione: {instruction}\nQuery: {query}"
        else:
            prompted_query = query

        pairs = [[prompted_query, chunk.text] for chunk in chunks]
        
        try:
            scores = self.model.predict(pairs, convert_to_numpy=True)
            
            import math
            def sigmoid(x):
                return 1 / (1 + math.exp(-x))
            
            for chunk, score in zip(chunks, scores):
                # Normalize raw logits to [0, 1] probability
                prob = sigmoid(float(score))
                chunk.score = prob
                chunk.source += "+rerank"

            # Sort by descending score
            chunks.sort(key=lambda x: x.score, reverse=True)
            
            # Apply score threshold filter
            min_score = settings.RERANK_MIN_SCORE
            before_filter = len(chunks)
            filtered_chunks = []
            for c in chunks:
                if c.score >= min_score:
                    logger.info(f"⚖️ Reranker: Promosso '{c.expression_id}' (Score: {c.score:.4f})")
                    filtered_chunks.append(c)
                else:
                    logger.info(f"✂️ Reranker: Scartato '{c.expression_id}' (Score: {c.score:.4f} < {min_score})")
            
            # Retain at least MIN_RERANK_KEEP chunks if available to preserve sufficient context
            MIN_RERANK_KEEP = 3
            if len(filtered_chunks) < MIN_RERANK_KEEP and len(chunks) >= MIN_RERANK_KEEP:
                logger.info(f"Rerank Filter: Ripescati i top {MIN_RERANK_KEEP - len(filtered_chunks)} chunk sotto soglia per garantire un minimo di contesto all'LLM.")
                filtered_chunks = chunks[:MIN_RERANK_KEEP]
            elif len(filtered_chunks) < MIN_RERANK_KEEP and len(chunks) < MIN_RERANK_KEEP:
                filtered_chunks = chunks

            chunks = filtered_chunks
            
            if len(chunks) < before_filter:
                logger.info(f"Rerank Filter: scartati totali {before_filter - len(chunks)} chunk sotto soglia {min_score}")
            
            if chunks:
                logger.info(f"Reranking completato. Miglior score: {chunks[0].score:.4f}")
            else:
                logger.warning("Rerank Filter: NESSUN chunk ha superato la soglia di pertinenza.")
                
            return chunks
        except Exception as e:
            logger.error(f"Errore durante il reranking: {e}")
            return chunks

async def rerank_node(state: RagState) -> dict:
    """LangGraph node to apply cross-encoder reranking, slicing, and safety netting."""
    import time
    start = time.perf_counter()
    query = state["query"]
    fused_chunks = state.get("fused_chunks", [])
    reranker: Reranker = state.get("_reranker")
    
    if not fused_chunks or not reranker:
        return {"final_chunks": fused_chunks}

    # Extract task instruction from analyzed query if available
    instruction = ""
    if state.get("analyzed_query"):
        instruction = state["analyzed_query"].reranker_instruction

    # Execute reranking and filtering
    final_chunks = reranker.rerank(query, fused_chunks, instruction=instruction)
    
    # Truncate to top final_k results
    final_k = state.get("final_k", 10)
    final_chunks = final_chunks[:final_k]
    
    # Safety net: guarantee retention of top vector anchor chunk
    vector_anchor_id = state.get("vector_anchor_id")
    if vector_anchor_id:
        anchor_present = any(
            c.expression_id == vector_anchor_id or c.expression_id == f"merged_{vector_anchor_id}"
            for c in final_chunks
        )
        if not anchor_present:
            for c in fused_chunks:
                if c.expression_id == vector_anchor_id or c.expression_id == f"merged_{vector_anchor_id}":
                    logger.info(f"🛡️ Safety Net: ripescato anchor vettoriale '{c.expression_id}'")
                    final_chunks.append(c)
                    break
    
    elapsed = time.perf_counter() - start
    top_score = final_chunks[0].score if final_chunks else 0
    logger.info(
        f"[4/6] RERANK — {len(fused_chunks)} → {len(final_chunks)} chunk | "
        f"Top: {top_score:.4f} | {elapsed:.2f}s"
    )
    return {
        "final_chunks": final_chunks,
    }
