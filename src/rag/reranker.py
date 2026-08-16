import logging
import os
import torch
from typing import List
from sentence_transformers import CrossEncoder
from src.config import settings
from src.rag.models import RetrievedChunk, RagState

# Silenzia warning di Hugging Face non critici
os.environ["TOKENIZERS_PARALLELISM"] = "false"

logger = logging.getLogger(__name__)

class Reranker:
    """
    Reranker basato su Cross-Encoder con supporto GPU e filtraggio per soglia.
    """

    def __init__(self, model_name: str = None):
        model_name = model_name or settings.RERANKER_MODEL_NAME
        device = "cuda" if torch.cuda.is_available() else "cpu"
        # Supporto per Mac M1/M2/M3
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
        """
        Applica il reranking e filtra i risultati sotto la soglia minima.
        Supporta l'iniezione di istruzioni per modelli task-aware.
        """
        if not self.model or not chunks:
            return chunks
        from copy import copy
        chunks = [copy(c) for c in chunks]

        # Se presente un'istruzione, formattiamo la query per il modello instruction-aware
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
                # Normalizziamo il raw logit in una probabilità [0,1]
                prob = sigmoid(float(score))
                chunk.score = prob
                chunk.source += "+rerank"

            # Riordina
            chunks.sort(key=lambda x: x.score, reverse=True)
            
            # Applica il filtro di qualità (Thresholding)
            min_score = settings.RERANK_MIN_SCORE
            before_filter = len(chunks)
            filtered_chunks = []
            for c in chunks:
                if c.score >= min_score:
                    logger.info(f"⚖️ Reranker: Promosso '{c.expression_id}' (Score: {c.score:.4f})")
                    filtered_chunks.append(c)
                else:
                    logger.info(f"✂️ Reranker: Scartato '{c.expression_id}' (Score: {c.score:.4f} < {min_score})")
            
            # Garantisci un minimo di 3 chunk all'LLM (se ce ne sono abbastanza in partenza)
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
    """
    Nodo LangGraph: applica il reranking e pulisce i risultati finali.
    """
    import time
    start = time.perf_counter()
    query = state["query"]
    fused_chunks = state.get("fused_chunks", [])
    reranker: Reranker = state.get("_reranker")
    
    if not fused_chunks or not reranker:
        return {"final_chunks": fused_chunks}

    # Estraiamo l'istruzione dall'analisi della query (se disponibile)
    instruction = ""
    if state.get("analyzed_query"):
        instruction = state["analyzed_query"].reranker_instruction

    # Applichiamo il reranking e il filtraggio
    final_chunks = reranker.rerank(query, fused_chunks, instruction=instruction)
    
    # Taglio al final_k (es. top 5 dei migliori sopra soglia)
    final_k = state.get("final_k", 10)
    final_chunks = final_chunks[:final_k]
    
    # --- SAFETY NET ---
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
    # ------------------
    
    elapsed = time.perf_counter() - start
    top_score = final_chunks[0].score if final_chunks else 0
    logger.info(
        f"[4/6] RERANK — {len(fused_chunks)} → {len(final_chunks)} chunk | "
        f"Top: {top_score:.4f} | {elapsed:.2f}s"
    )
    return {
        "final_chunks": final_chunks,
    }
