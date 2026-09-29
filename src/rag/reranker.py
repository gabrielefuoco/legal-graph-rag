"""Cross-Encoder reranking module with score thresholding and safety netting.

Two interchangeable backends are available (settings.RERANKER_BACKEND):
  - "local": Qwen3-Reranker CrossEncoder on GPU/CPU (~1.2 GB VRAM, ~3s per query).
  - "llm":   listwise reranking through a chat model on OpenRouter. OpenRouter exposes no
             dedicated rerank models, so the LLM judges all candidates in a single call.
"""

import asyncio
import logging
import os
import torch
from typing import List
from pydantic import BaseModel, Field
from sentence_transformers import CrossEncoder
from src.config import settings
from src.rag.models import RetrievedChunk, RagState

# Silence non-critical Hugging Face tokenizer warnings
os.environ["TOKENIZERS_PARALLELISM"] = "false"

logger = logging.getLogger(__name__)

# Minimum number of chunks kept even when every score falls below the threshold
MIN_RERANK_KEEP = 3


def sigmoid(x: float) -> float:
    """Map a raw CrossEncoder logit to a [0, 1] relevance probability."""
    import math
    return 1 / (1 + math.exp(-x))

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
            
            chunks = apply_rerank_threshold(filtered_chunks, chunks, min_score)
            
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


def apply_rerank_threshold(filtered_chunks: List[RetrievedChunk], ranked_chunks: List[RetrievedChunk], min_score: float) -> List[RetrievedChunk]:
    """Keep threshold-passing chunks, restoring the top candidates when too few survive."""
    if len(filtered_chunks) < MIN_RERANK_KEEP and len(ranked_chunks) >= MIN_RERANK_KEEP:
        logger.info(
            f"Rerank Filter: Ripescati i top {MIN_RERANK_KEEP - len(filtered_chunks)} chunk "
            f"sotto soglia per garantire un minimo di contesto all'LLM."
        )
        return ranked_chunks[:MIN_RERANK_KEEP]
    if len(filtered_chunks) < MIN_RERANK_KEEP and len(ranked_chunks) < MIN_RERANK_KEEP:
        return ranked_chunks
    return filtered_chunks


class RerankItem(BaseModel):
    """Relevance judgement for a single candidate chunk (1-based index)."""

    index: int = Field(description="Indice 1-based del documento nell'elenco fornito")
    relevance: float = Field(description="Pertinenza da 0.0 (irrilevante) a 1.0 (perfettamente pertinente)")


class RerankOutput(BaseModel):
    """Listwise relevance ranking of the candidate chunks."""

    items: List[RerankItem] = Field(description="Tutti i documenti valutati, dal più al meno pertinente")


class LLMReranker:
    """Listwise reranker backed by an OpenRouter chat model.

    Exposes the same interface as the local CrossEncoder reranker so the two backends are
    interchangeable: `rerank(query, chunks, instruction)` returns chunks with `score` in
    [0, 1], sorted by descending relevance and already thresholded.
    """

    MAX_DOC_CHARS = 1200

    def __init__(self, model_name: str = None):
        from src.rag.llm_factory import get_llm

        self.model_name = model_name or settings.RERANKER_LLM_MODEL or settings.OPENROUTER_MODEL_NAME
        logger.info(f"LLMReranker: modello '{self.model_name}' su {settings.OPENROUTER_BASE_URL}")
        self.llm = get_llm(temperature=0.0, reasoning=False, model_name=self.model_name)
        self.structured_llm = self.llm.with_structured_output(RerankOutput)
        self.system_prompt = (
            "Sei un giudice di pertinenza per la ricerca giuridica italiana. Ricevi una domanda e un "
            "elenco di documenti normativi/giurisprudenziali numerati. Valuta OGNI documento e assegna "
            "un punteggio di pertinenza da 0.0 a 1.0 rispetto alla domanda, poi restituisci l'elenco "
            "ordinato dal più al meno pertinente. Valuta la pertinenza sostanziale: un documento che "
            "risponde alla domanda, che ne definisce un termine o che ne disciplina un'eccezione è "
            "pertinente; un testo con parole chiave simili ma oggetto diverso non lo è."
        )

    def _build_messages(self, query: str, chunks: List[RetrievedChunk], instruction: str = None):
        from langchain_core.messages import SystemMessage, HumanMessage

        docs_text = ""
        for i, chunk in enumerate(chunks, 1):
            text = getattr(chunk, "expanded_text", None) or chunk.text or ""
            if len(text) > self.MAX_DOC_CHARS:
                text = text[: self.MAX_DOC_CHARS] + "..."
            source = chunk.structural_context or chunk.work_urn or "Fonte sconosciuta"
            docs_text += f"Documento {i} (Fonte: {source}):\n{text}\n\n"

        task = f"Istruzione: {instruction}\n" if instruction else ""
        user_message = f"{task}Domanda: {query}\n\nDocumenti:\n{docs_text}"
        return [SystemMessage(content=self.system_prompt), HumanMessage(content=user_message)]

    async def rerank(self, query: str, chunks: List[RetrievedChunk], instruction: str = None) -> List[RetrievedChunk]:
        """Score and reorder candidate chunks with a single listwise LLM call."""
        if not chunks:
            return chunks
        from copy import copy

        candidates = [copy(c) for c in chunks]
        messages = self._build_messages(query, candidates, instruction)

        try:
            response: RerankOutput = await self.structured_llm.ainvoke(messages)
        except Exception as e:
            logger.error(f"Errore durante il reranking LLM ({e}): ordine originale mantenuto.")
            return candidates

        # Guard against hallucinated indices: default missing items to 0.0
        relevance = {}
        for item in response.items:
            if 1 <= item.index <= len(candidates):
                relevance[item.index - 1] = max(0.0, min(1.0, float(item.relevance)))

        for idx, chunk in enumerate(candidates):
            chunk.score = relevance.get(idx, 0.0)
            chunk.source += "+rerank_llm"

        candidates.sort(key=lambda x: x.score, reverse=True)

        min_score = settings.RERANK_MIN_SCORE
        filtered = [c for c in candidates if c.score >= min_score]
        before_filter = len(filtered)
        kept = apply_rerank_threshold(filtered, candidates, min_score)

        logger.info(
            f"Rerank LLM: {len(candidates)} → {len(kept)} chunk "
            f"(scartati {len(candidates) - before_filter} sotto soglia {min_score})"
        )
        if kept:
            logger.info(f"Reranking LLM completato. Miglior score: {kept[0].score:.4f}")
        return kept


def build_reranker():
    """Instantiate the reranker backend configured in settings.

    Falls back to the local CrossEncoder when the LLM backend is requested but the active
    provider is not OpenRouter (no chat API available for the listwise judgement).
    """
    backend = (getattr(settings, "RERANKER_BACKEND", "local") or "local").strip().lower()
    provider = (settings.LLM_PROVIDER or "").strip().lower()

    if backend == "llm":
        if provider != "openrouter":
            logger.warning(
                f"RERANKER_BACKEND=llm richiesto ma LLM_PROVIDER='{provider}': uso il reranker locale."
            )
        else:
            return LLMReranker()
    return Reranker()

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

    # Execute reranking and filtering (the LLM backend is asynchronous)
    final_chunks = reranker.rerank(query, fused_chunks, instruction=instruction)
    if asyncio.iscoroutine(final_chunks):
        final_chunks = await final_chunks
    
    # Truncate to top final_k results
    final_k = state.get("final_k", 10)
    final_chunks = final_chunks[:final_k]

    # Safety net: provisions resolved from the reference named in the question must reach the
    # judge/generator even if the listwise reranker ranked them below the cut-off.
    resolved = [c for c in fused_chunks if "citation_resolve" in (c.source or "")]
    if resolved:
        kept_ids = {c.expression_id for c in final_chunks}
        for chunk in resolved:
            if chunk.expression_id not in kept_ids:
                final_chunks.append(chunk)
                kept_ids.add(chunk.expression_id)
                logger.info(f"🛡️ Rinvio risolto reinserito dopo il reranking: {chunk.expression_id}")
    
    # Safety net: the best chunks of the base channels are a floor. Graph-injected context may add
    # to them but must never push them out of the window (that was the recall loss).
    priority_ids = state.get("priority_base_ids") or []
    if priority_ids:
        kept_ids = {c.expression_id for c in final_chunks}
        for priority_id in priority_ids:
            if priority_id in kept_ids or f"merged_{priority_id}" in kept_ids:
                continue
            for c in fused_chunks:
                if c.expression_id == priority_id or c.expression_id == f"merged_{priority_id}":
                    final_chunks.append(c)
                    kept_ids.add(c.expression_id)
                    logger.info(f"🛡️ Chunk base prioritario mantenuto dopo il reranking: {c.expression_id}")
                    break

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
