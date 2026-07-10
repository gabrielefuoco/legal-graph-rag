import logging
import asyncio
from typing import List, Dict, Any

from langchain_ollama import ChatOllama
from langchain_core.messages import SystemMessage, HumanMessage

from src.config import settings
from src.rag.models import RagState, RetrievedChunk

logger = logging.getLogger(__name__)

class RetrievalGrader:
    """
    Valuta la pertinenza dei documenti recuperati rispetto alla query,
    usando un LLM-as-a-judge (Qwen in locale).
    """
    def __init__(self):
        self.llm = ChatOllama(
            base_url=settings.QWEN3_ENDPOINT,
            model=settings.GENERATIVE_MODEL_NAME,
            temperature=0.0,
        )
        self.system_prompt = (
            "Sei un valutatore di pertinenza (Grader). Il tuo compito è valutare "
            "se un determinato documento contiene informazioni utili e pertinenti "
            "per rispondere a una specifica domanda.\n"
            "Rispondi ESCLUSIVAMENTE con la parola 'yes' se il documento è utile, "
            "o con la parola 'no' se il documento è inutile o fuori contesto.\n"
            "Non aggiungere nessun'altra parola o spiegazione."
        )

    async def grade_chunk(self, query: str, chunk: RetrievedChunk) -> bool:
        user_message = f"Domanda: {query}\n\nDocumento: {chunk.text}"
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=user_message)
        ]
        try:
            response = await self.llm.ainvoke(messages)
            content = response.content.strip().lower()
            return "yes" in content
        except Exception as e:
            logger.error(f"Errore durante il grading del chunk: {e}")
            # Fallback: se l'LLM fallisce, consideriamo il documento utile per non perderlo
            return True 

class QueryRewriter:
    """
    Riformula una query utente che non ha prodotto documenti rilevanti,
    cercando di renderla più adatta al retrieval semantico.
    """
    def __init__(self):
        self.llm = ChatOllama(
            base_url=settings.QWEN3_ENDPOINT,
            model=settings.GENERATIVE_MODEL_NAME,
            temperature=0.3, # Leggera variabilità per favorire la riformulazione
        )
        self.system_prompt = (
            "Sei un esperto legale e un ottimizzatore di query di ricerca. "
            "Il tuo compito è prendere una domanda utente, che non ha prodotto risultati soddisfacenti, "
            "e riformularla in modo che sia più adatta a un motore di ricerca (RAG).\n"
            "Cerca di evidenziare i concetti chiave, espandere eventuali acronimi impliciti "
            "e formulare la query in modo chiaro e diretto. Se ci sono tentativi precedenti, "
            "cerca di esplorare sinonimi o terminologia legale equivalente.\n"
            "Rispondi ESCLUSIVAMENTE con la nuova query riformulata, senza aggiungere introduzioni o spiegazioni."
        )

    async def rewrite(self, query: str, history: List[str]) -> str:
        history_text = "\n".join([f"- {q}" for q in history]) if history else "Nessuno."
        user_message = (
            f"Domanda attuale: {query}\n"
            f"Tentativi passati: {history_text}\n\n"
            "Fornisci solo la nuova query riformulata:"
        )
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=user_message)
        ]
        try:
            response = await self.llm.ainvoke(messages)
            return response.content.strip()
        except Exception as e:
            logger.error(f"Errore durante il rewriting della query: {e}")
            return query


async def grade_documents_node(state: RagState) -> dict:
    """Nodo LangGraph per valutare i documenti (Chunk-by-Chunk)."""
    grader: RetrievalGrader = state.get("_grader")
    if not grader:
        grader = RetrievalGrader()
    
    query = state["query"]
    chunks = state.get("final_chunks") or state.get("fused_chunks") or []
    
    if not chunks:
        logger.info("Nessun documento da valutare in grade_documents.")
        return {"final_chunks": []}
    
    logger.info(f"Avvio grading di {len(chunks)} documenti per la query: '{query}'")
    tasks = [grader.grade_chunk(query, chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks)
    
    filtered_chunks = []
    for chunk, is_relevant in zip(chunks, results):
        if is_relevant:
            filtered_chunks.append(chunk)
        else:
            logger.debug(f"Scartato chunk (URN: {chunk.work_urn}) in quanto valutato irrilevante.")
            
    logger.info(f"Documenti pertinenti dopo grading: {len(filtered_chunks)} su {len(chunks)}")
    return {"final_chunks": filtered_chunks}


async def rewrite_query_node(state: RagState) -> dict:
    """Nodo LangGraph per riformulare la query e incrementare l'iterazione."""
    rewriter: QueryRewriter = state.get("_rewriter")
    if not rewriter:
        rewriter = QueryRewriter()
        
    current_query = state["query"]
    iterations = state.get("iterations", 0)
    rewritten_history = state.get("rewritten_queries", [])
    
    logger.info(f"Iterazione {iterations}/{settings.MAX_AGENTIC_ITERATIONS}: Riscrittura della query '{current_query}'")
    
    new_query = await rewriter.rewrite(current_query, rewritten_history)
    logger.info(f"Query riformulata: '{new_query}'")
    
    new_history = list(rewritten_history)
    new_history.append(current_query)
    
    return {
        "query": new_query,
        "iterations": iterations + 1,
        "rewritten_queries": new_history
    }
