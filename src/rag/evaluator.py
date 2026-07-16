import logging
import asyncio
from typing import List, Dict, Any

from langchain_ollama import ChatOllama
from langchain_core.messages import SystemMessage, HumanMessage
from pydantic import BaseModel, Field

from src.config import settings
from src.rag.models import RagState, RetrievedChunk

logger = logging.getLogger(__name__)

class GraderOutput(BaseModel):
    reasoning: str = Field(
        description="Breve ragionamento per spiegare quali documenti sono utili o vanno scartati e perché."
    )
    relevant_chunks: List[int] = Field(
        description="Lista dei numeri identificativi dei documenti pertinenti. Lista vuota se nessuno è pertinente."
    )

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
            num_ctx=4096,
            reasoning=False,
        )
        self.structured_llm = self.llm.with_structured_output(GraderOutput)
        self.system_prompt = (
            "Sei un valutatore di pertinenza (Grader) per un dominio legale. "
            "Il tuo compito è valutare rigorosamente quali dei documenti forniti sono utili per rispondere alla domanda dell'utente.\n\n"
            "CRITERI DI INCLUSIONE (Considera un documento PERTINENTE se soddisfa almeno UNA di queste condizioni):\n"
            "1. Risponde direttamente o parzialmente alla domanda.\n"
            "2. Fornisce la definizione di un termine o istituto giuridico citato nella domanda.\n"
            "3. Contiene una deroga, un'eccezione o una modifica normativa a quanto richiesto.\n"
            "4. Definisce l'ambito di applicazione della norma in questione.\n\n"
            "CRITERI DI ESCLUSIONE (SCARTA il documento se):\n"
            "1. Contiene parole chiave simili ma si riferisce a un contesto o istituto giuridico palesemente diverso.\n"
            "2. È un frammento procedurale marginale che non aggiunge valore sostanziale.\n\n"
            "Nel dubbio, sii permissivo e considera il documento PERTINENTE per non perdere informazioni vitali.\n"
            "Analizza prima il contesto e formula un breve ragionamento, poi restituisci l'elenco degli ID pertinenti."
        )

    async def grade_chunks_batch(self, query: str, chunks: List[RetrievedChunk]) -> List[bool]:
        docs_text = ""
        for i, chunk in enumerate(chunks, 1):
            truncated_text = chunk.text[:1500] + "..." if len(chunk.text) > 1500 else chunk.text
            docs_text += f"Documento {i}:\n{truncated_text}\n\n"
            
        user_message = f"Domanda: {query}\n\n{docs_text}"
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=user_message)
        ]
        
        try:
            response: GraderOutput = await self.structured_llm.ainvoke(messages)
            relevant = response.relevant_chunks if response.relevant_chunks else []
            logger.info(f"[GRADE] Output strutturato LLM: documenti pertinenti {relevant}")
            
            results = []
            for i in range(1, len(chunks) + 1):
                results.append(i in relevant)
            return results
            
        except Exception as e:
            logger.error(f"Errore durante il grading batch strutturato dei chunk: {e}")
            # Fallback: keep all documents
            return [True] * len(chunks)

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
            reasoning=False,
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
            from src.rag.think_filter import strip_thinking_tags
            response = await self.llm.ainvoke(messages)
            return strip_thinking_tags(response.content).strip()
        except Exception as e:
            logger.error(f"Errore durante il rewriting della query: {e}")
            return query


async def grade_documents_node(state: RagState) -> dict:
    """Nodo LangGraph per valutare i documenti (Chunk-by-Chunk)."""
    import time
    start = time.perf_counter()
    grader: RetrievalGrader = state.get("_grader")
    if not grader:
        grader = RetrievalGrader()
    
    query = state["query"]
    chunks = state.get("final_chunks") or state.get("fused_chunks") or []
    
    if not chunks:
        logger.info("Nessun documento da valutare in grade_documents.")
        return {"final_chunks": []}
    
    logger.info(f"[GRADE] Avvio batch grading di {len(chunks)} documenti...")
    results = await grader.grade_chunks_batch(query, chunks)
    
    filtered_chunks = []
    for chunk, is_relevant in zip(chunks, results):
        if is_relevant:
            filtered_chunks.append(chunk)
        else:
            logger.debug(f"Scartato chunk (URN: {chunk.work_urn}) in quanto valutato irrilevante.")
            
    elapsed = time.perf_counter() - start
    logger.info(f"[GRADE] Completato in {elapsed:.1f}s — {len(filtered_chunks)}/{len(chunks)} pertinenti")
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
    
    if not new_query or len(new_query.strip()) < 5:
        logger.warning(f"[REWRITE] Query riformulata vuota o troppo corta, mantengo l'originale: '{current_query}'")
        new_query = current_query
        
    logger.info(f"Query riformulata: '{new_query}'")
    
    new_history = list(rewritten_history)
    new_history.append(current_query)
    
    return {
        "query": new_query,
        "iterations": iterations + 1,
        "rewritten_queries": new_history
    }
