import logging
import asyncio
from typing import AsyncGenerator
from src.rag.engine import RagEngine

logger = logging.getLogger(__name__)

def run_async(coro):
    """Esegue una coroutine async nel contesto sincrono di Streamlit."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

async def query_rag_with_trace(engine: RagEngine, query: str, config: dict, chat_history: list) -> tuple:
    """
    Esegue il retrieval RAG saltando il nodo di generazione interno del grafo.
    Restituisce i chunk finali, i metadati della traccia e un generatore per lo streaming
    testuale diretto dall'LLM.
    """
    try:
        # Usa il nuovo metodo che espone la traccia, chiedendo di saltare la generazione
        final_chunks, trace, _ = await engine.retrieve_with_trace(
            query=query,
            reference_date=None,
            top_k=config.get("top_k", 15),
            final_k=config.get("final_k", 5),
            enable_graph_search=config.get("enable_graph_search", True),
            enable_multi_hop=config.get("enable_multi_hop", True),
            chat_history=chat_history,
            skip_generation=True
        )

        # Fallback se nessun documento è stato trovato dopo tutti i tentativi
        if not final_chunks and trace.get("iterations", 0) >= 5: # MAX_AGENTIC_ITERATIONS
            def fallback_gen():
                yield "Non dispongo di informazioni sufficienti per rispondere a questa domanda."
            return final_chunks, trace, fallback_gen()

        # Generazione reale token-by-token
        # Passiamo la query finale riscritta (se presente in trace) o quella originale
        stream_query = trace.get("query", query)
        
        # Iniziamo la stream usando il LegalGenerator
        async_gen = engine.generator.generate_stream(stream_query, final_chunks)
        
        # Converte il generatore asincrono in un generatore sincrono compatibile con st.write_stream
        def sync_gen():
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            while True:
                try:
                    chunk = loop.run_until_complete(async_gen.__anext__())
                    yield chunk
                except StopAsyncIteration:
                    break

        return final_chunks, trace, sync_gen()

    except Exception as e:
        logger.error(f"Errore: {e}")
        def sync_err_gen():
            yield f"Errore tecnico: {str(e)}"
        return [], {}, sync_err_gen()
