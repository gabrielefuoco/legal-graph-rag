import logging
import asyncio
from langchain_core.messages import HumanMessage, AIMessage

from src.rag.supervisor import SupervisorAgent

logger = logging.getLogger(__name__)

def run_async(coro):
    """Esegue una coroutine async nel contesto sincrono di Streamlit."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

async def query_rag_with_trace(supervisor: SupervisorAgent, query: str, config: dict, chat_history: list, status_callback=None) -> tuple:
    """
    Esegue il Supervisor Agent natively via astream_events.
    Ritorna dei reference (liste/dizionari) che verranno popolati durante lo streaming.
    """
    try:
        messages = []
        for msg in chat_history:
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))
            elif msg["role"] == "assistant":
                messages.append(AIMessage(content=msg["content"]))
        
        messages.append(HumanMessage(content=query))
        
        graph = supervisor.get_graph(status_callback, config)

        out_chunks = []
        out_trace = {}

        async def async_gen():
            logger.info("Avvio astream_events del Supervisor")
            try:
                # Usiamo stream_mode='messages' per isolare solo i messaggi del Supervisor
                async for msg, metadata in graph.astream(
                    {"messages": messages}, 
                    stream_mode="messages",
                    config={"recursion_limit": 50}
                ):
                    node = metadata.get("langgraph_node")
                    # Rimosso il log per evitare flood del terminale per ogni singolo token
                    # logger.info(f"LangGraph Stream Event - Node: {node}, Type: {type(msg)}")
                    
                    # Intercettiamo i token in output ESCLUSIVAMENTE dal nodo 'agent' (il Supervisor)
                    if node == "agent":
                        if msg.content and isinstance(msg.content, str):
                            # Se msg ha tool_call_chunks, non stamparlo per evitare log JSON sporchi
                            if hasattr(msg, "tool_call_chunks") and msg.tool_call_chunks:
                                pass
                            else:
                                yield msg.content
                    
                    # Hack: aggiorniamo costantemente out_chunks e out_trace
                    if hasattr(supervisor.engine, "_temp_chunks") and supervisor.engine._temp_chunks:
                        out_chunks.clear()
                        out_chunks.extend(supervisor.engine._temp_chunks)
                    if hasattr(supervisor.engine, "_temp_trace") and supervisor.engine._temp_trace:
                        out_trace.update(supervisor.engine._temp_trace)
            except Exception as e:
                logger.error(f"Errore durante astream: {e}")
                yield f"Errore: {str(e)}"
            
        def sync_gen():
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Prepariamo l'iteratore
            agen = async_gen()
            try:
                while True:
                    try:
                        chunk = loop.run_until_complete(agen.__anext__())
                        if chunk:
                            yield chunk
                    except StopAsyncIteration:
                        break
            except Exception as e:
                logger.error(f"Errore durante sync_gen: {e}")
                yield f"Errore tecnico: {str(e)}"

        return sync_gen()

    except Exception as e:
        logger.error(f"Errore: {e}")
        err_msg = str(e)
        def sync_err_gen():
            yield f"Errore tecnico: {err_msg}"
        return [], {}, sync_err_gen()
