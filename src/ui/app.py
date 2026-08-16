import os
import streamlit as st
import asyncio
import nest_asyncio
import logging

from src.logging_config import setup_logging
setup_logging()

nest_asyncio.apply()

from src.rag.engine import RagEngine
from src.rag.supervisor import SupervisorAgent
from src.ui.components import render_retrieved_docs, render_graph_trace, render_agentic_log
from src.ui.rag_bridge import query_rag_with_trace, run_async

st.set_page_config(page_title="Legal GraphRAG", layout="wide", page_icon="⚖️")

# Inietta il CSS minimale
css_path = os.path.join(os.path.dirname(__file__), "style.css")
try:
    with open(css_path, "r") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
except FileNotFoundError:
    pass

@st.cache_resource
def get_engine():
    return RagEngine()

def get_supervisor(_engine):
    return SupervisorAgent(_engine)

engine = get_engine()
supervisor = get_supervisor(engine)

# --- SIDEBAR: Pannello di Ablazione ---
with st.sidebar:
    st.title("⚙️ Configurazioni")
    st.markdown("Usa questi controlli per lo **Studio di Ablazione**.")
    
    enable_graphrag = st.toggle("Attiva GraphRAG (Completo)", value=True, help="Disabilita per passare a un RAG vettoriale/BM25 di base.")
    
    with st.expander("🛠 Scaling Hardware (VRAM)", expanded=False):
        topo_max_chars = st.slider("Max Caratteri Espansione", 2000, 30000, 6000, step=1000)
        generator_num_ctx = st.slider("Context RAG (Generator)", 2048, 32768, 4096, step=1024)
        supervisor_num_ctx = st.slider("Context Supervisore", 4096, 32768, 16384, step=1024)
    
    st.markdown("---")
    top_k = st.slider("Top-K Retrieval (per canale)", 1, 30, 15)
    final_k = st.slider("Final-K (Post-Rerank)", 1, 20, 10)
    max_citation_hops = st.slider("Salti Multi-Hop (Citazioni)", 1, 5, 1)
    
    st.markdown("---")
    if st.button("🗑 Reset Conversazione"):
        st.session_state.messages = []
        st.session_state.traces = []
        st.rerun()

# --- GESTIONE STATO ---
if "messages" not in st.session_state:
    st.session_state.messages = []
if "traces" not in st.session_state:
    st.session_state.traces = []

# --- MAIN AREA ---
st.title("⚖️ Legal GraphRAG")
st.markdown("Assistente legale basato su Knowledge Graph e LLM locale.")

# Mostra lo storico dei messaggi e le tracce (XAI)
for i, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        
        # Mostra gli expander XAI se il messaggio è dell'assistant e abbiamo la traccia
        if msg["role"] == "assistant" and i//2 < len(st.session_state.traces):
            trace_data = st.session_state.traces[i//2]
            if trace_data:
                chunks = trace_data.get("chunks", [])
                state = trace_data.get("state", {})
                render_retrieved_docs(chunks)
                render_graph_trace(state)
                render_agentic_log(state)

# Gestione Input Utente
if prompt := st.chat_input("Poni una domanda (es. 'Quali sono le competenze delle regioni?')"):
    # Mostra messaggio utente
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Preparazione config per l'engine
    config = {
        "top_k": top_k,
        "final_k": final_k,
        "enable_graph_search": enable_graphrag,
        "enable_multi_hop": enable_graphrag,
        "max_citation_hops": max_citation_hops,
        "enable_topological_expansion": enable_graphrag,
        "topo_max_chars": topo_max_chars,
        "generator_num_ctx": generator_num_ctx,
        "supervisor_num_ctx": supervisor_num_ctx
    }

    # Ottenimento risposta
    with st.chat_message("assistant"):
        status = st.status("⚖️ Pipeline RAG in corso...", expanded=True)
        
        def status_callback(node_name: str, state_update: dict):
            import logging
            logging.getLogger(__name__).info(f"STATUS CALLBACK: {node_name}")
            node_map = {
                "supervisor_thinking": "Supervisore in fase di analisi...",
                "supervisor_tool_rag": "Il Supervisore sta interrogando il database...",
                "supervisor_generating_rag": "Il Supervisore sintetizza i risultati della ricerca...",
                "supervisor_generating_chat": "Il Supervisore formula la risposta conversazionale...",
                "contextualize_query": "Comprensione contesto multi-turno...",
                "analyze_query": "Analisi semantica e arricchimento TESEO...",
                "retrieve_all": "Interrogazione canali (Vector, BM25, Graph)...",
                "fuse_and_filter": "Fusione dei risultati e filtraggio temporale...",
                "rerank": "Reranking semantico dei documenti...",
                "expand_citations": "Recupero delle citazioni multi-hop...",
                "grade_documents": "Valutazione della pertinenza (Grading)...",
                "rewrite_query": "Riformulazione della query (Self-Reflection)..."
            }
            label = node_map.get(node_name, f"Esecuzione nodo: {node_name}...")
            status.update(label=label)
            
            # Generazione dei dettagli intelligenti basati sul nodo
            details = ""
            if node_name == "supervisor_tool_rag":
                q = state_update.get("query", "")
                details = f"Query inviata a GraphRAG: '{q}'"
            elif node_name == "analyze_query":
                analyzed = state_update.get("analyzed_query")
                query_text = analyzed.original_query if analyzed else ""
                details = f"Query analizzata: '{query_text}'"
            elif node_name == "retrieve_all":
                v = len(state_update.get("vector_results", []))
                b = len(state_update.get("bm25_results", []))
                g = len(state_update.get("graph_results", []))
                details = f"Trovati {v} (Vector), {b} (BM25), {g} (Graph) risultati preliminari."
            elif node_name == "fuse_and_filter":
                f = len(state_update.get("fused_chunks", []))
                details = f"Fusi e de-duplicati in {f} documenti unici."
            elif node_name == "rerank":
                r = len(state_update.get("final_chunks", []))
                details = f"Selezionati i migliori {r} documenti."
            elif node_name == "expand_citations":
                h = state_update.get("hop_count", 0)
                details = f"Eseguito hop citazionale #{h}."
            elif node_name == "grade_documents":
                docs = state_update.get("final_chunks", [])
                details = f"{len(docs)} documenti ritenuti pertinenti dall'LLM."
            elif node_name == "rewrite_query":
                iter = state_update.get("iterations", 1)
                new_q = state_update.get("query", "")
                details = f"Iterazione {iter}: Query riscritta in '{new_q}'"
            
            if details:
                status.write(f"🔹 **{node_map.get(node_name, node_name).split('...')[0]}**: {details}")
            else:
                status.write(f"🔹 {label}")

        # Costruisci lo storico per l'LLM con Sliding Window intelligente basata sui token
        # Stimiamo ~3 caratteri per token in italiano. Soglia massima: 12.000 token (su 32K).
        MAX_HISTORY_TOKENS = 12000
        chat_history = []
        current_tokens = 0
        
        # Iteriamo a ritroso, saltando l'ultimo messaggio (che è il prompt attuale, già gestito)
        for m in reversed(st.session_state.messages[:-1]):
            # Stima token (lunghezza testo / 3)
            msg_tokens = len(m["content"]) // 3
            if current_tokens + msg_tokens > MAX_HISTORY_TOKENS:
                break
            
            # Inseriamo in testa alla lista per mantenere l'ordine cronologico
            chat_history.insert(0, {"role": m["role"], "content": m["content"]})
            current_tokens += msg_tokens
        
        # Esecuzione async (Streamlit is sync, we use run_async)
        coro = query_rag_with_trace(supervisor, prompt, config, chat_history, status_callback)
        sync_generator = run_async(coro)
        
        # Ora eseguiamo lo stream della risposta visualmente. Durante questo stream, la pipeline e l'agente lavorano, e status_callback viene invocato!
        full_response = st.write_stream(sync_generator)
        
        # RECUPERIAMO i chunk e trace estratti dall'engine SOLO DOPO l'esecuzione del generatore
        final_chunks = getattr(supervisor.engine, "_temp_chunks", [])
        trace = getattr(supervisor.engine, "_temp_trace", {})

        # Alla fine, chiudiamo lo status
        status.update(label="✅ Ricerca e Generazione completate.", state="complete", expanded=False)

        # Mostriamo subito gli expander per la nuova risposta
        render_retrieved_docs(final_chunks)
        render_graph_trace(trace)
        render_agentic_log(trace)

    # Salviamo messaggio e traccia
    st.session_state.messages.append({"role": "assistant", "content": full_response})
    st.session_state.traces.append({
        "chunks": final_chunks,
        "state": trace
    })
