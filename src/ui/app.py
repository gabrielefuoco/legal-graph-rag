import os
import streamlit as st
import asyncio
import nest_asyncio

nest_asyncio.apply()

from src.rag.engine import RagEngine
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
    """Inizializza l'engine RAG una sola volta."""
    return RagEngine()

engine = get_engine()

# --- SIDEBAR: Pannello di Ablazione ---
with st.sidebar:
    st.title("⚙️ Configurazioni")
    st.markdown("Usa questi controlli per lo **Studio di Ablazione**.")
    
    enable_graph = st.toggle("Attiva Graph Search (TESEO)", value=True)
    enable_multihop = st.toggle("Attiva Multi-hop Citazionale", value=True)
    
    st.markdown("---")
    top_k = st.slider("Top-K Retrieval (per canale)", 1, 30, 15)
    final_k = st.slider("Final-K (Post-Rerank)", 1, 10, 5)
    
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
        "enable_graph_search": enable_graph,
        "enable_multi_hop": enable_multihop
    }

    # Ottenimento risposta
    with st.chat_message("assistant"):
        with st.spinner("Analisi e recupero informazioni..."):
            # Costruisci lo storico per l'LLM: prendi gli ultimi 4 turni
            chat_history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[:-1][-4:]]
            
            # Esecuzione async (Streamlit is sync, we use run_async)
            coro = query_rag_with_trace(engine, prompt, config, chat_history)
            final_chunks, trace, sync_generator = run_async(coro)

        # Ora eseguiamo lo stream della risposta visualmente
        full_response = st.write_stream(sync_generator)

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
