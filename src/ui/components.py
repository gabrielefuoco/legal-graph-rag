import streamlit as st

def get_badge_html(source_string: str) -> str:
    """Restituisce HTML per i badge dei canali di origine."""
    badges = []
    if "vector" in source_string:
        badges.append('<span class="badge-vector">Vector</span>')
    if "bm25" in source_string:
        badges.append('<span class="badge-bm25">BM25</span>')
    if "graph" in source_string:
        badges.append('<span class="badge-graph">Graph</span>')
    if "citation_hop" in source_string:
        badges.append('<span class="badge-hop">Hop</span>')
    if "rerank" in source_string:
        badges.append('<span class="badge-rerank">Reranked</span>')
        
    return " ".join(badges) if badges else source_string

def render_retrieved_docs(chunks):
    """Renderizza la lista dei documenti recuperati in un expander."""
    with st.expander(f"🔍 Documenti Recuperati ({len(chunks)})", expanded=False):
        if not chunks:
            st.write("Nessun documento recuperato.")
            return

        for i, chunk in enumerate(chunks, 1):
            st.markdown(f"**[{i}] {chunk.structural_context or 'Documento'}** (Score: `{chunk.score:.4f}`)")
            st.markdown(f"Origine: {get_badge_html(chunk.source)}", unsafe_allow_html=True)
            if chunk.work_urn and chunk.work_urn != "urn:unknown":
                st.caption(f"URN: `{chunk.work_urn}`")
            
            # Mostra solo i primi 300 caratteri per non ingombrare troppo
            text_preview = chunk.text
            if len(text_preview) > 300:
                text_preview = text_preview[:300] + "..."
            st.markdown(f"> {text_preview}")
            st.divider()

def render_graph_trace(state: dict):
    """Renderizza la traccia di esecuzione del grafo (TESEO e multi-hop)."""
    with st.expander("🕸️ Graph Trace (TESEO & Multi-hop)", expanded=False):
        if not state.get("enable_graph_search", True) and not state.get("enable_multi_hop", True):
            st.info("Ricerca su grafo e Multi-hop sono attualmente disabilitati.")
            return

        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### TESEO Concepts")
            analyzed = state.get("analyzed_query")
            if analyzed:
                if analyzed.expanded_labels:
                    st.success(f"Trovati {len(analyzed.teseo_concept_ids)} concetti.")
                    st.write(", ".join(analyzed.expanded_labels))
                else:
                    st.write("Nessun concetto TESEO individuato nella query.")
            else:
                st.write("Analisi query non disponibile.")
                
        with col2:
            st.markdown("#### Citation Multi-hop")
            hop_count = state.get("hop_count", 0)
            if not state.get("enable_multi_hop", True):
                st.write("Espansione citazioni disabilitata.")
            elif hop_count > 0:
                st.success(f"Eseguiti {hop_count} hop citazionali.")
            else:
                st.write("Nessuna citazione trovata nel contesto primario.")
                
        st.markdown("#### Metriche di Retrieval Parziale")
        st.write(f"- Nodi Vettoriali: {state.get('vector_results_count', 0)}")
        st.write(f"- Nodi BM25: {state.get('bm25_results_count', 0)}")
        st.write(f"- Nodi Grafo (TESEO): {state.get('graph_results_count', 0)}")

def render_agentic_log(state: dict):
    """Renderizza i log del loop agentico self-reflective."""
    with st.expander("🤖 Agentic Loop (Self-Reflective)", expanded=False):
        iterations = state.get("iterations", 0)
        
        if iterations == 0:
            st.success("Nessuna iterazione aggiuntiva necessaria. I primi documenti recuperati erano pertinenti.")
        else:
            st.warning(f"Rilevata scarsa pertinenza iniziale. Eseguite {iterations} iterazioni di correzione.")
            
            rewritten = state.get("rewritten_queries", [])
            if rewritten:
                st.markdown("##### Storico riformulazioni query:")
                for i, q in enumerate(rewritten, 1):
                    st.markdown(f"{i}. `{q}`")
