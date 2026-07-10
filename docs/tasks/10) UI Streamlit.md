# Fase 10: UI Streamlit (Demo Interattiva)

L'obiettivo finale del progetto di tesi è dimostrare in modo visivo, concreto e interattivo i vantaggi del GraphRAG rispetto al RAG tradizionale. Streamlit offre gli strumenti perfetti per creare una dashboard di test.

---

## 1. Struttura dell'Interfaccia

### Sidebar (Pannello di Controllo)
Conterrà le configurazioni di runtime, fondamentali per lo **Studio di Ablazione**:
- **Selettore Modello LLM**: Dropdown per selezionare quale LLM usare per la generazione.
- **Selettore Top-K**: Slider per decidere quanti documenti recuperare da Weaviate (es. 1-15).
- **Toggle "Attiva Graph Search"**: Mappa direttamente la variabile `enable_graph_search` in `RagEngine`.
- **Toggle "Attiva Multi-hop Citazionale"**: Mappa `enable_multi_hop`.
*(Disattivando entrambi, il sistema agirà come un normale Naive RAG Vettoriale).*

### Main Area (Chat e Risultati)
L'area principale simulerà un assistente virtuale.
- Utilizzo di `st.chat_input` e `st.chat_message` per memorizzare il log delle conversazioni nel session state.

## 2. Visualizzazione della Trasparenza (XAI)
Poiché l'obiettivo della tesi è analizzare *come* il grafo migliora i risultati, la UI non deve mostrare solo la risposta finale, ma anche le "scatole nere" del processo.

Per ogni risposta generata, la UI mostrerà componenti `st.expander` (menu a tendina espandibili):
1. **🔍 Documenti Recuperati:** Una tabella o lista con i chunk, i loro metadati, da quale sorgente provengono (Vettoriale, BM25 o TESEO) e lo Score RRF finale.
2. **🕸️ Graph Trace (Log Multi-hop):** Quali nodi (es. Atti o Articoli) il grafo ha deciso di espandere tramite la relazione `:CITES`. Qual è stato il path seguito su Neo4j.
3. **🤖 Agentic Loop (se la Fase 9 è attiva):** I log del "pensiero" del sistema (es. "Documenti insufficienti, riscrivo la query in: [...]").

## 3. Gestione di LangGraph in Streamlit
Poiché Streamlit esegue il codice in modo sincrono ad ogni re-render, e `RagEngine` usa `asyncio`, sarà necessario un wrapper.
```python
import asyncio
import streamlit as st

def run_rag_query(query, conf):
    engine = RagEngine()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Ritorna l'ultimo stato del grafo
    return loop.run_until_complete(engine.retrieve(query, **conf))
```

## 4. Criteri di Accettazione
1. La UI deve permettere confronti side-by-side: testare la stessa query con "Graph Search" a OFF, leggere la risposta, e poi ripetere con "Graph Search" a ON per mostrare il recupero di informazioni che un semplice semantic search mancava.
2. La UI non deve bloccarsi o svuotare la cronologia chat a ogni nuovo messaggio.
