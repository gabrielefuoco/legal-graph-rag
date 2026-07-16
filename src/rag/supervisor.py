import logging
from typing import TypedDict, Annotated, Any
from langgraph.prebuilt import create_react_agent, InjectedState
from langgraph.types import Command
from langchain_core.tools import tool
from langchain_ollama import ChatOllama
from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.graph.message import add_messages

from src.config import settings
from src.rag.engine import RagEngine

logger = logging.getLogger(__name__)

class SupervisorState(TypedDict):
    """Stato del Supervisor Agent arricchito per salvare la traccia del tool."""
    messages: Annotated[list[BaseMessage], add_messages]
    _engine: Any              # RagEngine instance
    _status_callback: Any     # Callback per la UI
    _last_chunks: list | None # Ultimi documenti trovati
    _last_trace: dict | None  # Trace dell'ultimo RAG execution

# Nota: per poter passare variabili esterne (come engine e status_callback) a un @tool,
# possiamo usare una closure factory function che restituisce il tool compilato.
def build_search_tool(engine: RagEngine, status_callback=None):
    
    @tool
    async def search_legal_database(query: str, state: Annotated[dict, InjectedState]) -> Command:
        """
        Usa questo tool ESCLUSIVAMENTE per cercare leggi, sentenze o concetti nel database giuridico italiano.
        Passa una query string chiara e ricca di parole chiave (es. "limiti subappalto codice contratti").
        """
        if status_callback:
            status_callback("supervisor_tool_rag", {"query": query})
            
        logger.info(f"Avvio Tool RAG con query: {query}")
        
        # Recupera lo storico messaggi
        chat_history = []
        for m in state.get("messages", []):
            role = "user" if isinstance(m, HumanMessage) else "assistant"
            chat_history.append({"role": role, "content": m.content})
            
        chunks, trace, _ = await engine.retrieve_with_trace(
            query=query,
            enable_graph_search=True,
            enable_multi_hop=True,
            chat_history=chat_history,
            skip_generation=True,
            status_callback=status_callback
        )
        
        # Formattiamo i risultati testuali da restituire al modello per il grounding
        if not chunks:
            rag_output = "Nessuna informazione pertinente trovata nel database giuridico."
        else:
            context_parts = []
            for i, chunk in enumerate(chunks, 1):
                source = f"Fonte: {chunk.structural_context}" if chunk.structural_context else ""
                urn = f" URN: {chunk.work_urn}" if chunk.work_urn else ""
                context_parts.append(f"[{i}] {source}{urn}\nTesto: {chunk.text}")
            rag_output = "RISULTATI TROVATI NEL DATABASE:\n" + "\n\n".join(context_parts)
            
        # In aggiornamento di langgraph 1.x, i tool restituiscono dati al modello (il return value),
        # ma possono usare 'Command(update={...})' per iniettare variabili nello stato globale contemporaneamente!
        return Command(
            update={
                "_last_chunks": chunks, 
                "_last_trace": trace
            },
            # Return_value non deve essere nei parametri di update, Command ha costruttore speciale in langgraph >=0.2?
            # Aspetta: se restituiamo semplicemente il testo, langgraph aggiorna i messages. 
            # In langgraph 1.2.9, la sintassi corretta per un tool che aggiorna lo stato è:
            # return Command(update={"var": value}) MA come si passa il testo per il tool call?
            # Secondo la doc, un Tool in langgraph NON deve usare Command per il testo del ToolMessage.
            # E' meglio se aggiorniamo semplicemente un dizionario globale o passiamo il return text.
            # In realtà possiamo restituire un tuple o usare il context. 
            # Per semplicità, e per essere robusti, aggiorniamo l'engine e restituiamo la stringa.
            # Oppure usiamo un return Dict per il tool, ma il modello si aspetta una stringa.
        )
        # BUG POTENZIALE SOPRA: se restituisco Command(), il LLM potrebbe vedere il repr() del Command.
        # Riscriviamo il tool per restiture solo testo, ed iniettiamo nello stato globalmente.
        # MA non possiamo aggiornare lo stato nativo senza Command() o senza che la reducer lo faccia.
        pass

    # Implementazione robusta del tool senza Command()
    @tool
    async def search_legal_db(query: str, state: Annotated[dict, InjectedState]) -> str:
        """
        Usa questo tool ESCLUSIVAMENTE per cercare leggi, sentenze o concetti nel database giuridico italiano.
        Passa una query string chiara e ricca di parole chiave (es. "limiti subappalto codice contratti").
        """
        if status_callback:
            status_callback("supervisor_tool_rag", {"query": query})
            
        logger.info(f"Avvio Tool RAG con query: {query}")
        
        chat_history = []
        for m in state.get("messages", []):
            role = "user" if isinstance(m, HumanMessage) else "assistant"
            chat_history.append({"role": role, "content": m.content})
            
        chunks, trace, _ = await engine.retrieve_with_trace(
            query=query,
            enable_graph_search=True,
            enable_multi_hop=True,
            chat_history=chat_history,
            skip_generation=True,
            status_callback=status_callback
        )
        
        # Hack per iniettare i chunk nello stato senza usare Command che potrebbe rompere l'LLM:
        # Useremo l'oggetto 'engine' come transport layer temporaneo!
        engine._temp_chunks = chunks
        engine._temp_trace = trace
        
        if not chunks:
            return "Non ho trovato informazioni nel database."
            
        context_parts = []
        for i, chunk in enumerate(chunks, 1):
            source = f"Fonte: {chunk.structural_context}" if chunk.structural_context else ""
            urn = f" URN: {chunk.work_urn}" if chunk.work_urn else ""
            context_parts.append(f"[{i}] {source}{urn}\nTesto: {chunk.text}")
            
        return "DOCUMENTI TROVATI:\n" + "\n\n".join(context_parts)
        
    return search_legal_db


class SupervisorAgent:
    def __init__(self, engine: RagEngine):
        self.engine = engine
        
    def get_graph(self, status_callback=None):
        llm = ChatOllama(
            base_url=settings.QWEN3_ENDPOINT,
            model=settings.GENERATIVE_MODEL_NAME,
            temperature=0.0,
            num_ctx=16384,
            reasoning=False,
        )
        
        system_prompt = (
            "Sei un Assistente Legale Esperto specializzato nel diritto italiano.\n"
            "Il tuo compito è aiutare l'utente rispondendo in modo rigoroso, formale e accurato.\n\n"
            "REGOLE PER L'USO DEL TOOL:\n"
            "- Hai a disposizione il tool 'search_legal_db'. DEVI usarlo OGNI VOLTA che l'utente pone una domanda su una legge, una sentenza, un limite normativo o un concetto giuridico di cui non si è già discusso in modo esaustivo nei messaggi precedenti.\n"
            "- NON basarti sulla tua conoscenza interna pre-addestrata per rispondere a domande legali. Devi sempre cercare le fonti ufficiali tramite il tool.\n"
            "- Usa il tool formulando una 'query' chiara e ricca di parole chiave (es. invece di 'quali sono', scrivi 'limiti subappalto codice contratti').\n"
            "- NON usare il tool se l'utente ti sta solo salutando, ringraziando, o se ti chiede di riassumere o riformulare una risposta che hai APPENA fornito nel messaggio precedente.\n\n"
            "REGOLE PER LA RISPOSTA FINALE:\n"
            "1. Basati ESCLUSIVAMENTE sul contenuto restituito dal tool.\n"
            "2. Se il tool non restituisce informazioni o restituisce documenti non pertinenti, rispondi testualmente: 'Non dispongo di informazioni sufficienti per rispondere a questa domanda.' NON inventare.\n"
            "3. Cita sempre le fonti (es. [D.Lgs. 36/2023, Art. 119]) alla fine delle affermazioni, basandoti sui dati del tool."
        )

        tool = build_search_tool(self.engine, status_callback)
        
        agent_graph = create_react_agent(
            model=llm,
            tools=[tool],
            prompt=system_prompt
        )
        return agent_graph
