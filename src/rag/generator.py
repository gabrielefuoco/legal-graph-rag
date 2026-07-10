import logging
from typing import List
from langchain_ollama import ChatOllama
from langchain_core.messages import SystemMessage, HumanMessage
from src.rag.models import RagState, RetrievedChunk
from src.config import settings

logger = logging.getLogger(__name__)

class LegalGenerator:
    """
    Gestisce la sintesi della risposta finale utilizzando un LLM (Qwen3.5).
    Prende i chunk recuperati e filtrati e genera una risposta legale strutturata.
    """

    def __init__(self):
        self.llm = ChatOllama(
            base_url=settings.QWEN3_ENDPOINT,
            model=settings.GENERATIVE_MODEL_NAME,
            temperature=0.0,  # Massima precisione per il dominio legale
        )

    def _format_context(self, chunks: List[RetrievedChunk]) -> str:
        """Formatta i chunk in una stringa di contesto leggibile dall'LLM."""
        if not chunks:
            return "Nessuna informazione rilevante trovata nei documenti ufficiali."
        
        context_parts = []
        for i, chunk in enumerate(chunks, 1):
            source_info = f"Fonte: {chunk.structural_context}" if chunk.structural_context else ""
            urn_info = f" (URN: {chunk.work_urn})" if chunk.work_urn else ""
            context_parts.append(f"[{i}] {source_info}{urn_info}\nTesto: {chunk.text}")
            
        return "\n\n".join(context_parts)

    def _build_messages(self, query: str, chunks: List[RetrievedChunk]) -> list:
        """Costruisce la lista di messaggi (System + Human) per il LLM."""
        context = self._format_context(chunks)
        
        system_prompt = (
            "Sei un assistente legale virtuale esperto specializzato nel diritto italiano.\n"
            "Il tuo compito è rispondere alla domanda dell'utente basandoti ESCLUSIVAMENTE sul contesto fornito.\n\n"
            "Regole operative TASSATIVE da seguire:\n"
            "1. GROUNDING RIGIDO: Rispondi esclusivamente usando i fatti e le disposizioni esplicitamente citati nel contesto. Non fare assunzioni, non estrapolare e non usare conoscenze esterne al contesto.\n"
            "2. GESTIONE MANCANZA INFORMAZIONI: Se il contesto non contiene le informazioni necessarie per rispondere alla domanda in modo completo, o se la domanda è del tutto irrilevante rispetto al contesto, devi rispondere ESATTAMENTE con la seguente frase di fallback e nient'altro:\n"
            "   \"Non dispongo di informazioni sufficienti per rispondere a questa domanda.\"\n"
            "3. FORMATO CITAZIONI: Cita la fonte per ogni affermazione rilevante alla fine della frase, inserendo il riferimento tra parentesi quadre nel formato [Titolo dell'Atto, Articolo X] o [Articolo X] (es. [Costituzione, Art. 2]).\n"
            "4. FORMATTAZIONE PULITA: Vai dritto al punto. Non inserire alcun preambolo (es. 'Ecco la risposta:', 'Certamente,', 'Sulla base del contesto fornito...'), nessuna introduzione e nessuna conclusione di cortesia. Genera solo la risposta fattuale.\n"
            "5. TONE: Mantieni un tono rigoroso, professionale, formale ed oggettivo.\n\n"
            f"CONTESTO UFFICIALE DA UTILIZZARE:\n{context}"
        )
        
        from langchain_core.messages import SystemMessage, HumanMessage
        return [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Domanda: {query}")
        ]

    async def generate(self, state: RagState) -> dict:
        """
        Nodo LangGraph per la generazione della risposta.
        """
        query = state["query"]
        chunks = state.get("final_chunks") or state.get("fused_chunks") or []
        
        messages = self._build_messages(query, chunks)
        
        try:
            logger.info(f"Generazione risposta per query: {query}")
            response = await self.llm.ainvoke(messages)
            return {"generation": response.content}
        except Exception as e:
            logger.error(f"Errore durante la generazione della risposta: {e}")
            return {"generation": "Si è verificato un errore tecnico durante la generazione della risposta."}

    async def generate_stream(self, query: str, chunks: List[RetrievedChunk]):
        """
        Generatore asincrono per lo streaming della risposta.
        """
        messages = self._build_messages(query, chunks)
        
        try:
            logger.info(f"Generazione risposta in streaming per query: {query}")
            async for chunk in self.llm.astream(messages):
                yield chunk.content
        except Exception as e:
            logger.error(f"Errore durante lo streaming della risposta: {e}")
            yield "Si è verificato un errore tecnico durante lo streaming della risposta."

async def generation_node(state: RagState) -> dict:
    """Wrapper per il nodo LangGraph."""
    generator = state.get("_llm")
    if not generator:
        # Fallback se non iniettato correttamente
        generator = LegalGenerator()
    return await generator.generate(state)
