"""Generation module for legal query answering with Map-Reduce and streaming support."""

import logging
from typing import List
from langchain_ollama import ChatOllama
from langchain_core.messages import SystemMessage, HumanMessage
from src.rag.models import RagState, RetrievedChunk
from src.config import settings
from src.rag.llm_factory import get_llm

logger = logging.getLogger(__name__)

class LegalGenerator:
    """Synthesizes final answers using an LLM over retrieved and filtered legal chunks."""

    def __init__(self, num_ctx: int = None):
        """Build the generator LLM, defaulting the context window to the configured value."""
        from src.rag.llm_factory import get_llm
        self.num_ctx = num_ctx or settings.GENERATOR_NUM_CTX
        self.llm = get_llm(num_ctx=self.num_ctx, temperature=0.0, reasoning=False)

    def _format_context(self, chunks: List[RetrievedChunk]) -> str:
        """Format retrieved chunks into a standardized context string for the LLM."""
        if not chunks:
            return "Nessuna informazione rilevante trovata nei documenti ufficiali."
        
        context_parts = []
        for i, chunk in enumerate(chunks, 1):
            source_info = f"Fonte: {chunk.structural_context}" if chunk.structural_context else ""
            urn_info = f" (URN: {chunk.work_urn})" if chunk.work_urn else ""
            # Use expanded text if present, otherwise base text
            chunk_text = getattr(chunk, 'expanded_text', None) or chunk.text
            graph_info = self._format_graph_metadata(chunk)
            context_parts.append(f"[{i}] {source_info}{urn_info}{graph_info}\nTesto: {chunk_text}")
            
        return "\n\n".join(context_parts)

    def _format_graph_metadata(self, chunk: RetrievedChunk) -> str:
        """Render the knowledge-graph annotations of a chunk (TESEO institutes, citation links).

        This is information only the graph holds: it does not change the ranking, it helps the
        model interpret *why* a provision is relevant (normative institute, act that cites it).
        """
        if not getattr(settings, "GRAPH_METADATA_IN_CONTEXT", True):
            return ""

        bits = []
        labels = chunk.metadata.get("matched_concept_labels") or chunk.metadata.get("matched_concepts")
        if labels:
            clean = [str(x) for x in labels if x][:5]
            if clean:
                bits.append(f"Istituti giuridici (TESEO): {', '.join(clean)}")

        trace = chunk.expansion_trace or {}
        if trace.get("citing_title"):
            rinvio = f" — rinvio: \"{trace['citation_text']}\"" if trace.get("citation_text") else ""
            bits.append(f"Disposizione richiamata da: {trace['citing_title']}{rinvio}")

        return ("\n" + " | ".join(bits)) if bits else ""

    def _build_messages(self, query: str, chunks: List[RetrievedChunk], chat_history: list = None) -> list:
        """Construct the prompt messages (System, History, Human) for the LLM."""
        context = self._format_context(chunks)
        
        system_prompt = (
            "Sei un assistente legale virtuale esperto specializzato nel diritto italiano.\n"
            "Il tuo compito è rispondere alla domanda dell'utente basandoti ESCLUSIVAMENTE sul contesto fornito.\n\n"
            "Regole operative TASSATIVE da seguire:\n"
            "1. GROUNDING RIGIDO: Rispondi esclusivamente usando i facts e le disposizioni esplicitamente citati nel contesto. Non fare assunzioni, non estrapolare e non usare conoscenze esterne al contesto.\n"
            "2. GESTIONE MANCANZA INFORMAZIONI: Se il contesto non contiene le informazioni necessarie per rispondere alla domanda in modo completo, o se la domanda è del tutto irrilevante rispetto al contesto, devi rispondere ESATTAMENTE con la seguente frase di fallback e nient'altro:\n"
            "   \"Non dispongo di informazioni sufficienti per rispondere a questa domanda.\"\n"
            "3. FORMATO CITAZIONI: Cita la fonte per ogni affermazione rilevante alla fine della frase, inserendo il riferimento tra parentesi quadre nel formato [Titolo dell'Atto, Articolo X] o [Articolo X] (es. [Costituzione, Art. 2]).\n"
            "4. FORMATTAZIONE PULITA: Vai dritto al punto. Non inserire alcun preambolo (es. 'Ecco la risposta:', 'Certamente,', 'Sulla base del contesto fornito...'), nessuna introduzione e nessuna conclusione di cortesia. Genera solo la risposta fattuale.\n"
            "5. METADATI DEL GRAFO: alcune fonti riportano una riga 'Istituti giuridici (TESEO)' o 'Disposizione richiamata da'. Sono annotazioni del grafo di conoscenza: usale per inquadrare correttamente l'istituto giuridico e i rapporti tra norme, ma NON inventare contenuti che non siano nel testo della fonte.\n"
            "6. TONE: Mantieni un tono rigoroso, professionale, formale ed oggettivo.\n\n"
            f"CONTESTO UFFICIALE DA UTILIZZARE:\n{context}"
        )
        
        from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
        
        messages = [SystemMessage(content=system_prompt)]
        
        if chat_history:
            for msg in chat_history:
                if msg.get("role") == "user":
                    messages.append(HumanMessage(content=msg["content"]))
                elif msg.get("role") == "assistant":
                    messages.append(AIMessage(content=msg["content"]))
                    
        messages.append(HumanMessage(content=f"Domanda: {query}"))
        return messages

    async def _map_extract(self, query: str, chunk: RetrievedChunk, num_ctx: int) -> str:
        """Extract relevant facts from a single chunk during the Map phase.
        
        Splits text into overlapping sliding windows if it exceeds the model context.
        """
        chunk_text = getattr(chunk, 'expanded_text', None) or chunk.text
        
        # Estimate character limit fitting the model context (~9,000 chars for num_ctx=4096)
        max_map_chars = (num_ctx - 1000) * 3
        
        if len(chunk_text) > max_map_chars:
            # Sliding window with 500 characters of overlap
            overlap = 500
            step = max_map_chars - overlap
            windows = []
            for i in range(0, len(chunk_text), step):
                windows.append(chunk_text[i:i + max_map_chars])
            
            logger.info(f"  📑 Chunk {chunk.expression_id} troppo grande ({len(chunk_text)} chars). Split in {len(windows)} finestre da ~{max_map_chars} chars.")
            
            window_extracts = []
            for j, window in enumerate(windows):
                ext = await self._map_extract_window(query, window, chunk.structural_context, num_ctx)
                if "NESSUNA_INFO" not in ext:
                    window_extracts.append(ext)
            
            if window_extracts:
                return "\n".join(window_extracts)
            return "NESSUNA_INFO"
        else:
            return await self._map_extract_window(query, chunk_text, chunk.structural_context, num_ctx)

    async def _map_extract_window(self, query: str, text: str, structural_context: str | None, num_ctx: int) -> str:
        """Extract relevant facts from a single text window during the Map phase."""
        from langchain_ollama import ChatOllama
        from langchain_core.messages import SystemMessage, HumanMessage
        from src.rag.think_filter import strip_thinking_tags
        
        system_prompt = (
            "Sei un assistente legale. Estrai dal seguente testo normativo SOLO le "
            "informazioni direttamente rilevanti per rispondere alla domanda dell'utente.\n"
            "Se il testo NON contiene informazioni utili per la domanda, "
            "rispondi esattamente e solo con: NESSUNA_INFO\n\n"
            f"Testo normativo (Fonte: {structural_context or 'Sconosciuta'}):\n"
            f"{text}"
        )
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Domanda: {query}")
        ]
        
        from src.rag.llm_factory import get_llm
        llm = get_llm(
            num_ctx=num_ctx,
            temperature=0.0,
            reasoning=False,
        )
        
        try:
            resp = await llm.ainvoke(messages)
            content = strip_thinking_tags(resp.content).strip()
            return content
        except Exception as e:
            logger.error(f"Errore nella fase MAP window: {e}")
            return "NESSUNA_INFO"

    async def generate(self, state: RagState) -> dict:
        """Generate the final legal answer using either direct stuffing or Map-Reduce."""
        from src.rag.think_filter import strip_thinking_tags
        from src.rag.models import RetrievedChunk
        query = state["query"]
        chunks = state.get("expanded_chunks") or state.get("final_chunks") or state.get("fused_chunks") or []
        chat_history = state.get("chat_history") or []
        
        try:
            logger.info(f"Generazione risposta per query: {query}")
            from langchain_ollama import ChatOllama
            num_ctx = state.get("generator_num_ctx", settings.GENERATOR_NUM_CTX)
            
            total_chars = sum(len(getattr(c, 'expanded_text', None) or c.text) for c in chunks)
            stuff_threshold = getattr(settings, 'GENERATOR_STUFF_THRESHOLD', 8000)
            
            if total_chars > stuff_threshold:
                logger.info(f"Contesto troppo lungo ({total_chars} > {stuff_threshold} chars). Attivazione Map-Reduce.")
                extracts = []
                for chunk in chunks:
                    ext = await self._map_extract(query, chunk, num_ctx)
                    if "NESSUNA_INFO" not in ext:
                        extracts.append((chunk, ext))
                
                if not extracts:
                    logger.warning("Fase MAP non ha prodotto risultati. Fallback a stuffing base.")
                    messages = self._build_messages(query, chunks, chat_history)
                else:
                    synthetic_chunks = [
                        RetrievedChunk(
                            text=ext,
                            expression_id=c.expression_id,
                            structural_context=c.structural_context,
                            work_urn=c.work_urn
                        ) for c, ext in extracts
                    ]
                    messages = self._build_messages(query, synthetic_chunks, chat_history)
                    logger.info(f"📦 REDUCE: passo all'LLM {len(synthetic_chunks)} micro-estratti.")
            else:
                logger.info(f"📦 Prompt Pronto: Passo all'LLM {len(chunks)} documenti per un totale di {total_chars} caratteri (num_ctx: {num_ctx}).")
                messages = self._build_messages(query, chunks, chat_history)
            
            from src.rag.llm_factory import get_llm
            llm = get_llm(
                num_ctx=num_ctx,
                temperature=0.0,
                reasoning=False,
            )
            
            response = await llm.ainvoke(messages)
            content = strip_thinking_tags(response.content)
            return {"generation": content}
        except Exception as e:
            logger.error(f"Errore durante la generazione della risposta: {e}")
            return {"generation": "Si è verificato un errore tecnico durante la generazione della risposta."}

    async def generate_stream(self, query: str, chunks: List[RetrievedChunk], chat_history: list = None):
        """Asynchronous token generator streaming the model response."""
        from src.rag.think_filter import filter_think_stream
        import time
        messages = self._build_messages(query, chunks, chat_history)
        
        try:
            logger.info(f"[5/6] GENERATE — Avvio streaming con {len(chunks)} chunk di contesto")
            start = time.perf_counter()
            token_count = 0
            
            raw_stream = self.llm.astream(messages)
            async def content_stream():
                """Yield the raw token stream of the underlying LLM."""
                async for chunk in raw_stream:
                    yield chunk.content
                    
            async for token in filter_think_stream(content_stream()):
                token_count += 1
                yield token
                
            elapsed = time.perf_counter() - start
            logger.info(f"[5/6] GENERATE — Completato in {elapsed:.1f}s | ~{token_count} token generati")
        except Exception as e:
            logger.error(f"Errore durante lo streaming della risposta: {e}")
            yield "Si è verificato un errore tecnico durante lo streaming della risposta."

async def generation_node(state: RagState) -> dict:
    """LangGraph node wrapper for legal answer generation."""
    generator = state.get("_llm")
    if not generator:
        # Fallback if generator is not injected in state
        generator = LegalGenerator()
    return await generator.generate(state)
