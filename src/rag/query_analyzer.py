"""
Query Analyzer — Step 1 del Retrieval Engine.

Analizza la query dell'utente:
1. Matching TESEO (Aho-Corasick) per estrarre concetti semantici
2. Espansione BROADER/NARROWER via Neo4j (se disponibili)
3. Calcolo dell'embedding della query per la Vector Search
"""
import logging
from typing import List

from neo4j import AsyncDriver

from src.config import settings
from src.parsing.teseo_matcher import TESEOMatcher
from src.parsing.vector_engine import VectorEngine
from src.rag.models import RagState, AnalyzedQuery

logger = logging.getLogger(__name__)


class QueryAnalyzer:
    """
    Analizza e arricchisce la query dell'utente prima del retrieval.

    Utilizza il TESEOMatcher (Aho-Corasick) per estrarre concetti
    dal thesaurus del Senato e il VectorEngine per calcolare l'embedding.
    """

    def __init__(self, teseo_matcher: TESEOMatcher, vector_engine: VectorEngine):
        self.teseo_matcher = teseo_matcher
        self.vector_engine = vector_engine

    async def _expand_teseo_concepts(self, driver: AsyncDriver, concept_ids: List[str]) -> tuple[List[str], List[str]]:
        """
        Naviga le relazioni BROADER/NARROWER nel grafo Neo4j
        per espandere i concetti TESEO trovati.

        Returns:
            Tuple di (expanded_ids, expanded_labels)
        """
        if not concept_ids:
            return [], []

        expanded_ids = []
        expanded_labels = []

        try:
            async with driver.session() as session:
                # Cerca i figli (narrower) dei concetti trovati
                result = await session.run(
                    """
                    MATCH (child:TESEO_Concept)-[:BROADER]->(parent:TESEO_Concept)
                    WHERE parent.id IN $concept_ids
                    RETURN DISTINCT child.id AS id, child.prefLabel AS label
                    """,
                    concept_ids=concept_ids,
                )
                async for record in result:
                    expanded_ids.append(record["id"])
                    if record.get("label"):
                        expanded_labels.append(record["label"].lower())
        except Exception as e:
            # Le relazioni BROADER potrebbero non esistere nel grafo
            logger.debug(f"Espansione TESEO BROADER fallita (probabilmente assente): {e}")

        # Fallback: se BROADER non ha trovato nulla, cerchiamo concetti
        # con prefLabel simili ai concetti trovati (espansione per label)
        if not expanded_ids and concept_ids:
            try:
                async with driver.session() as session:
                    # Recupera le prefLabel dei concetti trovati
                    result = await session.run(
                        """
                        MATCH (t:TESEO_Concept)
                        WHERE t.id IN $concept_ids AND t.prefLabel IS NOT NULL
                        RETURN t.prefLabel AS label
                        """,
                        concept_ids=concept_ids,
                    )
                    async for record in result:
                        if record.get("label"):
                            expanded_labels.append(record["label"].lower())
            except Exception as e:
                logger.debug(f"Recupero prefLabel fallito: {e}")

        return expanded_ids, expanded_labels

    async def _resolve_teseo_by_label(self, driver: AsyncDriver, labels: List[str]) -> List[str]:
        """
        Cerca concetti TESEO per prefLabel (case-insensitive).
        Utile quando l'utente scrive parole che corrispondono a un concetto
        ma l'Aho-Corasick non le trova (es. per differenze di normalizzazione).
        """
        if not labels:
            return []

        found_ids = []
        try:
            async with driver.session() as session:
                result = await session.run(
                    """
                    MATCH (t:TESEO_Concept)
                    WHERE toLower(t.prefLabel) IN $labels
                    RETURN t.id AS id
                    """,
                    labels=[l.lower() for l in labels],
                )
                async for record in result:
                    found_ids.append(record["id"])
        except Exception as e:
            logger.debug(f"Resolve TESEO by label fallito: {e}")

        return found_ids
async def analyze_query(state: RagState) -> dict:
    """
    Nodo LangGraph: analizza la query e produce AnalyzedQuery + embedding.

    Legge: state["query"]
    Scrive: state["analyzed_query"], state["query_embedding"]
    """
    import time
    start = time.perf_counter()
    query = state["query"]
    analyzer: QueryAnalyzer = state["_analyzer"]  # Iniettato dall'engine
    enable_graph_search = state.get("enable_graph_search", True)

    all_concept_ids = []
    all_labels = []
    expanded_query_text = query

    if enable_graph_search:
        # 1. Matching TESEO (Aho-Corasick sulla query)
        topics = await analyzer.teseo_matcher.extract_topics(query, analyzer.vector_engine)
        concept_ids = [t["teseo_id"] for t in topics]
        matched_labels = [t["label"] for t in topics]

        logger.info(f"TESEO match sulla query: {len(topics)} concetti trovati: {matched_labels}")

        # 1b. Fallback: se Aho-Corasick non ha trovato nulla, proviamo per prefLabel nel DB
        if not concept_ids:
            # Estrai parole significative dalla query (> 3 caratteri)
            query_words = [w for w in query.lower().split() if len(w) > 3]
            if query_words:
                db_ids = await analyzer._resolve_teseo_by_label(state["_driver"], query_words)
                if db_ids:
                    concept_ids = db_ids
                    matched_labels = query_words
                    logger.info(f"TESEO fallback (DB lookup): {len(db_ids)} concetti trovati")

        # 2. Espansione BROADER/NARROWER
        expanded_ids, expanded_labels = await analyzer._expand_teseo_concepts(state["_driver"], concept_ids)
        all_concept_ids = list(set(concept_ids + expanded_ids))

        if expanded_ids:
            logger.info(f"Espansione TESEO BROADER: +{len(expanded_ids)} concetti narrower")
        if expanded_labels:
            logger.info(f"Label espanse per BM25: {expanded_labels}")

        # 3. Costruzione query espansa per BM25
        all_labels = list(set(matched_labels + expanded_labels))
        if all_labels:
            # Aggiungiamo i concetti trovati alla query originale per migliorare il BM25
            expanded_query_text += " " + " ".join(all_labels)
    else:
        logger.info("Ricerca semantica via TESEO disabilitata: skip matching ed espansione concetti.")

    # 3b. Istruzione Reranker (Generica per il dominio legale)
    reranker_instruction = (
        "Sei un assistente legale esperto. Trova i frammenti normativi, le disposizioni di legge "
        "o i passaggi giurisprudenziali che rispondono in modo diretto e fattuale alla query dell'utente."
    )

    analyzed = AnalyzedQuery(
        original_query=query,
        teseo_concept_ids=all_concept_ids,
        expanded_labels=all_labels,
        expanded_query_text=expanded_query_text,
        reranker_instruction=reranker_instruction,
    )

    # 4. Calcolo embedding della query (riutilizza se già calcolato in extract_topics)
    query_embedding = None
    if hasattr(analyzer.teseo_matcher, 'last_query_embedding') and analyzer.teseo_matcher.last_query_embedding is not None:
        query_embedding = analyzer.teseo_matcher.last_query_embedding
        logger.debug("Riutilizzato embedding dalla fase TESEO")
    else:
        try:
            embeddings = await analyzer.vector_engine.compute_embeddings_batch([query])
            query_embedding = embeddings[0]
        except Exception as e:
            logger.error(f"Errore nel calcolo embedding della query: {e}")
            query_embedding = None

    elapsed = time.perf_counter() - start
    logger.info(
        f"[1/6] ANALYZE_QUERY — Completato in {elapsed:.1f}s | "
        f"TESEO: {len(all_concept_ids)} concetti | "
        f"Labels: {all_labels} | "
        f"Embedding: {'✓' if query_embedding else '✗'}"
    )

    return {
        "analyzed_query": analyzed,
        "query_embedding": query_embedding,
    }


async def contextualize_query(state: RagState) -> dict:
    """
    Nodo LangGraph: riscrive la query dell'utente alla luce della chat history
    per produrre una standalone query che racchiude tutto il contesto conversazionale.
    """
    query = state["query"]
    chat_history = state.get("chat_history") or []

    if not chat_history:
        logger.info("Chat history vuota. Passaggio query originale: %s", query)
        return {"query": query}

    generator = state.get("_llm")
    if generator and hasattr(generator, "llm"):
        llm = generator.llm
    else:
        from langchain_ollama import ChatOllama
        llm = ChatOllama(
            base_url=settings.QWEN3_ENDPOINT,
            model=settings.GENERATIVE_MODEL_NAME,
            temperature=0.0,
        )

    # Formattiamo la chat history in un formato testuale leggibile
    formatted_history = ""
    for turn in chat_history:
        role = turn.get("role", "user").upper()
        content = turn.get("content", "")
        formatted_history += f"{role}: {content}\n"

    system_prompt = (
        "Sei un assistente AI specializzato nella contestualizzazione di query legali.\n"
        "Data la seguente cronologia della conversazione (chat history) e l'ultima domanda dell'utente,\n"
        "formula una domanda standalone, autonoma e indipendente che possa essere compresa senza fare riferimento\n"
        "ai turni precedenti della conversazione.\n"
        "NON rispondere alla domanda, limitati a riscriverla riformulandola in modo chiaro ed esplicito.\n"
        "Se la domanda dell'utente è già autonoma e indipendente, restituisci la domanda originale così com'è.\n"
        "Non aggiungere preamboli o spiegazioni, restituisci SOLO la domanda riscritta.\n\n"
        f"CRONOLOGIA CONVERSAZIONE:\n{formatted_history}\n"
        f"ULTIMA DOMANDA: {query}"
    )

    from langchain_core.messages import SystemMessage, HumanMessage
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content="Riformula la domanda standalone.")
    ]

    try:
        from src.rag.think_filter import strip_thinking_tags
        logger.info(f"Riformulazione query con chat history di lunghezza {len(chat_history)}")
        response = await llm.ainvoke(messages)
        standalone_query = strip_thinking_tags(response.content).strip()
        
        if not standalone_query or len(standalone_query) < 5:
            logger.warning(f"Query contestualizzata vuota o troppo corta, mantengo l'originale: '{query}'")
            return {"query": query}
            
        logger.info(f"Query originaria: '{query}' -> Query riscritta standalone: '{standalone_query}'")
        return {"query": standalone_query}
    except Exception as e:
        logger.error(f"Errore nella contestualizzazione della query: {e}")
        # In caso di errore, fallback alla query originale
        return {"query": query}

