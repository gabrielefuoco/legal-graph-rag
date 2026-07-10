import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.rag.engine import RagEngine, RAGResult
from src.rag.models import RetrievedChunk

def test_rag_result_wrapper():
    chunks = [
        RetrievedChunk(text="Test chunk 1", expression_id="1"),
        RetrievedChunk(text="Test chunk 2", expression_id="2"),
    ]
    res = RAGResult(chunks, answer="Questo è il test.")
    
    # Si comporta come una lista
    assert len(res) == 2
    assert res[0].text == "Test chunk 1"
    assert list(res) == chunks
    
    # Contiene la proprietà answer
    assert res.answer == "Questo è il test."

@pytest.mark.asyncio
@patch("src.rag.engine.AsyncGraphDatabase.driver")
@patch("src.rag.engine.VectorEngine")
@patch("src.rag.engine.TESEOMatcher")
@patch("src.rag.engine.Reranker")
@patch("src.rag.engine.LegalGenerator")
async def test_retrieve_with_chat_history(mock_gen_cls, mock_reranker_cls, mock_teseo_matcher_cls, mock_vector_engine_cls, mock_driver_cls):
    # Setup mocks
    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()
    mock_driver_cls.return_value = mock_driver
    
    mock_vector_engine = MagicMock()
    mock_vector_engine.compute_embeddings_batch = AsyncMock(return_value=[[0.1] * 768])
    mock_vector_engine_cls.return_value = mock_vector_engine
    
    mock_teseo_matcher = MagicMock()
    mock_teseo_matcher.extract_topics = AsyncMock(return_value=[])
    mock_teseo_matcher_cls.return_value = mock_teseo_matcher
    
    mock_reranker = MagicMock()
    mock_reranker.rerank = MagicMock(side_effect=lambda q, chunks, instruction=None: chunks)
    mock_reranker_cls.return_value = mock_reranker
    
    # Mock LLM inside generator for contextualize_query
    mock_gen = MagicMock()
    mock_llm_response = MagicMock()
    mock_llm_response.content = "Quali sono le competenze dello Stato?"
    mock_gen.llm = AsyncMock()
    mock_gen.llm.ainvoke = AsyncMock(return_value=mock_llm_response)
    
    # Mock generate method of LegalGenerator
    mock_chunks = [RetrievedChunk(text="Competenze Stato", expression_id="expr_1")]
    async def mock_generate(state):
        return {"generation": "Risposta generata dallo Stato."}
    mock_gen.generate = mock_generate
    mock_gen_cls.return_value = mock_gen
    
    # Initialize engine
    engine = RagEngine()
    
    # Mock Neo4j session
    mock_session = AsyncMock()
    mock_driver.session.return_value = mock_session
    mock_session.__aenter__.return_value = mock_session
    
    # Eseguiamo query con chat history
    chat_history = [
        {"role": "user", "content": "Quali sono le competenze delle regioni?"},
        {"role": "assistant", "content": "Le regioni hanno competenza concorrente."}
    ]
    
    # Mock session run to return fresh AsyncMocks for each parallel call (needs to be async def)
    async def mock_run_side_effect(query_str, *args, **kwargs):
        items = []
        # Return a record for vector or BM25 searches
        if "vector.queryNodes" in query_str or "fulltext.queryNodes" in query_str:
            items = [{
                "text": "Competenze Stato",
                "expression_id": "expr_1",
                "work_urn": "urn:test-urn",
                "work_title": "Test Title",
                "structural_tag": "Art. 1",
                "vigenza_start": None,
                "vigenza_end": None,
                "score": 0.9,
                "breadcrumb_path": []
            }]
            
        res = AsyncMock()
        res.__aiter__.return_value = items
        return res

    mock_session.run = MagicMock(side_effect=mock_run_side_effect)
    
    result = await engine.retrieve(
        query="E quelle dello Stato?",
        chat_history=chat_history
    )
    
    # Verifichiamo che il LLM sia stato chiamato per la contestualizzazione
    mock_gen.llm.ainvoke.assert_called_once()
    
    # Verifichiamo che la risposta generata corrisponda al mock
    assert result.answer == "Risposta generata dallo Stato."
    
    await engine.close()

@pytest.mark.asyncio
@patch("src.rag.engine.AsyncGraphDatabase.driver")
@patch("src.rag.engine.VectorEngine")
@patch("src.rag.engine.TESEOMatcher")
@patch("src.rag.engine.Reranker")
@patch("src.rag.engine.LegalGenerator")
async def test_fallback_when_no_documents(mock_gen_cls, mock_reranker_cls, mock_teseo_matcher_cls, mock_vector_engine_cls, mock_driver_cls):
    # Setup mocks
    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()
    mock_driver_cls.return_value = mock_driver
    
    mock_vector_engine = MagicMock()
    mock_vector_engine.compute_embeddings_batch = AsyncMock(return_value=[[0.1] * 768])
    mock_vector_engine_cls.return_value = mock_vector_engine
    
    mock_teseo_matcher = MagicMock()
    mock_teseo_matcher.extract_topics = AsyncMock(return_value=[])
    mock_teseo_matcher_cls.return_value = mock_teseo_matcher
    
    # Reranker scarta tutti i documenti
    mock_reranker = MagicMock()
    mock_reranker.rerank = MagicMock(return_value=[])
    mock_reranker_cls.return_value = mock_reranker
    
    mock_gen = MagicMock()
    mock_gen_cls.return_value = mock_gen
    
    engine = RagEngine()
    
    mock_session = AsyncMock()
    mock_driver.session.return_value = mock_session
    mock_session.__aenter__.return_value = mock_session
    
    # Eseguiamo query. Visto che il Reranker ritorna una lista vuota, il nodo fallback_generation deve essere eseguito
    result = await engine.retrieve(query="Qualcosa che non esiste.")
    
    # Verifichiamo che il generatore LLM reale non sia stato chiamato
    mock_gen.generate.assert_not_called() if hasattr(mock_gen.generate, "assert_not_called") else None
    
    # La risposta deve essere quella di fallback
    assert result.answer == "Non dispongo di informazioni sufficienti per rispondere a questa domanda."
    
    await engine.close()
