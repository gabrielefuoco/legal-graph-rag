import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.rag.engine import RagEngine
from src.rag.models import RetrievedChunk, AnalyzedQuery

@pytest.mark.asyncio
@patch("src.rag.engine.AsyncGraphDatabase.driver")
@patch("src.rag.engine.VectorEngine")
@patch("src.rag.engine.TESEOMatcher")
@patch("src.rag.engine.Reranker")
async def test_rag_ablation_disabled(mock_reranker_cls, mock_teseo_matcher_cls, mock_vector_engine_cls, mock_driver_cls):
    # Setup mocks
    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()
    mock_driver_cls.return_value = mock_driver
    
    mock_vector_engine = MagicMock()
    mock_vector_engine.compute_embeddings_batch = AsyncMock(return_value=[[0.1] * 768])
    mock_vector_engine_cls.return_value = mock_vector_engine
    
    mock_teseo_matcher = MagicMock()
    # If graph search is enabled, extract_topics would be called
    mock_teseo_matcher.extract_topics = AsyncMock(return_value=[])
    mock_teseo_matcher_cls.return_value = mock_teseo_matcher
    
    mock_reranker = MagicMock()
    # Mock rerank to return whatever is passed in
    mock_reranker.rerank = MagicMock(side_effect=lambda q, chunks, instruction=None: chunks)
    mock_reranker_cls.return_value = mock_reranker
    
    # Initialize engine
    engine = RagEngine()
    
    # Mock Neo4j session.run for query_analyzer and retriever
    mock_session = AsyncMock()
    mock_driver.session.return_value = mock_session
    mock_session.__aenter__.return_value = mock_session
    
    # We will mock the database results.
    # When vector search is called, return empty results
    mock_session.run.return_value = MagicMock() # Will return empty async generator
    
    # 1. TEST WITH ABLATION ACTIVE (enable_graph_search=False, enable_multi_hop=False)
    chunks = await engine.retrieve(
        query="Quali sono le competenze delle regioni?",
        enable_graph_search=False,
        enable_multi_hop=False
    )
    
    # Assertions
    # TESEO Matcher extract_topics should NOT have been called
    mock_teseo_matcher.extract_topics.assert_not_called()
    
    # The session run should NOT have query matching/expansion patterns for TESEO (like MATCH (child:TESEO_Concept)...)
    # Let's verify that the only run calls were for BM25 and Vector Search (if mock_session.run was called at all)
    # Actually, let's verify mock_session.run is NOT called for graph search.
    # In graph_search: if enable_graph_search is False, it returns empty dict and doesn't run the graph Cypher query.
    for call in mock_session.run.call_args_list:
        query_arg = call[0][0]
        assert "HAS_TOPIC" not in query_arg
        assert "TESEO_Concept" not in query_arg

    # Close engine
    await engine.close()


@pytest.mark.asyncio
@patch("src.rag.engine.AsyncGraphDatabase.driver")
@patch("src.rag.engine.VectorEngine")
@patch("src.rag.engine.TESEOMatcher")
@patch("src.rag.engine.Reranker")
async def test_rag_ablation_enabled(mock_reranker_cls, mock_teseo_matcher_cls, mock_vector_engine_cls, mock_driver_cls):
    # Setup mocks
    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()
    mock_driver_cls.return_value = mock_driver
    
    mock_vector_engine = MagicMock()
    mock_vector_engine.compute_embeddings_batch = AsyncMock(return_value=[[0.1] * 768])
    mock_vector_engine_cls.return_value = mock_vector_engine
    
    mock_teseo_matcher = MagicMock()
    mock_teseo_matcher.extract_topics = AsyncMock(return_value=[{"teseo_id": "topic_1", "label": "regioni"}])
    mock_teseo_matcher_cls.return_value = mock_teseo_matcher
    
    mock_reranker = MagicMock()
    mock_reranker.rerank = MagicMock(side_effect=lambda q, chunks, instruction=None: chunks)
    mock_reranker_cls.return_value = mock_reranker
    
    # Initialize engine
    engine = RagEngine()
    
    # Mock Neo4j session.run for query_analyzer and retriever
    mock_session = AsyncMock()
    mock_driver.session.return_value = mock_session
    mock_session.__aenter__.return_value = mock_session
    
    # 2. TEST WITH ABLATION INACTIVE (enable_graph_search=True, enable_multi_hop=True)
    chunks = await engine.retrieve(
        query="Quali sono le competenze delle regioni?",
        enable_graph_search=True,
        enable_multi_hop=True
    )
    
    # Assertions
    # TESEO Matcher extract_topics SHOULD have been called
    mock_teseo_matcher.extract_topics.assert_called_once()
    
    # Close engine
    await engine.close()
