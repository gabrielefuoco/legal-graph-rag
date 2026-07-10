import pytest
from unittest.mock import AsyncMock, MagicMock
from src.rag.models import RagState, RetrievedChunk
from src.rag.evaluator import grade_documents_node, rewrite_query_node, RetrievalGrader, QueryRewriter
from src.rag.engine import post_grading_router
from src.config import settings

@pytest.fixture
def mock_grader():
    grader = MagicMock(spec=RetrievalGrader)
    grader.grade_chunk = AsyncMock(return_value=False)
    return grader

@pytest.fixture
def mock_rewriter():
    rewriter = MagicMock(spec=QueryRewriter)
    rewriter.rewrite = AsyncMock(return_value="query riformulata")
    return rewriter

@pytest.mark.asyncio
async def test_grade_documents_node_filters_chunks(mock_grader):
    chunks = [
        RetrievedChunk(text="test 1", expression_id="1"),
        RetrievedChunk(text="test 2", expression_id="2"),
    ]
    state: RagState = {
        "query": "domanda",
        "final_chunks": chunks,
        "_grader": mock_grader
    } # type: ignore
    
    # Grader boccia tutto
    mock_grader.grade_chunk.side_effect = [False, False]
    result = await grade_documents_node(state)
    assert len(result["final_chunks"]) == 0
    
    # Se il grader approva uno e boccia l'altro
    mock_grader.grade_chunk.side_effect = [True, False]
    result2 = await grade_documents_node(state)
    assert len(result2["final_chunks"]) == 1
    assert result2["final_chunks"][0].expression_id == "1"

@pytest.mark.asyncio
async def test_rewrite_query_node(mock_rewriter):
    state: RagState = {
        "query": "domanda sbagliata",
        "iterations": 1,
        "rewritten_queries": ["tentativo 1"],
        "_rewriter": mock_rewriter
    } # type: ignore
    
    result = await rewrite_query_node(state)
    assert result["query"] == "query riformulata"
    assert result["iterations"] == 2
    assert "domanda sbagliata" in result["rewritten_queries"]
    assert "tentativo 1" in result["rewritten_queries"]

def test_post_grading_router():
    # Caso 1: Documenti presenti
    state1 = {"final_chunks": [RetrievedChunk(text="test", expression_id="1")]}
    assert post_grading_router(state1) == "generate"
    
    # Caso 2: Nessun documento, sotto il limite
    state2 = {"final_chunks": [], "iterations": settings.MAX_AGENTIC_ITERATIONS - 1}
    assert post_grading_router(state2) == "rewrite_query"
    
    # Caso 3: Nessun documento, al limite
    state3 = {"final_chunks": [], "iterations": settings.MAX_AGENTIC_ITERATIONS}
    assert post_grading_router(state3) == "fallback_generation"
