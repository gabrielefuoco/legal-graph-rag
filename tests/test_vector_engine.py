import pytest
from unittest.mock import AsyncMock, patch
from src.parsing.vector_engine import VectorEngine
from src.parsing.models import GraphNodeDTO, NodeType

@pytest.fixture
def engine():
    with patch("langchain_community.embeddings.OllamaEmbeddings") as mock_ollama:
        engine = VectorEngine()
        yield engine

def test_build_vector_payload(engine):
    node = GraphNodeDTO(id="n1", type=NodeType.EXPRESSION, tag_name="art", text_display="Testo")
    payload = engine.build_vector_payload(node, "Costituzione > Titolo I")
    assert "Contesto: Costituzione > Titolo I" in payload
    assert "Testo: Testo" in payload

@pytest.mark.asyncio
async def test_compute_embeddings_batch(engine):
    with patch("langchain_community.embeddings.OllamaEmbeddings.aembed_documents", new_callable=AsyncMock) as mock_aembed:
        mock_aembed.return_value = [[0.1, 0.2]]
        
        vectors = await engine.compute_embeddings_batch(["test"])
        assert vectors == [[0.1, 0.2]]
        assert isinstance(vectors[0][0], float)
