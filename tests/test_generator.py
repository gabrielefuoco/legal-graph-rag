import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch
from src.rag.generator import LegalGenerator
from src.rag.models import RetrievedChunk, RagState

@pytest.fixture
def mock_chunks():
    return [
        RetrievedChunk(
            text="La Repubblica riconosce e garantisce i diritti inviolabili dell'uomo, sia come singolo sia nelle formazioni sociali.",
            expression_id="expr_1",
            work_urn="urn:nir:stato:costituzione:1947-12-27#art2",
            structural_context="Costituzione > Art. 2",
            score=0.9
        ),
        RetrievedChunk(
            text="La difesa della Patria è sacro dovere del cittadino. Il servizio militare è obbligatorio nei limiti e modi stabiliti dalla legge.",
            expression_id="expr_2",
            work_urn="urn:nir:stato:costituzione:1947-12-27#art52",
            structural_context="Costituzione > Art. 52",
            score=0.85
        )
    ]

def test_build_messages(mock_chunks):
    generator = LegalGenerator()
    query = "Quali sono i diritti inviolabili riconosciuti?"
    
    messages = generator._build_messages(query, mock_chunks)
    
    assert len(messages) == 2
    system_msg = messages[0].content
    human_msg = messages[1].content
    
    # Verifica che il contesto e le regole siano presenti nel prompt di sistema
    assert "Costituzione > Art. 2" in system_msg
    assert "diritti inviolabili dell'uomo" in system_msg
    assert "GROUNDING RIGIDO" in system_msg
    assert "Non dispongo di informazioni sufficienti" in system_msg
    assert "[Titolo dell'Atto, Articolo X]" in system_msg
    assert "Quali sono i diritti inviolabili riconosciuti?" in human_msg

@pytest.mark.asyncio
@patch("src.rag.generator.ChatOllama")
async def test_generate_mocked(mock_chat_ollama_cls, mock_chunks):
    # Setup mock per LLM
    mock_llm = MagicMock()
    mock_response = MagicMock()
    mock_response.content = "La Repubblica riconosce i diritti inviolabili dell'uomo [Costituzione, Art. 2]."
    mock_llm.ainvoke = AsyncMock(return_value=mock_response)
    mock_chat_ollama_cls.return_value = mock_llm
    
    generator = LegalGenerator()
    state: RagState = {
        "query": "Quali sono i diritti inviolabili?",
        "reference_date": None,
        "top_k": 5,
        "final_k": 5,
        "enable_graph_search": True,
        "enable_multi_hop": True,
        "analyzed_query": None,
        "query_embedding": None,
        "vector_results": [],
        "bm25_results": [],
        "graph_results": [],
        "fused_chunks": mock_chunks,
        "hop_count": 0,
        "final_chunks": mock_chunks,
        "_driver": None,
        "_analyzer": None,
        "_reranker": None
    }
    
    res = await generator.generate(state)
    assert "generation" in res
    assert "diritti inviolabili" in res["generation"]
    assert "[Costituzione, Art. 2]" in res["generation"]
    mock_llm.ainvoke.assert_called_once()

@pytest.mark.asyncio
@patch("src.rag.generator.ChatOllama")
async def test_generate_stream_mocked(mock_chat_ollama_cls, mock_chunks):
    # Setup mock per streaming
    mock_llm = MagicMock()
    
    # Creatore di async generator finto
    async def mock_astream(messages):
        chunks = ["La ", "Repubblica ", "riconosce ", "i ", "diritti."]
        for c in chunks:
            chunk_mock = MagicMock()
            chunk_mock.content = c
            yield chunk_mock
            
    mock_llm.astream = mock_astream
    mock_chat_ollama_cls.return_value = mock_llm
    
    generator = LegalGenerator()
    
    collected_chunks = []
    async for chunk in generator.generate_stream("Quali sono i diritti?", mock_chunks):
        collected_chunks.append(chunk)
        
    full_text = "".join(collected_chunks)
    assert full_text == "La Repubblica riconosce i diritti."

async def is_ollama_online() -> bool:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get("http://localhost:11434/api/tags", timeout=1.0)
            return resp.status_code == 200
    except Exception:
        return False

@pytest.mark.asyncio
async def test_generate_live_if_online(mock_chunks):
    online = await is_ollama_online()
    if not online:
        pytest.skip("Ollama offline in locale, skip del test live.")
        
    generator = LegalGenerator()
    
    # 1. Test query con risposta presente nel contesto
    query_valid = "Qual è il sacro dovere del cittadino?"
    state_valid = {
        "query": query_valid,
        "final_chunks": mock_chunks,
    }
    res_valid = await generator.generate(state_valid)
    assert "sacro dovere" in res_valid["answer"].lower()
    # Verifica presenza citazione
    assert "52" in res_valid["answer"]
    
    # 2. Test query fuori contesto (fallout zero-allucinazione)
    query_pizza = "Come si cucina la pizza margherita?"
    state_pizza = {
        "query": query_pizza,
        "final_chunks": mock_chunks,
    }
    res_pizza = await generator.generate(state_pizza)
    # Deve corrispondere alla stringa di fallback
    assert res_pizza["answer"].strip() == "Non dispongo di informazioni sufficienti per rispondere a questa domanda."
