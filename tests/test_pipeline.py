import pytest
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch
from src.parsing.transformers import enrich_and_load_pipeline
from src.parsing.models import DocumentDTO, FRBRMetadata

@pytest.fixture
def mock_jsonl(tmp_path):
    p = tmp_path / "test.jsonl"
    doc = DocumentDTO(
        frbr=FRBRMetadata(urn="urn:test", title="Test Doc", date="2024-01-01", source="test"),
        nodes=[],
        edges=[]
    )
    with open(p, "w", encoding="utf-8") as f:
        f.write(doc.model_dump_json() + "\n")
    return str(p)

@pytest.mark.asyncio
async def test_enrich_and_load_pipeline_mocked(mock_jsonl):
    # Mock Neo4j
    with patch("src.ingestion.neo4j_loader.AsyncGraphDatabase.driver") as mock_driver, \
         patch("src.parsing.vector_engine.OllamaEmbeddings") as mock_ollama, \
         patch("src.parsing.teseo_matcher.Graph") as mock_rdf:
        
        # Setup mocks
        mock_driver_instance = MagicMock()
        mock_driver.return_value = mock_driver_instance
        
        mock_session = AsyncMock()
        mock_driver_instance.session.return_value = mock_session
        mock_session.__aenter__.return_value = mock_session
        
        # Run pipeline
        # We need a fake RDF file path
        with open("fake.rdf", "w") as f: f.write("")
        
        try:
            await enrich_and_load_pipeline(mock_jsonl, "fake.rdf")
            
            # Verify setup and load calls
            assert mock_session.run.call_count >= 5 # Schema setup
            assert mock_session.execute_write.called
        finally:
            if os.path.exists("fake.rdf"): os.remove("fake.rdf")
