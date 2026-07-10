import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.ingestion.neo4j_loader import AsyncNeo4jLoader

@pytest.fixture
def loader():
    with patch("neo4j.AsyncGraphDatabase.driver") as mock_driver:
        loader = AsyncNeo4jLoader()
        yield loader

@pytest.mark.asyncio
async def test_setup_schema(loader):
    mock_session = AsyncMock()
    loader.driver.session = MagicMock(return_value=mock_session)
    mock_session.__aenter__.return_value = mock_session
    
    await loader.setup_schema()
    
    assert mock_session.run.call_count >= 5
    calls = [call[0][0] for call in mock_session.run.call_args_list]
    assert any("CREATE CONSTRAINT work_id_unique" in q for q in calls)
    assert any("CREATE CONSTRAINT expression_id_unique" in q for q in calls)
    assert any("CREATE CONSTRAINT structural_id_unique" in q for q in calls)
    assert any("CREATE VECTOR INDEX" in q for q in calls)

@pytest.mark.asyncio
async def test_load_works(loader):
    tx = AsyncMock()
    batch = [{"urn": "urn:test", "title": "Test Title", "date": "2024-01-01", "source": "test"}]
    
    await loader._load_works(tx, batch)
    
    tx.run.assert_called_once()
    query = tx.run.call_args[0][0]
    assert "MERGE (w:Work {id: row.urn})" in query
    assert tx.run.call_args[1]["batch"] == batch

@pytest.mark.asyncio
async def test_load_expressions(loader):
    tx = AsyncMock()
    batch = [{
        "work_urn": "urn:test",
        "eId": "art1",
        "text_display": "Hello",
        "embedding": [0.1],
        "tag_name": "comma",
        "vigenza_start": "2024-01-01",
        "vigenza_end": None
    }]
    
    await loader._load_expressions(tx, batch)
    
    tx.run.assert_called_once()
    query = tx.run.call_args[0][0]
    assert "MERGE (e:Expression {id: row.id})" in query
    assert "MERGE (e)-[:PART_OF]->(w)" in query

@pytest.mark.asyncio
async def test_load_batch_orchestrator(loader):
    mock_session = AsyncMock()
    loader.driver.session = MagicMock(return_value=mock_session)
    mock_session.__aenter__.return_value = mock_session
    
    # Mock execute_write to just call the function
    async def mock_execute_write(fn, *args):
        return await fn(AsyncMock(), *args)
    
    mock_session.execute_write = AsyncMock(side_effect=mock_execute_write)
    
    nodes = [{"type": "WORK", "urn": "u1"}, {"type": "EXPRESSION", "id": "e1"}]
    edges = [{"type": "PART_OF", "source": "s", "target": "t"}]
    
    await loader.load_batch(nodes, edges)
    
    assert mock_session.execute_write.call_count == 1
