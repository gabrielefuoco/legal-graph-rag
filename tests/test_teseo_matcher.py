import pytest
from src.parsing.teseo_matcher import TESEOMatcher

@pytest.fixture
def matcher():
    m = TESEOMatcher()
    # Manually populate for unit testing without a real RDF file
    m.matcher.add_word("diritto civile", ("diritto civile", "http://teseo/1"))
    m.matcher.add_word("codice penale", ("codice penale", "http://teseo/2"))
    m.matcher.make_automaton()
    return m

def test_normalize_text(matcher):
    assert matcher.normalize_text("Diritto   Civile!!!") == "diritto civile"
    assert matcher.normalize_text(None) == ""

@pytest.mark.asyncio
async def test_extract_topics(matcher):
    text = "Il diritto civile e il codice penale sono fondamentali."
    topics = await matcher.extract_topics(text)
    
    assert len(topics) == 2
    ids = [t["teseo_id"] for t in topics]
    assert "http://teseo/1" in ids
    assert "http://teseo/2" in ids
    assert topics[0]["score"] == 1.0
