import pytest
from src.parsing.teseo_matcher import TESEOMatcher
from unittest.mock import MagicMock

@pytest.mark.asyncio
async def test_teseo_boundary_matching():
    # Setup matcher with a mock automaton or real load if small
    matcher = TESEOMatcher()
    # Mocking the internal matcher/label_to_id for a quick test
    matcher.label_to_id = {"sole": "http://teseo/sole"}
    matcher.matcher.add_word("sole", ("sole", "http://teseo/sole"))
    matcher.matcher.make_automaton()
    
    # Test cases
    text_with_boundary = "il sole sorge a est"
    text_inside_word = "le console dei videogiochi"
    text_end_boundary = "un raggio di sole"
    
    matches_boundary = await matcher.extract_topics(text_with_boundary)
    matches_inside = await matcher.extract_topics(text_inside_word)
    matches_end = await matcher.extract_topics(text_end_boundary)
    
    assert len(matches_boundary) == 1, "Should match 'sole' with boundaries"
    assert len(matches_inside) == 0, "Should NOT match 'sole' inside 'console'"
    assert len(matches_end) == 1, "Should match 'sole' at the end of string"

@pytest.mark.asyncio
async def test_teseo_normalization_preserved():
    matcher = TESEOMatcher()
    # Matcher should normalize both the search term and the text
    matcher.label_to_id = {"energia solare": "id1"}
    matcher.matcher.add_word("energia solare", ("energia solare", "id1"))
    matcher.matcher.make_automaton()
    
    match = await matcher.extract_topics("L'Energia Solare è pulita")
    assert len(match) == 1
    assert match[0]["teseo_id"] == "id1"
