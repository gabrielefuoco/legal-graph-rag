import os
import tempfile
import pytest
from scripts.grid_search_stage2 import (
    split_into_sentences,
    cosine_similarity,
    fallback_text_similarity,
    compute_similarity,
    GenerationCache,
    build_default_stage2_grid,
    Stage2Config
)

def test_split_and_similarity():
    text = "Questa è la prima frase del testo. Questa è la seconda frase del testo legislativo!"
    sentences = split_into_sentences(text)
    assert len(sentences) == 2

    # Test cosine similarity
    assert cosine_similarity([1.0, 0.0], [1.0, 0.0]) == 1.0
    assert cosine_similarity([1.0, 0.0], [0.0, 1.0]) == 0.0

    # Test fallback text similarity
    sim = fallback_text_similarity("articolo 2 costituzione diritti inviolabili", "costituzione articolo 2 diritti inviolabili")
    assert sim == 1.0

@pytest.mark.asyncio
async def test_compute_similarity_fallback():
    golden = "La Repubblica riconosce i diritti inviolabili dell'uomo."
    gen = "I diritti inviolabili dell'uomo sono riconosciuti dalla Repubblica."
    score = await compute_similarity(golden, gen, vector_engine=None)
    assert score > 0.5

def test_generation_cache():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = os.path.join(tmpdir, "test_cache.json")
        cache = GenerationCache(cache_path)
        key = cache.get_key("query", "context", 8192, "qwen3.5")
        assert cache.get(key) is None

        cache.set(key, "risposta generata")
        assert cache.get(key) == "risposta generata"

        # Reload from disk
        cache2 = GenerationCache(cache_path)
        assert cache2.get(key) == "risposta generata"

def test_build_default_stage2_grid():
    s1_configs = [{
        "weight_vector": 1.0,
        "weight_bm25": 1.0,
        "weight_graph": 1.2,
        "rrf_k": 60,
        "rerank_top_k": 20,
        "rerank_min_score": 0.05,
        "final_k": 10
    }]
    configs = build_default_stage2_grid(s1_configs)
    assert len(configs) == 3 * 3 * 3 # 3 num_ctx * 3 stuff * 3 topo
    assert isinstance(configs[0], Stage2Config)
