"""Test script for QueryAnalyzer extraction of TESEO concepts and labels."""

import asyncio
from src.config import settings
from src.parsing.teseo_matcher import TESEOMatcher
from src.parsing.vector_engine import VectorEngine
from src.rag.query_analyzer import QueryAnalyzer

async def test():
    """Analyze sample query and print extracted TESEO concepts and labels."""
    teseo = TESEOMatcher(settings.TESEO_RDF_PATH)
    vector = VectorEngine()
    analyzer = QueryAnalyzer(teseo, vector, None)
    
    query = "limiti subappalto codice dei contratti pubblici"
    result = await analyzer.analyze(query)
    print(f"Query: {query}")
    print(f"TESEO concepts: {result.teseo_concepts}")
    print(f"Labels: {result.labels}")

asyncio.run(test())
