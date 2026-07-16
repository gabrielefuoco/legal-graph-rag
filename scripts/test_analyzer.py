import asyncio
from src.config import settings
from src.parsing.teseo_matcher import TESEOMatcher
from src.parsing.vector_engine import VectorEngine
from src.rag.query_analyzer import QueryAnalyzer

async def test():
    teseo = TESEOMatcher(settings.TESEO_RDF_PATH)
    vector = VectorEngine()
    analyzer = QueryAnalyzer(teseo, vector, None)
    
    query = "limiti subappalto codice dei contratti pubblici"
    result = await analyzer.analyze(query)
    print(f"Query: {query}")
    print(f"TESEO concepts: {result.teseo_concepts}")
    print(f"Labels: {result.labels}")

asyncio.run(test())
