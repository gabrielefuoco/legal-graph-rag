import asyncio
import logging
from src.rag.engine import RagEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)-30s | %(levelname)-5s | %(message)s")

async def test_rag():
    engine = RagEngine()
    query = "Quali sono i limiti previsti dal Codice dei Contratti Pubblici in materia di subappalti?"
    print(f"Testing RAG with query: {query}")
    
    # We use engine.retrieve to test retrieval
    results = await engine.retrieve(query)
    
    print(f"\n--- RAG RETRIEVAL RESULTS ---")
    if not results:
         print("No results found!")
    else:
         for r in results:
              print(f"Source: {r.get('metadata', {}).get('source', 'Unknown')} | Score: {r.get('score', 0):.2f}")
              text = r.get('text', '')
              print(f"Text snippet: {text[:150]}...")

if __name__ == "__main__":
    asyncio.run(test_rag())
