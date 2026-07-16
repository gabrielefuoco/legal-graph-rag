import asyncio
import numpy as np
from src.config import settings
from src.parsing.teseo_matcher import TESEOMatcher
from src.parsing.vector_engine import VectorEngine

async def test():
    teseo = TESEOMatcher('data/external/teseo_full.ttl')
    vector = VectorEngine()
    await teseo.precompute_embeddings(vector)
    
    text = "Il presente codice dei contratti pubblici disciplina l'affidamento di lavori, servizi e forniture tramite contratti di appalto e subappalto."
    emb = await vector.compute_embeddings_batch([text])
    
    topics = teseo.extract_topics_with_embedding(text, emb[0])
    for t in topics:
        print(f"Topic: {t['label']} | Score: {t['score']}")

if __name__ == "__main__":
    asyncio.run(test())
