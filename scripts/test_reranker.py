import asyncio
from src.rag.reranker import rerank
from src.rag.models import RagState, RetrievedChunk

async def test():
    state = RagState(
        original_query="Quali sono le soglie economiche per gli affidamenti diretti di lavori, servizi e forniture?",
        chat_history=[],
        _driver=None,
        fused_chunks=[
            RetrievedChunk(text="Art. 50. affidamento diretto per lavori di importo inferiore a 150.000 euro; affidamento diretto dei servizi e forniture di importo inferiore a 140.000 euro.", score=0, source="test", structural_context="Art 50", work_urn="urn:1", expression_id="1"),
            RetrievedChunk(text="Art. 62. Tutte le stazioni appaltanti possono procedere direttamente e autonomamente all'acquisizione di forniture e servizi di importo non superiore alle soglie previste per gli affidamenti diretti, e all'affidamento di lavori d'importo pari o inferiore a 500.000 euro.", score=0, source="test", structural_context="Art 62", work_urn="urn:2", expression_id="2")
        ]
    )
    res = await rerank(state)
    for c in res["reranked_chunks"]:
        print(f"[{c.structural_context}] Score: {c.score}")

asyncio.run(test())
