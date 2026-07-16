import asyncio
import logging
from src.rag.models import RagState
from src.rag.engine import RagEngine
from src.config import settings

logging.basicConfig(level=logging.INFO)

async def test_retrieval():
    engine = RagEngine()
    
    query = "Quali sono le soglie economiche per gli affidamenti diretti di lavori, servizi e forniture?"
    
    # Run pipeline step by step
    state = RagState(
        original_query=query,
        chat_history=[],
        _driver=engine.driver
    )
    
    print("1. Analyze")
    from src.rag.query_analyzer import analyze_query
    state = await analyze_query(state)
    
    print("2. Retrieve")
    from src.rag.retriever import vector_search, bm25_search, graph_search
    v_res = await vector_search(state)
    b_res = await bm25_search(state)
    g_res = await graph_search(state)
    
    state.update(v_res)
    state.update(b_res)
    state.update(g_res)
    
    print("3. Fusion")
    from src.rag.fusion import fuse_and_filter
    f_res = await fuse_and_filter(state)
    state.update(f_res)
    
    print("4. Rerank")
    from src.rag.reranker import rerank
    r_res = await rerank(state)
    
    chunks = r_res["reranked_chunks"]
    print(f"\nTop {len(chunks)} chunks after Reranker:")
    for i, c in enumerate(chunks):
        print(f"[{i+1}] Score: {c.score} | Context: {c.structural_context} | URN: {c.work_urn}")
        print(f"Text snippet: {c.text[:150]}...")
        print("-" * 50)

if __name__ == "__main__":
    asyncio.run(test_retrieval())
