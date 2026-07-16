import asyncio
import logging
logging.basicConfig(level=logging.ERROR)
from src.rag.engine import RagEngine
from src.rag.models import RagState

async def main():
    engine = RagEngine()
    state = RagState(
        query="Quali sono le soglie economiche per gli affidamenti diretti di lavori, servizi e forniture?",
        chat_history=[],
        _driver=engine.driver,
        top_k=15,
        final_k=20,
        enable_graph_search=True,
        enable_multi_hop=True,
        iterations=0,
        skip_generation=False,
        rewritten_queries=[],
        vector_results=[],
        bm25_results=[],
        graph_results=[],
        fused_chunks=[],
        final_chunks=[]
    )
    
    # Analyze
    from src.rag.query_analyzer import analyze_query
    state = await analyze_query(state)
    
    # Retrieve
    from src.rag.retriever import retrieve_all_node
    state = await retrieve_all_node(state)
    
    # Fusion
    from src.rag.fusion import fuse_and_filter
    state = await fuse_and_filter(state)
    
    # Check if Art 50 is in fused
    chunks = state.get("fused_chunks", [])
    print(f"\nTop 20 Fused Chunks:")
    found = False
    for i, c in enumerate(chunks):
        if "150.000 euro" in c.text or "50" in str(c.structural_context):
            print(f"[{i+1}] MATCH: {c.structural_context} (URN: {c.work_urn})")
            print(f"Testo: {c.text[:200]}...")
            found = True
        else:
            print(f"[{i+1}] {c.structural_context}")
    if not found:
        print("Art 50 NOT FOUND in top 20 fused chunks!")

asyncio.run(main())
