import asyncio
import logging
import sys
sys.path.append(r"C:\Users\gabri\APP\Università\Tesi")
from src.rag.engine import RagEngine

logging.basicConfig(level=logging.INFO, format="%(message)s")

async def test():
    engine = RagEngine()
    
    # Questa query riguarda il Codice degli Appalti (D.Lgs 50/2016)
    query = "esclusioni operatori gara codice appalti 2016 D.Lgs. 50/2016 art. 94"
    
    print("=" * 80)
    print(f"ESEGUENDO RETRIEVAL RAG CON MULTI-HOP CITAZIONALE")
    print(f"QUERY: {query}")
    print("=" * 80)
    
    results, trace, _ = await engine.retrieve_with_trace(
        query, max_citation_hops=1, enable_multi_hop=True, skip_generation=True
    )
    
    print("\n" + "=" * 80)
    print(f"RISULTATI RECUPERATI: {len(results)}")
    print(f"MULTI-HOP EFFETTUATI: {trace.get('hop_count')}")
    print("=" * 80)
    
    for i, r in enumerate(results[:5]):
        meta = r.get('metadata', {})
        score = r.get('score', 0.0)
        source = meta.get('source', '')
        
        # Vediamo se nei metadati c'è qualche flag che indica che è stato recuperato tramite espansione CITES
        expanded_via = meta.get('expanded_via', '')
        
        print(f"\n[{i+1}] Score: {score:.3f} | Source: {source}")
        if expanded_via:
            print(f"    >>> ESTRATTO TRAMITE ESPANSIONE MULTI-HOP: {expanded_via}")
        print(f"    Text: {r.get('text', '').replace(chr(10), ' ')[:200]}...")

if __name__ == "__main__":
    asyncio.run(test())
