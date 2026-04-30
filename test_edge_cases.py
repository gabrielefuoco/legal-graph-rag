import asyncio
from neo4j import AsyncGraphDatabase
from src.config import settings
from src.ingestion.neo4j_loader import AsyncNeo4jLoader
import warnings

async def test_edge_cases():
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )
    
    print("\\n--- [ RICERCA CASI LIMITE E ANOMALIE ] ---\\n")
    
    async with driver.session() as session:
        # Edge Case 1: Nodi Orfani (Expression senza PART_OF)
        orphan_query = """
        MATCH (e:Expression)
        WHERE NOT (e)-[:PART_OF]->()
        RETURN count(e) as orphans
        """
        orphans = await session.run(orphan_query)
        orphan_count = (await orphans.single())["orphans"]
        if orphan_count > 0:
            print(f"⚠️ TROVATI NODI ORFANI: {orphan_count} nodi 'Expression' non sono collegati a nessun 'StructuralUnit' o 'Work'.")
            print("   Implicazione: La query RAG non avrà un contesto gerarchico (es. 'Articolo X') per questi frammenti.\\n")
        else:
            print("✅ Nessun nodo orfano strutturale rilevato.\\n")

        # Edge Case 2: Link Silenziosamente Falliti (Source o Target mancanti)
        # We can't query this directly from Neo4j since they were dropped, but we can check if there are 
        # disconnected "StructuralUnit" nodes.
        struct_orphan = """
        MATCH (s:StructuralUnit)
        WHERE NOT (s)-[:PART_OF]->() AND s.level > 0
        RETURN count(s) as struct_orphans
        """
        s_orphans = await session.run(struct_orphan)
        s_orphan_count = (await s_orphans.single())["struct_orphans"]
        if s_orphan_count > 0:
            print(f"⚠️ TROVATI Nodi Strutturali non linkati al Work: {s_orphan_count}.")
            print("   Causa: Edge cases nel parse XML in cui il target_id strutturale puntava a un ID che non esiste nel batch.\\n")
        else:
            print("✅ Nessuna unità strutturale intermedia isolata.\\n")
            
        # Edge Case 3: Nodi multipli sovrascritti sotto URN Sconosciuto
        urn_query = """
        MATCH (w:Work)
        WHERE w.urn CONTAINS 'unknown' OR w.urn = ''
        RETURN count(w) as unknown_urns
        """
        urns = await session.run(urn_query)
        unknown_urn_count = (await urns.single())["unknown_urns"]
        if unknown_urn_count > 0:
            print(f"⚠️ TROVATI WORK CON URN NON DEFINITO: {unknown_urn_count} occorrenze.")
            print("   Causa: Il parser non riesce a derivare l'URN. Essendo usato come 'id', i successivi Work non validi andranno in collisione sovrascrivendosi, corrompendo gli archi PART_OF!\\n")

        # Edge Case 4: Embedding Nulli in Vector Index
        embed_query = """
        MATCH (e:Expression)
        WHERE e.embedding IS NULL
        RETURN count(e) as null_embeds
        """
        embeds = await session.run(embed_query)
        null_embed_count = (await embeds.single())["null_embeds"]
        if null_embed_count > 0:
            print(f"⚠️ TROVATI EMBEDDINGS MANCANTI: {null_embed_count} nodi.")
            print("   Implicazione: Il fallback è gestito (i nodi ci sono), ma finché il container Vector/Ollama è offline, l'Hybrid Search nel RAG fallirà per loro.\\n")

    await driver.close()

if __name__ == "__main__":
    asyncio.run(test_edge_cases())
