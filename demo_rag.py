import asyncio
from neo4j import AsyncGraphDatabase
from src.config import settings
from src.parsing.vector_engine import VectorEngine

async def run_demo():
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )
    
    query = "Come funzionano le Zone Logistiche Semplificate (ZLS)?"
    print(f"\n🚀 QUERY DEMO: '{query}'")
    
    # 1. Vector Search
    vector_engine = VectorEngine()
    query_vector = (await vector_engine.compute_embeddings_batch([query]))[0]
    
    async with driver.session() as session:
        print("\n--- [ 🤖 RISULTATI VETTORIALI (SIMILARITÀ) ] ---")
        vector_query = """
        CALL db.index.vector.queryNodes('expression_embedding_vector', 2, $query_vector)
        YIELD node AS e, score
        OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
        RETURN e.text_display AS text, u.tag_name AS unit, score
        """
        records = await session.run(vector_query, query_vector=query_vector)
        async for record in records:
            print(f"[{record['score']:.3f}] {record['unit']}: {record['text'][:150].strip()}...")

        print("\n--- [ 🕸️ RISULTATI GRAFO (TESEO TOPICS) ] ---")
        # Find a topic related to 'MINISTRI' or 'BILANCIO' (common in sample)
        graph_query = """
        MATCH (t:TESEO_Concept)
        WHERE t.id CONTAINS '00002232' OR t.id CONTAINS '00001019'
        MATCH (e:Expression)-[r:HAS_TOPIC]->(t)
        OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
        RETURN e.text_display AS text, u.tag_name AS unit, t.id AS topic, r.score AS rel_score
        LIMIT 3
        """
        records = await session.run(graph_query)
        async for record in records:
            print(f"[Topic: {record['topic']}] {record['unit']}: {record['text'][:150].strip()}...")

    await driver.close()

if __name__ == "__main__":
    asyncio.run(run_demo())
