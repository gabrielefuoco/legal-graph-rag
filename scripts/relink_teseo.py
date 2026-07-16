import asyncio
import logging
from neo4j import AsyncGraphDatabase
from src.config import settings
from src.parsing.teseo_matcher import TESEOMatcher
from src.parsing.vector_engine import VectorEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)-30s | %(levelname)-5s | %(message)s")
logger = logging.getLogger(__name__)

async def relink_teseo():
    logger.info("Initializing VectorEngine and TESEOMatcher...")
    vector_engine = VectorEngine()
    teseo_matcher = TESEOMatcher('data/external/teseo_full.ttl')
    await teseo_matcher.precompute_embeddings(vector_engine)
    
    logger.info("Connecting to Neo4j...")
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )
    
    batch_size = 500
    skip = 0
    total_processed = 0
    total_edges = 0
    
    async with driver.session() as session:
        while True:
            logger.info(f"Fetching Expressions batch (SKIP {skip})...")
            result = await session.run(
                "MATCH (e:Expression) WHERE e.text_display IS NOT NULL RETURN e.id AS id, e.text_display AS text, e.embedding AS embedding SKIP $skip LIMIT $limit",
                skip=skip, limit=batch_size
            )
            records = await result.data()
            if not records:
                break
                
            batch_updates = []
            for record in records:
                topics = teseo_matcher.extract_topics_with_embedding(record["text"], record["embedding"])
                for topic in topics:
                    # Filter out low confidence matches
                    if topic["score"] > 0.35:
                        batch_updates.append({
                            "expression_id": record["id"],
                            "teseo_id": topic["teseo_id"],
                            "score": topic["score"]
                        })
            
            if batch_updates:
                logger.info(f"Creating {len(batch_updates)} HAS_TOPIC edges...")
                query = """
                UNWIND $batch AS row
                MATCH (e:Expression {id: row.expression_id})
                MERGE (t:TESEO_Concept {id: row.teseo_id})
                MERGE (e)-[r:HAS_TOPIC]->(t)
                SET r.score = row.score
                """
                await session.run(query, batch=batch_updates)
                total_edges += len(batch_updates)
            
            total_processed += len(records)
            skip += batch_size
            
    await driver.close()
    logger.info(f"Relinking complete! Processed {total_processed} expressions, created {total_edges} new semantic links.")

if __name__ == "__main__":
    asyncio.run(relink_teseo())
