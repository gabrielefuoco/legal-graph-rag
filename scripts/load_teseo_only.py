import asyncio
import logging
from src.ingestion.neo4j_loader import AsyncNeo4jLoader

logging.basicConfig(level=logging.INFO)

async def main():
    loader = AsyncNeo4jLoader()
    try:
        await loader.load_teseo_ontology('data/external/teseo_full.ttl')
    finally:
        await loader.close()

if __name__ == "__main__":
    asyncio.run(main())
