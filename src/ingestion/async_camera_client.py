import asyncio
import logging
import aiohttp
from typing import List, Dict, Any
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

CAMERA_SPARQL_ENDPOINT = "https://dati.camera.it/sparql"

class AsyncCameraClient:
    """
    Asynchronous client for Camera dei Deputati Linked Open Data.
    Fetches Iter Legis steps (presentations, votes, assignments).
    """

    def __init__(self, output_dir: str = "data/raw/camera"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def fetch_iter_legis(self, legislature: int = 19, limit: int = 100, keyword: str = None) -> List[Dict[str, Any]]:
        """
        Fetches Iter Legis events for a specific legislature.
        Optionally filters by a keyword in the title.
        """
        keyword_filter = ""
        if keyword:
            keyword_lower = keyword.lower().replace('"', '')
            keyword_filter = f'FILTER(CONTAINS(TOLOWER(STR(?titolo)), "{keyword_lower}"))'

        query = f"""
        SELECT DISTINCT ?atto ?numero ?titolo ?data ?tipo
        WHERE {{
            ?atto a <http://dati.camera.it/ocd/atto> .
            ?atto <http://dati.camera.it/ocd/rif_leg> <http://dati.camera.it/ocd/legislatura.rdf/repubblica_{legislature}> .
            ?atto <http://purl.org/dc/elements/1.1/title> ?titolo .
            ?atto <http://purl.org/dc/elements/1.1/date> ?data .
            ?atto <http://purl.org/dc/elements/1.1/identifier> ?numero .
            {keyword_filter}
        }}
        LIMIT {limit}
        """
        
        async with aiohttp.ClientSession() as session:
            try:
                headers = {
                    "Accept": "application/sparql-results+json",
                    "User-Agent": "Mozilla/5.0 (compatible; LegalGraphRAG/1.0)"
                }
                async with session.get(CAMERA_SPARQL_ENDPOINT, params={"query": query}, headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"Camera SPARQL Query failed: {response.status}")
                        return []
                    
                    data = await response.json()
                    results = []
                    for binding in data.get("results", {}).get("bindings", []):
                        results.append({
                            "uri": binding["atto"]["value"],
                            "number": binding["numero"]["value"],
                            "title": binding["titolo"]["value"],
                            "date": binding["data"]["value"],
                            "authority": "Camera dei Deputati"
                        })
                    
                    logger.info(f"Retrieved {len(results)} Camera Iter Legis records.")
                    return results

            except Exception as e:
                logger.error(f"Error fetching Camera data: {e}")
                return []

    async def save_metadata(self, results: List[Dict[str, Any]], filename: str = "camera_acts_metadata.jsonl") -> str:
        """Saves metadata to a JSONL file."""
        import json
        filepath = self.output_dir / filename
        
        # Override file to avoid appending forever in the same topic run
        with open(filepath, "w", encoding="utf-8") as f:
            for item in results:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        
        logger.info(f"Saved {len(results)} records to {filepath}")
        return str(filepath)

    async def run(self, keyword: str = None):
        """Main execution."""
        logger.info(f"Starting Async Camera Client (Topic: {keyword})...")
        results = await self.fetch_iter_legis(limit=20, keyword=keyword)
        
        if results:
            await self.save_metadata(results)
            for res in results:
                logger.debug(f" - {res['date']}: DDL {res['number']}")
        else:
            logger.info("No Camera records found.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = AsyncCameraClient()
    asyncio.run(client.run(keyword="appalti"))
