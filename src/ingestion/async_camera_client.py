import asyncio
import logging
import aiohttp
from typing import List, Dict, Any

# Configure logging
logger = logging.getLogger(__name__)

CAMERA_SPARQL_ENDPOINT = "https://dati.camera.it/sparql"

class AsyncCameraClient:
    """
    Asynchronous client for Camera dei Deputati Linked Open Data.
    Fetches Iter Legis steps (presentations, votes, assignments).
    """

    async def fetch_iter_legis(self, legislature: int = 19, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetches Iter Legis events for a specific legislature.
        """
        # Query to get Acts (Atto) and their events (Stato/Passaggio)
        # Simplified query for demonstration - focused on Act presentation
        query = f"""
        SELECT DISTINCT ?atto ?numero ?titolo ?data ?tipo
        WHERE {{
            ?atto a <http://dati.camera.it/ocd/atto> .
            ?atto <http://dati.camera.it/ocd/rif_leg> <http://dati.camera.it/ocd/legislatura.rdf/repubblica_{legislature}> .
            ?atto <http://purl.org/dc/elements/1.1/title> ?titolo .
            ?atto <http://purl.org/dc/elements/1.1/date> ?data .
            ?atto <http://purl.org/dc/elements/1.1/identifier> ?numero .
        }}
        LIMIT {limit}
        """
        
        async with aiohttp.ClientSession() as session:
            try:
                headers = {"Accept": "application/sparql-results+json"}
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
        
        # Append mode to support multiple runs/pagination
        with open(filepath, "a", encoding="utf-8") as f:
            for item in results:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        
        logger.info(f"Saved {len(results)} records to {filepath}")
        return str(filepath)

    async def run(self):
        """Main execution."""
        logger.info("Starting Async Camera Client...")
        self.output_dir = Path("data/raw/camera") # Ensure default is set
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        results = await self.fetch_iter_legis(limit=20)
        
        if results:
            await self.save_metadata(results)
            for res in results:
                logger.debug(f" - {res['date']}: DDL {res['number']}")
        else:
            logger.info("No Camera records found.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = AsyncCameraClient()
    asyncio.run(client.run())
