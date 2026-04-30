import asyncio
import logging
import os
import aiohttp
from pathlib import Path
from rdflib import Graph

# Configure logging
logger = logging.getLogger(__name__)

class AsyncTeseoClient:
    """
    Asynchronous client for TESEO download.
    """
    SPARQL_ENDPOINT = "http://dati.senato.it/sparql"
    
    def __init__(self, output_dir="data/external"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def download_teseo_rdf(self):
        """
        Attempts to download the full classification scheme asynchronously.
        """
        query = """
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        CONSTRUCT {
            ?s ?p ?o
        } WHERE {
            ?s a skos:Concept .
            ?s ?p ?o .
        } LIMIT 1000
        """
        
        logger.info("Querying TESEO endpoint...")
        async with aiohttp.ClientSession() as session:
            try:
                headers = {
                    "Accept": "application/rdf+xml",
                    "User-Agent": "Mozilla/5.0 (compatible; LegalGraphRAG/1.0)"
                }
                async with session.get(self.SPARQL_ENDPOINT, params={'query': query, 'format': 'application/rdf+xml'}, headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"Failed to query TESEO: {response.status}")
                        return

                    content = await response.read()
                    
                    filepath = self.output_dir / "teseo_sample.rdf"
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    
                    logger.info(f"Downloaded TESEO sample to {filepath}")
                    
                    # Validation (Sync is fine here as it's CPU bound and fast for sample)
                    try:
                        g = Graph()
                        g.parse(filepath, format="xml")
                        logger.info(f"Parsed {len(g)} triples successfully.")
                    except Exception as e:
                        logger.error(f"Validation failed for TESEO data: {e}")

            except Exception as e:
                logger.error(f"Failed to download TESEO data: {e}")

    async def run(self):
        """Main execution wrapper."""
        await self.download_teseo_rdf()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(AsyncTeseoClient().run())
