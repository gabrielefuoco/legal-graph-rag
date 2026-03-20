import asyncio
import logging
import aiohttp
import xml.etree.ElementTree as ET
from typing import List, Dict, Any
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

SENATO_SPARQL_ENDPOINT = "https://dati.senato.it/sparql"

class AsyncSenatoScraper:
    """
    Asynchronous scraper for Senato della Repubblica data.
    Uses SPARQL to discover documents and aiohttp to download AKN XMLs.
    """

    def __init__(self, output_dir: str = "data/raw/senato"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def fetch_ddl_metadata(self, limit: int = 100) -> List[Dict[str, str]]:
        """
        Fetches metadata for recent DDLs (Disegni di Legge) via SPARQL.
        """
        query = f"""
        SELECT DISTINCT ?ddl ?titolo ?numero ?url
        WHERE {{
            ?ddl a <http://dati.senato.it/osr/Ddl> .
            OPTIONAL {{ ?ddl <http://dati.senato.it/osr/titolo> ?titolo }}
            OPTIONAL {{ ?ddl <http://dati.senato.it/osr/numero> ?numero }}
            OPTIONAL {{ ?ddl <http://dati.senato.it/osr/xmlUrl> ?url }}
        }}
        LIMIT {limit}
        """
        
        async with aiohttp.ClientSession() as session:
            try:
                headers = {
                    "Accept": "application/sparql-results+json",
                    "User-Agent": "Mozilla/5.0 (compatible; LegalGraphRAG/1.0)"
                }
                async with session.get(SENATO_SPARQL_ENDPOINT, params={"query": query}, headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"SPARQL Query failed: {response.status}")
                        return []
                    
                    data = await response.json()
                    results = []
                    for binding in data.get("results", {}).get("bindings", []):
                        item = {
                            "uri": binding.get("ddl", {}).get("value"),
                            "number": binding.get("numero", {}).get("value", "N/A"),
                            "title": binding.get("titolo", {}).get("value", "No Title"),
                            "xml_url": binding.get("url", {}).get("value")
                        }
                        if not item["xml_url"]:
                            # Attempt to construct or just warn
                            item["xml_url"] = self._construct_xml_url(item["uri"])
                        
                        if item["xml_url"]:
                            results.append(item)
                    
                    logger.info(f"Retrieved {len(results)} DDL metadata records.")
                    return results

            except Exception as e:
                logger.error(f"Error fetching Senato metadata: {e}")
                return []

    def _construct_xml_url(self, uri: str) -> str | None:
        """
        Heuristic to construct XML URL from DDL URI if missing.
        Example URI: http://dati.senato.it/osr/Ddl/2013-03-15/54
        """
        # For now, return None as we don't have a reliable mapping without more data
        return None

    async def download_file(self, session: aiohttp.ClientSession, url: str, filename: str) -> bool:
        """Downloads a single file asynchronously."""
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    filepath = self.output_dir / filename
                    content = await response.read()
                    with open(filepath, "wb") as f:
                        f.write(content)
                    logger.info(f"Downloaded: {filename}")
                    return True
                else:
                    logger.warning(f"Failed to download {url}: Status {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return False

    async def run(self, limit: int = 50):
        """Main execution method."""
        logger.info("Starting Async Senato Scraper...")
        
        # 1. Get Metadata
        ddls = await self.fetch_ddl_metadata(limit)
        
        # 2. Download XMLs concurrently
        async with aiohttp.ClientSession() as session:
            tasks = []
            for ddl in ddls:
                if ddl.get("xml_url"):
                    filename = f"ddl_{ddl['number']}.xml"
                    tasks.append(self.download_file(session, ddl["xml_url"], filename))
            
            if tasks:
                results = await asyncio.gather(*tasks)
                logger.info(f"Downloaded {sum(results)}/{len(tasks)} files.")
            else:
                logger.info("No XML URLs found to download.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = AsyncSenatoScraper()
    asyncio.run(scraper.run(limit=10))
