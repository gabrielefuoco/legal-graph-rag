"""Asynchronous scraper and client for Senato della Repubblica data."""

import asyncio
import logging
import aiohttp
import xml.etree.ElementTree as ET
from typing import List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

SENATO_SPARQL_ENDPOINT = "https://dati.senato.it/sparql"

class AsyncSenatoScraper:
    """Asynchronous scraper for Senato della Repubblica data.

    Discovers legislative documents via SPARQL and downloads Akoma Ntoso XML files.
    """

    def __init__(self, output_dir: str = "data/raw/senato"):
        """Initialize scraper with target output directory.

        Args:
            output_dir: Directory where downloaded Senato documents are stored.
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def fetch_ids_by_topic(self, topic: str, limit: int = 500) -> List[str]:
        """Fetch Senato Bill identifiers (DDL) related to a specific topic via SPARQL.

        Args:
            topic: Keyword or topic name to match in bill titles.
            limit: Maximum number of bill identifiers to retrieve.

        Returns:
            List of bill numbers/identifiers to match with bulk data XMLs.
        """
        topic_lower = topic.lower().replace('"', '')
        query = f"""
        SELECT DISTINCT ?ddl ?numero
        WHERE {{
            ?ddl a <http://dati.senato.it/osr/Ddl> .
            ?ddl <http://dati.senato.it/osr/titolo> ?titolo .
            ?ddl <http://dati.senato.it/osr/numero> ?numero .
            FILTER(CONTAINS(TOLOWER(STR(?titolo)), "{topic_lower}"))
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
                        logger.error(f"Senato SPARQL Query failed: {response.status}")
                        return []
                    
                    data = await response.json()
                    results = []
                    for binding in data.get("results", {}).get("bindings", []):
                        num = binding.get("numero", {}).get("value")
                        if num:
                            results.append(num)
                    
                    logger.info(f"Senato Scraper found {len(results)} IDs for topic '{topic}'.")
                    return results
            except Exception as e:
                logger.error(f"Error fetching Senato IDs by topic: {e}")
                return []

    async def fetch_ddl_metadata(self, limit: int = 100) -> List[Dict[str, str]]:
        """Fetch metadata and download URLs for recent bills (Disegni di Legge).

        Args:
            limit: Maximum number of DDL metadata records to retrieve.

        Returns:
            List of dictionaries containing URI, number, title, and XML URL.
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
        """Construct an XML URL from a DDL URI if missing."""
        return None

    async def download_file(self, session: aiohttp.ClientSession, url: str, filename: str) -> bool:
        """Download a single XML file asynchronously.

        Args:
            session: Active aiohttp client session.
            url: Remote file URL to download.
            filename: Destination file name.

        Returns:
            True if download succeeded, False otherwise.
        """
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
        """Discover and download recent Senato DDL XML files.

        Args:
            limit: Maximum number of records to process.
        """
        logger.info("Starting Async Senato Scraper...")
        
        ddls = await self.fetch_ddl_metadata(limit)
        
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
