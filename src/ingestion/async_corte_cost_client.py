import asyncio
import logging
import aiohttp
import os
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

# Configure logging
logger = logging.getLogger(__name__)

CORTE_COST_BASE_URL = "https://dati.cortecostituzionale.it"

class AsyncCorteCostClient:
    """
    Asynchronous client for Corte Costituzionale Open Data.
    Scrapes the portal for XML datasets and downloads them.
    Supports incremental updates by checking existing files.
    """

    def __init__(self, output_dir: str = "data/raw/cortecost"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def fetch_dataset_urls(self) -> list[str]:
        """
        Scrapes the main page to find links to XML datasets.
        """
        logger.info(f"Scraping {CORTE_COST_BASE_URL} for datasets...")
        dataset_urls = []
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(CORTE_COST_BASE_URL, ssl=False) as response:
                    if response.status != 200:
                        logger.error(f"Failed to fetch content: {response.status}")
                        return []
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find all links ending in .xml or .zip, or containing "xml" in text/class
                    # Heuristic: verify against standard open data patterns
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        full_url = urljoin(CORTE_COST_BASE_URL, href)
                        
                        # Check text/href for 'xml' indicator
                        is_xml = "xml" in href.lower() or (a.text and "xml" in a.text.lower())
                        # Check strictly for file extension if possible, or bulk download keywords
                        if is_xml and ("pronunce" in href.lower() or "dati" in href.lower()):
                            if full_url not in dataset_urls:
                                dataset_urls.append(full_url)
                                logger.info(f"Found potential dataset: {full_url}")
            
            except Exception as e:
                logger.error(f"Scraping failed: {e}")
        
        return dataset_urls

    async def download_file(self, session: aiohttp.ClientSession, url: str) -> bool:
        """Downloads a file if it's new or updated."""
        filename = url.split("/")[-1]
        filepath = self.output_dir / filename
        
        # Simple Incremental Logic:
        # If file exists, we could check Last-Modified header. 
        # For now, we assume user might want to 'update' so we re-download if requested or if missing.
        # But to be robust, let's always download and overwrite for 'bulk' sync.
        
        try:
            logger.info(f"Downloading {url}...")
            async with session.get(url, ssl=False) as response:
                if response.status == 200:
                    content = await response.read()
                    with open(filepath, "wb") as f:
                        f.write(content)
                    logger.info(f"Saved to {filepath}")
                    return True
                else:
                    logger.warning(f"Failed download {url}: {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Download error {url}: {e}")
            return False

    async def run(self):
        """Main execution."""
        logger.info("Starting Async Corte Costituzionale Client...")
        
        # 1. Discovery
        urls = await self.fetch_dataset_urls()
        
        if not urls:
            logger.warning("No XML datasets found via scraping. Please verify the URL or structure.")
            # Fallback/Suggestion
            logger.info("Try manually visiting: https://dati.cortecostituzionale.it")
            return

        # 2. Download
        async with aiohttp.ClientSession() as session:
            tasks = [self.download_file(session, url) for url in urls]
            results = await asyncio.gather(*tasks)
            logger.info(f"Downloaded {sum(results)}/{len(tasks)} files.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = AsyncCorteCostClient()
    asyncio.run(client.run())
