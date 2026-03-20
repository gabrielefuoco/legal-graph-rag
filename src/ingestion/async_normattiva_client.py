import asyncio
import logging
import os
import time
from typing import Dict, Any, Optional
import aiohttp
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

class AsyncNormattivaClient:
    """
    Asynchronous client for Normattiva API.
    Handles async search requests and downloads.
    """
    # URL pattern confirmed by documentation
    BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
    
    def __init__(self, output_dir="data/raw/normattiva"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def search_async(self, search_params: Dict[str, Any], email: str = "", fmt: str = "XML") -> Optional[str]:
        """
        Initiates an asynchronous search.
        """
        url = f"{self.BASE_URL}/ricerca-asincrona/nuova-ricerca"
        
        payload = {
            "formato": fmt, 
            "richiestaExport": "M", 
            "modalita": "C", 
            "tipoRicerca": "A", 
            "email": email,
            "parametriRicerca": search_params
        }

        async with aiohttp.ClientSession() as session:
            try:
                logger.info(f"Initiating search with params: {search_params}")
                async with session.post(url, json=payload) as response:
                    response.raise_for_status()
                    text = await response.text()
                    token = text.strip().replace('"', '')
                    logger.info(f"Search initiated. Token: {token}")
                    
                    # Confirm the search
                    if await self._confirm_search(session, token):
                        return token
                    else:
                        return None
            except Exception as e:
                logger.error(f"Error initiating search: {e}")
                return None

    async def _confirm_search(self, session: aiohttp.ClientSession, token: str) -> bool:
        """
        Confirms the async search request.
        """
        url = f"{self.BASE_URL}/ricerca-asincrona/conferma-ricerca"
        payload = {"token": token}
        
        try:
            async with session.put(url, json=payload) as response:
                response.raise_for_status()
                logger.info(f"Search {token} confirmed.")
                return True
        except Exception as e:
            logger.error(f"Failed to confirm search {token}: {e}")
            return False

    async def check_status(self, session: aiohttp.ClientSession, token: str) -> Dict[str, Any]:
        """
        Checks the status of the async search.
        """
        url = f"{self.BASE_URL}/ricerca-asincrona/check-status/{token}"
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Error checking status: {e}")
            return {}

    async def wait_and_download(self, token: str, poll_interval=5, timeout=300):
        """
        Polls status until complete, then downloads.
        """
        start_time = time.time()
        async with aiohttp.ClientSession() as session:
            while time.time() - start_time < timeout:
                status_dto = await self.check_status(session, token)
                status_code = status_dto.get("stato")
                
                if status_code == 3: # Success
                    logger.info("Search completed. Downloading results...")
                    return await self._download_collection(session, token)
                elif status_code == 4:
                    logger.error(f"Search failed: {status_dto.get('descrizioneErrore')}")
                    return None
                elif status_code in [0, 1, 2, 6]:
                    logger.info(f"Status {status_code}: {status_dto.get('descrizioneStato')}. Waiting...")
                    await asyncio.sleep(poll_interval)
                else:
                    logger.warning(f"Unknown status {status_code}")
                    await asyncio.sleep(poll_interval)
                    
        logger.error("Timeout waiting for search results.")
        return None

    async def _download_collection(self, session: aiohttp.ClientSession, token: str):
        """
        Downloads the ZIP collection.
        """
        url = f"{self.BASE_URL}/collections/download/collection-asincrona/{token}"
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                
                filename = f"normattiva_export_{token}.zip"
                filepath = self.output_dir / filename
                
                content = await response.read()
                with open(filepath, 'wb') as f:
                    f.write(content)
                    
                logger.info(f"Downloaded: {filepath}")
                return str(filepath)
        except Exception as e:
            logger.error(f"Error downloading collection: {e}")
            return None

    async def download_by_date(self, date_str: str, email: str = "ingestion@example.com", fmt: str = "XML") -> Optional[str]:
        """
        Download acts for a specific date (YYYY-MM-DD).
        """
        logger.info(f"Starting bulk download for Date {date_str}...")
        try:
            year = int(date_str.split("-")[0])
            params = {
                "annoProvvedimento": year,
                "dataInizioPubProvvedimento": date_str,
                "dataFinePubProvvedimento": date_str,
                "filtriMap": {},
                "orderType": "score",
                "limitaAnniVigenza": False
            }
            
            token = await self.search_async(params, email=email, fmt=fmt)
            if token:
                logger.info(f"Search for {date_str} initiated (Token: {token}). Waiting...")
                return await self.wait_and_download(token, poll_interval=5, timeout=300)
            return None
        except Exception as e:
            logger.error(f"Error in download_by_date: {e}")
            return None

    async def run(self, date: str = None):
        """
        Main execution method.
        If no date is provided, defaults to yesterday's date.
        """
        if not date:
            # Default to yesterday
            import datetime
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            date = yesterday.strftime("%Y-%m-%d")
            
        logger.info(f"Starting Async Normattiva Client for date: {date}")
        result = await self.download_by_date(date)
        if result:
            logger.info("Normattiva ingestion successful.")
        else:
            logger.warning("Normattiva ingestion returned no results or failed.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = AsyncNormattivaClient()
    # Example usage
    asyncio.run(client.run()) 
