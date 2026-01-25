import requests
import time
import os
import logging
import sys
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NormattivaClient:
    # URL pattern confirmed by documentation:
    # https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1/ricerca-asincrona/nuova-ricerca
    BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
    
    def __init__(self, output_dir="data/raw/normattiva"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def search_async(self, search_params: Dict[str, Any], email: str = "") -> Optional[str]:
        """
        Initiates an asynchronous search.
        Args:
            search_params: Dictionary matching RicercaAvanzataFilterDto (e.g. {'dataGU': '...'})
            email: Optional email for notifications
        Returns:
            token: The search token if successful
        """
        url = f"{self.BASE_URL}/ricerca-asincrona/nuova-ricerca"
        
        # Construct the valid request body structure
        payload = {
            "formato": "XML", # or AKN, HTML, PDF
            "richiestaExport": "M", # M=Multivigente, O=Originario, V=Vigente
            "modalita": "C", # C=Classica
            "tipoRicerca": "A", # A=Avanzata, S=Semplice (Fixed from "AVANZATA")
            "email": email,
            "parametriRicerca": search_params
        }

        try:
            logger.info(f"Initiating search with params: {search_params}")
            response = requests.post(url, json=payload)
            response.raise_for_status()
            
            # API returns a string token directly or inside a wrapper? 
            # OAS says response 202 content schema is just "type: string"
            # But normally APIs return JSON. Let's assume it might be text or JSON wrapped.
            # Reading OAS: "Nuova ricerca... restituito in risposta il token"
            
            token = response.text.strip().replace('"', '') # Clean up potential JSON string quotes
            logger.info(f"Search initiated. Token: {token}")
            
            # CRITICAL STEP: Confirm the search
            if self._confirm_search(token):
                return token
            else:
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error initiating search: {e}")
            if response is not None:
                logger.error(f"Response body: {response.text}")
            return None

    def _confirm_search(self, token: str) -> bool:
        """
        Confirms the async search request (PUT /conferma-ricerca).
        """
        url = f"{self.BASE_URL}/ricerca-asincrona/conferma-ricerca"
        payload = {"token": token}
        
        try:
            response = requests.put(url, json=payload)
            response.raise_for_status()
            logger.info(f"Search {token} confirmed.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to confirm search {token}: {e}")
            return False

    def check_status(self, token: str) -> Dict[str, Any]:
        """
        Checks the status of the async search.
        Returns the DTO with 'stato' (0-6).
        """
        url = f"{self.BASE_URL}/ricerca-asincrona/check-status/{token}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error checking status: {e}")
            return {}

    def wait_and_download(self, token: str, poll_interval=5, timeout=300):
        """
        Polls status until complete, then downloads.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            status_dto = self.check_status(token)
            status_code = status_dto.get("stato")
            
            if status_code == 3: # "Ricerca elaborata con successo"
                logger.info("Search completed. Downloading results...")
                return self._download_collection(token)
            elif status_code == 4:
                logger.error(f"Search failed: {status_dto.get('descrizioneErrore')}")
                return None
            elif status_code in [0, 1, 2, 6]:
                logger.info(f"Status {status_code}: {status_dto.get('descrizioneStato')}. Waiting...")
                time.sleep(poll_interval)
            else:
                logger.warning(f"Unknown status {status_code}")
                time.sleep(poll_interval)
                
        logger.error("Timeout waiting for search results.")
        return None

    def _download_collection(self, token: str):
        """
        Downloads the ZIP collection (GET /collections/download/collection-asincrona/{token}).
        """
        url = f"{self.BASE_URL}/collections/download/collection-asincrona/{token}"
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            filename = f"normattiva_export_{token}.zip"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Downloaded: {filepath}")
            return filepath
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading collection: {e}")
            return None

    def download_by_year(self, year: int, email: str = "ingestion@example.com") -> Optional[str]:
        """
        Convenience method to download all acts for a given year.
        Uses the async search endpoint to prevent timeouts.
        """
        logger.info(f"Starting bulk download for Year {year} (Email: {email})...")
        
        # Define the date range for the entire year
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
        # Configure search parameters for the 'Avanzata' search
        params = {
            "annoProvvedimento": year,
            "dataInizioPubProvvedimento": start_date,
            "dataFinePubProvvedimento": end_date,
            "filtriMap": {},
            "orderType": "score",
            "limitaAnniVigenza": False
        }
        
        token = self.search_async(params, email=email)
        if token:
            logger.info(f"Bulk search for {year} initiated (Token: {token}). Waiting for results...")
            # Increased timeout for bulk operations
            return self.wait_and_download(token, poll_interval=10, timeout=1200) # 20 mins timeout
        else:
            logger.error(f"Failed to initiate bulk search for {year}")
            return None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Normattiva Ingestion Client")
    parser.add_argument("--year", type=int, help="Download all acts for a specific year (e.g. 2024)")
    parser.add_argument("--email", type=str, default="test@example.com", help="Email for notifications (required by API)")
    
    args = parser.parse_args()
    
    client = NormattivaClient()
    
    if args.year:
        result = client.download_by_year(args.year, email=args.email)
        if result:
            logger.info(f"Successfully downloaded archive to {result}")
        else:
            sys.exit(1)
    else:
        logger.info("Normattiva Client Initialized. Use --year <YYYY> to download data.")


