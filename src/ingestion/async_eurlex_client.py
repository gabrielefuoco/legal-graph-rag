import asyncio
import logging
import aiohttp
from typing import List, Dict
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

EURLEX_SPARQL_ENDPOINT = "http://publications.europa.eu/webapi/rdf/sparql"

class AsyncEurLexClient:
    """
    Asynchronous client for EUR-Lex (EU Law).
    Queries the EU Publications Office SPARQL endpoint.
    Downloads HTML/XML content.
    """

    def __init__(self, output_dir: str = "data/raw/eurlex"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def fetch_legislation_metadata(self, limit: int = 100) -> List[Dict[str, str]]:
        """
        Queries EUR-Lex for recent Regulations and Directives.
        """
        # Query for Regulations (320...) and Directives (320...)
        # cellar: is the ontology prefix.
        # Query for Regulations (R), Directives (L), Decisions (D) from 2024 onwards
        query = f"""
        SELECT DISTINCT ?work ?title ?date
        WHERE {{
            ?work <http://publications.europa.eu/ontology/cdm#work_date_document> ?date .
            FILTER(STR(?date) >= "2024-01-01")
            ?work <http://publications.europa.eu/ontology/cdm#resource_legal_type> ?type .
            FILTER(STR(?type) = "R" || STR(?type) = "L" || STR(?type) = "D")
            OPTIONAL {{ 
                ?work <http://publications.europa.eu/ontology/cdm#work_title> ?title .
                FILTER(LANG(?title) = "it" || LANG(?title) = "")
            }}
        }}
        LIMIT {limit}
        """
        
        async with aiohttp.ClientSession() as session:
            try:
                headers = {
                    "Accept": "application/sparql-results+json",
                    "User-Agent": "Mozilla/5.0 (compatible; LegalGraphRAG/1.0)"
                }
                async with session.get(EURLEX_SPARQL_ENDPOINT, params={"query": query}, headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"EUR-Lex SPARQL Query failed: {response.status}")
                        return []
                    
                    data = await response.json()
                    results = []
                    for binding in data.get("results", {}).get("bindings", []):
                        results.append({
                            "uri": binding.get("work", {}).get("value"),
                            "title": binding.get("title", {}).get("value", "No Title"),
                            "date": binding.get("date", {}).get("value", "N/A")
                        })
                    logger.info(f"Retrieved {len(results)} EUR-Lex records.")
                    return results

            except Exception as e:
                logger.error(f"Error fetching EUR-Lex data: {e}")
                return []

    async def download_document(self, session: aiohttp.ClientSession, celex: str, title: str, date: str) -> bool:
        """
        Downloads the document content in XML (Formex) format using the CELEX ID.
        URL format: https://eur-lex.europa.eu/legal-content/IT/TXT/XML/?uri=CELEX:{celex}
        """
        try:
            # Clean title for filename
            clean_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()[:100]
            filename = f"{date}_{clean_title}.xml"
            filepath = self.output_dir / filename
            
            if filepath.exists():
                logger.info(f"Skipping existing: {filepath}")
                return True

            # Use the reliable CELEX-based XML download URL
            url = f"https://eur-lex.europa.eu/legal-content/IT/TXT/XML/?uri=CELEX:{celex}"
            
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    
                    # Verify if we actually got XML (sometimes it's an error page with 200)
                    if b"<?xml" in content or b"<html" not in content[:100].lower():
                        with open(filepath, "wb") as f:
                            f.write(content)
                        logger.info(f"Downloaded XML (CELEX {celex}): {filepath}")
                        return True
                    else:
                        logger.warning(f"Response for {celex} does not look like XML.")
                        return False
                
                logger.warning(f"Failed to download CELEX {celex}: {response.status}")
                return False
        except Exception as e:
            logger.error(f"Download error for CELEX {celex}: {e}")
            return False

    async def run(self, start_date: str = "2024-01-01", limit: int = 100):
        """
        Main execution with date filtering.
        """
        logger.info(f"Starting Async EUR-Lex Client (Start Date: {start_date})...")
        
        # Override query date filter dynamically
        # Simple string injection for now, could be improved
        original_fetch = self.fetch_legislation_metadata
        
        async def fetch_with_date(limit_val):
            query = f"""
            SELECT DISTINCT ?work ?title ?date ?celex
            WHERE {{
                ?work <http://publications.europa.eu/ontology/cdm#work_date_document> ?date .
                FILTER(STR(?date) >= "{start_date}")
                ?work <http://publications.europa.eu/ontology/cdm#resource_legal_type> ?type .
                FILTER(STR(?type) = "R" || STR(?type) = "L" || STR(?type) = "D")
                ?work <http://publications.europa.eu/ontology/cdm#resource_legal_id_celex> ?celex .
                OPTIONAL {{ 
                    ?work <http://publications.europa.eu/ontology/cdm#work_title> ?title .
                    FILTER(LANG(?title) = "it" || LANG(?title) = "")
                }}
            }}
            LIMIT {limit_val}
            """
            async with aiohttp.ClientSession() as session:
                try:
                    headers = {
                        "Accept": "application/sparql-results+json",
                        "User-Agent": "Mozilla/5.0 (compatible; LegalGraphRAG/1.0)"
                    }
                    async with session.get(EURLEX_SPARQL_ENDPOINT, params={"query": query}, headers=headers) as response:
                        if response.status != 200:
                            logger.error(f"Query failed: {response.status}")
                            return []
                        data = await response.json()
                        results = []
                        for binding in data.get("results", {}).get("bindings", []):
                            results.append({
                                "uri": binding.get("work", {}).get("value"),
                                "celex": binding.get("celex", {}).get("value"),
                                "title": binding.get("title", {}).get("value", "No Title"),
                                "date": binding.get("date", {}).get("value", "N/A")
                            })
                        return results
                except Exception as e:
                    logger.error(f"Error fetching data: {e}")
                    return []

        # Use the dynamic fetch
        metadata = await fetch_with_date(limit)
        
        logger.info(f"[DEBUG] Metadata fetch result: {len(metadata) if metadata else 0} items")

        if not metadata:
            logger.info("No documents found for the given criteria.")
            return

        logger.info(f"Found {len(metadata)} documents. Starting download...")
        
        async with aiohttp.ClientSession() as session:
            tasks = [self.download_document(session, item['celex'], item['title'], item['date']) for item in metadata]
            results = await asyncio.gather(*tasks)
            logger.info(f"Batch completed. Downloaded {sum(results)}/{len(tasks)}.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = AsyncEurLexClient()
    # Example usage
    asyncio.run(client.run(start_date="2024-01-01", limit=5))
