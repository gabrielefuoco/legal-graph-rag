import requests
import os
import logging
from rdflib import Graph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TeseoDownloader:
    # TESEO is accessible via SPARQL or specific RDF dumps
    # Using a generic SPARQL CONSTRUCT query approach if specific dump URL is missing
    SPARQL_ENDPOINT = "http://dati.senato.it/sparql"
    
    def __init__(self, output_dir="data/external"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def download_teseo_rdf(self):
        """
        Attempts to download the full classification scheme.
        """
        # Note: A full dump might be heavy via SPARQL. Preferable to find the dump file.
        # Fallback: Query for top concepts
        query = """
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        CONSTRUCT {
            ?s ?p ?o
        } WHERE {
            ?s a skos:Concept .
            ?s ?p ?o .
        } LIMIT 1000
        """
        # In a real scenario, we would need pagination or a proper dump URL.
        
        logger.info("Querying TESEO endpoint...")
        try:
            # Check if we can just GET the endpoint with a 'query' param
            response = requests.get(self.SPARQL_ENDPOINT, params={'query': query, 'format': 'application/rdf+xml'})
            response.raise_for_status()
            
            filepath = os.path.join(self.output_dir, "teseo_sample.rdf")
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded TESEO sample to {filepath}")
            
            # Validation
            g = Graph()
            g.parse(filepath, format="xml")
            logger.info(f"Parsed {len(g)} triples successfully.")
            
        except Exception as e:
            logger.error(f"Failed to download TESEO data: {e}")

if __name__ == "__main__":
    downloader = TeseoDownloader()
    downloader.download_teseo_rdf()
