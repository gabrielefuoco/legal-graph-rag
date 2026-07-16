import logging

def setup_logging():
    """Configura il logging strutturato per l'intera applicazione."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)-30s | %(levelname)-5s | %(message)s",
        datefmt="%H:%M:%S"
    )
    
    # Silenzia i warning di Neo4j per proprietà/relazioni inesistenti
    logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)
    
    # Silenzia httpx (log ogni singola chiamata HTTP a Ollama)
    logging.getLogger("httpx").setLevel(logging.WARNING)
