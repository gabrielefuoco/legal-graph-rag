"""Logging configuration for the application."""
import logging

def setup_logging():
    """Configure structured logging for the entire application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)-30s | %(levelname)-5s | %(message)s",
        datefmt="%H:%M:%S"
    )
    
    # Suppress Neo4j notifications for non-existent properties or relationships
    logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)
    
    # Suppress verbose HTTP logging from httpx
    logging.getLogger("httpx").setLevel(logging.WARNING)

