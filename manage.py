import sys
import os
import logging
from src.ingestion.teseo_downloader import TeseoDownloader
from src.ingestion.normattiva_api import NormattivaClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_all():
    logger.info("Starting Full Ingestion Pipeline...")
    
    # 1. TESEO
    logger.info("--- Step 1: TESEO Thesaurus ---")
    teseo = TeseoDownloader()
    teseo.download_teseo_rdf()
    
    # 2. Senato Bulk (Clone handled manually)
    logger.info("--- Step 2: Senato Bulk Data ---")
    logger.info("Skipping download: User taking care of cloning repository to data/raw/")

    # 3. Normattiva
    logger.info("--- Step 3: Normattiva API ---")
    normattiva = NormattivaClient()
    logger.info("Normattiva client ready (Async search requires valid parameters)")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "ingest":
        ingest_all()
    else:
        print("Usage: python manage.py ingest")
