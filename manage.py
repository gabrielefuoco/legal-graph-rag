import sys
import os
import logging
import asyncio
import argparse
from src.ingestion.async_normattiva_client import AsyncNormattivaClient
from src.ingestion.async_teseo_client import AsyncTeseoClient
from src.ingestion.async_senato_scraper import AsyncSenatoScraper
from src.ingestion.async_camera_client import AsyncCameraClient
from src.ingestion.async_eurlex_client import AsyncEurLexClient
from src.ingestion.async_corte_cost_client import AsyncCorteCostClient

from src.parsing.transformers import enrich_and_load_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def run_pipeline(start_date: str = None, limit: int = 100):
    """
    Runs the full ingestion pipeline asynchronously.
    """
    logger.info(f"Starting Full Ingestion Pipeline (Start Date: {start_date}, Limit: {limit})...")

    # 1. TESEO (Async)
    logger.info("--- Step 1: TESEO Thesaurus ---")
    try:
        await AsyncTeseoClient().run()
    except Exception as e:
        logger.error(f"TESEO ingestion failed: {e}")

    # 2. Senato (Async)
    logger.info("--- Step 2: Senato Ingestion ---")
    try:
        await AsyncSenatoScraper().run()
    except Exception as e:
        logger.error(f"Senato ingestion failed: {e}")

    # 3. Camera (Async)
    logger.info("--- Step 3: Camera dei Deputati ---")
    try:
        camera_client = AsyncCameraClient()
        await camera_client.run() 
    except Exception as e:
        logger.error(f"Camera ingestion failed: {e}")

    # 4. EUR-Lex (Async)
    logger.info("--- Step 4: EUR-Lex ---")
    try:
        eurlex_client = AsyncEurLexClient()
        s_date = start_date if start_date else "2024-01-01"
        await eurlex_client.run(start_date=s_date, limit=limit)
    except Exception as e:
        logger.error(f"EUR-Lex ingestion failed: {e}")

    # 5. Normattiva (Async)
    logger.info("--- Step 5: Normattiva API ---")
    try:
        normattiva_client = AsyncNormattivaClient()
        await normattiva_client.run(date=start_date)
    except Exception as e:
        logger.error(f"Normattiva ingestion failed: {e}")

    # 6. Corte Costituzionale (Async)
    logger.info("--- Step 6: Corte Costituzionale ---")
    try:
        await AsyncCorteCostClient().run()
    except Exception as e:
        logger.error(f"Corte Costituzionale ingestion failed: {e}")

def main():
    parser = argparse.ArgumentParser(description="Legal GraphRAG Management CLI.")
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")

    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Run the ingestion pipeline.")
    ingest_parser.add_argument("--start-date", type=str, default=None, help="Start date (YYYY-MM-DD)")
    ingest_parser.add_argument("--limit", type=int, default=100, help="Limit items per source")

    # Enrich and Load command (Phase 3)
    enrich_parser = subparsers.add_parser("enrich-and-load", help="Enrich DTOs and load into Neo4j.")
    enrich_parser.add_argument("--input", type=str, required=True, help="Path to input JSONL file (DocumentDTOs)")
    enrich_parser.add_argument("--teseo-rdf", type=str, required=True, help="Path to TESEO RDF file")

    # Parse Raw command
    parse_parser = subparsers.add_parser("parse-raw", help="Parse raw XML files into DocumentDTO JSONL.")
    parse_parser.add_argument("--dir", type=str, required=True, help="Directory containing XML files")
    parse_parser.add_argument("--output", type=str, required=True, help="Output JSONL file path")
    parse_parser.add_argument("--limit", type=int, default=None, help="Limit number of files to parse")

    args = parser.parse_args()

    if args.command == "ingest":
        asyncio.run(run_pipeline(start_date=args.start_date, limit=args.limit))
    elif args.command == "enrich-and-load":
        asyncio.run(enrich_and_load_pipeline(input_jsonl=args.input, teseo_rdf=args.teseo_rdf))
    elif args.command == "parse-raw":
        from src.parsing.parser import AknParser
        from pathlib import Path
        
        parser_inst = AknParser()
        xml_files = list(Path(args.dir).rglob("*.xml"))
        if args.limit:
            xml_files = xml_files[:args.limit]
            
        logger.info(f"Parsing {len(xml_files)} files from {args.dir}...")
        
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            for xml_file in xml_files:
                try:
                    doc = parser_inst.parse_file(str(xml_file))
                    f.write(doc.model_dump_json() + "\n")
                except Exception as e:
                    logger.error(f"Failed to parse {xml_file}: {e}")
        logger.info(f"Done. Processed metadata saved to {args.output}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
