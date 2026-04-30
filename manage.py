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


async def run_retrieve(
    query: str, 
    reference_date: str = None, 
    top_k: int = 10, 
    final_k: int = 5,
    full_text: bool = False,
    verbose: bool = False
):
    """
    Esegue il retrieval ibrido e stampa i risultati in modo leggibile.
    """
    from src.rag.engine import RagEngine
    
    # Silenzia i log rumorosi durante il retrieval per un output pulito
    logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    logger.info(f"Esecuzione Retrieval per: '{query}'...")

    engine = RagEngine()
    try:
        chunks = await engine.retrieve(
            query=query, 
            reference_date=reference_date,
            top_k=top_k,
            final_k=final_k
        )

        print("\n" + "="*60)
        print(f"  RISULTATI RETRIEVAL IBRIDO ({len(chunks)} chunk)")
        print("="*60 + "\n")

        for i, chunk in enumerate(chunks, 1):
            source_label = chunk.source.upper()
            print(f"  [{i}] Score: {chunk.score:.4f} | Fonte: {source_label}")
            # Visualizzazione URN e Titolo
            urn_display = chunk.work_urn or "urn:unknown"
            work_title = chunk.metadata.get("work_title")
            
            # Fallback URN se unknown: cerchiamo nel testo
            if urn_display == "urn:unknown":
                import re
                urn_match = re.search(r"urn:nir:[a-z0-9\.\-;~:]+", chunk.text, re.IGNORECASE)
                if urn_match:
                    urn_display = urn_match.group(0) + " (estratto da testo)"

            # Stampa Titolo (se disponibile) e URN (solo se non sconosciuto)
            if work_title:
                print(f"      Atto: {work_title}")
            if urn_display != "urn:unknown":
                print(f"      URN: {urn_display}")

            if verbose:
                # Mostra motivazioni specifiche basate sui metadati
                matched = chunk.metadata.get("matched_concepts")
                if matched:
                    # Rendiamo gli URI più leggibili (prendiamo l'ultima parte)
                    short_concepts = [c.split('/')[-1].split('#')[-1] for c in matched]
                    concepts_str = ", ".join(short_concepts)
                    print(f"      Motivo: Match concetti TESEO [{concepts_str}]")
                
                if "+" in chunk.source: # Fusione (es. BM25+GRAPH)
                    print(f"      Motivo: Risultato trovato in più canali di ricerca")
                
                # Debug ID se necessario
                if chunk.expression_id:
                    print(f"      ID Nodo: {chunk.expression_id}")

            # Pulizia testo per terminale (prevenzione bug visivi di wrap stringhe lunghe)
            import re
            clean_text = re.sub(r'\s+', ' ', chunk.text).strip()
            
            # Gestione troncamento
            if not full_text:
                limit = 1500
                if len(clean_text) > limit:
                    clean_text = clean_text[:limit] + "..."
            
            # Avvolgimento riga per terminale per evitare artefatti visivi
            import textwrap
            wrapped_text = textwrap.fill(clean_text, width=120)
            
            # Stampiamo con un prefisso per ogni riga in modo che sia allineato
            print("      Testo:")
            for line in wrapped_text.split('\n'):
                print(f"        {line}")
            
            print("-" * 40)

        if not chunks:
            print("  Nessun risultato trovato.\n")
    finally:
        await engine.close()

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

    # Retrieve command (RAG)
    retrieve_parser = subparsers.add_parser("retrieve", help="Run hybrid retrieval on the knowledge graph.")
    retrieve_parser.add_argument("--query", type=str, required=True, help="Query in linguaggio naturale")
    retrieve_parser.add_argument("--date", type=str, default=None, help="Data di riferimento (YYYY-MM-DD) per filtro vigenza")
    retrieve_parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Numero di risultati da estrarre PER CANALE (default: 10)"
    )
    retrieve_parser.add_argument(
        "--final-k",
        type=int,
        default=5,
        help="Numero di risultati finali da mostrare post-fusione (default: 5)"
    )
    retrieve_parser.add_argument("--full-text", action="store_true", help="Mostra il testo completo delle norme")
    retrieve_parser.add_argument("--verbose", action="store_true", help="Mostra motivi del recupero e metadati")

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
    elif args.command == "retrieve":
        asyncio.run(run_retrieve(
            query=args.query, 
            reference_date=args.date, 
            top_k=args.top_k, 
            final_k=args.final_k,
            full_text=args.full_text,
            verbose=args.verbose
        ))
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
