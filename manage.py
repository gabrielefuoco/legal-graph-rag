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
    """Runs the full ingestion pipeline asynchronously."""
    logger.info(f"Starting Full Ingestion Pipeline (Start Date: {start_date}, Limit: {limit})...")

    try:
        await AsyncTeseoClient().run()
        await AsyncSenatoScraper().run()
        await AsyncCameraClient().run() 
        
        eurlex_client = AsyncEurLexClient()
        s_date = start_date if start_date else "2024-01-01"
        await eurlex_client.run(start_date=s_date, limit=limit)
        
        await AsyncNormattivaClient().run(date=start_date)
        await AsyncCorteCostClient().run()
    except Exception as e:
        logger.error(f"Pipeline error: {e}")

async def run_targeted_ingest(source: str, params: dict):
    """Esegue un'ingestione mirata (es. per parola chiave o anno) su una specifica fonte."""
    logger.info(f"Targeted Ingestion started for {source} with params: {params}")
    
    if source == "normattiva":
        client = AsyncNormattivaClient()
        token = await client.search_async(params)
        if token:
            return await client.wait_and_download(token)
    elif source == "eurlex":
        client = AsyncEurLexClient()
        await client.run(limit=params.get("limit", 5))
    return None

async def run_parse_and_load(raw_dir: str, output_jsonl: str, teseo_rdf: str, limit: int = None):
    """Orchestra il parsing di file XML e il caricamento in Neo4j."""
    from src.parsing.parser import AknParser
    from pathlib import Path
    
    parser_inst = AknParser()
    xml_files = list(Path(raw_dir).rglob("*.xml"))
    if limit: xml_files = xml_files[:limit]
        
    logger.info(f"Parsing {len(xml_files)} files...")
    os.makedirs(os.path.dirname(output_jsonl), exist_ok=True)
    
    success = 0
    with open(output_jsonl, "w", encoding="utf-8") as f:
        for xml_file in xml_files:
            try:
                doc = parser_inst.parse_file(str(xml_file))
                f.write(doc.model_dump_json() + "\n")
                success += 1
            except: pass
                
    if success > 0:
        await enrich_and_load_pipeline(input_jsonl=output_jsonl, teseo_rdf=teseo_rdf)

async def run_topic_pipeline(topic: str):
    logger.info(f"Starting Holistic Topic-Based Pipeline for topic: '{topic}'")
    
    base_raw = Path("data/raw")
    senato_repo = base_raw / "senato" / "AkomaNtosoBulkData"
    normattiva_dir = base_raw / "normattiva"
    cortecost_dir = base_raw / "cortecost"
    
    logger.info("--- PHASE 1: Fetch & Update ---")
    
    # 1.1 Senato Bulk Data (Git Pull)
    if senato_repo.exists() and (senato_repo / ".git").exists():
        import subprocess
        try:
            logger.info("Updating Senato Bulk Data via git pull...")
            subprocess.run(["git", "-C", str(senato_repo), "pull"], check=True, capture_output=True)
            logger.info("Senato Bulk Data updated.")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to update Senato repo: {e}")
    else:
        logger.warning(f"Senato repo not found at {senato_repo}. Please clone it first.")
        
    # 1.2 Fetch Data
    logger.info(f"Fetching Normattiva for '{topic}'...")
    norm_client = AsyncNormattivaClient(output_dir=str(normattiva_dir))
    token = await norm_client.search_async({"testo": topic})
    if token:
        await norm_client.wait_and_download(token)
    
    logger.info(f"Fetching Senato IDs for '{topic}'...")
    senato_scraper = AsyncSenatoScraper()
    senato_ids = await senato_scraper.fetch_ids_by_topic(topic)
    
    logger.info(f"Fetching Camera Iter Legis for '{topic}'...")
    camera_client = AsyncCameraClient(output_dir=str(camera_dir))
    await camera_client.run(keyword=topic)
    
    logger.info("Fetching recent Corte Costituzionale judgements...")
    corte_client = AsyncCorteCostClient(output_dir=str(cortecost_dir))
    await corte_client.run()
    
    logger.info("--- PHASE 2 & 3: Parse & Unify & Load ---")
    output_jsonl = "data/processed/knowledge_graph.jsonl"
    teseo_rdf = "data/external/teseo_sample.rdf"
    
    from src.parsing.parser import AknParser
    from src.parsing.transformers import transform_cortecost_to_judgements, transform_camera_to_iter_legis
    from pathlib import Path
    import os
    
    parser_inst = AknParser()
    os.makedirs(os.path.dirname(output_jsonl), exist_ok=True)
    
    success = 0
    with open(output_jsonl, "w", encoding="utf-8") as f:
        # Normattiva
        norm_files = list(normattiva_dir.rglob("*.xml"))
        for xml_file in norm_files:
            try:
                doc = parser_inst.parse_file(str(xml_file))
                f.write(doc.model_dump_json() + "\n")
                success += 1
            except: pass
            
        # Senato
        if senato_ids:
            senato_files = list(senato_repo.rglob("*.xml"))
            for xml_file in senato_files:
                # Euristic filter: does filename contain the bill number?
                if any(f"_{sid}_" in xml_file.name or xml_file.name.endswith(f"{sid}.xml") for sid in senato_ids):
                    try:
                        doc = parser_inst.parse_file(str(xml_file))
                        f.write(doc.model_dump_json() + "\n")
                        success += 1
                    except: pass
                    
        # Corte Costituzionale & Camera Iter Legis
        judgements = transform_cortecost_to_judgements(str(cortecost_dir))
        camera_jsonl = camera_dir / "camera_acts_metadata.jsonl"
        iter_legis = transform_camera_to_iter_legis(str(camera_jsonl)) if camera_jsonl.exists() else []
        
        if judgements or iter_legis:
            from src.parsing.models import DocumentDTO, FrbrMetadataDTO
            dummy_frbr = FrbrMetadataDTO(urn="urn:dummy:meta", title="Metadati Aggiuntivi", date_promulgation="2024-01-01", country="IT")
            dummy_doc = DocumentDTO(frbr=dummy_frbr, nodes=[], edges=[], judgements=judgements, iter_legis=iter_legis)
            f.write(dummy_doc.model_dump_json() + "\n")
            success += 1

    if success > 0:
        logger.info(f"Generated {success} DTO records. Proceeding to Semantic Enrichment & Load.")
        await enrich_and_load_pipeline(input_jsonl=output_jsonl, teseo_rdf=teseo_rdf)

async def run_docker(action: str = "up"):
    """Gestisce i container Docker."""
    import subprocess
    try:
        if action == "up":
            subprocess.run(["docker", "compose", "up", "-d", "--build"], check=True)
        elif action == "down":
            subprocess.run(["docker", "compose", "down"], check=True)
    except Exception as e:
        logger.error(f"Docker error: {e}")

def check_health():
    """Verifica lo stato dei servizi."""
    import requests
    from neo4j import GraphDatabase
    from src.config import settings
    status = {"neo4j": "🔴 Offline", "ollama": "🔴 Offline"}
    try:
        driver = GraphDatabase.driver(settings.NEO4J_URI, auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD))
        with driver.session() as s: s.run("RETURN 1")
        status["neo4j"] = "🟢 Online"
    except: pass
    try:
        # Pulisce l'endpoint rimuovendo /api o /v1 se presenti
        base_url = settings.QWEN3_ENDPOINT.rstrip('/')
        if requests.get(f"{base_url}/api/tags", timeout=2).status_code == 200:
            status["ollama"] = "🟢 Online"
    except: pass
    return status

async def run_retrieve(query: str, reference_date: str = None, top_k: int = 10, final_k: int = 5, verbose: bool = False):
    """Esegue il retrieval e stampa i risultati."""
    from src.rag.engine import RagEngine
    engine = RagEngine()
    try:
        chunks = await engine.retrieve(query=query, reference_date=reference_date, top_k=top_k, final_k=final_k)
        print(f"\n--- Risultati per: {query} ---")
        for i, c in enumerate(chunks, 1):
            print(f"[{i}] {c.score:.4f} | {c.source} | {c.metadata.get('work_title', 'N/A')}")
            if verbose: print(f"Text: {c.text[:200]}...")
    finally:
        await engine.close()

def main():
    parser = argparse.ArgumentParser(description="Legal GraphRAG CLI")
    subparsers = parser.add_subparsers(dest="command")
    
    b_graph = subparsers.add_parser("build-graph")
    b_graph.add_argument("--topic", required=True)
    
    subparsers.add_parser("ingest")
    target = subparsers.add_parser("target-ingest")
    target.add_argument("--source", required=True)
    target.add_argument("--testo")
    target.add_argument("--anno", type=int)
    
    subparsers.add_parser("docker-up")
    subparsers.add_parser("docker-down")
    subparsers.add_parser("status")
    subparsers.add_parser("streamlit")
    
    ret = subparsers.add_parser("retrieve")
    ret.add_argument("--query", required=True)
    
    args = parser.parse_args()
    if args.command == "build-graph": asyncio.run(run_topic_pipeline(args.topic))
    elif args.command == "ingest": asyncio.run(run_pipeline())
    elif args.command == "target-ingest":
        p = {"testo": args.testo, "annoProvvedimento": args.anno}
        asyncio.run(run_targeted_ingest(args.source, p))
    elif args.command == "docker-up": asyncio.run(run_docker("up"))
    elif args.command == "docker-down": asyncio.run(run_docker("down"))
    elif args.command == "status":
        s = check_health()
        print(f"Neo4j: {s['neo4j']} | Ollama: {s['ollama']}")
    elif args.command == "streamlit":
        import subprocess
        subprocess.run(["streamlit", "run", "src/ui/app.py"])
    elif args.command == "retrieve":
        asyncio.run(run_retrieve(args.query))

if __name__ == "__main__":
    main()
