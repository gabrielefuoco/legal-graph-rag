import asyncio
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Optional, List, AsyncGenerator

from lxml import etree

from src.parsing.models import (
    IterLegisStepDTO, 
    JudgementDTO, 
    DocumentDTO, 
    GraphNodeDTO, 
    GraphEdgeDTO, 
    EdgeType
)
from src.parsing.teseo_matcher import TESEOMatcher
from src.parsing.vector_engine import VectorEngine
from src.ingestion.neo4j_loader import AsyncNeo4jLoader

logger = logging.getLogger(__name__)

# BATCH_SIZE for Neo4j and Embedding inference
BATCH_SIZE = 128


# ---------------------------------------------------------------------------
# Camera → IterLegisStepDTO
# ---------------------------------------------------------------------------

# Keywords for classifying step types from titles
_STEP_TYPE_PATTERNS = {
    "VOTE": re.compile(r"votazion|scrutinio|approvat|respint", re.IGNORECASE),
    "ASSIGNMENT": re.compile(r"assegna|commission|conferit", re.IGNORECASE),
    "AMENDMENT": re.compile(r"emendament|modific", re.IGNORECASE),
    "DISCUSSION": re.compile(r"discuss|dibattit|esame", re.IGNORECASE),
}


def _classify_step_type(title: str) -> str:
    """Classify an iter legis step type from its title using keyword matching."""
    for step_type, pattern in _STEP_TYPE_PATTERNS.items():
        if pattern.search(title):
            return step_type
    return "DDL_PRESENTATION"


def _generate_step_id(uri: str, date: str) -> str:
    """Generate a deterministic ID for an iter legis step."""
    composite = f"{uri}#{date}"
    return hashlib.sha256(composite.encode("utf-8")).hexdigest()[:16]


def _uri_to_work_urn(uri: str) -> str:
    """Convert a Camera/Senato URI to a work URN for matching."""
    # Example: http://dati.camera.it/ocd/atto/123 → urn:camera:atto:123
    if "dati.camera.it" in uri:
        parts = uri.rstrip("/").split("/")
        # Take the last 2 meaningful segments (atto/number)
        if len(parts) >= 2:
            return f"urn:camera:{':'.join(parts[-2:])}"
    if "dati.senato.it" in uri:
        parts = uri.rstrip("/").split("/")
        if len(parts) >= 2:
            return f"urn:senato:{':'.join(parts[-2:])}"
    return uri


def transform_camera_to_iter_legis(jsonl_path: str) -> list[IterLegisStepDTO]:
    """
    Transform Camera dei Deputati JSONL data into IterLegisStepDTO objects.

    Reads the JSONL file produced by AsyncCameraClient.save_metadata()
    and maps each record to a validated DTO.

    Args:
        jsonl_path: Path to the JSONL file.

    Returns:
        List of validated IterLegisStepDTO objects.
    """
    path = Path(jsonl_path)
    if not path.exists():
        logger.warning(f"Camera JSONL file not found: {jsonl_path}")
        return []

    results = []
    line_count = 0

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            line_count += 1

            try:
                record = json.loads(line)
                uri = record.get("uri", "")
                date_str = record.get("date", "")
                title = record.get("title", "")
                authority = record.get("authority", "Camera dei Deputati")

                step = IterLegisStepDTO(
                    id=_generate_step_id(uri, date_str),
                    date=date_str,
                    description=title,
                    step_type=_classify_step_type(title),
                    authority=authority,
                    related_work_urn=_uri_to_work_urn(uri),
                )
                results.append(step)

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"Skipping malformed JSONL line {line_count}: {e}")

    logger.info(f"Transformed {len(results)}/{line_count} Camera records to IterLegisStepDTO")
    return results


# ---------------------------------------------------------------------------
# Corte Costituzionale → JudgementDTO (Scaffold with Graceful Fallback)
# ---------------------------------------------------------------------------

# Expected XML structure (best-effort, will be refined with real data)
_PRONOUNCEMENT_TAGS = {"sentenza", "ordinanza", "pronuncia", "decisione"}
_URN_PATTERN = re.compile(r"urn:nir:[^\s<\"']+", re.IGNORECASE)


def transform_cortecost_to_judgements(xml_dir: str) -> list[JudgementDTO]:
    """
    Transform Corte Costituzionale XML datasets into JudgementDTO objects.

    **Scaffold implementation**: attempts extraction with graceful fallback.
    If the XML structure doesn't match expectations, logs a warning and
    returns an empty list instead of raising exceptions.

    Args:
        xml_dir: Path to the directory containing downloaded XML files.

    Returns:
        List of JudgementDTO objects (may be empty if format is unexpected).
    """
    dir_path = Path(xml_dir)
    if not dir_path.exists() or not dir_path.is_dir():
        logger.warning(f"Corte Cost. directory not found: {xml_dir}")
        return []

    xml_files = list(dir_path.glob("*.xml"))
    if not xml_files:
        logger.info(f"No XML files found in {xml_dir}")
        return []

    results = []

    for xml_file in xml_files:
        try:
            judgements = _parse_cortecost_xml(xml_file)
            results.extend(judgements)
        except Exception as e:
            logger.warning(
                f"Could not parse Corte Cost. file {xml_file.name}: {e}. "
                f"This is expected if the XML format differs from the scaffold. "
                f"The file will be skipped."
            )

    logger.info(f"Transformed {len(results)} Corte Cost. records to JudgementDTO")
    return results


def _parse_cortecost_xml(filepath: Path) -> list[JudgementDTO]:
    """
    Parse a single Corte Costituzionale XML file.

    Best-effort extraction:
    1. Tries to find pronouncement elements
    2. Extracts date, type, description
    3. Searches for URN references to affected norms
    """
    parser = etree.XMLParser(recover=True, encoding="utf-8")

    try:
        tree = etree.parse(str(filepath), parser)
    except Exception:
        # Try latin-1 fallback
        try:
            content = filepath.read_bytes()
            text = content.decode("latin-1")
            tree = etree.fromstring(text.encode("utf-8"), parser)
            tree = tree.getroottree() if hasattr(tree, "getroottree") else tree
        except Exception as e:
            raise ValueError(f"Cannot parse XML: {e}") from e

    root = tree.getroot() if hasattr(tree, "getroot") else tree

    results = []

    # Strategy 1: Look for pronouncement-like elements
    for element in root.iter():
        tag_name = element.tag
        if isinstance(tag_name, str):
            local = tag_name.split("}")[-1].lower() if "}" in tag_name else tag_name.lower()
            if local in _PRONOUNCEMENT_TAGS:
                judgement = _extract_judgement(element, filepath.stem)
                if judgement:
                    results.append(judgement)

    # Strategy 2: If no structured pronouncements found, try extracting from root
    if not results:
        judgement = _extract_judgement(root, filepath.stem)
        if judgement:
            results.append(judgement)

    return results


def _extract_judgement(element: etree._Element, fallback_id: str) -> Optional[JudgementDTO]:
    """
    Extract a JudgementDTO from an XML element.

    Best-effort: returns None if the element doesn't contain enough data.
    """
    # Extract date
    date_str = element.get("date", element.get("data", ""))
    if not date_str:
        # Search in child elements
        for child in element:
            tag_local = child.tag.split("}")[-1].lower() if isinstance(child.tag, str) and "}" in child.tag else (child.tag.lower() if isinstance(child.tag, str) else "")
            if tag_local in ("data", "date"):
                date_str = child.text or child.get("valore", "")
                break

    if not date_str:
        return None  # non posso creare un giudizio senza data

    # Extract type
    tag_name = element.tag
    local_tag = tag_name.split("}")[-1].lower() if isinstance(tag_name, str) and "}" in tag_name else (tag_name.lower() if isinstance(tag_name, str) else "")
    judgement_type = "Sentenza" if "sentenza" in local_tag else "Ordinanza" if "ordinanza" in local_tag else "Pronuncia"

   
    full_text = "".join(element.itertext()).strip()
    description = full_text[:500] if full_text else ""

    affected_urns = list(set(_URN_PATTERN.findall(full_text)))

    # Generate ID
    jid = hashlib.sha256(f"cortecost:{fallback_id}:{date_str}".encode()).hexdigest()[:16]

    return JudgementDTO(
        id=jid,
        date=date_str,
        description=description,
        court="Corte Costituzionale",
        judgement_type=judgement_type,
        judgement_result=None,  # Would need more structured data to extract
        affected_urns=affected_urns,
    )


# ---------------------------------------------------------------------------
# Document Enrichment
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Phase 3: Streaming, Enrichment & Neo4j Loading
# ---------------------------------------------------------------------------

async def read_jsonl_stream(filepath: str) -> AsyncGenerator[DocumentDTO, None]:
    """
    Asynchronous generator that yields DocumentDTO objects from a JSONL file.
    Memory efficient for large datasets.
    """
    path = Path(filepath)
    if not path.exists():
        logger.error(f"JSONL file not found for streaming: {filepath}")
        return

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                yield DocumentDTO(**data)
            except Exception as e:
                logger.warning(f"Skipping invalid JSONL line: {e}")


async def process_batch(
    docs_batch: List[DocumentDTO], 
    vector_engine: VectorEngine, 
    teseo_matcher: TESEOMatcher,
    neo4j_loader: AsyncNeo4jLoader
):
    """
    Processes a batch of documents: enrich with vectors and topics, then load to Neo4j.
    """
    all_nodes = []
    all_edges = []
    
    # collezioniamo i nodi EXPRESSION per l'arricchimento vettoriale
    expression_nodes: List[GraphNodeDTO] = []
    payloads: List[str] = []
    
    for doc in docs_batch:
        all_nodes.append({
            "type": "WORK",
            "urn": doc.frbr.urn,
            "title": doc.frbr.title,
            "date": doc.frbr.date_promulgation,
            "source": doc.frbr.country  # Usiamo il paese come fonte per ora o "Normattiva"
        })

        # 2. Estraggo i nodi EXPRESSION per l'arricchimento
        for node in doc.nodes:
            if node.type.value == "EXPRESSION":
                expression_nodes.append(node)
                # Costruisco il payload per l'arricchimento vettoriale
                payload = vector_engine.build_vector_payload(node, doc.frbr.title)
                payloads.append(payload)
                
                # 3. Arricchimento Semantico (TESEO) - eseguito in modo sincrono per nodo (veloce)
                topics = teseo_matcher.extract_topics(node.text_display or "")
                for topic in topics:
                    all_edges.append({
                        "type": EdgeType.HAS_TOPIC.value,
                        "expression_id": node.id,
                        "teseo_id": topic["teseo_id"],
                        "score": topic["score"]
                    })
            
            # Aggiungo il nodo alla lista di persistenza (dopo il potenziale arricchimento)
            node_dict = node.model_dump()
            node_dict["work_urn"] = doc.frbr.urn # Injection per le query di linking
            node_dict["vigenza_start"] = doc.frbr.vigenza_start
            node_dict["vigenza_end"] = doc.frbr.vigenza_end
            all_nodes.append(node_dict)

        # 4. Aggiungo gli archi strutturali esistenti
        for edge in doc.edges:
            all_edges.append(edge.model_dump())

    # 5. Inferenza Vettoriale in Batch (Parallel)
    if expression_nodes:
        logger.info(f"Computing embeddings for {len(expression_nodes)} nodes...")
        try:
            embeddings = await vector_engine.compute_embeddings_batch(payloads)
            
            # Inietto gli embeddings nei dizionari all_nodes
            expr_id_to_vector = {
                expression_nodes[i].id: embeddings[i] 
                for i in range(len(expression_nodes))
            }
            
            for n_dict in all_nodes:
                if n_dict.get("id") in expr_id_to_vector:
                    n_dict["embedding"] = expr_id_to_vector[n_dict["id"]]
        except Exception as e:
            logger.error(f"Embedding failed for a batch. Nodes will be loaded without vectors. Error: {e}")

    # 6. Persistence to Neo4j
    logger.info(f"Loading batch of {len(docs_batch)} docs to Neo4j ({len(all_nodes)} nodes, {len(all_edges)} edges)...")
    await neo4j_loader.load_batch(all_nodes, all_edges)


async def enrich_and_load_pipeline(input_jsonl: str, teseo_rdf: str):
    """
    Main entry point for Phase 3 pipeline.
    Streams, enriches and loads.
    """
    vector_engine = VectorEngine()
    teseo_matcher = TESEOMatcher(teseo_rdf)
    neo4j_loader = AsyncNeo4jLoader()
    
    await neo4j_loader.setup_schema()
    
    try:
        current_batch = []
        async for doc in read_jsonl_stream(input_jsonl):
            current_batch.append(doc)
            if len(current_batch) >= BATCH_SIZE:
                await process_batch(current_batch, vector_engine, teseo_matcher, neo4j_loader)
                current_batch = []
        
        # Process remaining
        if current_batch:
            await process_batch(current_batch, vector_engine, teseo_matcher, neo4j_loader)
            
    finally:
        await neo4j_loader.close()
        logger.info("Pipeline completed.")
