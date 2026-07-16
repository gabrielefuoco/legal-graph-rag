import logging
from neo4j import AsyncGraphDatabase
from src.config import settings

logger = logging.getLogger(__name__)

class AsyncNeo4jLoader:
    """
    Handles asynchronous batch loading of legal documents into Neo4j.
    Implements schema setup and idempotent writing strategies.
    """

    def __init__(self):
        """Initialize the Neo4j driver using global settings."""
        self.driver = AsyncGraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
        )

    async def close(self):
        """Close the driver resources."""
        await self.driver.close()

    async def setup_schema(self):
        """
        Creates mandatory unique constraints and indices.
        Required for performance and data integrity.
        """
        queries = [
            # Constraints
            "CREATE CONSTRAINT work_id_unique IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE",
            "CREATE CONSTRAINT expression_id_unique IF NOT EXISTS FOR (e:Expression) REQUIRE e.id IS UNIQUE",
            "CREATE CONSTRAINT structural_id_unique IF NOT EXISTS FOR (u:StructuralUnit) REQUIRE u.id IS UNIQUE",
            "CREATE CONSTRAINT teseo_id_unique IF NOT EXISTS FOR (t:TESEO_Concept) REQUIRE t.id IS UNIQUE",
            
            # Full-text Index for search
            """
            CREATE FULLTEXT INDEX expression_text_fulltext IF NOT EXISTS
            FOR (e:Expression)
            ON EACH [e.text_display]
            """,
            
            # Vector Index for RAG
            f"""
            CREATE VECTOR INDEX expression_embedding_vector IF NOT EXISTS
            FOR (e:Expression)
            ON (e.embedding)
            OPTIONS {{
                indexConfig: {{
                    `vector.dimensions`: {settings.EMBEDDING_DIMENSIONS},
                    `vector.similarity_function`: 'cosine'
                }}
            }}
            """
        ]
        
        async with self.driver.session() as session:
            for query in queries:
                try:
                    await session.run(query)
                except Exception as e:
                    logger.error(f"Error executing schema query: {query}. Error: {e}")
                    raise

    async def load_teseo_ontology(self, rdf_path: str):
        """Loads TESEO concepts and BROADER relations from RDF."""
        from rdflib import Graph, URIRef, Literal
        from rdflib.namespace import SKOS
        
        logger.info(f"Loading TESEO ontology to Neo4j from {rdf_path}...")
        g = Graph()
        fmt = "turtle" if rdf_path.endswith(".ttl") else "xml"
        try:
            g.parse(rdf_path, format=fmt)
        except Exception as e:
            logger.error(f"Failed to parse RDF for Neo4j: {e}")
            raise

        nodes_batch = []
        edges_batch = []
        
        import re
        def norm(t):
            t = t.lower()
            t = re.sub(r'[^\w\s]', '', t)
            return re.sub(r'\s+', ' ', t).strip()

        # Load nodes with prefLabel
        for s, p, o in g.triples((None, SKOS.prefLabel, None)):
            if isinstance(o, Literal) and (o.language == 'it' or not o.language):
                nodes_batch.append({
                    "id": str(s),
                    "prefLabel": str(o),
                    "normalizedLabel": norm(str(o))
                })
                
        # Load BROADER edges
        for s, p, o in g.triples((None, SKOS.broader, None)):
            edges_batch.append({
                "source_id": str(s),
                "target_id": str(o)
            })

        async def _execute_teseo(tx):
            nodes_query = """
            UNWIND $batch AS row
            MERGE (t:TESEO_Concept {id: row.id})
            SET t.prefLabel = row.prefLabel,
                t.normalizedLabel = row.normalizedLabel
            """
            await tx.run(nodes_query, batch=nodes_batch)
            
            edges_query = """
            UNWIND $batch AS row
            MATCH (child:TESEO_Concept {id: row.source_id})
            MATCH (parent:TESEO_Concept {id: row.target_id})
            MERGE (child)-[:BROADER]->(parent)
            """
            await tx.run(edges_query, batch=edges_batch)

        try:
            async with self.driver.session() as session:
                await session.execute_write(_execute_teseo)
                logger.info(f"Loaded {len(nodes_batch)} TESEO concepts and {len(edges_batch)} BROADER relations.")
        except Exception as e:
            logger.error(f"Error loading TESEO ontology: {e}")
            raise

    async def _load_works(self, tx, batch: list[dict]):
        """Batch load Work nodes."""
        query = """
        UNWIND $batch AS row
        MERGE (w:Work {id: row.urn})
        ON CREATE SET 
            w.urn = row.urn,
            w.title = row.title,
            w.date = date(row.date),
            w.source = row.source,
            w.created_at = datetime()
        ON MATCH SET
            w.updated_at = datetime()
        """
        await tx.run(query, batch=batch)

    async def _load_expressions(self, tx, batch: list[dict]):
        """Batch load Expression nodes with embeddings."""
        query = """
        UNWIND $batch AS row
        MATCH (w:Work {id: row.work_urn})
        MERGE (e:Expression {id: row.id})
        ON CREATE SET
            e.eId = row.eid,
            e.text_display = row.text_display,
            e.embedding = row.embedding,
            e.tag_name = row.tag_name,
            e.vigenza_start = date(row.vigenza_start),
            e.vigenza_end = date(row.vigenza_end),
            e.created_at = datetime()
        ON MATCH SET
            e.embedding = row.embedding,
            e.updated_at = datetime()
        
        // Link Expression to Work
        MERGE (e)-[:PART_OF]->(w)
        """
        await tx.run(query, batch=batch)

    async def _load_structural_units(self, tx, batch: list[dict]):
        """Batch load non-leaf Structural units (Book, Title, etc.)."""
        query = """
        UNWIND $batch AS row
        MATCH (w:Work {id: row.work_urn})
        MERGE (u:StructuralUnit {id: row.id})
        ON CREATE SET
            u.eId = row.eid,
            u.tag_name = row.tag_name,
            u.created_at = datetime()
        ON MATCH SET
            u.updated_at = datetime()
        
        // Link Structural Unit to Work
        MERGE (u)-[:PART_OF]->(w)
        """
        await tx.run(query, batch=batch)

    async def _load_judgements(self, tx, batch: list[dict]):
        """Batch load Judgement nodes."""
        query = """
        UNWIND $batch AS row
        MERGE (j:Judgement {id: row.id})
        ON CREATE SET
            j.date = date(row.date),
            j.description = row.description,
            j.court = row.court,
            j.judgement_type = row.judgement_type,
            j.judgement_result = row.judgement_result,
            j.created_at = datetime()
        ON MATCH SET
            j.updated_at = datetime()
        """
        await tx.run(query, batch=batch)

    async def _load_structural_edges(self, tx, batch: list[dict]):
        """Batch load internal structural edges (PART_OF, NEXT)."""
        # We use a broad match with UNION to ensure index seeks instead of label-less full DB scans
        alt_query_part_of = """
        UNWIND $batch AS row
        WITH row WHERE row.type = 'PART_OF'
        CALL {
            WITH row MATCH (s:Expression {id: row.source_id}) RETURN s
            UNION
            WITH row MATCH (s:StructuralUnit {id: row.source_id}) RETURN s
            UNION
            WITH row MATCH (s:Work {id: row.source_id}) RETURN s
        }
        CALL {
            WITH row MATCH (t:Expression {id: row.target_id}) RETURN t
            UNION
            WITH row MATCH (t:StructuralUnit {id: row.target_id}) RETURN t
            UNION
            WITH row MATCH (t:Work {id: row.target_id}) RETURN t
        }
        MERGE (s)-[:PART_OF]->(t)
        """
        alt_query_next = """
        UNWIND $batch AS row
        WITH row WHERE row.type = 'NEXT'
        CALL {
            WITH row MATCH (s:Expression {id: row.source_id}) RETURN s
            UNION
            WITH row MATCH (s:StructuralUnit {id: row.source_id}) RETURN s
            UNION
            WITH row MATCH (s:Work {id: row.source_id}) RETURN s
        }
        CALL {
            WITH row MATCH (t:Expression {id: row.target_id}) RETURN t
            UNION
            WITH row MATCH (t:StructuralUnit {id: row.target_id}) RETURN t
            UNION
            WITH row MATCH (t:Work {id: row.target_id}) RETURN t
        }
        MERGE (s)-[:NEXT]->(t)
        """
        await tx.run(alt_query_part_of, batch=batch)
        await tx.run(alt_query_next, batch=batch)

    async def _load_semantic_edges(self, tx, batch: list[dict]):
        """Batch load semantic edges (HAS_TOPIC)."""
        query = """
        UNWIND $batch AS row
        MATCH (e:Expression {id: row.expression_id})
        MERGE (t:TESEO_Concept {id: row.teseo_id})
        MERGE (e)-[r:HAS_TOPIC]->(t)
        SET r.score = row.score
        """
        await tx.run(query, batch=batch)

    async def _load_interprets_edges(self, tx, batch: list[dict]):
        """Batch load INTERPRETS edges."""
        query = """
        UNWIND $batch AS row
        MATCH (j:Judgement {id: row.source_id})
        CALL {
            WITH row MATCH (w:Work {id: row.target_id}) RETURN w AS target
            UNION
            WITH row MATCH (e:Expression {id: row.target_id}) RETURN e AS target
        }
        MERGE (j)-[:INTERPRETS]->(target)
        """
        await tx.run(query, batch=batch)

    async def _load_iter_legis(self, tx, batch: list[dict]):
        """Batch load ITER_LEGIS nodes."""
        query = """
        UNWIND $batch AS row
        MERGE (i:IterLegisStep {id: row.id})
        SET i.date = row.date,
            i.description = row.description,
            i.step_type = row.step_type,
            i.authority = row.authority
        """
        await tx.run(query, batch=batch)

    async def _load_iter_edges(self, tx, batch: list[dict]):
        """Batch load ITER_STEP edges."""
        query = """
        UNWIND $batch AS row
        MATCH (i:IterLegisStep {id: row.source_id})
        MATCH (w:Work {id: row.target_id})
        MERGE (i)-[:ITER_STEP]->(w)
        """
        await tx.run(query, batch=batch)

    async def load_batch(self, nodes_batch: list[dict], edges_batch: list[dict]):
        """
        Orchestrate the loading of a complete document batch.
        Uses a single transaction to ensure consistency.
        """
        import time
        start_time = time.time()
        
        # We split nodes by type for specific query logic
        works = [n for n in nodes_batch if n.get('type') == 'WORK']
        expressions = [n for n in nodes_batch if n.get('type') == 'EXPRESSION']
        structural = [n for n in nodes_batch if n.get('type') == 'STRUCTURAL']
        judgements = [n for n in nodes_batch if n.get('type') == 'JUDGEMENT']
        iter_legis = [n for n in nodes_batch if n.get('type') == 'ITER_LEGIS']
        
        # We split edges by type
        structural_edges = [e for e in edges_batch if e.get('type') in ('PART_OF', 'NEXT')]
        semantic_edges = [e for e in edges_batch if e.get('type') == 'HAS_TOPIC']
        interprets_edges = [e for e in edges_batch if e.get('type') == 'INTERPRETS']
        iter_edges = [e for e in edges_batch if e.get('type') == 'ITER_STEP']

        async def _execute_all(tx):
            try:
                if works:
                    logger.debug(f"Loading {len(works)} WORKS")
                    await self._load_works(tx, works)
                if expressions:
                    logger.debug(f"Loading {len(expressions)} EXPRESSIONS")
                    await self._load_expressions(tx, expressions)
                if structural:
                    logger.debug(f"Loading {len(structural)} STRUCTURAL UNITS")
                    await self._load_structural_units(tx, structural)
                if structural_edges:
                    logger.debug(f"Loading {len(structural_edges)} STRUCTURAL EDGES")
                    await self._load_structural_edges(tx, structural_edges)
                if semantic_edges:
                    logger.debug(f"Loading {len(semantic_edges)} SEMANTIC EDGES")
                    await self._load_semantic_edges(tx, semantic_edges)
                if judgements:
                    logger.debug(f"Loading {len(judgements)} JUDGEMENTS")
                    await self._load_judgements(tx, judgements)
                if interprets_edges:
                    logger.debug(f"Loading {len(interprets_edges)} INTERPRETS EDGES")
                    await self._load_interprets_edges(tx, interprets_edges)
                if iter_legis:
                    logger.debug(f"Loading {len(iter_legis)} ITER LEGIS")
                    await self._load_iter_legis(tx, iter_legis)
                if iter_edges:
                    logger.debug(f"Loading {len(iter_edges)} ITER EDGES")
                    await self._load_iter_edges(tx, iter_edges)
            except Exception as e:
                logger.error(f"Error during transaction execution: {e}")
                raise
        
        # We split edges by type
        structural_edges = [e for e in edges_batch if e.get('type') in ('PART_OF', 'NEXT')]
        semantic_edges = [e for e in edges_batch if e.get('type') == 'HAS_TOPIC']
        interprets_edges = [e for e in edges_batch if e.get('type') == 'INTERPRETS']
        iter_edges = [e for e in edges_batch if e.get('type') == 'ITER_STEP']

        async def _execute_all(tx):
            try:
                if works:
                    logger.debug(f"Loading {len(works)} WORKS")
                    await self._load_works(tx, works)
                if expressions:
                    logger.debug(f"Loading {len(expressions)} EXPRESSIONS")
                    await self._load_expressions(tx, expressions)
                if structural:
                    logger.debug(f"Loading {len(structural)} STRUCTURAL UNITS")
                    await self._load_structural_units(tx, structural)
                if structural_edges:
                    logger.debug(f"Loading {len(structural_edges)} STRUCTURAL EDGES")
                    await self._load_structural_edges(tx, structural_edges)
                if semantic_edges:
                    logger.debug(f"Loading {len(semantic_edges)} SEMANTIC EDGES")
                    await self._load_semantic_edges(tx, semantic_edges)
                if judgements:
                    logger.debug(f"Loading {len(judgements)} JUDGEMENTS")
                    await self._load_judgements(tx, judgements)
                if interprets_edges:
                    logger.debug(f"Loading {len(interprets_edges)} INTERPRETS EDGES")
                    await self._load_interprets_edges(tx, interprets_edges)
                if iter_legis:
                    logger.debug(f"Loading {len(iter_legis)} ITER LEGIS")
                    await self._load_iter_legis(tx, iter_legis)
                if iter_edges:
                    logger.debug(f"Loading {len(iter_edges)} ITER EDGES")
                    await self._load_iter_edges(tx, iter_edges)
            except Exception as e:
                logger.error(f"Error during transaction execution: {e}")
                raise

        try:
            async with self.driver.session() as session:
                await session.execute_write(_execute_all)
                elapsed = time.time() - start_time
                logger.info(f"Successfully loaded batch in {elapsed:.2f}s "
                            f"(Nodes: {len(nodes_batch)}, Edges: {len(edges_batch)})")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Failed to load batch into Neo4j after {elapsed:.2f}s. Error: {e}")
            raise
