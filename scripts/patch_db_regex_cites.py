import asyncio
import logging
import re
import sys
import time

sys.path.append(r"C:\Users\gabri\APP\Università\Tesi")
from src.config import settings
from neo4j import AsyncGraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Regex robusta per estrarre citazioni dal testo nudo
PATTERN = re.compile(
    r'(?P<tipo>legge|decreto\s+legislativo|d\.lgs\.?|decreto\s+legge|d\.l\.?)\s+'
    r'(?:(?:del\s+)?(?P<giorno>\d{1,2})\s+(?P<mese>[a-z]+)\s+(?P<anno1>\d{4})\s*,?\s*(?:n\.?\s*)?(?P<num1>\d+)'
    r'|(?:n\.?\s*)?(?P<num2>\d+)/(?P<anno2>\d{4}))',
    re.IGNORECASE
)

async def patch_regex_cites():
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )

    try:
        logger.info("Connessione a Neo4j per estrarre il testo delle Expression...")
        
        async with driver.session() as session:
            # 1. Recupero tutti i nodi Expression con il loro testo
            result = await session.run("MATCH (e:Expression) WHERE e.text_display IS NOT NULL RETURN e.id AS id, e.text_display AS text")
            records = await result.data()
            
        logger.info(f"Trovati {len(records)} nodi Expression da analizzare con Regex.")
        
        cites_batch = []
        for record in records:
            text = record["text"]
            source_id = record["id"]
            
            # Applichiamo la Regex
            for match in PATTERN.finditer(text):
                d = match.groupdict()
                tipo = d['tipo'].lower()
                if 'legge' in tipo and 'decreto' not in tipo:
                    urn_tipo = 'legge'
                elif 'lgs' in tipo or 'legislativo' in tipo:
                    urn_tipo = 'decreto.legislativo'
                else:
                    urn_tipo = 'decreto.legge'
                    
                num = d['num1'] or d['num2']
                anno = d['anno1'] or d['anno2']
                
                # URN standard approssimato
                target_urn = f"urn:nir:stato:{urn_tipo}:{anno};{num}"
                quoted_text = match.group(0)
                
                cites_batch.append({
                    "source_id": source_id,
                    "target_id": target_urn,
                    "text": quoted_text
                })
                
        logger.info(f"La Regex ha trovato {len(cites_batch)} citazioni testuali (archi CITES potenziali).")
        
        if not cites_batch:
            logger.info("Nessun arco da iniettare.")
            return

        # 2. Caricamento in Neo4j
        logger.info("Caricamento archi CITES in Neo4j (con creazione Work placeholders se assenti)...")
        
        query_cites = """
        UNWIND $batch AS row
        MATCH (s:Expression {id: row.source_id})
        MERGE (target:Work {id: row.target_id})
        ON CREATE SET target.urn = row.target_id, target.title = "Riferimento Esterno (" + row.text + ")"
        MERGE (s)-[r:CITES]->(target)
        SET r.extracted_by = 'REGEX', r.text = row.text
        """

        async def _execute_patch(tx):
            chunk_size = 5000
            for i in range(0, len(cites_batch), chunk_size):
                await tx.run(query_cites, batch=cites_batch[i:i+chunk_size])

        start = time.time()
        async with driver.session() as session:
            await session.execute_write(_execute_patch)
            
        logger.info(f"Patch REGEX completata in {time.time()-start:.2f} secondi. Database RAG pronto per il Multi-Hop!")

    except Exception as e:
        logger.error(f"Errore: {e}")
    finally:
        await driver.close()

if __name__ == "__main__":
    asyncio.run(patch_regex_cites())
