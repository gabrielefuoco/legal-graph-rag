import asyncio
import json
import logging
import sys
from pydantic import BaseModel, Field

sys.path.append(r"C:\Users\gabri\APP\Università\Tesi")
from src.config import settings
from neo4j import AsyncGraphDatabase
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage, SystemMessage

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

class GoldenItem(BaseModel):
    query: str = Field(description="Una domanda legale realistica, complessa e specifica a cui il testo risponde.")
    expected_keywords: list[str] = Field(description="3-5 parole chiave o concetti chiave estratti dal testo.")
    golden_answer: str = Field(description="La risposta corretta, fattuale e sintetica basata solo sul testo.")

async def generate_golden():
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )
    
    # Inizializziamo l'LLM locale
    llm = ChatOllama(
        base_url=settings.QWEN3_ENDPOINT,
        model=settings.GENERATIVE_MODEL_NAME,
        temperature=0.2, # Bassa temperatura per maggior aderenza ai fatti
        format="json" # Chiediamo esplicitamente l'output in JSON per Pydantic
    )
    
    # Facciamo in modo che il modello restituisca un output aderente allo schema Pydantic
    llm_structured = llm.with_structured_output(GoldenItem)
    
    dataset = []
    
    try:
        async with driver.session() as session:
            # Peschiamo 15 nodi a caso che siano abbastanza lunghi (es. > 400 caratteri) per garantire sostanza
            query = """
            MATCH (e:Expression)
            WHERE size(e.text_display) > 400
            WITH e, rand() AS r
            ORDER BY r
            LIMIT 15
            RETURN e.id AS id, e.text_display AS text
            """
            result = await session.run(query)
            records = await result.data()
            
            logger.info(f"Selezionati {len(records)} nodi casuali da Neo4j. Avvio generazione LLM...")
            
            for i, rec in enumerate(records):
                text = rec['text']
                node_id = rec['id']
                
                logger.info(f"[{i+1}/{len(records)}] Generazione per il nodo {node_id}...")
                
                system_prompt = (
                    "Sei un assistente legale esperto. Il tuo compito è creare un dataset di valutazione (Golden Dataset) per un sistema RAG.\n"
                    "Riceverai il testo di un comma o articolo di legge.\n"
                    "Devi generare:\n"
                    "1. Una 'query' che un avvocato o cittadino potrebbe porre, a cui questo testo risponde esattamente. La domanda deve essere naturale, non troppo banale, e menzionare l'argomento.\n"
                    "2. Una 'golden_answer' che risponde alla domanda usando solo le info del testo.\n"
                    "3. Delle 'expected_keywords' (3-5 termini cruciali presenti nel testo).\n"
                )
                
                try:
                    # Invochiamo l'LLM chiedendo il formato strutturato
                    res = await llm_structured.ainvoke([
                        SystemMessage(content=system_prompt),
                        HumanMessage(content=f"Genera l'output richiesto basandoti ESCLUSIVAMENTE su questo testo normativo:\n\n{text}")
                    ])
                    
                    # Convertiamo in dizionario compatibile con il formato richiesto dal valutatore
                    item = {
                        "query": res.query,
                        "expected_ids": [node_id],
                        "expected_keywords": res.expected_keywords,
                        "golden_answer": res.golden_answer
                    }
                    dataset.append(item)
                    logger.info(f"   -> Query generata: {res.query}")
                    
                except Exception as e:
                    logger.error(f"Errore nella generazione LLM per il nodo {node_id}: {e}")
                    
        # Salviamo il dataset su file
        output_path = "data/eval_dataset.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(dataset, f, indent=4, ensure_ascii=False)
            
        logger.info(f"Golden Dataset generato con successo in {output_path} ({len(dataset)} record).")
            
    finally:
        await driver.close()

if __name__ == "__main__":
    asyncio.run(generate_golden())
