import asyncio
import logging
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage
from src.config import settings

logging.basicConfig(level=logging.INFO)

async def test_supervisor_llm():
    # Simuliamo un payload massiccio
    huge_text = "Questo è un finto documento lunghissimo. " * 500  # Circa 4000 parole
    
    prompt = f"""
Sei un assistente legale. Rispondi alla domanda usando SOLO questo contesto:
{huge_text}

Domanda: Quali sono le regole?
"""
    
    llm = ChatOllama(
        base_url=settings.QWEN3_ENDPOINT,
        model=settings.GENERATIVE_MODEL_NAME,
        temperature=0.0,
        num_ctx=8192,
        reasoning=False,
    )
    
    print("Invocando LLM...")
    try:
        response = await llm.ainvoke([HumanMessage(content=prompt)])
        print(f"Risposta: {response.content}")
    except Exception as e:
        print(f"ERRORE LLM: {e}")

asyncio.run(test_supervisor_llm())
