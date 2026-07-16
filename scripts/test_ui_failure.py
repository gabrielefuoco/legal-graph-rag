import asyncio
import logging
from src.rag.engine import RagEngine
from src.rag.supervisor import SupervisorAgent
from src.ui.rag_bridge import query_rag_with_trace
from langchain_core.messages import HumanMessage, AIMessage

logging.basicConfig(level=logging.INFO)

async def test_full_pipeline():
    engine = RagEngine()
    supervisor = SupervisorAgent(engine)
    
    # User's exact prompt from screenshot
    query = "Quali sono le soglie economiche per gli affidamenti diretti di lavori, servizi e forniture?"
    
    # Chat history with the previous answer, just like what Streamlit did
    chat_history = [
        {"role": "user", "content": "soglie economiche affidamento diretto lavori servizi forniture D.Lgs 36/2023"},
        {"role": "assistant", "content": "In base alla normativa vigente..."}
    ]
    
    config = {}
    
    def cb(label):
        print(f"CB: {label}")
        
    print("Avvio pipeline...")
    coro = query_rag_with_trace(supervisor, query, config, chat_history, cb)
    
    async def run_it():
        out_chunks, out_trace, gen = await coro
        print("GEN RECEIVED")
        for chunk in gen:
            print(chunk, end="")
        print("\nDONE")
    
    await run_it()

asyncio.run(test_full_pipeline())
