import asyncio
import logging
from src.rag.engine import RagEngine
from src.rag.supervisor import SupervisorAgent
from langchain_core.messages import HumanMessage, AIMessage

logging.basicConfig(level=logging.INFO)

async def run_langgraph_directly():
    engine = RagEngine()
    supervisor = SupervisorAgent(engine)
    graph = supervisor.get_graph()
    
    query = "Quali sono le soglie economiche per gli affidamenti diretti di lavori, servizi e forniture?"
    messages = [
        HumanMessage(content="soglie economiche affidamento diretto lavori servizi forniture D.Lgs 36/2023"),
        AIMessage(content="In base alla normativa vigente, le soglie sono 150k e 140k."),
        HumanMessage(content=query)
    ]
    
    print("Avvio graph.astream()...")
    async for msg, metadata in graph.astream({"messages": messages}, stream_mode="messages"):
        node = metadata.get("langgraph_node")
        content = msg.content if hasattr(msg, "content") else ""
        print(f"[{node}] {type(msg).__name__}: {content[:50]}...")
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            print(f"  -> Tool Calls: {msg.tool_calls}")

asyncio.run(run_langgraph_directly())
