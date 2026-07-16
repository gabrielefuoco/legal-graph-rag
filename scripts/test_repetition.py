import asyncio
import logging
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from src.config import settings
from langgraph.prebuilt import create_react_agent

logging.basicConfig(level=logging.INFO)

async def test_llm_repetition():
    llm = ChatOllama(
        base_url=settings.QWEN3_ENDPOINT,
        model=settings.GENERATIVE_MODEL_NAME,
        temperature=0.0,
        num_ctx=8192,
        reasoning=False,
    )
    
    system_prompt = "Sei un Assistente Legale. Usa il tool per rispondere."
    
    def fake_tool(query: str) -> str:
        """Cerca nel db"""
        return "DOCUMENTI TROVATI:\n[1] Art 50. Soglia 150k."
    
    agent = create_react_agent(llm, tools=[fake_tool], prompt=system_prompt)
    
    messages = [
        HumanMessage(content="Quali sono le soglie economiche?"),
        AIMessage(content="Le soglie economiche sono 150k per lavori e 140k per servizi."),
        HumanMessage(content="Quali sono le soglie economiche?")
    ]
    
    print("Eseguendo agente con history ripetitiva...")
    async for msg, metadata in agent.astream({"messages": messages}, stream_mode="messages"):
        if metadata.get("langgraph_node") == "agent":
            if msg.content:
                print(f"CHUNK: {msg.content}", end="")
    print("\nFinito.")

asyncio.run(test_llm_repetition())
