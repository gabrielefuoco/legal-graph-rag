import logging
from typing import List
from langchain_community.embeddings import OllamaEmbeddings # Adjust based on actual deployment
from src.parsing.models import GraphNodeDTO
from src.config import settings

logger = logging.getLogger(__name__)

class VectorEngine:
    """
    Handles generation of dense vectors for legal chunks.
    Integrates with LangChain to use Qwen3 models via local or remote endpoints.
    """

    def __init__(self):
        # Using OllamaEmbeddings as a placeholder for the Qwen3 endpoint
        self.embeddings_model = OllamaEmbeddings(
            base_url=settings.QWEN3_ENDPOINT,
            model=settings.EMBEDDING_MODEL_NAME
        )

    def build_vector_payload(self, node: GraphNodeDTO, hierarchy_context: str) -> str:
        """
        Formats the text for embedding by injecting hierarchical context.
        As specified in Fase 4.2.
        """
        text = node.text_vector or node.text_display or ""
        payload = f"Contesto: {hierarchy_context}\n\nTesto: {text}"
        
        # Hard truncate to avoid "input length exceeds context length" API errors
        # Default Ollama num_ctx is 2048 tokens. Capping at 3500 chars (approx 1000 tokens)
        max_chars = 3500
        if len(payload) > max_chars:
            logger.warning(f"Truncating payload for node {node.id} to avoid exceeding model context limit.")
            payload = payload[:max_chars]
            
        return payload

    async def compute_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generates embeddings for a list of texts in parallel.
        Ensures output is cast to native Python float lists.
        """
        if not texts:
            return []
            
        try:
            # LangChain's aembed_documents is asynchronous
            embeddings = await self.embeddings_model.aembed_documents(texts)
            
            # Ensure float casting as per Fase 4.4
            return [[float(val) for val in vector] for vector in embeddings]
        except Exception as e:
            logger.error(f"Error during batch embedding inference: {e}")
            raise
