"""Vector engine for dense embedding generation of legal text chunks."""
import logging
from typing import List
from langchain_ollama import OllamaEmbeddings
from src.parsing.models import GraphNodeDTO
from src.config import settings

logger = logging.getLogger(__name__)

class VectorEngine:
    """Generate dense vectors for legal chunks.

    Integrates with LangChain to use embedding models via local or remote endpoints.
    """

    def __init__(self):
        # Using OllamaEmbeddings as a placeholder for the Qwen3 endpoint
        self.embeddings_model = OllamaEmbeddings(
            base_url=settings.QWEN3_ENDPOINT,
            model=settings.EMBEDDING_MODEL_NAME
        )

    def build_vector_payload(self, node: GraphNodeDTO, hierarchy_context: str) -> str:
        """Format node text for embedding by injecting hierarchical context."""
        text = node.text_vector or node.text_display or ""
        payload = f"Contesto: {hierarchy_context}\n\nTesto: {text}"
        
        # Hard truncate to avoid "input length exceeds context length" API errors
        # Default Ollama num_ctx is 2048 tokens. Capping at 3500 chars (approx 1000 tokens)
        max_chars = 3500
        if len(payload) > max_chars:
            logger.warning(f"Truncating payload for node {node.id} to avoid exceeding model context limit.")
            payload = payload[:max_chars]
            
        return payload

    async def compute_query_embedding(self, text: str) -> List[float]:
        """Compute the dense embedding of a retrieval query.

        Qwen3-Embedding models are instruction-tuned: the task instruction belongs on the
        QUERY side only (documents stay instruction-free), so enabling or changing it never
        requires re-embedding the corpus.
        """
        embeddings = await self.compute_embeddings_batch([self._build_query_payload(text)])
        return embeddings[0] if embeddings else []

    def _build_query_payload(self, text: str) -> str:
        """Prepend the configured instruction prefix to a query when supported by the model."""
        instruction = (getattr(settings, "EMBEDDING_QUERY_INSTRUCTION", "") or "").strip()
        if not instruction:
            return text
        if "qwen3" not in (settings.EMBEDDING_MODEL_NAME or "").lower():
            # Instruction prefixes are model specific: apply only where they are supported
            return text
        return f"{instruction}{text}"

    async def compute_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate dense embeddings for a list of texts in parallel.

        Ensures output is cast to native Python float lists.
        """
        if not texts:
            return []
            
        try:
            # LangChain's aembed_documents is asynchronous
            embeddings = await self.embeddings_model.aembed_documents(texts)
            
            # Ensure elements are cast to native Python floats
            return [[float(val) for val in vector] for vector in embeddings]
        except Exception as e:
            logger.error(f"Error during batch embedding inference: {e}")
            raise

