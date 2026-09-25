"""Application configuration settings for the Legal Graph RAG system."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

# Instruction prefix prepended to the QUERY embedding only (document embeddings are
# instruction-free by design for Qwen3-Embedding). Keeping it here lets scripts reference
# the built-in default even when .env disables or overrides it.
DEFAULT_EMBEDDING_QUERY_INSTRUCTION = (
    "Instruct: Dato un quesito giuridico, recupera le disposizioni normative e i "
    "passaggi giurisprudenziali che rispondono alla domanda.\nQuery: "
)

class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    Uses pydantic-settings for validation and automatic .env loading.
    """
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Neo4j Database
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USERNAME: str = "neo4j"
    NEO4J_PASSWORD: str = "password"

    # LLM & Embedding (Local & Remote)
    LLM_PROVIDER: str = "ollama"  # "ollama" or "openrouter"
    QWEN3_ENDPOINT: str = "http://localhost:11434"
    EMBEDDING_MODEL_NAME: str = "qwen3-embedding:0.6b"
    EMBEDDING_DIMENSIONS: int = 1024  # Embedding dimension for the selected model
    GENERATIVE_MODEL_NAME: str = "qwen3.5:4b"
    # Instruction prefix prepended to the QUERY embedding only (document embeddings are
    # instruction-free by design for Qwen3-Embedding). Set to "" to disable. Ingestion is
    # unaffected: no re-indexing is required to change this value.
    EMBEDDING_QUERY_INSTRUCTION: str = DEFAULT_EMBEDDING_QUERY_INSTRUCTION

    # OpenRouter Settings
    OPENROUTER_API_KEY: Optional[str] = None
    OPENROUTER_BASE_URL: str = "https://openrouter.ai/api/v1"
    OPENROUTER_MODEL_NAME: str = "qwen/qwen3.5-9b"

    # RAG Retrieval
    TESEO_RDF_PATH: str = "data/external/teseo_full.ttl"
    RAG_TOP_K: int = 15
    RRF_WEIGHT_VECTOR: float = 1.0
    RRF_WEIGHT_BM25: float = 1.0
    RRF_WEIGHT_GRAPH: float = 1.2
    RRF_K: int = 60
    MAX_CITATION_HOPS: int = 1
    RAG_MIN_SCORE: float = 0.01
    RAG_VECTOR_MIN_SCORE: float = 0.3
    MAX_AGENTIC_ITERATIONS: int = 5

    # Reranker
    RERANKER_MODEL_NAME: str = "Qwen/Qwen3-Reranker-0.6B"
    RERANKER_BACKEND: str = "local"  # "local" (CrossEncoder on GPU/CPU) or "llm" (OpenRouter listwise)
    RERANKER_LLM_MODEL: Optional[str] = None  # Defaults to OPENROUTER_MODEL_NAME when using the LLM backend
    RERANK_TOP_K: int = 20  # Number of candidate chunks passed to reranker after fusion
    RERANK_MIN_SCORE: float = 0.05  # Minimum relevance score threshold (post-sigmoid)

    # TESEO Matching
    TESEO_DENSE_THRESHOLD: float = 0.45        # Cosine similarity threshold for dense semantic matching
    TESEO_SPARSE_BOOST: float = 1.0            # Score assigned to exact Aho-Corasick matches
    TESEO_MAX_CONCEPTS: int = 10               # Maximum number of TESEO concepts returned per chunk/query

    # Topological Expansion
    TOPOLOGICAL_MAX_CHARS: int = 6000           # Maximum character limit for expanded super-chunk
    TOPOLOGICAL_EXPAND_NEXT: bool = True        # Expand via :NEXT relationships (+-1 comma)
    TOPOLOGICAL_EXPAND_PARENT: bool = True      # Expand via :PART_OF relationship (parent article)
    TOPOLOGICAL_EXPAND_CITES: bool = True       # Expand via :CITES/:MODIFIES relationships

    # Hardware-ready context window limits
    GENERATOR_NUM_CTX: int = 4096              # Generator context window size
    GENERATOR_STUFF_THRESHOLD: int = 8000      # Maximum characters for prompt stuffing before switching to Map-Reduce
    SUPERVISOR_NUM_CTX: int = 16384            # Supervisor context window size

# Singleton instance for the application
settings = Settings()
