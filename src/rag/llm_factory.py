"""LLM Factory — Centralized factory for BaseChatModel instances (Ollama or OpenRouter).

Provides a unified abstraction allowing transparent switching between
local Ollama instances and remote OpenRouter APIs.
"""
import logging
from typing import Optional
from langchain_core.language_models.chat_models import BaseChatModel

from src.config import settings

logger = logging.getLogger(__name__)


def get_llm(
    num_ctx: Optional[int] = None,
    temperature: float = 0.0,
    reasoning: bool = False,
    model_name: Optional[str] = None,
    provider: Optional[str] = None,
) -> BaseChatModel:
    """Return a BaseChatModel instance (ChatOllama or ChatOpenAI) based on the configured provider.

    Args:
        num_ctx: Context window size (used by Ollama; OpenRouter manages context natively).
        temperature: Sampling temperature (default 0.0 for deterministic tasks).
        reasoning: Flag to enable/disable thinking tags on compatible models (e.g., Ollama).
        model_name: Optional override for model name.
        provider: Optional provider override ("ollama" or "openrouter"). Defaults to settings.LLM_PROVIDER.

    Returns:
        Configured BaseChatModel instance.
    """
    active_provider = (provider or settings.LLM_PROVIDER).strip().lower()

    if active_provider == "openrouter":
        from langchain_openai import ChatOpenAI

        target_model = model_name or settings.OPENROUTER_MODEL_NAME
        api_key = settings.OPENROUTER_API_KEY

        if not api_key:
            logger.warning(
                "OPENROUTER_API_KEY non trovata nelle variabili d'ambiente. "
                "Le chiamate verso OpenRouter potrebbero fallire."
            )

        kwargs = {
            "base_url": settings.OPENROUTER_BASE_URL,
            "api_key": api_key or "dummy_key",
            "model": target_model,
            "temperature": temperature,
            "default_headers": {
                "HTTP-Referer": "https://github.com/gabrielefuoco/legal-graph-rag",
                "X-Title": "Legal GraphRAG",
            },
        }
        return ChatOpenAI(**kwargs)

    else:
        from langchain_ollama import ChatOllama

        target_model = model_name or settings.GENERATIVE_MODEL_NAME
        kwargs = {
            "base_url": settings.QWEN3_ENDPOINT,
            "model": target_model,
            "temperature": temperature,
            "reasoning": reasoning,
        }
        if num_ctx is not None:
            kwargs["num_ctx"] = num_ctx

        return ChatOllama(**kwargs)
