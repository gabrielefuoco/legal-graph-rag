"""
LLM Factory — Creazione centralizzata di istanze BaseChatModel (Ollama o OpenRouter).
Fornisce un'astrazione unificata che consente lo switch trasparente tra
l'infrastruttura locale Ollama e le API remote OpenRouter.
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
    """
    Restituisce un'istanza BaseChatModel (ChatOllama o ChatOpenAI) in base al provider configurato.

    :param num_ctx: Finestra di contesto (utilizzata da Ollama; OpenRouter gestisce la finestra nativamente).
    :param temperature: Temperatura per il campionamento (default 0.0 per compiti deterministici).
    :param reasoning: Flag per disabilitare/abilitare thinking tags su modelli compatibili (es. Ollama).
    :param model_name: Override opzionale del nome del modello.
    :param provider: Override opzionale ("ollama" o "openrouter"). Default da settings.LLM_PROVIDER.
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
