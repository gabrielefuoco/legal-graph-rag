"""LLM Factory — Centralized factory for BaseChatModel instances (Ollama or OpenRouter).

Provides a unified abstraction allowing transparent switching between
local Ollama instances and remote OpenRouter APIs.
"""
import asyncio
import logging
from typing import Optional
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_openai import ChatOpenAI
from src.config import settings

logger = logging.getLogger(__name__)

# Hard per-call guards: OpenRouter occasionally returns HTTP headers and then never delivers
# the body, ignoring the transport-level timeout. Without these the agentic loop hangs forever.
OPENROUTER_CALL_TIMEOUT_S = 45
OPENROUTER_CALL_ATTEMPTS = 3


class ResilientChatOpenAI(ChatOpenAI):
    """ChatOpenAI that hard-limits every call and retries stalled responses."""

    async def ainvoke(self, *args, **kwargs):
        last_error: Optional[Exception] = None
        for attempt in range(1, OPENROUTER_CALL_ATTEMPTS + 1):
            try:
                return await asyncio.wait_for(
                    super().ainvoke(*args, **kwargs), timeout=OPENROUTER_CALL_TIMEOUT_S
                )
            except asyncio.TimeoutError as e:
                last_error = e
                logger.warning(
                    f"[LLM] Nessuna risposta da OpenRouter entro {OPENROUTER_CALL_TIMEOUT_S}s "
                    f"(tentativo {attempt}/{OPENROUTER_CALL_ATTEMPTS})."
                )
        raise TimeoutError(
            f"OpenRouter non ha risposto entro {OPENROUTER_CALL_TIMEOUT_S}s "
            f"per {OPENROUTER_CALL_ATTEMPTS} tentativi"
        ) from last_error


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
            # Hard timeouts: without them a stalled OpenRouter response blocks the
            # whole pipeline indefinitely (observed: 200 OK headers, body never sent).
            "timeout": 120,
            "max_retries": 3,
            "default_headers": {
                "HTTP-Referer": "https://github.com/gabrielefuoco/legal-graph-rag",
                "X-Title": "Legal GraphRAG",
            },
        }
        if not reasoning:
            # OpenRouter aggregates hybrid reasoning models (e.g. qwen3.5) that think by
            # default. Without this flag each call burns thousands of reasoning tokens
            # (30-240s latency, and empty content when the budget is exhausted), which
            # defeats the reasoning=False intent used by every caller.
            kwargs["extra_body"] = {"reasoning": {"enabled": False}}
        return ResilientChatOpenAI(**kwargs)

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
