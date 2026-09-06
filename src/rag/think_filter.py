"""Thinking tag filtering utilities for reasoning LLM outputs."""

import re
import logging

logger = logging.getLogger(__name__)

def strip_thinking_tags(text: str) -> str:
    """Strip <think>...</think> reasoning blocks from full model completions."""
    if not text:
        return text
    cleaned = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    if cleaned != text:
        logger.debug(f"Rimossi tag <think> ({len(text)} → {len(cleaned)} chars)")
    return cleaned if cleaned else text  # fallback


async def filter_think_stream(async_gen):
    """Asynchronous generator wrapper filtering <think>...</think> token streams in real time."""
    buffer = ""
    inside_think = False
    
    async for token in async_gen:
        if not token:
            continue
            
        if inside_think:
            buffer += token
            if "</think>" in buffer:
                _, _, after = buffer.partition("</think>")
                after = after.lstrip("\n")
                if after:
                    yield after
                buffer = ""
                inside_think = False
        else:
            buffer += token
            if "<think>" in buffer:
                inside_think = True
                before, _, remainder = buffer.partition("<think>")
                if before.strip():
                    yield before
                buffer = remainder
            elif len(buffer) > 10:
                yield buffer
                buffer = ""
                async for remaining_token in async_gen:
                    if remaining_token:
                        yield remaining_token
                return
    
    if buffer and not inside_think:
        yield buffer
