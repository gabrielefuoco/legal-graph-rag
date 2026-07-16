import re
import logging

logger = logging.getLogger(__name__)

def strip_thinking_tags(text: str) -> str:
    """
    Rimuove i blocchi <think>...</think> da una risposta completa.
    Se il modello produce: "<think>ragionamento...</think>\n\nRisposta vera"
    restituisce solo: "Risposta vera"
    """
    if not text:
        return text
    cleaned = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    if cleaned != text:
        logger.debug(f"Rimossi tag <think> ({len(text)} → {len(cleaned)} chars)")
    return cleaned if cleaned else text  # fallback


async def filter_think_stream(async_gen):
    """
    Generatore asincrono wrapper che filtra i token <think> in tempo reale.
    """
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
