"""Topological context expander traversing graph relationships around retrieved chunks."""

import logging
from typing import List
from neo4j import AsyncDriver
from src.rag.models import RetrievedChunk, RagState
from src.config import settings
import copy

logger = logging.getLogger(__name__)

class TopologyExpander:
    """Expands retrieved chunks using graph topology (:NEXT, :PART_OF, :CITES, :MODIFIES)."""

    def __init__(self, driver: AsyncDriver):
        self.driver = driver

    async def expand(self, chunks: List[RetrievedChunk], topo_max_chars: int = 6000) -> List[RetrievedChunk]:
        """Expand chunks with structural and relational context within a maximum character budget."""
        expanded_chunks = []
        for chunk in chunks:
            logger.info(f"🔍 Avvio espansione topologica per il chunk: {chunk.expression_id}")
            trace = {"expanded": False, "next": False, "parent": False, "cites": False}
            
            # Expansion budget: remaining capacity after original text
            budget = max(0, topo_max_chars - len(chunk.text))
            
            if budget == 0:
                logger.info(f"  [!] Chunk già grande ({len(chunk.text)} >= {topo_max_chars}), skip espansione.")
                new_chunk = copy.copy(chunk)
                new_chunk.expanded_text = chunk.text
                new_chunk.expansion_trace = trace
                expanded_chunks.append(new_chunk)
                continue
            
            # Collect expansion prefixes and suffixes separately
            prefix_parts = []
            suffix_parts = []
            
            try:
                async with self.driver.session() as session:
                    # 1. :NEXT expansion (preceding and following paragraphs)
                    if settings.TOPOLOGICAL_EXPAND_NEXT:
                        result = await session.run(
                            """
                            OPTIONAL MATCH (prev)-[:NEXT]->(e:Expression {id: $chunk_id})
                            OPTIONAL MATCH (e:Expression {id: $chunk_id})-[:NEXT]->(next)
                            RETURN prev.text_display AS prev_text, next.text_display AS next_text
                            """,
                            chunk_id=chunk.expression_id
                        )
                        record = await result.single()
                        if record:
                            prev_text = record.get("prev_text")
                            next_text = record.get("next_text")
                            if prev_text:
                                prefix_parts.append(prev_text)
                                trace["next"] = True
                            if next_text:
                                suffix_parts.append(next_text)
                                trace["next"] = True

                    # 2. :PART_OF expansion (parent article heading)
                    if settings.TOPOLOGICAL_EXPAND_PARENT:
                        result = await session.run(
                            """
                            MATCH (e:Expression {id: $chunk_id})-[:PART_OF]->(parent:StructuralUnit)
                            RETURN parent.heading AS parent_heading, parent.unit_type AS parent_type
                            """,
                            chunk_id=chunk.expression_id
                        )
                        record = await result.single()
                        if record and record.get("parent_heading"):
                            heading = record.get("parent_heading")
                            prefix_parts.insert(0, f"[{heading}]")
                            trace["parent"] = True

                    # 3. :CITES / :MODIFIES relationship expansion
                    if settings.TOPOLOGICAL_EXPAND_CITES:
                        result = await session.run(
                            """
                            MATCH (e:Expression {id: $chunk_id})-[r:CITES|MODIFIES]->(target)
                            WHERE target:Expression OR target:Work
                            RETURN coalesce(target.text_display, target.title) AS cited_text, type(r) AS rel_type
                            LIMIT 3
                            """,
                            chunk_id=chunk.expression_id
                        )
                        cited_texts = []
                        async for record in result:
                            rel = record["rel_type"]
                            text = record["cited_text"]
                            if text:
                                text = text[:500] + ("..." if len(text) > 500 else "")
                                cited_texts.append(f"({rel}) {text}")
                        
                        if cited_texts:
                            suffix_parts.append("Norme correlate:\n" + "\n".join(cited_texts))
                            trace["cites"] = True

            except Exception as e:
                logger.error(f"Errore durante l'espansione topologica per il chunk {chunk.expression_id}: {e}")

            # Budget assembly: ensure original text is preserved completely
            prefix_text = "\n".join(prefix_parts)
            suffix_text = "\n\n".join(suffix_parts)
            
            total_additions = len(prefix_text) + len(suffix_text)
            
            if total_additions <= budget:
                pass
            else:
                # Truncate additions to fit budget
                half_budget = budget // 2
                if prefix_text and len(prefix_text) > half_budget:
                    prefix_text = prefix_text[:half_budget] + "..."
                    logger.info(f"  [!] Prefix troncato a {half_budget} chars.")
                remaining = budget - len(prefix_text)
                if suffix_text and len(suffix_text) > remaining:
                    suffix_text = suffix_text[:remaining] + "..."
                    logger.info(f"  [!] Suffix troncato a {remaining} chars.")
            
            # Final text assembly
            parts = []
            if prefix_text:
                parts.append(prefix_text)
                logger.info(f"  [+] Prefix: {len(prefix_text)} caratteri aggiunti.")
            parts.append(chunk.text)
            if suffix_text:
                parts.append(suffix_text)
                logger.info(f"  [+] Suffix: {len(suffix_text)} caratteri aggiunti.")
            
            expanded_text = "\n".join(parts)

            if prefix_text or suffix_text:
                trace["expanded"] = True

            new_chunk = copy.copy(chunk)
            new_chunk.expanded_text = expanded_text
            new_chunk.expansion_trace = trace
            expanded_chunks.append(new_chunk)
            
        return expanded_chunks


async def topological_expand_node(state: RagState) -> dict:
    """LangGraph node: expands validated chunks using graph relationships."""
    import time
    start = time.perf_counter()
    chunks = state.get("final_chunks") or []
    enable_expansion = state.get("enable_topological_expansion", True)
    topo_max_chars = state.get("topo_max_chars", settings.TOPOLOGICAL_MAX_CHARS)

    if not enable_expansion or not chunks:
        logger.info("Espansione topologica disabilitata o nessun chunk da espandere.")
        return {"expanded_chunks": chunks}

    driver = state["_driver"]
    expander = TopologyExpander(driver)
    
    expanded = await expander.expand(chunks, topo_max_chars)
    
    elapsed = time.perf_counter() - start
    logger.info(f"TOPOLOGICAL EXPAND — Completato in {elapsed:.1f}s | {len(expanded)} chunk arricchiti")
    
    return {"expanded_chunks": expanded}
