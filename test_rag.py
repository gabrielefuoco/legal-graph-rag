import asyncio
from neo4j import AsyncGraphDatabase
from src.config import settings
from src.parsing.vector_engine import VectorEngine

async def simulate_rag():
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
    )
    
    # 1. Simulate the User Query
    user_query = "Quali sono le disposizioni relative alla spesa pubblica o bilancio?"
    print(f"\\n--- [ SIMULAZIONE HYBRID RAG ] ---\\n")
    print(f"Domanda dell'Utente: '{user_query}'")
    
    # 1. Generate Embedding for the query
    vector_engine = VectorEngine()
    print("\\n[EMBEDDING] Calcolo dell'embedding locale per la query tramite Ollama...")
    try:
        query_vector_nested = await vector_engine.compute_embeddings_batch([user_query])
        query_vector = query_vector_nested[0]
        print("✅ Query convertita in spazio vettoriale con successo.")
    except Exception as e:
        print(f"⚠️ Errore generazione embedding: {e}")
        query_vector = None

    context_builder = []
    
    async with driver.session() as session:
        
        # --- A) VECTOR SEARCH ---
        if query_vector:
            print("\\n1. [RETRIEVAL VETTORIALE] Ricerca nello spazio topologico (Vector Index)...")
            vector_query = """
            CALL db.index.vector.queryNodes('expression_embedding_vector', 2, $query_vector)
            YIELD node AS e, score
            OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
            RETURN e.text_display AS text, u.tag_name AS parent_tag, e.eId as expression_id, score
            """
            try:
                records = await session.run(vector_query, query_vector=query_vector)
                idx = 0
                async for record in records:
                    idx += 1
                    text = record["text"] or ""
                    parent_tag = record["parent_tag"] or "Norma"
                    expression_id = record["expression_id"] or "Sconosciuto"
                    score = record["score"]
                    fragment = f"🎯 [VECTOR MATCH {idx} | Relatività: {score:.3f}] ({parent_tag} - id: {expression_id}):\\n'{text.strip()}'"
                    context_builder.append(fragment)
            except Exception as e:
                print(f"Errore nella Vector Search: {e} - Probabilmente l'indice non è stato trovato o gli array sono vuoti.")
        
        # --- B) SEMANTIC GRAPH SEARCH (TESEO) ---
        print("\\n2. [RETRIEVAL GRAFO] Estrazione del contesto semantico (TESEO Knowledge)...")
        # Trova il topic più ricorrente nel database giusto per avere risultati
        topic_result = await session.run("""
            MATCH ()-[r:HAS_TOPIC]->(t:TESEO_Concept)
            RETURN t.id AS topic_name, count(r) as hits
            ORDER BY hits DESC LIMIT 1
        """)
        topic_record = await topic_result.single()
        if not topic_record:
            print("Nessun topic trovato!")
            await driver.close()
            return
            
        target_topic = topic_record["topic_name"]
        print(f"   => Entity Extration ha mappato la domanda al concetto TESEO: '{target_topic}'")
        
        # Recupera le norme associate a questo topic (Graph Retrieval)
        retrieval_query = """
        MATCH (e:Expression)-[r:HAS_TOPIC]->(t:TESEO_Concept {id: $topic})
        // Fetch the structural parent for context
        OPTIONAL MATCH (e)-[:PART_OF]->(u:StructuralUnit)
        RETURN e.text_display AS text, u.tag_name AS parent_tag, u.id AS parent_id, e.eId as expression_id
        ORDER BY r.score DESC
        LIMIT 3
        """
        records = await session.run(retrieval_query, topic=target_topic)
        
        idx = 0
        async for record in records:
            idx += 1
            text = record["text"]
            parent_tag = record["parent_tag"] or "Norma"
            expression_id = record["expression_id"] or "Sconosciuto"
            
            fragment = f"🔗 [GRAPH MATCH {idx}] ({parent_tag} - id: {expression_id}):\\n'{text.strip()}'"
            context_builder.append(fragment)

    # 3. Augmentation Step
    print("\\n3. [AUGMENTATION] Costruzione del Prompt Ibrido per l'LLM...")
    context_str = "\\n\\n".join(context_builder)
    
    prompt = f"""Sei un assistente legale esperto. Rispondi alla domanda dell'utente basandoti ESCLUSIVAMENTE sul seguente contesto normativo estratto dal Knowledge Graph.

CONTESTO:
{context_str}

DOMANDA: {user_query}

RISPOSTA:
... (L'LLM genererà la risposta qui) ..."""

    print("-" * 50)
    print(prompt)
    print("-" * 50)
    
    await driver.close()

if __name__ == "__main__":
    asyncio.run(simulate_rag())
