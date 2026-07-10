import re
import logging
from typing import List, Dict, Set
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import SKOS
import ahocorasick

logger = logging.getLogger(__name__)

class TESEOMatcher:
    """
    Semantic engine for linking legal text to the TESEO thesaurus.
    Uses Aho-Corasick for O(n) string matching of prefLabels and altLabels.
    """

    def __init__(self, rdf_path: str = None):
        self.matcher = ahocorasick.Automaton()
        self.label_to_id = {}
        self.label_embeddings = {}
        self.label_norms = {}
        if rdf_path:
            self.load_ontology(rdf_path)

    def normalize_text(self, text: str) -> str:
        """Lowercases and removes punctuation for baseline matching."""
        if not text:
            return ""
        text = text.lower()
        # Replace anything that isn't alphanumeric or space with space
        text = re.sub(r'[^\w\s]', ' ', text)
        # Collapse multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def load_ontology(self, filepath: str):
        """
        Parses TESEO RDF/SKOS file and populates the Aho-Corasick automaton.
        Expects concepts with skos:prefLabel and optional skos:altLabel.
        """
        g = Graph()
        logger.info(f"Loading TESEO ontology from {filepath}...")
        try:
            g.parse(filepath, format="xml") # TESEO is usually RDF/XML
        except Exception as e:
            logger.error(f"Failed to parse RDF: {e}")
            raise

        count = 0
        for s, p, o in g.triples((None, SKOS.prefLabel, None)):
            if isinstance(o, Literal):
                # Accept if Italian or if no language is specified (common in some RDF exports)
                if o.language == 'it' or not o.language:
                    concept_id = str(s)
                    label = self.normalize_text(str(o))
                    if label:
                        self.label_to_id[label] = concept_id
                        self.matcher.add_word(label, (label, concept_id))
                        count += 1

        # Also load altLabels if available
        for s, p, o in g.triples((None, SKOS.altLabel, None)):
             if isinstance(o, Literal):
                if o.language == 'it' or not o.language:
                    concept_id = str(s)
                    label = self.normalize_text(str(o))
                    if label and label not in self.label_to_id:
                        self.label_to_id[label] = concept_id
                        self.matcher.add_word(label, (label, concept_id))
                        count += 1

        # finalize automaton
        self.matcher.make_automaton()
        
        if count > 0:
            logger.info(f"TESEO Matcher initialized with {count} labels.")
        else:
            logger.warning("No labels found in TESEO ontology.")

    async def precompute_embeddings(self, vector_engine):
        """
        Precomputes embeddings for all unique labels in the automaton using batching.
        Call this once at startup if semantic scoring is needed.
        """
        labels = list(self.label_to_id.keys())
        if not labels:
            return
            
        logger.info(f"Precomputing embeddings for {len(labels)} TESEO labels...")
        import numpy as np
        
        batch_size = 500
        for i in range(0, len(labels), batch_size):
            batch = labels[i:i+batch_size]
            try:
                emb_batch = await vector_engine.compute_embeddings_batch(batch)
                for j, label in enumerate(batch):
                    self.label_embeddings[label] = np.array(emb_batch[j])
                    self.label_norms[label] = np.linalg.norm(self.label_embeddings[label])
            except Exception as e:
                logger.error(f"Failed to precompute embeddings for a batch of TESEO labels: {e}")
                
        logger.info(f"TESEO label embeddings cached successfully ({len(self.label_embeddings)} loaded).")

    def extract_topics_with_embedding(self, text: str, text_embedding: list) -> List[Dict]:
        """
        Finds all TESEO concepts in the text and scores them using a precomputed document embedding.
        Returns a list of concepts with their cosine similarity score (O(1) memory lookup).
        """
        if not text:
            return []
            
        norm_text = self.normalize_text(text)
        matches = []
        seen_ids = set()

        for end_index, (label, concept_id) in self.matcher.iter(norm_text):
            start_index = end_index - len(label) + 1
            
            # Check boundaries to avoid substring matches
            is_start_boundary = start_index == 0 or not norm_text[start_index - 1].isalnum()
            is_end_boundary = end_index == len(norm_text) - 1 or not norm_text[end_index + 1].isalnum()
            
            if is_start_boundary and is_end_boundary and concept_id not in seen_ids:
                matches.append({
                    "teseo_id": concept_id,
                    "label": label,
                    "score": 1.0 # Default fallback
                })
                seen_ids.add(concept_id)
        
        if not matches or not text_embedding:
            return matches

        # In-memory semantic scoring
        import numpy as np
        try:
            text_emb = np.array(text_embedding)
            norm_text_emb = np.linalg.norm(text_emb)
            
            if norm_text_emb > 0:
                for match in matches:
                    label = match["label"]
                    if label in self.label_embeddings:
                        label_emb = self.label_embeddings[label]
                        norm_label_emb = self.label_norms.get(label, 0)
                        
                        if norm_label_emb > 0:
                            sim = np.dot(text_emb, label_emb) / (norm_text_emb * norm_label_emb)
                            match["score"] = float(sim)
        except Exception as e:
            logger.error(f"Error computing in-memory semantic score: {e}")

        return matches

    async def extract_topics(self, text: str, vector_engine=None) -> List[Dict]:
        """
        Finds all TESEO concepts in the normalized text.
        Returns a list of unique concepts with scores based on cosine similarity if vector_engine is provided.
        """
        if not text:
            return []
            
        norm_text = self.normalize_text(text)
        matches = []
        seen_ids = set()

        for end_index, (label, concept_id) in self.matcher.iter(norm_text):
            start_index = end_index - len(label) + 1
            
            # Check boundaries to avoid substring matches (e.g., "sole" in "console")
            is_start_boundary = start_index == 0 or not norm_text[start_index - 1].isalnum()
            is_end_boundary = end_index == len(norm_text) - 1 or not norm_text[end_index + 1].isalnum()
            
            if is_start_boundary and is_end_boundary and concept_id not in seen_ids:
                matches.append({
                    "teseo_id": concept_id,
                    "label": label,
                    "score": 1.0 # Default if no vector engine
                })
                seen_ids.add(concept_id)
        
        if not matches or not vector_engine:
            return matches

        # Semantic scoring via cosine similarity
        try:
            import numpy as np
            texts_to_embed = [text] + [m["label"] for m in matches]
            embeddings = await vector_engine.compute_embeddings_batch(texts_to_embed)
            
            text_emb = np.array(embeddings[0])
            norm_text_emb = np.linalg.norm(text_emb)
            
            for i, match in enumerate(matches):
                if norm_text_emb == 0:
                    break
                label_emb = np.array(embeddings[i+1])
                norm_label_emb = np.linalg.norm(label_emb)
                if norm_label_emb == 0:
                    continue
                
                sim = np.dot(text_emb, label_emb) / (norm_text_emb * norm_label_emb)
                match["score"] = float(sim)
        except Exception as e:
            logger.error(f"Error computing semantic score: {e}")

        return matches
