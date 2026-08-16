import re
import logging
from typing import List, Dict, Set
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import SKOS
import ahocorasick
from src.config import settings

logger = logging.getLogger(__name__)

class TESEOMatcher:
    """
    Semantic engine for linking legal text to the TESEO thesaurus.
    Uses Aho-Corasick for O(n) string matching of prefLabels and altLabels,
    and Full Semantic Matching (Dense) via numpy broadcasting.
    """

    def __init__(self, rdf_path: str = None):
        self.matcher = ahocorasick.Automaton()
        self.label_to_id = {}
        self.label_embeddings = {}
        self.label_norms = {}
        self.last_query_embedding = None
        self._embedding_matrix_normalized = None
        self._matrix_labels = None
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
            fmt = "turtle" if filepath.endswith(".ttl") else "xml"
            g.parse(filepath, format=fmt)
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
        import pickle
        import os
        
        cache_file = "data/external/teseo_embeddings.pkl"
        if os.path.exists(cache_file):
            logger.info(f"Loading TESEO label embeddings from cache: {cache_file}")
            try:
                with open(cache_file, "rb") as f:
                    cache_data = pickle.load(f)
                    self.label_embeddings = cache_data.get("label_embeddings", {})
                    self.label_norms = cache_data.get("label_norms", {})
                logger.info(f"TESEO label embeddings cached successfully ({len(self.label_embeddings)} loaded).")
                self._initialize_dense_matrix()
                return
            except Exception as e:
                logger.error(f"Failed to load cache: {e}. Recomputing...")
        
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
                
        try:
            with open(cache_file, "wb") as f:
                pickle.dump({
                    "label_embeddings": self.label_embeddings,
                    "label_norms": self.label_norms
                }, f)
            logger.info(f"TESEO label embeddings saved to cache ({len(self.label_embeddings)} loaded).")
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")

        # Inizializza la matrice per il dense matching
        self._initialize_dense_matrix()

    def _initialize_dense_matrix(self):
        """Builds the normalized numpy matrix for fast broadcasting."""
        import numpy as np
        labels_list = list(self.label_embeddings.keys())
        if not labels_list:
            return
            
        vectors = [self.label_embeddings[l] for l in labels_list]
        matrix = np.array(vectors, dtype=np.float32)
        
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        self._embedding_matrix_normalized = matrix / norms
        self._matrix_labels = labels_list
        logger.info(f"Initialized Dense Matching matrix of shape {self._embedding_matrix_normalized.shape}")

    def dense_match_all(self, query_embedding: list[float], threshold: float = None, max_results: int = None) -> List[Dict]:
        """
        Confronta un embedding con TUTTI i concetti TESEO tramite dot product broadcasting.
        Restituisce i concetti con score >= threshold.
        """
        if self._embedding_matrix_normalized is None:
            return []
            
        threshold = threshold or settings.TESEO_DENSE_THRESHOLD
        max_results = max_results or settings.TESEO_MAX_CONCEPTS
        
        import numpy as np
        query_vec = np.array(query_embedding, dtype=np.float32)
        query_norm = np.linalg.norm(query_vec)
        if query_norm == 0:
            return []
        query_normalized = query_vec / query_norm
        
        scores = self._embedding_matrix_normalized @ query_normalized
        
        mask = scores >= threshold
        indices = np.where(mask)[0]
        
        results = []
        for idx in indices:
            label = self._matrix_labels[idx]
            concept_id = self.label_to_id.get(label)
            if concept_id:
                results.append({"teseo_id": concept_id, "label": label, "score": float(scores[idx])})
        
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:max_results]

    def _aho_corasick_match(self, text: str) -> List[Dict]:
        """Esegue il match lessicale esatto tramite automa Aho-Corasick."""
        norm_text = self.normalize_text(text)
        matches = []
        seen_ids = set()

        for end_index, (label, concept_id) in self.matcher.iter(norm_text):
            start_index = end_index - len(label) + 1
            
            is_start_boundary = start_index == 0 or not norm_text[start_index - 1].isalnum()
            is_end_boundary = end_index == len(norm_text) - 1 or not norm_text[end_index + 1].isalnum()
            
            if is_start_boundary and is_end_boundary and concept_id not in seen_ids:
                matches.append({
                    "teseo_id": concept_id,
                    "label": label,
                    "score": settings.TESEO_SPARSE_BOOST
                })
                seen_ids.add(concept_id)
        return matches

    def extract_topics_with_embedding(self, text: str, text_embedding: list) -> List[Dict]:
        """
        Esegue matching ibrido: match esatti Aho-Corasick + Full Semantic Matching.
        """
        if not text:
            return []
            
        # 1. Sparse
        sparse_matches = self._aho_corasick_match(text)
        
        # 2. Dense
        dense_matches = []
        if text_embedding and self._embedding_matrix_normalized is not None:
            dense_matches = self.dense_match_all(text_embedding)
            
        # 3. Fusione: sparse ha priorità (sovrascrive eventuali score inferiori del dense)
        results = {}
        for m in dense_matches:
            results[m["teseo_id"]] = m
        for m in sparse_matches:
            results[m["teseo_id"]] = m
            
        final = sorted(results.values(), key=lambda x: x["score"], reverse=True)
        return final[:settings.TESEO_MAX_CONCEPTS]

    async def extract_topics(self, text: str, vector_engine=None) -> List[Dict]:
        """
        Versione asincrona usata dal QueryAnalyzer. Supporta il matching ibrido se c'è un vector_engine.
        """
        if not text:
            return []
            
        # 1. Sparse
        sparse_matches = self._aho_corasick_match(text)
        
        if not vector_engine:
            return sparse_matches
            
        # 2. Dense on the fly
        try:
            embeddings = await vector_engine.compute_embeddings_batch([text])
            if embeddings:
                self.last_query_embedding = embeddings[0]
            else:
                return sparse_matches
            
            dense_matches = []
            if self._embedding_matrix_normalized is not None:
                dense_matches = self.dense_match_all(self.last_query_embedding)
                
            # Fusione
            results = {}
            for m in dense_matches:
                results[m["teseo_id"]] = m
            for m in sparse_matches:
                results[m["teseo_id"]] = m
                
            final = sorted(results.values(), key=lambda x: x["score"], reverse=True)
            return final[:settings.TESEO_MAX_CONCEPTS]
            
        except Exception as e:
            logger.error(f"Error computing semantic score in extract_topics: {e}")
            return sparse_matches
