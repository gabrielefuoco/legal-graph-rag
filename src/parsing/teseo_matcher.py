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
        if rdf_path:
            self.load_ontology(rdf_path)

    def normalize_text(self, text: str) -> str:
        """Lowercases and removes punctuation for baseline matching."""
        if not text:
            return ""
        text = text.lower()
        # Remove anything that isn't alphanumeric or space
        text = re.sub(r'[^\w\s]', '', text)
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

    def extract_topics(self, text: str) -> List[Dict]:
        """
        Finds all TESEO concepts in the normalized text.
        Returns a list of unique concepts with scores (currently 1.0 for exact match).
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
                    "score": 1.0 # TODO: Implementar score semantico - ATTUALMENTE HARDCODED
                })
                seen_ids.add(concept_id)
        
        return matches
