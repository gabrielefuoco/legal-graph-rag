"""
Orchestratore Pipeline — Main AKN Parser.

Coordina le tre fasi di parsing (Meta → Body → Edges) e produce un output DocumentDTO validato.
"""
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

from lxml import etree

from src.parsing.namespaces import detect_namespace
from src.parsing.models import DocumentDTO
from src.parsing.meta_parser import parse_meta
from src.parsing.body_parser import parse_body

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main Parser Class
# ---------------------------------------------------------------------------

class AknParser:
    """
    Akoma Ntoso deterministic parser.

    Transforms AKN XML documents into validated DocumentDTO objects,
    ready for Neo4j batch ingestion.
    """

    def __init__(self, recover: bool = True):
        """
        Args:
            recover: If True, lxml will try to recover from malformed XML.
        """
        self.recover = recover

    def parse_file(self, filepath: str) -> DocumentDTO:
        """
        Parse a single Akoma Ntoso XML file.

        Args:
            filepath: Path to the XML file.

        Returns:
            A validated DocumentDTO.

        Raises:
            ValueError: If the file cannot be parsed or has no valid content.
        """
        logger.info(f"Parsing file: {filepath}")
        path = Path(filepath)

        # --- ROB-5/ROB-6: Handle encoding and BOM ---
        xml_bytes = path.read_bytes()

        # Strip BOM if present (ROB-6)
        if xml_bytes.startswith(b"\xef\xbb\xbf"):
            xml_bytes = xml_bytes[3:]
            logger.debug("Stripped UTF-8 BOM")

        # Parse with lxml (ROB-1: recover mode)
        parser = etree.XMLParser(
            recover=self.recover,
            encoding="utf-8",
            remove_blank_text=True,
        )

        try:
            tree = etree.fromstring(xml_bytes, parser=parser)
        except etree.XMLSyntaxError:
            # ROB-5: Prova la codifica latin-1
            logger.warning(f"UTF-8 parsing failed for {filepath}, trying latin-1")
            try:
                text = xml_bytes.decode("latin-1")
                parser_latin = etree.XMLParser(
                    recover=self.recover,
                    remove_blank_text=True,
                )
                tree = etree.fromstring(text.encode("utf-8"), parser=parser_latin)
            except Exception as e:
                raise ValueError(f"Cannot parse XML file {filepath}: {e}") from e

        root = tree if isinstance(tree, etree._Element) else tree.getroot()

        # Rileva la convenzione del namespace
        ns_map = detect_namespace(root)
        logger.debug(f"Detected namespace: {ns_map}")

        # --- Fase A: Meta Parsing ---
        try:
            frbr = parse_meta(root, ns_map)
        except Exception as e:
            logger.error(f"Meta parsing failed: {e}")
            raise

        logger.info(
            f"Fase A completata — URN: {frbr.urn}, Type: {frbr.doc_type}, "
            f"Date: {frbr.date_promulgation}"
        )

        # --- Fase B+C: Body Parsing (con estrazione degli archi integrata) ---
        try:
            nodes, edges = parse_body(root, ns_map, frbr.urn)
        except Exception as e:
            logger.error(f"Body parsing failed: {e}")
            raise

        # --- Output Phase: Build and validate DTO ---
        doc = DocumentDTO(
            frbr=frbr,
            nodes=nodes,
            edges=edges,
        )

        logger.info(
            f"✅ Parse complete — {len(nodes)} nodes, {len(edges)} edges"
        )

        return doc

    def parse_directory(self, dirpath: str) -> list[DocumentDTO]:
        """
        Parse all XML files in a directory recursively.

        Args:
            dirpath: Path to the directory.

        Returns:
            List of successfully parsed DocumentDTO objects.
        """
        path = Path(dirpath)
        if not path.is_dir():
            raise ValueError(f"Not a directory: {dirpath}")

        xml_files = list(path.rglob("*.xml"))
        logger.info(f"Found {len(xml_files)} XML files in {dirpath}")

        results: list[DocumentDTO] = []
        errors: list[tuple[str, str]] = []

        for xml_file in xml_files:
            try:
                doc = self.parse_file(str(xml_file))
                results.append(doc)
            except Exception as e:
                errors.append((str(xml_file), str(e)))
                logger.error(f"Failed to parse {xml_file}: {e}")

        logger.info(
            f"Directory parsing complete: {len(results)} success, "
            f"{len(errors)} failures out of {len(xml_files)} files"
        )

        if errors:
            logger.warning("Failed files:")
            for fpath, err in errors:
                logger.warning(f"  {fpath}: {err}")

        return results


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main():
    """Command-line interface for the AKN parser."""
    parser = argparse.ArgumentParser(
        description="Akoma Ntoso Parser — Parse AKN XML to graph DTOs"
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Input file or directory path",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output JSON file path (default: stdout)",
    )
    parser.add_argument(
        "--recover",
        action="store_true",
        default=True,
        help="Enable XML error recovery (default: True)",
    )
    parser.add_argument(
        "--no-recover",
        action="store_true",
        help="Disable XML error recovery",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    recover = not args.no_recover
    akn_parser = AknParser(recover=recover)

    input_path = Path(args.input)

    if input_path.is_dir():
        documents = akn_parser.parse_directory(str(input_path))
        output_data = [doc.model_dump(mode="json") for doc in documents]
    elif input_path.is_file():
        doc = akn_parser.parse_file(str(input_path))
        output_data = doc.model_dump(mode="json")
    else:
        print(f"Error: {args.input} is not a valid file or directory", file=sys.stderr)
        sys.exit(1)

    # Output
    json_str = json.dumps(output_data, ensure_ascii=False, indent=2, default=str)

    if args.output:
        Path(args.output).write_text(json_str, encoding="utf-8")
        print(f"Output written to {args.output}")
    else:
        print(json_str)


if __name__ == "__main__":
    main()
