"""Pytest configuration for document-graph tests."""

import sys
from pathlib import Path

# Add src to path so tests can import document_graph
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
