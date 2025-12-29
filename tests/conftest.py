"""Pytest configuration - adds src/ to Python path."""
import sys
from pathlib import Path

# Add src/ directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
