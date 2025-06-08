# scripts/project_root.py

"""
Sets up the root of the project dynamically so scripts can import modules cleanly.
"""

import sys
from pathlib import Path

def add_project_root():
    """
    Adds the root of the project (the directory containing 'scripts/') to sys.path.
    """
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent  # Goes from scripts/ to rate_test/
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))