"""Tests for the Python-to-notebook converter."""


import nbformat

from workflow_generator.converter import python_to_notebook


def test_basic_conversion(tmp_path):
    """A simple script with section markers should produce multiple cells."""
    script = tmp_path / "test_workflow.py"
    script.write_text("""\
#!/usr/bin/env python3
# --- Workflow: Test ---
# This is a test workflow.

import sys
sys.path.insert(0, "/opt/homebrew/opt/pegasus/lib/pegasus/python")

# --- Section: Setup ---
# Configure the properties.
x = 1

# --- Section: Build DAG ---
# Create the workflow graph.
y = x + 1
print(y)
""")

    nb_path = python_to_notebook(script)
    assert nb_path.exists()
    assert nb_path.suffix == ".ipynb"

    nb = nbformat.read(str(nb_path), as_version=4)
    # Should have: header markdown, import code, Setup markdown, setup code,
    # Build DAG markdown, dag code
    assert len(nb.cells) >= 4

    # First cell should be markdown (header)
    assert nb.cells[0].cell_type == "markdown"
    assert "Test" in nb.cells[0].source


def test_custom_output_path(tmp_path):
    script = tmp_path / "wf.py"
    script.write_text("x = 1\n")

    out = tmp_path / "custom.ipynb"
    result = python_to_notebook(script, out)
    assert result == out
    assert out.exists()
