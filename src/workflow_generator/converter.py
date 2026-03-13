"""Convert generated workflow Python scripts to Jupyter notebooks (.ipynb).

Splits Python source into notebook cells using section markers:
  # --- Section: Name ---   -> Markdown heading cell + following code cell
  Module docstring           -> First Markdown cell
  Inline comments above code -> Extracted into Markdown explanation cells

The resulting notebooks are suitable for interactive testing in JupyterLab,
where each section can be executed independently.
"""

from __future__ import annotations

import re
from pathlib import Path

import nbformat
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook


SECTION_PATTERN = re.compile(r"^\s*#\s*---\s*Section:\s*(.+?)\s*---\s*$")
HEADER_COMMENT = re.compile(r"^\s*#(?!\s*---)")


def python_to_notebook(source_path: Path, output_path: Path | None = None) -> Path:
    """Convert a workflow Python script to a Jupyter notebook.

    Args:
        source_path: Path to the .py file.
        output_path: Path for the .ipynb file. Defaults to same name with .ipynb extension.

    Returns:
        Path to the created notebook file.
    """
    if output_path is None:
        output_path = source_path.with_suffix(".ipynb")

    source = source_path.read_text()
    lines = source.splitlines(keepends=True)

    nb = new_notebook()
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }

    cells: list = []

    # Extract module-level docstring or header comments as first markdown cell
    header_lines, body_start = _extract_header(lines)
    if header_lines:
        cells.append(new_markdown_cell("".join(header_lines)))

    # Parse remaining lines into section-delimited cells
    current_code: list[str] = []
    i = body_start

    while i < len(lines):
        line = lines[i]
        section_match = SECTION_PATTERN.match(line)

        if section_match:
            # Flush any accumulated code
            if current_code:
                code_text = "".join(current_code).strip()
                if code_text:
                    cells.append(new_code_cell(code_text))
                current_code = []

            # Create markdown cell for the section heading
            section_name = section_match.group(1)
            # Collect comment lines immediately after the section marker
            section_comments = [f"## {section_name}\n"]
            i += 1
            while i < len(lines) and HEADER_COMMENT.match(lines[i]):
                comment_text = re.sub(r"^\s*#\s?", "", lines[i])
                section_comments.append(comment_text)
                i += 1
            cells.append(new_markdown_cell("".join(section_comments)))
            continue

        current_code.append(line)
        i += 1

    # Flush final code block
    if current_code:
        code_text = "".join(current_code).strip()
        if code_text:
            cells.append(new_code_cell(code_text))

    nb.cells = cells
    nbformat.write(nb, str(output_path))
    return output_path


def _extract_header(lines: list[str]) -> tuple[list[str], int]:
    """Extract the script header (shebang + top comment block or docstring).

    Returns (header_lines_as_markdown, index_of_first_non_header_line).
    """
    header: list[str] = []
    i = 0

    # Skip shebang
    if i < len(lines) and lines[i].startswith("#!"):
        i += 1

    # Collect leading comment block (the workflow description).
    # Include all comment lines, even those with '---', as long as they
    # are NOT "# --- Section: ..." markers (which delimit notebook cells).
    while i < len(lines):
        line = lines[i]
        if SECTION_PATTERN.match(line):
            break  # A section marker ends the header
        elif line.strip().startswith("#"):
            header.append(re.sub(r"^\s*#\s?", "", line))
            i += 1
        elif line.strip() == "":
            if header:  # blank line after comments = end of header
                i += 1
                break
            i += 1
        else:
            break

    return header, i
