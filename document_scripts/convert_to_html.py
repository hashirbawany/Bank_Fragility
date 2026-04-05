"""
Converts the EDA notebook to a self-contained HTML file.
Output is saved to the documents/ folder alongside the notebook.
"""

import subprocess
import sys
from pathlib import Path

ROOT      = Path(__file__).resolve().parent.parent
NOTEBOOK  = ROOT / "documents" / "eda.ipynb"
OUTPUT    = ROOT / "documents" / "eda.html"


def main() -> None:
    if not NOTEBOOK.exists():
        raise FileNotFoundError(f"Notebook not found: {NOTEBOOK}")

    print(f"Converting {NOTEBOOK.name} to HTML...")

    result = subprocess.run(
        [
            sys.executable, "-m", "nbconvert",
            "--to", "html",
            "--no-input",               # hides code cells, shows outputs only
            "--output", str(OUTPUT),
            str(NOTEBOOK),
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print("nbconvert failed:")
        print(result.stderr)
        sys.exit(1)

    print(f"Saved -> {OUTPUT}")


if __name__ == "__main__":
    main()
