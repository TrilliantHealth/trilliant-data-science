"""Identity of the gent package's repo location and user-facing files.

These constants back URLs that get shown to users (error messages in
utils.py, the generated bare-repo README in readme.py). Update them
together with file renames or moves; the drift-catching tests in
tests/test_gent/test_repo.py assert the referenced files exist on disk.
"""

from __future__ import annotations

GENT_REPO_PATH = "libs/gent"

# Files inside libs/gent that we link users to.
GENT_README = "README.md"
GENT_BARE_SETUP = "BARE_SETUP.md"

GITHUB_REPO_BASE = "https://github.com/TrilliantHealth/trilliant-data-science"


def github_tree_url(branch: str = "main", path: str = "") -> str:
    """Build a /tree/<branch>/<path> URL into the canonical repo."""
    suffix = f"/{path}" if path else ""
    return f"{GITHUB_REPO_BASE}/tree/{branch}{suffix}"
