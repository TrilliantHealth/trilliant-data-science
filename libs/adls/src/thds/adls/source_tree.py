from thds.core.source.tree import SourceTree

from . import fqn, list_fast, uri


def from_path(adls_path: uri.UriIsh, match_suffix: str = "") -> SourceTree:
    """Creates a SourceTree object where the logical root is the final piece of the
    provided adls path.
    """
    root_fqn = uri.parse_any(adls_path)

    return SourceTree(
        sources=list(
            list_fast.multilayer_yield_sources(
                root_fqn, layers=0, filter_=lambda blob: blob.path.endswith(match_suffix)
            )
        ),
        higher_logical_root=fqn.split(root_fqn)[-1],
    )
