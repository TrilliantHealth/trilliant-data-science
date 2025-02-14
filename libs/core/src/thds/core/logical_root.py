import typing as ty


def find_common_prefix(uris: ty.Iterable[str]) -> ty.List[str]:
    uri_parts_list = [uri.split("/") for uri in uris]
    if not uri_parts_list:
        return list()

    reference = uri_parts_list[0]

    for i, part in enumerate(reference):
        for uri_parts in uri_parts_list[1:]:
            if i >= len(uri_parts) or uri_parts[i] != part:
                if i == 0:
                    raise ValueError(f"Paths have no common prefix: {uris}")
                return reference[:i]
    return reference  # the whole thing must be the common prefix


def find(paths: ty.Iterable[str], higher_logical_root: str = "") -> str:
    common = find_common_prefix(paths)
    if not higher_logical_root:
        return "/".join(common)

    # Split higher_logical_root into components
    root_parts = higher_logical_root.split("/")

    # Look for the sequence of parts in common
    for i in range(len(common) - len(root_parts) + 1):
        if common[i : i + len(root_parts)] == root_parts:
            return "/".join(common[: i + len(root_parts)])

    raise ValueError(f"Higher root '{higher_logical_root}' not found")
