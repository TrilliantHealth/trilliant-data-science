"""Markdown documentation renderer for tabularasa schemas.

This module mirrors the structure of sphinx.py but outputs markdown format
suitable for MkDocs documentation.
"""

import os
import re
import textwrap
import urllib.parse
from functools import lru_cache
from itertools import chain
from operator import itemgetter
from pathlib import Path
from typing import Any, Dict, Final, Iterable, List, Mapping, Optional, Sequence, Tuple, Union
from warnings import warn

import networkx as nx
from pydantic import AnyUrl

from thds.tabularasa.schema import metaschema
from thds.tabularasa.schema.dtypes import DType
from thds.tabularasa.schema.files import FileSourceMixin, LocalFileSourceMixin, TabularFileSource
from thds.tabularasa.schema.util import snake_to_title

# Column headers
METADATA_COLUMNS: Final = ("Name", "Value")
TABLE_COLUMNS: Final = ("Name", "Type", "Nullable?", "Description")
SOURCE_COLUMNS: Final = (
    "Source",
    "Update Frequency",
    "Last Checked",
    "Last Changed",
    "Contributes To",
    "Authority",
)

BADGE_EXTENSION: Final = "*.svg"
DERIVATION_TITLE: Final = "Derivation"
DEPENDENCIES_TITLE: Final = "Sources"
METADATA_FIELDS = FileSourceMixin.model_fields
UNICODE_MAPPING: Final = {
    ">=": ">=",
    "<=": "<=",
    ">": ">",
    "<": "<",
}
MISSING_BADGE_MSG: Final = (
    "Curation badges could not be rendered. Make sure that curation_badge_path "
    "and source_docs_path are both supplied in schema.build_options."
)

# For detecting RST heading underlines to convert
RST_HEADING_UNDERLINE_CHARS = '=-^"'
RST_HEADING_UNDERLINE_RE = re.compile(
    "|".join(f"({re.escape(c)})+" for c in RST_HEADING_UNDERLINE_CHARS)
)


class DontSplitMe(str):
    """A string that should not be split by textwrap."""

    pass


def _wrap_table_field(max_width: int, text: Any) -> str:
    """Wrap a table field to the specified max width."""
    if isinstance(text, (AnyUrl, DontSplitMe)):
        return str(text)
    wrapped = textwrap.wrap(str(text), width=max_width, break_long_words=False)
    return " ".join(wrapped) if wrapped else ""


def split_long_fields(
    table_data: Iterable[Sequence], max_field_width: int = 80
) -> List[Tuple[str, ...]]:
    """Splits long row fields by wrapping them."""
    return [tuple(_wrap_table_field(max_field_width, field) for field in row) for row in table_data]


def join_blocks(blocks: Iterable[str], sep: str) -> str:
    """Join non-empty blocks with a separator."""
    return sep.join(filter(bool, blocks))


def heading(title: str, level: int) -> str:
    """Create a markdown heading at the specified level."""
    return f"{'#' * level} {title}"


def bold(text: str) -> str:
    """Bold text in markdown."""
    return f"**{text}**"


def italic(text: str) -> str:
    """Italic text in markdown."""
    return f"*{text}*"


def front_matter(title: str) -> str:
    """Create YAML front matter with title for MkDocs navigation."""
    return f'---\ntitle: "{title}"\n---'


def anchor(label: str) -> str:
    """Create an HTML anchor for cross-referencing."""
    return f'<a id="{label}"></a>'


def crossref(label: str) -> str:
    """Create a markdown link to an anchor in the same document."""
    return f"[{label}](#{label})"


def type_crossref(label: str) -> str:
    """Create a markdown link to a type definition in types.md.

    Type documentation is generated in a separate file (types.md), so links from
    table documentation files need to reference ../types.md#type_name instead of
    just #type_name.
    """
    return f"[{label}](../types.md#{label})"


def docref(doc_path: str) -> str:
    """Create a markdown link to another document."""
    return f"[{doc_path}]({doc_path}.md)"


def hyperlink(link_text: str, link: str) -> DontSplitMe:
    """Create a markdown hyperlink."""
    return DontSplitMe(f"[{link_text}]({link})")


def escape_markdown(text: str) -> str:
    """Escape special markdown characters in text."""
    # Escape pipe characters for table cells
    return text.replace("|", "\\|")


def escape_for_display(text: str) -> str:
    """Apply Unicode mapping for display characters."""
    for i, o in UNICODE_MAPPING.items():
        text = text.replace(i, o)
    return text


@lru_cache(maxsize=1)
def _get_tabulate() -> Optional[Any]:
    """Lazily import tabulate."""
    try:
        from tabulate import tabulate

        return tabulate
    except ImportError:
        warn(
            "tabulate is unavailable; can't render markdown documentation. "
            "Specify the 'cli' extra to ensure this dependency is present."
        )
        return None


def render_table(
    header: Tuple[str, ...],
    rows: Iterable[Sequence],
    max_field_width: Optional[int] = 80,
) -> str:
    """Render a markdown pipe table."""
    tabulate = _get_tabulate()
    if tabulate is None:
        raise RuntimeError("can't render tables in markdown without `tabulate` dependency")
    processed_rows = list(rows) if max_field_width is None else split_long_fields(rows, max_field_width)
    return tabulate(
        processed_rows,
        headers=header,
        tablefmt="pipe",
    )


def render_image(img_path: Path) -> str:
    """Render a markdown image."""
    return f"![{img_path.name}]({img_path})"


def convert_rst_headings_to_markdown(markup_text: str, base_level: int = 3) -> str:
    """Convert RST headings to markdown headings.

    RST heading levels are determined by underline characters:
    = is level 1, - is level 2, ^ is level 3, " is level 4

    Args:
        markup_text: Text that may contain RST headings
        base_level: The markdown heading level to start at for converted headings

    Returns:
        Text with RST headings converted to markdown
    """
    output = []
    lines = markup_text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        # Check if next line is an underline
        if i + 1 < len(lines):
            next_line = lines[i + 1]
            if (
                len(line) > 0
                and len(next_line) == len(line)
                and RST_HEADING_UNDERLINE_RE.fullmatch(next_line)
            ):
                # Determine RST level from underline character
                rst_level = RST_HEADING_UNDERLINE_CHARS.index(next_line[0]) + 1
                md_level = base_level + rst_level - 1
                output.append(heading(line, md_level))
                i += 2  # Skip both the title and underline
                continue
        output.append(line)
        i += 1

    return "\n".join(output)


# Column Rendering


def render_column_type(
    column_type: Union[
        DType,
        metaschema.AnonCustomType,
        metaschema.CustomType,
        metaschema.ArrayType,
        metaschema.MappingType,
    ],
) -> str:
    """Render the type of a column for display."""
    if isinstance(column_type, DType):
        return column_type.value
    elif isinstance(column_type, metaschema.CustomType):
        return type_crossref(column_type.name)
    elif isinstance(column_type, metaschema.AnonCustomType):
        return column_type.type.value
    elif isinstance(column_type, metaschema.ArrayType):
        return "list[" + render_column_type(column_type.values) + "]"
    elif isinstance(column_type, metaschema.MappingType):
        return (
            "map["
            + render_column_type(column_type.keys)
            + ": "
            + render_column_type(column_type.values)
            + "]"
        )
    else:
        return str(column_type)


def render_column_name(column: metaschema.Column, tbl: metaschema.Table) -> str:
    """Format column name specially if it is part of the table's primary key."""
    name = column.name
    return bold(name) if tbl.primary_key and name in tbl.primary_key else name


def render_column_table(tbl: metaschema.Table) -> str:
    """Render the column definitions table for a schema table."""
    return render_table(
        TABLE_COLUMNS,
        (
            (
                render_column_name(c, tbl),
                render_column_type(c.type),
                str(c.nullable),
                c.doc.replace("\n", " "),
            )
            for c in tbl.columns
        ),
    )


# Derivation Rendering


def render_derivation_doc(tbl: metaschema.Table) -> str:
    """Render derivation documentation for a table."""
    derivation_docs = tbl.dependencies.docstring if tbl.dependencies else None
    if derivation_docs:
        converted_docs = convert_rst_headings_to_markdown(derivation_docs)
        return join_blocks(
            [
                heading(DERIVATION_TITLE, 2),
                converted_docs,
            ],
            "\n\n",
        )
    else:
        return ""


# File metadata rendering


def format_repo_url(
    file: LocalFileSourceMixin,
    repo_root: Path,
    repo_url: str,
    name: Optional[str] = None,
) -> DontSplitMe:
    """Format a repository URL as a markdown link."""
    relative_file_path = str(file.full_path.absolute().relative_to(repo_root.absolute()))
    file_path_url = urllib.parse.quote(relative_file_path)
    url = f"{repo_url.rstrip('/')}/{file_path_url}"
    return hyperlink(name or relative_file_path, url)


def extract_file_sources(
    tbl: metaschema.Table,
    schema: metaschema.Schema,
) -> Dict[str, FileSourceMixin]:
    """Iterate through a table's dependencies, gathering metadata."""

    def inner(tbl: metaschema.Table, schema: metaschema.Schema):
        if isinstance(tbl.dependencies, metaschema.RawDataDependencies):
            for dep_name in tbl.dependencies.local:
                local_dep = schema.local_data[dep_name]
                yield metaschema.LocalRef(dep_name), local_dep
            for dep_name in tbl.dependencies.adls:
                remote_dep = schema.remote_data[dep_name]
                yield metaschema.ADLSRef(dep_name), remote_dep
            for dep_name in tbl.dependencies.reference:
                table_dep = schema.tables[dep_name]
                yield from inner(table_dep, schema)
        elif isinstance(tbl.dependencies, TabularFileSource):
            yield metaschema.TabularTextFileRef(tbl.name), tbl.dependencies

    return dict(inner(tbl, schema))


def render_file_source(
    meta: FileSourceMixin,
    header_title: Optional[str],
    repo_root: Path,
    repo_url: Optional[str] = None,
) -> str:
    """Render a metadata section for either a table or a dependency."""
    meta_dict = {name: getattr(meta, name) for name in METADATA_FIELDS if getattr(meta, name)}
    # Convert AnyUrl values to clickable markdown links so they render properly in tables
    for key, value in meta_dict.items():
        if isinstance(value, AnyUrl):
            meta_dict[key] = hyperlink(str(value), str(value))
    if repo_url and isinstance(meta, LocalFileSourceMixin):
        meta_dict.update(github_link=format_repo_url(meta, repo_root, repo_url))
    header = heading(header_title, 3) if header_title else ""
    if meta_dict:
        parts = [header, render_table(METADATA_COLUMNS, meta_dict.items())]
    else:
        parts = [header, "Not Available"]

    return join_blocks(parts, "\n\n")


def render_dependencies_doc(
    file_sources: Mapping[str, FileSourceMixin],
    repo_root: Path,
    repo_url: Optional[str] = None,
) -> str:
    """Render dependency documentation with a metadata table for each dependency."""
    if file_sources:
        sole_source = len(file_sources) == 1
        file_docs = (
            render_file_source(
                source,
                None if sole_source else snake_to_title(name, separator=" "),
                repo_root,
                repo_url,
            )
            for name, source in sorted(file_sources.items(), key=itemgetter(0))
        )
        return join_blocks(chain([heading(DEPENDENCIES_TITLE, 2)], file_docs), "\n\n")
    else:
        return ""


# Table Rendering


def render_table_doc(
    tbl: metaschema.Table,
    schema: metaschema.Schema,
    repo_root: Path,
    repo_url: Optional[str] = None,
) -> str:
    """Render a table's documentation in markdown format."""
    file_sources = extract_file_sources(tbl, schema)
    parts = [
        front_matter(tbl.doc_title),
        anchor(tbl.name),
        heading(tbl.doc_title, 1),
        tbl.doc,
        render_column_table(tbl),
        render_derivation_doc(tbl),
        render_dependencies_doc(file_sources, repo_root, repo_url),
    ]
    return join_blocks(parts, "\n\n")


def render_type_entry(custom_type: metaschema.CustomType) -> str:
    """Render a custom type's constraints as a list."""
    entries: List[str] = [custom_type.type.name.lower()]
    entries.extend(
        map(escape_for_display, filter(None, (c.comment_expr() for c in custom_type.constraints)))
    )
    enum = custom_type.enum
    if enum is not None:
        entries.append("one of " + ", ".join(map("`{}`".format, sorted(enum.enum))))

    return "\n".join(map("- {}".format, entries))


def render_constraint_docs(custom_type: metaschema.CustomType) -> str:
    """Render documentation for a custom type's constraints."""
    return custom_type.docstring or ""


def render_type_doc(custom_type: metaschema.CustomType) -> str:
    """Render a custom type's documentation in markdown format."""
    parts = [
        anchor(custom_type.name),
        heading(custom_type.class_name, 2),
        render_constraint_docs(custom_type),
        render_type_entry(custom_type),
    ]
    return join_blocks(parts, "\n\n")


def render_types_doc(schema: metaschema.Schema) -> str:
    """Render documentation for all custom types in the schema."""
    CUSTOM_TYPES = "Custom Types"
    return join_blocks(
        chain(
            [heading(CUSTOM_TYPES, 1)],
            map(render_type_doc, sorted(schema.types.values(), key=lambda t: t.class_name)),
        ),
        "\n\n",
    )


# Source Data + Curation Report Rendering


def render_curation_status(schema: metaschema.Schema, repo_root: Path) -> str:
    """Render the curation status section with badges."""
    CURATION_STATUS = "Curation Status"
    curation_badge_path = Path(p) if (p := schema.build_options.curation_badge_path) else None
    source_docs_path = Path(p) if (p := schema.build_options.source_docs_path) else None

    if (curation_badge_path is not None) and (source_docs_path is not None):
        badge_relpath = Path(os.path.relpath(curation_badge_path, source_docs_path.parent))
        badge_list = [bp.name for bp in (repo_root / curation_badge_path).glob(BADGE_EXTENSION)]
        curation_badge_block = [render_image(badge_relpath / b) for b in sorted(badge_list)]
    else:
        curation_badge_block = [italic(MISSING_BADGE_MSG)]

    return join_blocks(
        chain(
            [heading(CURATION_STATUS, 2)],
            curation_badge_block,
        ),
        "\n\n",
    )


def render_source_name(
    fs_name: str,
    fs_data: FileSourceMixin,
    repo_root: Path,
    repo_url: Optional[str] = None,
) -> DontSplitMe:
    """Render a source name with associated links."""
    links = []
    if fs_data.landing_page is not None:
        links.append(hyperlink("homepage", str(fs_data.landing_page)))
    if fs_data.url is not None:
        links.append(hyperlink("url", str(fs_data.url)))
    if repo_url and isinstance(fs_data, LocalFileSourceMixin):
        links.append(format_repo_url(fs_data, repo_root, repo_url, name="github"))

    return DontSplitMe(f"{fs_name} ({' | '.join(links)})" if links else fs_name)


def render_package_table_links(
    fs_name: str,
    schema: metaschema.Schema,
    dep_graph: nx.DiGraph,
    table_docs_relpath: Optional[Path] = None,
) -> str:
    """Get a list of all package tables that the source data contributes to."""
    descendants = chain.from_iterable(nx.dfs_successors(dep_graph, fs_name).values())
    pkg_tables = [
        table if table_docs_relpath is None else docref(str(table_docs_relpath / table))
        for table in descendants
        if not schema.tables[str(table)].transient
    ]
    return "; ".join(pkg_tables)


def render_source_info(
    fs_name: str,
    fs_data: FileSourceMixin,
    schema: metaschema.Schema,
    dep_graph: nx.DiGraph,
    repo_root: Path,
    repo_url: Optional[str] = None,
    table_docs_relpath: Optional[Path] = None,
) -> List[Any]:
    """Render source information as a table row."""
    return [
        render_source_name(fs_name, fs_data, repo_root, repo_url),
        fs_data.update_frequency,
        fs_data.last_checked,
        fs_data.last_updated,
        render_package_table_links(fs_name, schema, dep_graph, table_docs_relpath),
        fs_data.authority,
    ]


def build_source_metadata(schema: metaschema.Schema, repo_root: Path) -> Dict[str, List[Any]]:
    """Build metadata for all source data in the schema."""
    OPEN_ACCESS = "Open Access Data Sources"
    LICENSED = "Licensed Data Sources"
    INTERNAL = "Internal Data Sources"
    source_meta: Dict[str, List[Any]] = {OPEN_ACCESS: [], LICENSED: [], INTERNAL: []}
    sources: set = set()
    dep_graph = schema.dependency_dag()

    table_docs_dir = Path(p) if (p := schema.build_options.table_docs_dir) else None
    source_docs_path = Path(p) if (p := schema.build_options.source_docs_path) else None
    if (table_docs_dir is not None) and (source_docs_path is not None):
        table_docs_relpath = Path(os.path.relpath(table_docs_dir, source_docs_path.parent))
    else:
        table_docs_relpath = None

    for table in schema.package_tables:
        file_sources = extract_file_sources(table, schema)
        for fs_name, fs_data in file_sources.items():
            if fs_name in sources:
                continue
            sources.add(fs_name)
            fs_data_fmt = render_source_info(
                fs_name,
                fs_data,
                schema,
                dep_graph,
                table_docs_relpath=table_docs_relpath,
                repo_root=repo_root,
                repo_url=(str(schema.build_options.repo_url) if schema.build_options.repo_url else None),
            )
            if fs_data.is_open_access:
                stype = OPEN_ACCESS
            elif fs_data.authority == "Trilliant":
                stype = INTERNAL
            else:
                stype = LICENSED
            source_meta[stype].append(fs_data_fmt)

    for list_ in source_meta.values():
        list_.sort(key=lambda list_: list_[0].lower())

    return source_meta


def render_source_data_tables(schema: metaschema.Schema, repo_root: Path) -> str:
    """Render source data tables grouped by access type."""
    source_metadata = build_source_metadata(schema, repo_root)
    parts = []
    for k, source_data in source_metadata.items():
        parts.append(heading(k, 3))
        parts.append(render_table(SOURCE_COLUMNS, source_data))
    return join_blocks(parts, "\n\n")


def render_source_doc(
    schema: metaschema.Schema,
    repo_root: Path,
) -> str:
    """Render the source data documentation page."""
    if schema.build_options.source_docs_path is None:
        return ""

    REPORT_TITLE = "Source Data Updates & Curation Report"
    SOURCE_TITLE = "Source Data Update Status"

    parts = [
        heading(REPORT_TITLE, 1),
        render_curation_status(schema, repo_root),
        heading(SOURCE_TITLE, 2),
        render_source_data_tables(schema, repo_root),
    ]
    return join_blocks(parts, "\n\n")


def render_markdown_docs(
    schema: metaschema.Schema,
    repo_root: Path,
    repo_url: Optional[str] = None,
) -> Tuple[str, str, Dict[str, str]]:
    """Render all documentation in markdown format.

    Returns:
        A tuple of (types_doc, source_doc, table_docs) where:
        - types_doc: Markdown documentation for custom types
        - source_doc: Markdown documentation for source data
        - table_docs: Dict mapping table names to their markdown documentation
    """
    return (
        render_types_doc(schema),
        render_source_doc(schema, repo_root),
        {
            table.name: render_table_doc(
                table,
                schema,
                repo_root=repo_root,
                repo_url=repo_url,
            )
            for table in schema.package_tables
        },
    )
