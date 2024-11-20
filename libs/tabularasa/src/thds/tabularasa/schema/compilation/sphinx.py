import os
import re
import textwrap
import urllib.parse
from functools import lru_cache
from itertools import chain
from operator import itemgetter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union
from warnings import warn

import networkx as nx
from pydantic import AnyUrl

from thds.tabularasa.schema import metaschema
from thds.tabularasa.schema.dtypes import DType
from thds.tabularasa.schema.files import FileSourceMixin, LocalFileSourceMixin, TabularFileSource
from thds.tabularasa.schema.util import snake_to_title

# Misc
METADATA_COLUMNS = ("Name", "Value")
TABLE_COLUMNS = ("Name", "Type", "Nullable?", "Description")
SOURCE_COLUMNS = (
    "Source",
    "Update Frequency",
    "Last Checked",
    "Last Changed",
    "Contributes To",
    "Authority",
)
BADGE_EXTENSION = "*.svg"
HEADING_CHAR = "#"
DERIVATION_TITLE = "Derivation"
DEPENDENCIES_TITLE = "Sources"
METADATA_FIELDS = FileSourceMixin.__fields__
UNICODE_MAPPING = {
    ">=": "≥",
    "<=": "≤",
    ">": "﹥",
    "<": "﹤",
}
SEP = "/"
HEADING_UNDERLINE_CHARS = '=-^"'
MAX_HEADING_LEVEL = len(HEADING_UNDERLINE_CHARS)
HEADING_UNDERLINE_RE = re.compile("|".join(f"({re.escape(c)})+" for c in HEADING_UNDERLINE_CHARS))
MISSING_BADGE_MSG = (
    "Curation badges could not be rendered. Make sure that curation_badge_path "
    "and source_docs_path are both supplied in schema.build_options."
)


# Helper Classes/Functions


class DontSplitMe(str):
    """A string that should not be split by textwrap."""

    pass


def _wrap_table_field(max_width: int, text: Any) -> str:
    if isinstance(text, (AnyUrl, DontSplitMe)):
        return text
    return "\n\n".join(textwrap.wrap(str(text), width=max_width, break_long_words=False))


def split_long_fields(
    table_data: Iterable[Sequence], max_field_width: int = 80
) -> List[Tuple[str, ...]]:
    """Splits long row fields into multiple lines."""
    return [tuple(_wrap_table_field(max_field_width, field) for field in row) for row in table_data]


def join_blocks(blocks: Iterable[str], sep: str) -> str:
    return sep.join(filter(bool, blocks))


def heading(title: str, level: int) -> str:
    char = HEADING_UNDERLINE_CHARS[level - 1]
    return f"{title}\n{char * len(title)}"


def bold(text: str) -> str:
    return f"**{text}**"


def italic(text: str) -> str:
    return f"*{text}*"


def crossref_label(label: str) -> str:
    return f".. _{label}:"


def crossref(label: str) -> str:
    return f":ref:`{label}`"


def docref(doc_path: str) -> str:
    # doc_path is the relative path to the document being referenced without the extension
    # example: "tables/ds_esri_state"
    return f":doc:`{doc_path}`"


def anonymous_hyperlink(link_text: str, link: str) -> DontSplitMe:
    return DontSplitMe(f"`{link_text} <{link}>`__")


def escape(text: str) -> str:
    # stopgap measure; haven't been able to prevent sphinx from rendering e.g. `\>\=` as &amp;gt in html
    for i, o in UNICODE_MAPPING.items():
        text = text.replace(i, o)
    return text


@lru_cache
def __tabulate() -> Optional[Any]:
    try:
        from tabulate import tabulate

        return tabulate
    except ImportError:
        warn(
            "tabulate is unavailable; can't render sphinx documentation. "
            "Specify the 'cli' extra to ensure this dependency is present."
        )
        return None


def render_table(
    header: Tuple[str, ...],
    rows: Iterable[Sequence],
    tablefmt: str = "grid",
    max_field_width: Optional[int] = 80,
) -> str:
    tabulate = __tabulate()
    assert tabulate is not None, "can't render tables in rst without `tabulate` dependency"
    return tabulate(
        rows if max_field_width is None else split_long_fields(rows, max_field_width),
        headers=header,
        tablefmt=tablefmt,
    )


def render_figure(img_path: Path) -> str:
    return f".. figure:: {img_path}"


def demote_heading_levels(markup_text: str, table_name: str, levels: int = 1) -> str:
    """
    Demotes each heading in the rst document `markup_text` text `level` times.
    For example, if the text is

    FOO
    ===

    then `level=2` will result in text

    FOO
    ^^^

    Warns if demotion would exceed max heading levels.
    """
    exceeded_limit = False
    output = []
    lines = markup_text.splitlines()
    for prior_line, line in zip(chain([None], lines), lines):
        if (
            prior_line is not None
            and len(prior_line) == len(line)
            and (match := HEADING_UNDERLINE_RE.fullmatch(line))
        ):
            level = next(i for i, s in enumerate(match.groups(), 1) if s)
            new_level = level + levels
            exceeded_limit = exceeded_limit or new_level > MAX_HEADING_LEVEL
            new_level = min(new_level, MAX_HEADING_LEVEL)
            output.append(HEADING_UNDERLINE_CHARS[new_level - 1] * len(line))
        else:
            output.append(line)

    if exceeded_limit:
        warn(f"Demoting heading levels for table {table_name} will exceed max heading level.")

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
    if isinstance(column_type, DType):
        return column_type.value
    elif isinstance(column_type, metaschema.CustomType):
        return crossref(column_type.name)
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
    """Formats column name specially if it is part of the table's primary key."""
    name = column.name
    return bold(name) if tbl.primary_key and name in tbl.primary_key else name


def render_column_table(tbl: metaschema.Table) -> str:
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
    """Renders derivation docs. Markdown docs should not include a main title."""
    derivation_docs = tbl.dependencies.docstring if tbl.dependencies else None
    if derivation_docs:
        return join_blocks(
            [
                heading(DERIVATION_TITLE, 2),
                demote_heading_levels(derivation_docs, tbl.name, 2),
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
    relative_file_path = str(file.full_path.absolute().relative_to(repo_root.absolute()))
    file_path_url = urllib.parse.quote(relative_file_path)
    url = f"{repo_url.rstrip('/')}/{file_path_url}"
    # names may not be unique so we use anonymous refs (__) instead of named refs in those cases
    return DontSplitMe(f"`{name or relative_file_path} <{url}>`{'__' if name else '_'}")


def extract_file_sources(
    tbl: metaschema.Table,
    schema: metaschema.Schema,
) -> Dict[str, FileSourceMixin]:
    """Iterates through a tables dependencies, gathering metadata. Will recur through reference dependencies."""

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
    meta: FileSourceMixin, header_title: Optional[str], repo_root: Path, repo_url: Optional[str] = None
) -> str:
    """Renders a metadata section for either a table or a dependency."""
    meta_dict = {name: getattr(meta, name) for name in METADATA_FIELDS if getattr(meta, name)}
    if repo_url and isinstance(meta, LocalFileSourceMixin):
        meta_dict.update(github_link=format_repo_url(meta, repo_root, repo_url))
    header = heading(header_title, 3) if header_title else ""
    if meta_dict:
        parts = [header, render_table(METADATA_COLUMNS, meta_dict.items())]
    else:
        parts = [header, "Not Available"]

    return join_blocks(parts, "\n\n")


def render_dependencies_doc(
    file_sources: Mapping[str, FileSourceMixin], repo_root: Path, repo_url: Optional[str] = None
) -> str:
    """Renders dependency docs with a metadata table for each dependency."""
    if file_sources:
        sole_source = len(file_sources) == 1
        file_docs = (
            render_file_source(
                source, None if sole_source else snake_to_title(name, separator=" "), repo_root, repo_url
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
    """Renders a table's documentation."""
    file_sources = extract_file_sources(tbl, schema)
    parts = [
        crossref_label(tbl.name),
        heading(tbl.doc_title, 1),
        tbl.doc,
        render_column_table(tbl),
        render_derivation_doc(tbl),
        render_dependencies_doc(file_sources, repo_root, repo_url),
    ]
    return join_blocks(parts, "\n\n")


def render_type_entry(type: metaschema.CustomType) -> str:
    entries: List[str] = [type.type.name.lower()]
    entries.extend(map(escape, filter(None, (c.comment_expr() for c in type.constraints))))
    enum = type.enum
    if enum is not None:
        entries.append("one of " + ", ".join(map("``{}``".format, sorted(enum.enum))))

    return "\n".join(map("- {}".format, entries))


def render_constraint_docs(type: metaschema.CustomType) -> str:
    return type.docstring or ""


def render_type_doc(custom_type: metaschema.CustomType) -> str:
    """Renders a custom type's documentation"""
    parts = [
        crossref_label(custom_type.name),
        heading(custom_type.class_name, 2),
        render_constraint_docs(custom_type),
        render_type_entry(custom_type),
    ]
    return join_blocks(parts, "\n\n")


def render_types_doc(schema: metaschema.Schema) -> str:
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
    CURATION_STATUS = "Curation Status"
    curation_badge_path = Path(p) if (p := schema.build_options.curation_badge_path) else None
    source_docs_path = Path(p) if (p := schema.build_options.source_docs_path) else None

    if (curation_badge_path is not None) and (source_docs_path is not None):
        badge_relpath = Path(os.path.relpath(curation_badge_path, source_docs_path.parent))
        badge_list = [bp.name for bp in (repo_root / curation_badge_path).glob(BADGE_EXTENSION)]
        curation_badge_block = [render_figure(badge_relpath / b) for b in sorted(badge_list)]
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
    links = []
    if fs_data.landing_page is not None:
        links.append(anonymous_hyperlink("homepage", fs_data.landing_page))
    if fs_data.url is not None:
        links.append(anonymous_hyperlink("url", fs_data.url))
    if repo_url and isinstance(fs_data, LocalFileSourceMixin):
        links.append(format_repo_url(fs_data, repo_root, repo_url, name="github"))

    return DontSplitMe(f"{fs_name} ({' | '.join(links)})" if links else fs_name)


def render_package_table_links(
    fs_name: str,
    schema: metaschema.Schema,
    dep_graph: nx.DiGraph,
    table_docs_relpath: Optional[Path] = None,
) -> str:
    """Gets a list of all package tables that the source data contributes to."""
    descendants = chain.from_iterable(nx.dfs_successors(dep_graph, fs_name).values())
    pkg_tables = [
        table if table_docs_relpath is None else docref(table_docs_relpath / table)
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
    return [
        render_source_name(fs_name, fs_data, repo_root, repo_url),
        fs_data.update_frequency,
        fs_data.last_checked,
        fs_data.last_updated,
        render_package_table_links(fs_name, schema, dep_graph, table_docs_relpath),
        fs_data.authority,
    ]


def build_source_metadata(schema: metaschema.Schema, repo_root: Path) -> Dict[str, List[Any]]:
    OPEN_ACCESS = "Open Access Data Sources"
    LICENSED = "Licensed Data Sources"
    INTERNAL = "Internal Data Sources"
    source_meta: Dict[str, List[Any]] = {OPEN_ACCESS: [], LICENSED: [], INTERNAL: []}
    sources = set()
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
                repo_url=schema.build_options.repo_url,
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


def render_sphinx_docs(
    schema: metaschema.Schema,
    repo_root: Path,
    repo_url: Optional[str] = None,
) -> Tuple[str, str, Dict[str, str]]:
    """
    Returns (types_doc, table_docs)
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
