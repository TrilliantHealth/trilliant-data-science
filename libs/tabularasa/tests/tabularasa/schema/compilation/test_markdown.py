"""Tests for markdown documentation generation."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from thds.tabularasa.schema import metaschema
from thds.tabularasa.schema.compilation.markdown import (
    DontSplitMe,
    anchor,
    bold,
    convert_rst_headings_to_markdown,
    crossref,
    docref,
    escape_for_display,
    heading,
    hyperlink,
    italic,
    join_blocks,
    render_column_type,
    render_file_source,
    render_markdown_docs,
    render_table,
    render_type_entry,
    split_long_fields,
)
from thds.tabularasa.schema.dtypes import DType
from thds.tabularasa.schema.files import FileSourceMixin

# -----------------------------------------------------------------------------
# Tests: Markdown Helpers
# -----------------------------------------------------------------------------


def test_heading_level_1():
    result = heading("Title", 1)
    assert result == "# Title"


def test_heading_level_2():
    result = heading("Subtitle", 2)
    assert result == "## Subtitle"


def test_heading_level_3():
    result = heading("Section", 3)
    assert result == "### Section"


def test_bold():
    result = bold("text")
    assert result == "**text**"


def test_italic():
    result = italic("text")
    assert result == "*text*"


def test_anchor():
    result = anchor("my-anchor")
    assert result == '<a id="my-anchor"></a>'


def test_crossref():
    result = crossref("my_type")
    assert result == "[my_type](#my_type)"


def test_docref():
    result = docref("tables/my_table")
    assert result == "[tables/my_table](tables/my_table.md)"


def test_hyperlink():
    result = hyperlink("Click here", "https://example.com")
    assert result == "[Click here](https://example.com)"
    assert isinstance(result, DontSplitMe)


def test_escape_for_display():
    result = escape_for_display("value >= 5 and value <= 10")
    assert ">=" in result
    assert "<=" in result


def test_join_blocks_filters_empty():
    blocks = ["first", "", "second", None, "third"]
    result = join_blocks(filter(None, blocks), "\n\n")
    assert result == "first\n\nsecond\n\nthird"


# -----------------------------------------------------------------------------
# Tests: Markdown Table
# -----------------------------------------------------------------------------


def test_render_table_basic():
    header = ("Name", "Value")
    rows = [("key1", "value1"), ("key2", "value2")]
    result = render_table(header, rows)
    assert "| Name" in result
    assert "| Value" in result
    assert "key1" in result
    assert "value1" in result
    # Check for markdown table separator (pipe table format uses :---)
    assert "|:" in result or "|-" in result  # Table separator


def test_render_table_with_max_width():
    header = ("Col1", "Col2")
    rows = [("short", "A very long text that should be wrapped")]
    result = render_table(header, rows, max_field_width=20)
    assert "Col1" in result
    assert "Col2" in result


def test_split_long_fields():
    rows = [("short", "A very long text that exceeds the maximum field width limit")]
    result = split_long_fields(rows, max_field_width=20)
    assert len(result) == 1
    # The long field should be wrapped
    assert len(result[0][1]) <= 60  # Wrapped text is joined with spaces


# -----------------------------------------------------------------------------
# Tests: RST to Markdown Conversion
# -----------------------------------------------------------------------------


def test_convert_level_1_heading():
    rst_text = "Title\n====="
    result = convert_rst_headings_to_markdown(rst_text)
    assert "### Title" in result
    assert "=====" not in result


def test_convert_level_2_heading():
    rst_text = "Subtitle\n--------"
    result = convert_rst_headings_to_markdown(rst_text)
    assert "#### Subtitle" in result
    assert "--------" not in result


def test_convert_multiple_headings():
    rst_text = """Title
=====

Some content here.

Subtitle
--------

More content."""
    result = convert_rst_headings_to_markdown(rst_text)
    assert "### Title" in result
    assert "#### Subtitle" in result
    assert "Some content here." in result
    assert "More content." in result


def test_preserves_non_heading_text():
    rst_text = "This is regular text.\nNo headings here."
    result = convert_rst_headings_to_markdown(rst_text)
    assert result == rst_text


def test_custom_base_level():
    rst_text = "Title\n====="
    result = convert_rst_headings_to_markdown(rst_text, base_level=2)
    assert "## Title" in result


# -----------------------------------------------------------------------------
# Tests: Column Type Rendering
# -----------------------------------------------------------------------------


def test_render_dtype():
    result = render_column_type(DType.INT32)
    assert result == "int32"


def test_render_string_dtype():
    result = render_column_type(DType.STR)
    assert result == "str"


def test_render_custom_type():
    custom_type = MagicMock(spec=metaschema.CustomType)
    custom_type.name = "my_custom_type"
    result = render_column_type(custom_type)
    # Links to types.md in parent directory since table docs are in tables/ subdirectory
    assert result == "[my_custom_type](../types.md#my_custom_type)"


def test_render_array_type():
    array_type = MagicMock(spec=metaschema.ArrayType)
    array_type.values = DType.INT32
    result = render_column_type(array_type)
    assert result == "list[int32]"


def test_render_mapping_type():
    mapping_type = MagicMock(spec=metaschema.MappingType)
    mapping_type.keys = DType.STR
    mapping_type.values = DType.INT32
    result = render_column_type(mapping_type)
    assert result == "map[str: int32]"


# -----------------------------------------------------------------------------
# Tests: Type Entry Rendering
# -----------------------------------------------------------------------------


def test_render_type_entry_basic():
    custom_type = MagicMock(spec=metaschema.CustomType)
    custom_type.type = MagicMock()
    custom_type.type.name = "INT32"
    custom_type.constraints = []
    custom_type.enum = None
    result = render_type_entry(custom_type)
    assert "- int32" in result


def test_render_type_entry_with_enum():
    custom_type = MagicMock(spec=metaschema.CustomType)
    custom_type.type = MagicMock()
    custom_type.type.name = "STRING"
    custom_type.constraints = []
    enum_mock = MagicMock()
    enum_mock.enum = ["foo", "bar", "baz"]
    custom_type.enum = enum_mock
    result = render_type_entry(custom_type)
    assert "- string" in result
    assert "one of" in result
    assert "`bar`" in result
    assert "`baz`" in result
    assert "`foo`" in result


# -----------------------------------------------------------------------------
# Tests: Markdown Docs Integration
# -----------------------------------------------------------------------------


@pytest.fixture
def mock_schema():
    """Create a mock schema for testing."""
    schema = MagicMock(spec=metaschema.Schema)
    schema.types = {}
    schema.package_tables = []
    schema.build_options = MagicMock()
    schema.build_options.source_docs_path = None
    schema.build_options.curation_badge_path = None
    return schema


def test_render_markdown_docs_empty_schema(mock_schema):
    """Test rendering docs for an empty schema."""
    repo_root = Path("/fake/repo")
    types_doc, source_doc, table_docs = render_markdown_docs(mock_schema, repo_root)

    assert "# Custom Types" in types_doc
    assert source_doc == ""
    assert table_docs == {}


def test_render_markdown_docs_with_types(mock_schema):
    """Test rendering docs for a schema with custom types."""
    custom_type = MagicMock(spec=metaschema.CustomType)
    custom_type.name = "test_type"
    custom_type.class_name = "TestType"
    custom_type.type = MagicMock()
    custom_type.type.name = "STRING"
    custom_type.constraints = []
    custom_type.enum = None
    custom_type.docstring = "A test type for testing."

    mock_schema.types = {"test_type": custom_type}

    repo_root = Path("/fake/repo")
    types_doc, source_doc, table_docs = render_markdown_docs(mock_schema, repo_root)

    assert "# Custom Types" in types_doc
    assert "## TestType" in types_doc
    assert "A test type for testing." in types_doc
    assert '<a id="test_type"></a>' in types_doc


# -----------------------------------------------------------------------------
# Tests: render_file_source URL linking
# -----------------------------------------------------------------------------


def test_render_file_source_converts_urls_to_links():
    """AnyUrl values in file source metadata should render as clickable markdown links."""
    meta = FileSourceMixin(
        url="https://data.example.com/dataset.csv",
        landing_page="https://example.com/about",
        authority="Example Corp",
    )
    result = render_file_source(meta, "Test Source", repo_root=Path("/fake/repo"))
    # URLs should be wrapped in markdown link syntax, not bare
    assert "[https://data.example.com/dataset.csv](https://data.example.com/dataset.csv)" in result
    assert "[https://example.com/about](https://example.com/about)" in result


def test_render_file_source_no_urls():
    """File sources without URLs should render normally without link conversion."""
    meta = FileSourceMixin(authority="Internal")
    result = render_file_source(meta, "Internal Source", repo_root=Path("/fake/repo"))
    assert "Internal" in result
    # No markdown link syntax expected
    assert "](http" not in result


def test_render_file_source_only_url_no_landing_page():
    """Only the url field is set; landing_page is None."""
    meta = FileSourceMixin(url="https://files.example.com/data.zip")
    result = render_file_source(meta, None, repo_root=Path("/fake/repo"))
    assert "[https://files.example.com/data.zip](https://files.example.com/data.zip)" in result
