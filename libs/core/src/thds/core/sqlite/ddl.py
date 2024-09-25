def drop(full_name: str, is_view: bool = False) -> str:
    """Drop a table or view."""
    table_or_view = "TABLE" if not is_view else "VIEW"
    return f"DROP {table_or_view} IF EXISTS {full_name};"
