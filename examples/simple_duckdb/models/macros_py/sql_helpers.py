def sql_email_domain(col: str, engine: str = "duckdb") -> str:
    """
    Return a SQL snippet that extracts the email domain from a column/expression.
    Intended for DuckDB/Postgres. Use like: {{ sql_email_domain("u.email") }} AS email_domain
    """
    col = col.strip()
    base = f"coalesce({col}, '')"
    return f"lower(split_part({base}, '@', 2))"
