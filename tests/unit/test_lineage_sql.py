from fastflowtransform.lineage import infer_sql_lineage


def test_simple_alias_and_function_transformed():
    """
    Heuristics:
    - SELECT u.email AS email -> direct
    - SELECT upper(u.email) AS email_upper -> transformed
    - From-table alias 'u' resolves to relation 'users' when present in FROM/JOIN
    """
    sql = """
      -- FROM ref('users.ff') would render to physical 'users'
      select
        u.email as email,
        upper(u.email) as email_upper
      from users as u
    """
    lin = infer_sql_lineage(sql)
    # email: direct from users.email
    assert "email" in lin
    email_src = lin["email"]
    assert any(
        s["from_relation"] == "users" and s["from_column"] == "email" and s["transformed"] is False
        for s in email_src
    )
    # email_upper: transformed from users.email
    assert "email_upper" in lin
    email_upper_src = lin["email_upper"]
    assert any(
        s["from_relation"] == "users" and s["from_column"] == "email" and s["transformed"] is True
        for s in email_upper_src
    )


def test_passthrough_without_alias():
    """SELECT u.email -> output column name 'email' inferred as direct."""
    sql = "select u.email from users u"
    lin = infer_sql_lineage(sql)
    assert "email" in lin
    srcs = lin["email"]
    assert any(
        s["from_relation"] == "users"
        and s["from_column"] == "email"
        and (s["transformed"] is False)
        for s in srcs
    )
