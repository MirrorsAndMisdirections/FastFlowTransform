# flowforge/cli.py
from __future__ import annotations

import os
import textwrap
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, NoReturn, cast, Callable, Iterable
import logging
import fnmatch

import typer
import yaml
from jinja2 import Environment

from flowforge import __version__
from flowforge.executors import (
    BigQueryBFExecutor,
    BigQueryExecutor,
    DatabricksSparkExecutor,
    DuckExecutor,
    PostgresExecutor,
    SnowflakeSnowparkExecutor,
)

from . import testing
from .core import REGISTRY
from .dag import mermaid, topo_sort
from .docs import render_site
from .errors import DependencyNotFoundError, ProfileConfigError
from .seeding import seed_project
from .settings import EngineType, EnvSettings, Profile, resolve_profile
from .utest import discover_unit_specs, run_unit_specs

# ───────────────────────────────── App & Globals ─────────────────────────────────

app = typer.Typer(
    name="flowforge",
    help="FlowForge – kleine ELT/DAG-Engine (SQL + Python)",
    no_args_is_help=True,
    add_completion=False,
)


@dataclass
class DQResult:
    kind: str
    table: str
    column: str | None
    ok: bool
    msg: str | None
    ms: int


def _version_callback(value: bool | None):
    if value:
        typer.echo(__version__)
        raise typer.Exit()


# ────────────────────────────── Helpers / Error Patterns ───────────────────────────


def _resolve_project_path(project_arg: str) -> Path:
    """
    Validate a FlowForge project path:
      - must exist
      - must be a directory
      - must contain 'models/'
    """
    p = Path(project_arg).expanduser().resolve()
    if not p.exists():
        raise typer.BadParameter(
            f"Project path not found: {p}\n"
            "Tip: Benutze einen absoluten Pfad oder '.' im Projekt-Root."
        )
    if not p.is_dir():
        raise typer.BadParameter(
            f"Project path is not a directory: {p}\n"
            "Tip: Übergebe das Verzeichnis, nicht eine Datei."
        )
    models = p / "models"
    if not models.exists() or not models.is_dir():
        raise typer.BadParameter(
            f"Invalid project at {p}\n"
            "Erwartet ein Unterverzeichnis 'models/'.\n"
            "Tip: Wechsle ins Projekt und nutze '.'."
        )
    return p


def _die(msg: str, code: int = 1) -> NoReturn:
    typer.echo(f"\n❌ {msg}")
    raise typer.Exit(code)


def _load_project_and_env(project_arg: str) -> tuple[Path, Environment]:
    proj = _resolve_project_path(project_arg)
    try:
        REGISTRY.load_project(proj)
    except DependencyNotFoundError as e:
        # Surface validation errors early and clearly
        typer.echo(str(e))
        raise typer.Exit(1)

    jenv = REGISTRY.env
    if jenv is None:
        _die("Internal error: Jinja Environment not initialized after load_project()", code=99)
    return proj, cast(Environment, jenv)


def _resolve_profile(
    env_name: str, engine: EngineType | None, proj: Path
) -> tuple[EnvSettings, Profile]:
    env = EnvSettings()
    if engine is not None:
        env = env.model_copy(update={"ENGINE": engine})
    try:
        prof = resolve_profile(proj, env_name, env)
        return env, prof
    except ProfileConfigError as e:
        # pretty single-line message; exit code 1 at call site
        typer.echo(f"❌ {e}")
        raise typer.Exit(1)


def _pretty_exc(e: BaseException) -> str:
    return "".join(traceback.format_exception_only(type(e), e)).strip()


def _error_block(title: str, body: str, hint: str | None = None) -> str:
    border = "─" * 70
    lines = [f"✖ {title}", "", textwrap.dedent(body).rstrip()]
    if hint:
        lines += ["", "Hints:", textwrap.dedent(hint).rstrip()]
    text = "│ " + "\n│ ".join("\n".join(lines).splitlines())
    return f"\n┌{border}\n{text}\n└{border}\n"


def _get_test_con(executor: Any) -> Any:
    """
    Return a connection with .execute(...) that understands sequences and (sql, params).
    Reuse shims on the executor or build an appropriate one when needed.
    """
    # 1) Postgres: engine -> PG shim
    if hasattr(executor, "engine"):
        try:
            from .executors.postgres_exec import _SAConnShim  # type: ignore

            return _SAConnShim(executor.engine, schema=getattr(executor, "schema", None))
        except Exception:
            pass
    # 2) BigQuery (pandas variant): client + dataset -> BQConnShim
    if hasattr(executor, "client") and hasattr(executor, "dataset"):
        try:
            from .executors._shims import BigQueryConnShim

            return BigQueryConnShim(executor.client, executor.dataset, executor.location)
        except Exception:
            # 2b) BigFrames fallback: lightweight shim using the client
            try:
                from .executors.bigquery_bf_exec import _BQShim  # type: ignore

                return _BQShim(executor.client, getattr(executor, "location", None))
            except Exception:
                pass
    # 3) DuckDB: direct connection is fine
    if hasattr(executor, "con") and hasattr(executor.con, "execute"):
        return executor.con
    # 4) Fallback: use the executor itself – testing._exec(...) can cope
    return executor


def _make_executor(prof: Profile, jenv: Environment):
    """
    Factory for the engines; returns (executor, run_sql, run_py).
    """
    if prof.engine == "duckdb":
        ex = DuckExecutor(db_path=prof.duckdb.path)
        return ex, (lambda n: ex.run_sql(n, jenv)), ex.run_python

    if prof.engine == "postgres":
        if prof.postgres.dsn is None:
            raise RuntimeError("Postgres DSN must be set")

        ex = PostgresExecutor(dsn=prof.postgres.dsn, schema=prof.postgres.db_schema)
        return ex, (lambda n: ex.run_sql(n, jenv)), ex.run_python

    if prof.engine == "bigquery":
        if prof.bigquery.dataset is None:
            raise RuntimeError("BigQuery dataset must be set")
        
        if prof.bigquery.use_bigframes:
            ex = BigQueryBFExecutor(
                project=prof.bigquery.project or "",
                dataset=prof.bigquery.dataset,
                location=prof.bigquery.location,
            )
        else:
            ex = BigQueryExecutor(
                project=prof.bigquery.project or "",
                dataset=prof.bigquery.dataset,
                location=prof.bigquery.location,
            )
        return ex, (lambda n: ex.run_sql(n, jenv)), ex.run_python

    if prof.engine == "databricks_spark":
        ex = DatabricksSparkExecutor(
            master=prof.databricks_spark.master,
            app_name=prof.databricks_spark.app_name,
        )
        return ex, (lambda n: ex.run_sql(n, jenv)), ex.run_python

    if prof.engine == "snowflake_snowpark":
        cfg = {
            "account": prof.snowflake_snowpark.account,
            "user": prof.snowflake_snowpark.user,
            "password": prof.snowflake_snowpark.password,
            "warehouse": prof.snowflake_snowpark.warehouse,
            "database": prof.snowflake_snowpark.database,
            "schema": prof.snowflake_snowpark.db_schema,
        }
        if prof.snowflake_snowpark.role:
            cfg["role"] = prof.snowflake_snowpark.role
        ex = SnowflakeSnowparkExecutor(cfg)
        return ex, (lambda n: ex.run_sql(n, jenv)), ex.run_python

    _die(f"Unbekannter Engine-Typ: {getattr(prof, 'engine', None)}", code=1)
    raise AssertionError("unreachable")

def _parse_cli_vars(pairs: List[str]) -> Dict[str, object]:
    """
    Parse --vars key=value pairs. Values are YAML-parsed for light typing:
    --vars day='2025-10-01' limit=5 enabled=true tags='[a,b]'
    """
    out: Dict[str, object] = {}
    for item in pairs:
        if "=" not in item:
            raise typer.BadParameter(f"--vars expects key=value, got: {item}")
        k, v = item.split("=", 1)
        try:
            out[k] = yaml.safe_load(v)
        except Exception:
            out[k] = v
    return out

def _parse_select(parts: List[str]) -> List[str]:
    """Accept multiple --select occurrences or a single space-separated string."""
    out: List[str] = []
    for p in parts:
        out.extend(s for s in str(p).split() if s)
    return out

def _selector(predicates: Iterable[Callable[[Any], bool]]) -> Callable[[Any], bool]:
    preds = list(predicates)
    if not preds:
        return lambda n: True
    return lambda n: all(p(n) for p in preds)

def _build_predicates(tokens: List[str]) -> List[Callable[[Any], bool]]:
    """
    Supported tokens:
      - name glob: e.g. orders*, marts_*  (matches Node.name and physical relation name)
      - tag:<tag> : matches Node.meta['tags'] (list or str)
      - type:<view|table|ephemeral> : matches Node.meta['materialized'] (default 'table')
      - kind:<sql|python> : matches Node.kind
    AND across tokens.
    """
    from .core import relation_for  # local import to avoid cycles
    preds: List[Callable[[Any], bool]] = []
    for tok in tokens:
        if tok.startswith("tag:"):
            want = tok.split(":", 1)[1]
            def _p(n, w=want):
                tags = (getattr(n, "meta", {}) or {}).get("tags")
                if isinstance(tags, list):
                    return w in tags
                return tags == w
            preds.append(_p)
        elif tok.startswith("type:"):
            want = tok.split(":", 1)[1]
            preds.append(lambda n, w=want: (getattr(n, "meta", {}) or {}).get("materialized", "table") == w)
        elif tok.startswith("kind:"):
            want = tok.split(":", 1)[1]
            preds.append(lambda n, w=want: n.kind == w)
        else:
            pattern = tok
            preds.append(lambda n, pat=pattern: fnmatch.fnmatch(n.name, pat) or fnmatch.fnmatch(relation_for(n.name), pat))
    return preds


# ────────────────────────────── Logging setup ──────────────────────────────


LOG = logging.getLogger("flowforge")
SQL_LOG = logging.getLogger("flowforge.sql")

def _setup_logging(verbose: int, quiet: int) -> None:
    """
    Map verbosity to levels:
      -q        → ERROR
       (default)→ WARNING
      -v        → INFO
      -vv+      → DEBUG
    Also wires the SQL channel and keeps FLOWFORGE_SQL_DEBUG compatibility.
    """
    # clamp effective level in [-1, 2]
    eff = max(min(verbose - quiet, 2), -1)
    lvl = { -1: logging.ERROR, 0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG }[eff]

    # basicConfig only affects root; set explicit levels for our loggers
    logging.basicConfig(level=lvl, format="%(levelname)s %(message)s")
    LOG.setLevel(lvl)

    # SQL logger: DEBUG when either -vv or env var is set
    sql_debug_env = os.getenv("FLOWFORGE_SQL_DEBUG") == "1"
    SQL_LOG.setLevel(logging.DEBUG if (eff >= 2 or sql_debug_env) else logging.WARNING)

    # keep env var compatibility for existing code paths if user asked for -vv
    if eff >= 2 and not sql_debug_env:
        os.environ["FLOWFORGE_SQL_DEBUG"] = "1"


# ──────────────────────────────────── CLI Root ───────────────────────────────────


@app.callback()
def main(
    version: bool | None = typer.Option(
        None,
        "--version",
        "-V",
        help="Zeigt die Version und beendet.",
        callback=_version_callback,
        is_eager=True,
    ),
    verbose: int = typer.Option(0, "--verbose", "-v", count=True, help="Mehr Ausgaben (–v: INFO, –vv: DEBUG)"),
    quiet: int = typer.Option(0, "--quiet", "-q", count=True, help="Weniger Ausgaben (–q: ERROR)"),
):
    _setup_logging(verbose, quiet)
    return


# ──────────────────────────────────── Commands ───────────────────────────────────


@app.command(
    help="Lädt das Projekt, baut den DAG und führt alle Modelle aus.\n\nBeispiel:\n  flowforge run . --env dev"
)
def run(
    project: str = typer.Argument(".", help="Pfad zum FlowForge-Projekt (mit 'models/')."),
    env_name: str = typer.Option("dev", "--env", help="Profil-Umgebung: z. B. dev/stg/prod"),
    engine: EngineType | None = typer.Option(
        None,
        "--engine",
        help="Engine-Backend: duckdb | postgres | bigquery | bigquery_bf | databricks_spark | snowflake_snowpark (überschreibt Profile).",
        case_sensitive=False,
    ),
    vars: List[str] = typer.Option([], "--vars", help="Override template vars: key=value"),
    select: List[str] = typer.Option([], "--select", help="Filter: name-glob, tag:<t>, type:<view|table|ephemeral>, kind:<sql|python>"),
):
    proj, jenv = _load_project_and_env(project)
    REGISTRY.set_cli_vars(_parse_cli_vars(vars))
    _, prof = _resolve_profile(env_name, engine, proj)
    tokens = _parse_select(select)
    pred = _selector(_build_predicates(tokens))
    # topo order, then filter
    order = [n for n in topo_sort(REGISTRY.nodes) if pred(REGISTRY.nodes[n])]

    if LOG.isEnabledFor(logging.INFO):
        typer.echo(f"Profil: {env_name} | Engine: {prof.engine}")
    execu, run_sql, run_py = _make_executor(prof, jenv)

    for name in order:
        node = REGISTRY.nodes[name]
        if LOG.isEnabledFor(logging.INFO):
            typer.echo(f"→ Running {name} ({node.kind}) on {prof.engine}")
        try:
            (run_sql if node.kind == "sql" else run_py)(node)
        except KeyError as ke:
            typer.echo(
                _error_block(
                    f"Model failed: {name} (KeyError)",
                    _pretty_exc(ke),
                    "• Prüfe Spaltennamen deiner Upstream-Tabellen (Seeds/SQL).\n"
                    "• Bei >1 Deps: dict-Keys sind physische Relationen (relation_for), z. B. 'orders'.\n"
                    "• (Optional) Eingabespalten im Executor vor dem Call loggen.",
                )
            )
            raise typer.Exit(1)
        except Exception as e:
            body = _pretty_exc(e)
            if os.getenv("FLOWFORGE_TRACE") == "1":
                body += "\n\n" + "".join(traceback.format_exc())
            typer.echo(_error_block(f"Model failed: {name}", body, "• Siehe Ursache oben."))
            raise typer.Exit(1)

    typer.echo("✓ Done")


@app.command(
    help="Gibt den DAG als Mermaid oder erzeugt eine HTML-Seite.\n\nBeispiele:\n  flowforge dag .\n  flowforge dag . --env dev --html"
)
def dag(
    project: str = typer.Argument(".", help="Pfad zum FlowForge-Projekt (mit 'models/')."),
    env_name: str = typer.Option("dev", "--env", help="Profil-Umgebung: z. B. dev/stg/prod"),
    html: bool = typer.Option(False, "--html", help="Erzeuge HTML-DAG und Mini-Dokumentation"),
    engine: EngineType | None = typer.Option(
        None,
        "--engine",
        help="Engine-Backend: duckdb | postgres | bigquery | bigquery_bf | databricks_spark | snowflake_snowpark (überschreibt Profile).",
        case_sensitive=False,
    ),
    vars: List[str] = typer.Option([], "--vars"),
    select: List[str] = typer.Option([], "--select", help="Filter: name-glob, tag:<t>, type:<view|table|ephemeral>, kind:<sql|python>"),
):
    proj, jenv = _load_project_and_env(project)
    REGISTRY.cli_vars = _parse_cli_vars(vars)
    _, prof = _resolve_profile(env_name, engine, proj)

    tokens = _parse_select(select)
    pred = _selector(_build_predicates(tokens))
    filtered_nodes = {k: v for k, v in REGISTRY.nodes.items() if pred(v)}

    if html:
        out_dir = proj / "docs"
        ex, *_ = _make_executor(prof, jenv)
        render_site(out_dir, filtered_nodes, executor=ex)
        typer.echo(f"HTML-DAG geschrieben nach {out_dir / 'index.html'}")
    else:
        mm = mermaid(filtered_nodes)
        out = proj / "docs" / "dag.mmd"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(mm, encoding="utf-8")
        typer.echo(f"Mermaid DAG written to {out}")

    if LOG.isEnabledFor(logging.INFO):
        typer.echo(f"Profil: {env_name} | Engine: {prof.engine}")


@app.command(
    help="Materialisiert Modelle und führt konfigurierte Datenqualitäts-Checks aus.\n\nBeispiel:\n  flowforge test . --env dev --select batch"
)
def test(
    project: str = typer.Argument(".", help="Pfad zum FlowForge-Projekt (mit 'models/')."),
    env_name: str = typer.Option("dev", "--env", help="Profil-Umgebung: z. B. dev/stg/prod"),
    # select: str | None = typer.Option(
    #     None, "--select", help="Filter (z. B. 'streaming' oder 'batch')"
    # ),
    engine: EngineType | None = typer.Option(
        None,
        "--engine",
        help="Engine-Backend: duckdb | postgres | bigquery | bigquery_bf | databricks_spark | snowflake_snowpark (überschreibt Profile).",
        case_sensitive=False,
    ),
    vars: List[str] = typer.Option([], "--vars"),
    select: List[str] = typer.Option([], "--select", help="Filter models (name-glob, tag:<t>, type:<view|table|ephemeral>, kind:<sql|python>) or DQ tags (legacy single token)"),
):
    proj, jenv = _load_project_and_env(project)
    REGISTRY.cli_vars = _parse_cli_vars(vars)
    _, prof = _resolve_profile(env_name, engine, proj)
    execu, run_sql, run_py = _make_executor(prof, jenv)

    # 0) Shim/marker
    con = _get_test_con(execu)
    if os.getenv("FLOWFORGE_SQL_DEBUG") == "1":
        typer.echo(getattr(con, "marker", "NO_SHIM"))

    # 1) Run models
    # for name in topo_sort(REGISTRY.nodes):
    # 1) Run models (apply optional model filter)
    tokens = _parse_select(select)
    pred = _selector(_build_predicates(tokens))
    order = [n for n in topo_sort(REGISTRY.nodes) if pred(REGISTRY.nodes[n])]
    for name in order:
        node = REGISTRY.nodes[name]
        (run_sql if node.kind == "sql" else run_py)(node)

    # 2) Load/filter tests
    cfg_path = proj / "project.yml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) if cfg_path.exists() else {}
    tests = cfg.get("tests", []) or []

    # if select:
    # 2b) Legacy DQ tag filter: if user passed a single bare token without colon,
    # treat it as a DQ test tag (backwards compatible).
    legacy_tag = None
    if len(tokens) == 1 and all(not t.startswith(("tag:","type:","kind:")) for t in tokens):
        legacy_tag = tokens[0]
    if legacy_tag:
        def _has_tag(t: dict) -> bool:
            tags = t.get("tags") or []
            # return (select in tags) if isinstance(tags, list) else (select == tags)
            return (legacy_tag in tags) if isinstance(tags, list) else (legacy_tag == tags)

        tests = [t for t in tests if _has_tag(t)]
    if not tests:
        typer.echo("ℹ️  Keine Tests konfiguriert.")
        raise typer.Exit(code=0)

    # 3) Execute and collect
    results: list[DQResult] = []
    for t in tests:
        kind = t["type"]
        table = t.get("table")
        col = t.get("column")
        t0 = time.perf_counter()
        try:
            if kind == "not_null":
                testing.not_null(con, table, col)
            elif kind == "unique":
                testing.unique(con, table, col)
            elif kind == "greater_equal":
                testing.greater_equal(con, table, col, t.get("threshold", 0))
            elif kind == "non_negative_sum":
                testing.non_negative_sum(con, table, col)
            elif kind == "row_count_between":
                testing.row_count_between(con, table, t.get("min", 1), t.get("max"))
            elif kind == "freshness":
                testing.freshness(con, table, col, t["max_delay_minutes"])
            else:
                raise typer.BadParameter(f"Unknown test type: {kind}")
            ok, msg = True, None
        except testing.TestFailure as e:
            ok, msg = False, str(e)
        except Exception as e:
            ok, msg = False, f"Unexpected error: {e.__class__.__name__}: {e}"
        ms = int((time.perf_counter() - t0) * 1000)
        results.append(DQResult(kind=kind, table=table, column=col, ok=ok, msg=msg, ms=ms))

    # 4) Summary
    passed = sum(1 for r in results if r.ok)
    failed = len(results) - passed

    typer.echo("\nData Quality Summary")
    typer.echo("────────────────────")
    for r in results:
        mark = "✅" if r.ok else "❌"
        scope = f"{r.table}" + (f".{r.column}" if r.column else "")
        typer.echo(f"{mark} {r.kind:<18} {scope:<40} ({r.ms}ms)")
        if not r.ok and r.msg:
            typer.echo(f"   ↳ {r.msg}")

    typer.echo("\nTotals")
    typer.echo("──────")
    typer.echo(f"✓ passed: {passed}")
    typer.echo(f"✗ failed: {failed}")

    raise typer.Exit(code=2 if failed > 0 else 0)


@app.command(
    help="Seeds aus /seeds einspielen.\n\nBeispiele:\n  flowforge seed . --env dev\n  flowforge seed examples/postgres --env stg"
)
def seed(
    project: str = typer.Argument(
        ".", help="Pfad zum FlowForge-Projekt (mit 'models/' und optional 'seeds/')."
    ),
    env_name: str = typer.Option("dev", "--env", help="Profil-Umgebung: z. B. dev/stg/prod"),
    engine: EngineType | None = typer.Option(
        None,
        "--engine",
        help="Engine-Backend: duckdb | postgres | bigquery | bigquery_bf | databricks_spark | snowflake_snowpark (überschreibt Profile).",
        case_sensitive=False,
    ),
    vars: List[str] = typer.Option([], "--vars"),
):
    proj, jenv = _load_project_and_env(project)
    REGISTRY.cli_vars = _parse_cli_vars(vars)
    _, prof = _resolve_profile(env_name, engine, proj)
    execu, _, _ = _make_executor(prof, jenv)

    # Only Postgres exposes a schema that we pass to seed_project
    schema: str | None = None
    if prof.engine == "postgres":
        schema = prof.postgres.db_schema  # ← at this point prof is definitely a PostgresProfile

    n = seed_project(proj, execu, schema)
    typer.echo(f"✓ Seeded {n} table(s)")


@app.command()
def utest(
    project: str = typer.Argument(".", help="Pfad zum Projekt (mit tests/unit/*.yml)"),
    model: str | None = typer.Option(None, "--model", help="Nur ein Modell testen"),
    case: str | None = typer.Option(None, "--case", help="Nur einen Case ausführen"),
    env_name: str = typer.Option("dev", "--env", help="Profil-Umgebung"),
    engine: EngineType | None = typer.Option(
        None, "--engine", help="duckdb|postgres|bigquery (überschreibt Profile)"
    ),
    path: str | None = typer.Option(
        None, "--path", help="Eine einzelne YAML-Datei statt Discovery"
    ),
    vars: List[str] = typer.Option([], "--vars", help="Override template vars: key=value"),
):
    proj, jenv = _load_project_and_env(project)
    # make CLI vars visible to templates (var()) during unit-test renders
    REGISTRY.cli_vars = _parse_cli_vars(vars)
    _, prof = _resolve_profile(env_name, engine, proj)

    ex, run_sql, run_py = _make_executor(prof, jenv)

    specs = discover_unit_specs(proj, path=path, only_model=model)
    if not specs:
        typer.echo("ℹ️  Keine Unit-Tests gefunden (tests/unit/*.yml).")
        raise typer.Exit(0)

    failures = run_unit_specs(specs, ex, jenv, only_case=case)
    raise typer.Exit(code=2 if failures > 0 else 0)
