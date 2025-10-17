# flowforge/cli.py
from __future__ import annotations

import fnmatch
import logging
import os
import textwrap
import time
import traceback
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, NoReturn, cast

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
from flowforge.executors.base import BaseExecutor

from . import testing
from .core import REGISTRY, relation_for
from .dag import mermaid, topo_sort
from .docs import render_site
from .errors import DependencyNotFoundError, ProfileConfigError
from .executors._shims import BigQueryConnShim, SAConnShim
from .seeding import seed_project
from .settings import EngineType, EnvSettings, Profile, resolve_profile
from .utest import discover_unit_specs, run_unit_specs

# ───────────────────────────────── App & Globals ─────────────────────────────────

app = typer.Typer(
    name="flowforge",
    help="FlowForge - kleine ELT/DAG-Engine (SQL + Python)",
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


@dataclass
class CLIContext:
    project: Path
    jinja_env: Environment
    env_settings: EnvSettings
    profile: Profile

    def make_executor(self) -> tuple[Any, Callable, Callable]:
        return _make_executor(self.profile, self.jinja_env)


def _version_callback(value: bool | None) -> None:
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
        raise typer.Exit(1) from e

    jenv = REGISTRY.env
    if jenv is None:
        _die("Internal error: Jinja Environment not initialized after load_project()", code=99)
    return proj, cast(Environment, jenv)


def _prepare_context(
    project_arg: str,
    env_name: str,
    engine: EngineType | None,
    vars_opt: list[str] | None,
) -> CLIContext:
    """Shared CLI bootstrap: load project, apply CLI vars, resolve profile."""
    proj, jenv = _load_project_and_env(project_arg)
    REGISTRY.set_cli_vars(_parse_cli_vars(vars_opt or []))
    env_settings, prof = _resolve_profile(env_name, engine, proj)
    return CLIContext(project=proj, jinja_env=jenv, env_settings=env_settings, profile=prof)


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
        raise typer.Exit(1) from e


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
            return SAConnShim(executor.engine, schema=getattr(executor, "schema", None))
        except Exception:
            pass
    # 2) BigQuery (pandas variant): client + dataset -> BQConnShim
    if hasattr(executor, "client") and hasattr(executor, "dataset"):
        try:
            return BigQueryConnShim(executor.client, executor.dataset, executor.location)
        except Exception:
            # 2b) BigFrames fallback: lightweight shim using the client
            try:
                return BigQueryConnShim(executor.client, getattr(executor, "location", None))
            except Exception:
                pass
    # 3) DuckDB: direct connection is fine
    if hasattr(executor, "con") and hasattr(executor.con, "execute"):
        return executor.con
    # 4) Fallback: use the executor itself - testing._exec(...) can cope
    return executor


def _make_executor(prof: Profile, jenv: Environment) -> tuple[Any, Callable, Callable]:
    """
    Factory for the engines; returns (executor, run_sql, run_py).
    """
    ex: BaseExecutor
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


def _parse_cli_vars(pairs: list[str]) -> dict[str, object]:
    """
    Parse --vars key=value pairs. Values are YAML-parsed for light typing:
    --vars day='2025-10-01' limit=5 enabled=true tags='[a,b]'
    """
    out: dict[str, object] = {}
    for item in pairs:
        if "=" not in item:
            raise typer.BadParameter(f"--vars expects key=value, got: {item}")
        k, v = item.split("=", 1)
        try:
            out[k] = yaml.safe_load(v)
        except Exception:
            out[k] = v
    return out


def _parse_select(parts: list[str]) -> list[str]:
    """Accept multiple --select occurrences or a single space-separated string."""
    out: list[str] = []
    for p in parts:
        out.extend(s for s in str(p).split() if s)
    return out


def _selector(predicates: Iterable[Callable[[Any], bool]]) -> Callable[[Any], bool]:
    preds = list(predicates)
    if not preds:
        return lambda n: True
    return lambda n: all(p(n) for p in preds)


def _build_predicates(tokens: list[str]) -> list[Callable[[Any], bool]]:
    """
    Supported tokens:
      - name glob: e.g. orders*, marts_*  (matches Node.name and physical relation name)
      - tag:<tag> : matches Node.meta['tags'] (list or str)
      - type:<view|table|ephemeral> : matches Node.meta['materialized'] (default 'table')
      - kind:<sql|python> : matches Node.kind
    AND across tokens.
    """
    preds: list[Callable[[Any], bool]] = []
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
            preds.append(
                cast(
                    Callable[[Any], bool],
                    lambda n, w=want: (getattr(n, "meta", {}) or {}).get("materialized", "table")
                    == w,
                )
            )
        elif tok.startswith("kind:"):
            want = tok.split(":", 1)[1]
            preds.append(cast(Callable[[Any], bool], lambda n, w=want: n.kind == w))
        else:
            pattern = tok
            preds.append(
                cast(
                    Callable[[Any], bool],
                    lambda n, pat=pattern: fnmatch.fnmatch(n.name, pat)
                    or fnmatch.fnmatch(relation_for(n.name), pat),
                )
            )
    return preds


def _compile_selector(
    select_opt: list[str] | None,
) -> tuple[list[str], Callable[[Any], bool]]:
    """Normalize `--select` values and return tokens plus predicate."""
    tokens = _parse_select(select_opt or [])
    return tokens, _selector(_build_predicates(tokens))


def _execute_models(
    order: Iterable[str],
    run_sql: Callable[[Any], Any],
    run_py: Callable[[Any], Any],
    *,
    before: Callable[[str, Any], None] | None = None,
    on_error: Callable[[str, Any, Exception], None] | None = None,
) -> None:
    for name in order:
        node = REGISTRY.nodes[name]
        if before:
            before(name, node)
        try:
            (run_sql if node.kind == "sql" else run_py)(node)
        except Exception as exc:
            if on_error is None:
                raise
            on_error(name, node, exc)


def _resolve_dag_out_dir(proj: Path, override: Path | None) -> Path:
    if override:
        return override.expanduser().resolve()
    # project.yml optional lesen
    cfg_path = proj / "project.yml"
    try:
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) if cfg_path.exists() else {}
    except Exception:
        cfg = {}
    p = (cfg or {}).get("docs", {}).get("dag_dir")  # z. B. "site/dag" oder "./build/dag"
    if p:
        return (proj / p).expanduser().resolve()
    # Default
    return (proj / "site" / "dag").resolve()


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
    eff_level_threshold = 2
    # clamp effective level in [-1, 2]
    eff = max(min(verbose - quiet, 2), -1)
    lvl = {-1: logging.ERROR, 0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}[eff]

    # basicConfig only affects root; set explicit levels for our loggers
    logging.basicConfig(level=lvl, format="%(levelname)s %(message)s")
    LOG.setLevel(lvl)

    # SQL logger: DEBUG when either -vv or env var is set
    sql_debug_env = os.getenv("FLOWFORGE_SQL_DEBUG") == "1"
    SQL_LOG.setLevel(
        logging.DEBUG if (eff >= eff_level_threshold or sql_debug_env) else logging.WARNING
    )

    # keep env var compatibility for existing code paths if user asked for -vv
    if eff >= eff_level_threshold and not sql_debug_env:
        os.environ["FLOWFORGE_SQL_DEBUG"] = "1"


VarsOpt = Annotated[
    list[str] | None,
    typer.Option("--vars", help="Override template vars: key=value"),
]

CaseOpt = Annotated[str | None, typer.Option("--case", help="Nur einen Case ausführen")]

EnvOpt = Annotated[str, typer.Option("--env", help="Profil-Umgebung")]

EngineOpt = Annotated[
    EngineType | None,
    typer.Option("--engine", help="duckdb|postgres|bigquery (überschreibt Profile)"),
]

PathOpt = Annotated[
    str | None, typer.Option("--path", help="Eine einzelne YAML-Datei statt Discovery")
]

ProjectArg = Annotated[str, typer.Argument(help="Pfad zum Projekt (mit tests/unit/*.yml)")]

ModelOpt = Annotated[str | None, typer.Option("--model", help="Nur ein Modell testen")]

SelectOpt = Annotated[
    list[str] | None,
    typer.Option(
        "--select",
        help=(
            "Filter models (name-glob, tag:<t>, type:<view|table|ephemeral>, "
            "kind:<sql|python>) or DQ tags (legacy single token)"
        ),
    ),
]

OutOpt = Annotated[
    Path | None,
    typer.Option("--out", help="Output directory for DAG artifacts"),
]

HtmlOpt = Annotated[
    bool,
    typer.Option("--html", help="Erzeuge HTML-DAG und Mini-Dokumentation"),
]


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
    verbose: int = typer.Option(
        0, "--verbose", "-v", count=True, help="Mehr Ausgaben (-v: INFO, -vv: DEBUG)"
    ),
    quiet: int = typer.Option(0, "--quiet", "-q", count=True, help="Weniger Ausgaben (-q: ERROR)"),
) -> None:
    _setup_logging(verbose, quiet)


# ──────────────────────────────────── Commands ───────────────────────────────────


@app.command(
    help=(
        "Lädt das Projekt, baut den DAG und führt alle Modelle aus."
        "\n\nBeispiel:\n  flowforge run . --env dev"
    )
)
def run(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
) -> None:
    ctx = _prepare_context(project, env_name, engine, vars)
    _, pred = _compile_selector(select)

    if LOG.isEnabledFor(logging.INFO):
        typer.echo(f"Profil: {env_name} | Engine: {ctx.profile.engine}")
    _, run_sql, run_py = ctx.make_executor()

    def _before(name: str, node: Any) -> None:
        if LOG.isEnabledFor(logging.INFO):
            typer.echo(f"→ Running {name} ({node.kind}) on {ctx.profile.engine}")

    def _on_error(name: str, _node: Any, err: Exception) -> None:
        if isinstance(err, KeyError):
            typer.echo(
                _error_block(
                    f"Model failed: {name} (KeyError)",
                    _pretty_exc(err),
                    "• Prüfe Spaltennamen deiner Upstream-Tabellen (Seeds/SQL).\n"
                    "• Bei >1 Deps: dict-Keys sind physische Relationen (relation_for), "
                    "z. B. 'orders'.\n"
                    "• (Optional) Eingabespalten im Executor vor dem Call loggen.",
                )
            )
            raise typer.Exit(1) from err

        body = _pretty_exc(err)
        if os.getenv("FLOWFORGE_TRACE") == "1":
            body += "\n\n" + "".join(traceback.format_exc())
        typer.echo(_error_block(f"Model failed: {name}", body, "• Siehe Ursache oben."))
        raise typer.Exit(1) from err

    _run_models(pred, run_sql, run_py, before=_before, on_error=_on_error)

    typer.echo("✓ Done")


@app.command(
    help=(
        "Gibt den DAG als Mermaid oder erzeugt eine HTML-Seite.\n\nBeispiele:\n  "
        "flowforge dag .\n  flowforge dag . --env dev --html"
    )
)
def dag(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    html: HtmlOpt = False,
    engine: EngineOpt = None,
    out: OutOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
) -> None:
    if out is not None:
        out = out.resolve()
        out.mkdir(parents=True, exist_ok=True)

    ctx = _prepare_context(project, env_name, engine, vars)

    # Resolve output directory:
    dag_out = _resolve_dag_out_dir(ctx.project, out)
    dag_out.mkdir(parents=True, exist_ok=True)

    _, pred = _compile_selector(select)
    filtered_nodes = {k: v for k, v in REGISTRY.nodes.items() if pred(v)}

    if html:
        ex, *_ = ctx.make_executor()
        render_site(dag_out, filtered_nodes, executor=ex)
        typer.echo(f"HTML-DAG written to {dag_out / 'index.html'}")
    else:
        mm = mermaid(filtered_nodes)
        mmd = dag_out / "dag.mmd"
        mmd.write_text(mm, encoding="utf-8")
        typer.echo(f"Mermaid DAG written to {dag_out}")

    if LOG.isEnabledFor(logging.INFO):
        typer.echo(f"Profil: {env_name} | Engine: {ctx.profile.engine}")


@app.command(
    help=(
        "Materialisiert Modelle und führt konfigurierte Datenqualitäts-Checks aus."
        "\n\nBeispiel:\n  flowforge test . --env dev --select batch"
    )
)
def test(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
) -> None:
    # 0) Setup & Normalisierung
    ctx = _prepare_context(project, env_name, engine, vars)
    tokens, pred = _compile_selector(select)
    execu, run_sql, run_py = ctx.make_executor()

    # 1) Shim/Marker (optional)
    con = _get_test_con(execu)
    _maybe_print_marker(con)

    # 2) Modelle in Topo-Reihenfolge (mit optionalem Filter) ausführen
    _run_models(pred, run_sql, run_py)

    # 3) Tests laden & ggf. legacy Tag-Filter anwenden
    tests = _load_tests(ctx.project)
    tests = _apply_legacy_tag_filter(tests, tokens)
    if not tests:
        typer.secho("Keine Tests konfiguriert.", fg="bright_black")
        raise typer.Exit(code=0)

    # 4) Tests ausführen & Ergebnis zusammenfassen
    results = _run_dq_tests(con, tests)
    _print_summary(results)

    # 5) Exit-Code
    failed = sum(not r.ok for r in results)
    raise typer.Exit(code=2 if failed > 0 else 0)


# ----------------- Helper -----------------


def _maybe_print_marker(con: Any) -> None:
    if os.getenv("FLOWFORGE_SQL_DEBUG") == "1":
        typer.echo(getattr(con, "marker", "NO_SHIM"))


def _run_models(
    pred: Callable[[Any], bool],
    run_sql: Callable[[Any], Any],
    run_py: Callable[[Any], Any],
    *,
    before: Callable[[str, Any], None] | None = None,
    on_error: Callable[[str, Any, Exception], None] | None = None,
) -> None:
    order = [n for n in topo_sort(REGISTRY.nodes) if pred(REGISTRY.nodes[n])]
    _execute_models(order, run_sql, run_py, before=before, on_error=on_error)


def _load_tests(proj: Path) -> list[dict]:
    cfg_path = proj / "project.yml"
    if not cfg_path.exists():
        return []
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    return cfg.get("tests") or []


def _apply_legacy_tag_filter(tests: list[dict], tokens: list[str]) -> list[dict]:
    # Falls genau EIN Token ohne Präfix (tag:/type:/kind:) angegeben wurde,
    # als Legacy-DQ-Tag interpretieren.
    if len(tokens) != 1 or tokens[0].startswith(("tag:", "type:", "kind:")):
        return tests
    legacy_tag = tokens[0]

    def has_tag(t: dict) -> bool:
        tags = t.get("tags") or []
        return (legacy_tag in tags) if isinstance(tags, list) else (legacy_tag == tags)

    return [t for t in tests if has_tag(t)]


def _run_dq_tests(con: Any, tests: Iterable[dict]) -> list[DQResult]:
    results: list[DQResult] = []
    for t in tests:
        kind = t["type"]
        table = t.get("table")
        if not isinstance(table, str) or not table:
            raise typer.BadParameter("Missing or invalid 'table' in test config")
        col = t.get("column")

        t0 = time.perf_counter()
        ok, msg = _exec_test_kind(con, kind, t, table, col)
        ms = int((time.perf_counter() - t0) * 1000)

        results.append(DQResult(kind=kind, table=table, column=col, ok=ok, msg=msg, ms=ms))
    return results


def _exec_test_kind(con: Any, kind: str, t: dict, table: Any, col: Any) -> tuple[bool, str | None]:
    # Dispatch-Mapping statt großer if/elif-Kette
    try_map = {
        "not_null": lambda: testing.not_null(con, table, col),
        "unique": lambda: testing.unique(con, table, col),
        "greater_equal": lambda: testing.greater_equal(con, table, col, t.get("threshold", 0)),
        "non_negative_sum": lambda: testing.non_negative_sum(con, table, col),
        "row_count_between": lambda: testing.row_count_between(
            con, table, t.get("min", 1), t.get("max")
        ),
        "freshness": lambda: testing.freshness(con, table, col, t["max_delay_minutes"]),
    }

    fn = try_map.get(kind)
    if fn is None:
        raise typer.BadParameter(f"Unknown test type: {kind}")

    try:
        fn()
        return True, None
    except testing.TestFailure as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unexpected error: {e.__class__.__name__}: {e}"


def _print_summary(results: list[DQResult]) -> None:
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


@app.command(
    help=(
        "Seeds aus /seeds einspielen.\n\nBeispiele:\n  flowforge seed . "
        "--env dev\n  flowforge seed examples/postgres --env stg"
    )
)
def seed(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
) -> None:
    ctx = _prepare_context(project, env_name, engine, vars)
    execu, _, _ = ctx.make_executor()

    # Only Postgres exposes a schema that we pass to seed_project
    schema: str | None = None
    if ctx.profile.engine == "postgres":
        schema = ctx.profile.postgres.db_schema  # ← at this point profile is Postgres

    n = seed_project(ctx.project, execu, schema)
    typer.echo(f"✓ Seeded {n} table(s)")


@app.command()
def utest(
    project: ProjectArg = ".",
    model: ModelOpt = None,
    case: CaseOpt = None,
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    path: PathOpt = None,
    vars: VarsOpt = None,
) -> None:
    ctx = _prepare_context(project, env_name, engine, vars)

    ex, _, _ = ctx.make_executor()

    specs = discover_unit_specs(ctx.project, path=path, only_model=model)
    if not specs:
        typer.echo("ℹ️  Keine Unit-Tests gefunden (tests/unit/*.yml).")  # noqa: RUF001
        raise typer.Exit(0)

    failures = run_unit_specs(specs, ex, ctx.jinja_env, only_case=case)
    raise typer.Exit(code=2 if failures > 0 else 0)
