from __future__ import annotations

import logging
import os

LOG = logging.getLogger("flowforge")
SQL_LOG = logging.getLogger("flowforge.sql")


def _setup_logging(verbose: int, quiet: int) -> None:
    """
    Map verbosity to levels:
      -q        → ERROR
       (default)→ WARNING
      -v        → INFO
      -vv       → DEBUG
    Also wires the SQL channel and keeps FLOWFORGE_SQL_DEBUG compatibility.
    """
    eff_level_threshold = 2
    eff = max(min(verbose - quiet, 2), -1)
    lvl = {-1: logging.ERROR, 0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}[eff]

    logging.basicConfig(level=lvl, format="%(levelname)s %(message)s")
    LOG.setLevel(lvl)

    sql_debug_env = os.getenv("FLOWFORGE_SQL_DEBUG") == "1"
    SQL_LOG.setLevel(
        logging.DEBUG if (eff >= eff_level_threshold or sql_debug_env) else logging.WARNING
    )

    if eff >= eff_level_threshold and not sql_debug_env:
        os.environ["FLOWFORGE_SQL_DEBUG"] = "1"


__all__ = ["LOG", "SQL_LOG", "_setup_logging"]
