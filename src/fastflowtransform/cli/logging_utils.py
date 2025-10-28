# fastflowtransform/cli/logging_utils.py
# from __future__ import annotations

# import logging
# import os

# from fastflowtransform.logging import get_logger, setup_logging as _setup_pkg_logging

# # retained names so other code keeps working
# LOG = get_logger("cli")
# SQL_LOG = get_logger("sql")

# def _setup_logging(verbose: int, quiet: int) -> None:
#     """
#     CLI entrypoint config. Verbosity/quiet map to stdlib levels.
#     """
#     # base: WARNING; -v INFO; -vv DEBUG; -q ERROR
#     verbose_debug_level = 2
#     level = logging.WARNING
#     if quiet >= 1:
#         level = logging.ERROR
#     elif verbose == 1:
#         level = logging.INFO
#     elif verbose >= verbose_debug_level:
#         level = logging.DEBUG

#     # Env-controlled toggles
#     sql_debug_env = os.getenv("FFT_SQL_DEBUG", "").lower() in ("1", "true", "yes", "on")
#     json_env = os.getenv("FFT_LOG_JSON", "").lower() in ("1", "true", "yes", "on")

#     # Use package-wide setup (human console by default)
#     _setup_pkg_logging(
#         level=level,
#         json=json_env,
#         propagate_sql=(verbose >= verbose_debug_level) or sql_debug_env,
#     )
