"""
src/core/services/analytics — sqldim lazy analytical fact builders.

Two analytical patterns are available for post-load analytics on the
PostgreSQL gold layer:

* :func:`build_simples_history` — accumulates Simples Nacional tax-regime
  status for each CNPJ over consecutive monthly runs using
  :class:`~sqldim.core.loaders.LazyCumulativeLoader`.

* :func:`build_simples_bitmask` — converts a per-CNPJ list of active-month
  dates into a compact bitmask using
  :class:`~sqldim.core.loaders.LazyBitmaskLoader`.

Both functions operate entirely inside DuckDB (zero-copy, O(1) RAM) and
write results back to PostgreSQL through
:class:`~sqldim.sinks.PostgreSQLSink`.
"""

from .loaders import build_simples_history, build_simples_bitmask

__all__ = ["build_simples_history", "build_simples_bitmask"]
