"""
loaders.py — CNPJ analytical loaders (deferred).

The sqldim-based cumulative loaders (simples_history, simples_bitmask) have
been removed from the active loading pipeline as part of the sqldim refactor
(Phase 7).  They relied on sqldim.core.loaders.LazyCumulativeLoader which is
not yet stable for production use.

TODO: Re-implement using sqldim FactModel + LazyCumulativeLoader once the
      Gold layer schema for simples_history_cumulated is finalised.
"""

# Analytics loaders are deferred — no public API exported from this module.

__all__: list = []
