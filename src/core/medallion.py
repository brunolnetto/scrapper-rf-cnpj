"""
medallion.py — sqldim MedallionRegistry for the CNPJ pipeline.

The CNPJ ETL follows a three-layer medallion architecture:

* **Bronze** — raw ZIP and CSV files extracted from Receita Federal.
* **Silver** — schema-normalised Parquet files produced by the conversion
  step, one file per CNPJ entity type.
* **Gold** — dimension tables in PostgreSQL (SCD2 variants with ``metadata``
  JSONB bag, and simple reference look-up tables).

Usage::

    from src.core.medallion import CNPJ_REGISTRY, Layer

    layer = CNPJ_REGISTRY.get_layer("empresa")  # Layer.GOLD
    silver_datasets = CNPJ_REGISTRY.datasets_in(Layer.SILVER)
"""

from sqldim.medallion import Layer, MedallionRegistry

# ---------------------------------------------------------------------------
# Canonical entity table names (matches ``__tablename__`` in business.py and
# the ``SCD2_TABLES`` frozensets in the loading modules).
# ---------------------------------------------------------------------------

#: SCD2 dimension tables — large entities that track historical changes.
_SCD2_TABLES = frozenset({"empresa", "estabelecimento", "socios", "simples"})

#: Reference tables — small, mostly static look-up tables.
_REF_TABLES = frozenset({"quals", "moti", "natju", "munic", "cnae", "pais"})

#: All entity names, used to derive the silver Parquet datasets.
_ALL_TABLES = _SCD2_TABLES | _REF_TABLES


def _build_registry() -> MedallionRegistry:
    registry = MedallionRegistry()

    # Bronze — raw source files
    registry.register("cnpj_raw_zip",   Layer.BRONZE)
    registry.register("cnpj_raw_csv",   Layer.BRONZE)

    # Silver — one Parquet per entity type
    for name in _ALL_TABLES:
        registry.register(f"{name}.parquet", Layer.SILVER)

    # Gold — PostgreSQL dimension / reference tables
    for name in _ALL_TABLES:
        registry.register(name, Layer.GOLD)

    return registry


#: Pre-built singleton registry for the CNPJ pipeline.
CNPJ_REGISTRY: MedallionRegistry = _build_registry()

# ---------------------------------------------------------------------------
# Deterministic load order
# ---------------------------------------------------------------------------
# Reference tables are small and loaded first so the SCD2 dimensions can
# depend on them during validation.  Within each group, the order is stable.
#
# The order is consumed by FileLoadingService.load_multiple_tables() to replace
# the old file-size heuristic with a semantically correct dependency sequence.
# ---------------------------------------------------------------------------

#: Stable load order for all CNPJ Gold-layer tables.
CNPJ_LOAD_ORDER: list[str] = [
    # Reference look-up tables — load first (smallest, no FK deps)
    "quals",
    "moti",
    "natju",
    "munic",
    "cnae",
    "pais",
    # SCD2 dimension tables — load after references
    "empresa",
    "estabelecimento",
    "socios",
    "simples",
]

assert set(CNPJ_LOAD_ORDER) == _ALL_TABLES, (
    "CNPJ_LOAD_ORDER is out of sync with _ALL_TABLES — update the list above."
)

__all__ = ["CNPJ_REGISTRY", "CNPJ_LOAD_ORDER", "Layer", "MedallionRegistry"]
