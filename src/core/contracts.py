"""
contracts.py — sqldim DataContract definitions for all CNPJ pipeline tables.

Every table that crosses the Silver→Gold layer boundary has a contract here.
Contracts are validated by ContractEngine before each DuckDBLoader merge so
schema mismatches are caught before they corrupt the warehouse.

Contract structure:
  • SCD6 dimensions (empresa, estabelecimento, socios, simples):
      Natural-key columns  +  attributes (JSONB)  +  SCD2 bookkeeping fields
  • Reference tables (quals, moti, natju, munic, cnae, pais):
      codigo (PK)  +  descricao

Usage::

    from src.core.contracts import CNPJ_CONTRACTS, CNPJ_SOURCE_CONTRACTS

    # Full DataContract metadata (version, owner, SLA)
    contract = CNPJ_CONTRACTS["empresa"]

    # Lightweight SourceContract for ContractEngine validation
    source_contract = CNPJ_SOURCE_CONTRACTS["empresa"]
    report = ContractEngine().validate(conn, "pq_view", source_contract)
    if report.has_errors():
        raise ContractViolationError(report.summary())
"""

from __future__ import annotations

from sqldim.contracts import (
    ContractVersion,
    ColumnSpec,
    DataContract,
    ContractRegistry,
    NotNull,
    NoDuplicates,
    ColumnExists,
    SourceContract,
)
from sqldim.medallion import Layer


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _v1() -> ContractVersion:
    return ContractVersion(major=1, minor=0, patch=0)


def _scd2_bookkeeping() -> list[ColumnSpec]:
    """Columns added by SCD2Mixin on every large dimension."""
    return [
        ColumnSpec(name="is_current",  dtype="BOOLEAN",   nullable=False),
        ColumnSpec(name="valid_from",  dtype="TIMESTAMP",  nullable=False),
        ColumnSpec(name="valid_to",    dtype="TIMESTAMP",  nullable=True),
        ColumnSpec(name="row_hash",    dtype="VARCHAR",    nullable=False),
    ]


# ---------------------------------------------------------------------------
# SCD6 dimension contracts
# ---------------------------------------------------------------------------

def _empresa_contract() -> DataContract:
    columns = [
        ColumnSpec(name="cnpj_basico", dtype="VARCHAR", nullable=False, primary_key=True),
        ColumnSpec(name="metadata",    dtype="JSON",    nullable=True),
        ColumnSpec(name="metadata_diff", dtype="JSON",  nullable=True),
        *_scd2_bookkeeping(),
    ]
    return DataContract(
        name="empresa",
        version=_v1(),
        owner="scrapper-rf-cnpj",
        layer=Layer.GOLD,
        columns=columns,
    )


def _estabelecimento_contract() -> DataContract:
    columns = [
        ColumnSpec(name="cnpj_basico", dtype="VARCHAR", nullable=False, primary_key=True),
        ColumnSpec(name="cnpj_ordem",  dtype="VARCHAR", nullable=False, primary_key=True),
        ColumnSpec(name="cnpj_dv",     dtype="VARCHAR", nullable=False, primary_key=True),
        ColumnSpec(name="metadata",    dtype="JSON",    nullable=True),
        ColumnSpec(name="metadata_diff", dtype="JSON",  nullable=True),
        *_scd2_bookkeeping(),
    ]
    return DataContract(
        name="estabelecimento",
        version=_v1(),
        owner="scrapper-rf-cnpj",
        layer=Layer.GOLD,
        columns=columns,
    )


def _socios_contract() -> DataContract:
    columns = [
        ColumnSpec(name="cnpj_basico",              dtype="VARCHAR", nullable=False, primary_key=True),
        ColumnSpec(name="cpf_cnpj_socio",           dtype="VARCHAR", nullable=False, primary_key=True),
        ColumnSpec(name="nome_socio_razao_social",  dtype="VARCHAR", nullable=False, primary_key=True),
        ColumnSpec(name="metadata",                 dtype="JSON",    nullable=True),
        ColumnSpec(name="metadata_diff",            dtype="JSON",    nullable=True),
        *_scd2_bookkeeping(),
    ]
    return DataContract(
        name="socios",
        version=_v1(),
        owner="scrapper-rf-cnpj",
        layer=Layer.GOLD,
        columns=columns,
    )


def _simples_contract() -> DataContract:
    columns = [
        ColumnSpec(name="cnpj_basico", dtype="VARCHAR", nullable=False, primary_key=True),
        ColumnSpec(name="metadata",    dtype="JSON",    nullable=True),
        ColumnSpec(name="metadata_diff", dtype="JSON",  nullable=True),
        *_scd2_bookkeeping(),
    ]
    return DataContract(
        name="simples",
        version=_v1(),
        owner="scrapper-rf-cnpj",
        layer=Layer.GOLD,
        columns=columns,
    )


# ---------------------------------------------------------------------------
# Reference / lookup table contracts  (SCD1)
# ---------------------------------------------------------------------------

def _ref_contract(name: str) -> DataContract:
    """Shared schema for all six reference tables: codigo + descricao."""
    return DataContract(
        name=name,
        version=_v1(),
        owner="scrapper-rf-cnpj",
        layer=Layer.GOLD,
        columns=[
            ColumnSpec(name="codigo",   dtype="VARCHAR", nullable=False, primary_key=True),
            ColumnSpec(name="descricao", dtype="VARCHAR", nullable=True),
        ],
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

CNPJ_REGISTRY: ContractRegistry = ContractRegistry()

for _contract in (
    _empresa_contract(),
    _estabelecimento_contract(),
    _socios_contract(),
    _simples_contract(),
    _ref_contract("quals"),
    _ref_contract("moti"),
    _ref_contract("natju"),
    _ref_contract("munic"),
    _ref_contract("cnae"),
    _ref_contract("pais"),
):
    CNPJ_REGISTRY.register(_contract)

#: Convenience dict for direct lookup by table name.
CNPJ_CONTRACTS: dict[str, DataContract] = {
    name: CNPJ_REGISTRY.get(name)
    for name in CNPJ_REGISTRY.all_names()
}

# ---------------------------------------------------------------------------
# SourceContracts for runtime ContractEngine validation
# ---------------------------------------------------------------------------
# Each SourceContract carries the minimum rules that a Parquet source file
# MUST satisfy before DuckDBLoader is allowed to proceed.  Rules are kept
# intentionally lightweight — we assert column presence and non-null keys only.
# Heavier SCD2 structural invariants (SCD2Invariants, MonotonicValidFrom, etc.)
# are enforced on the target PostgreSQL table by the StateContract, not here.

def _scd6_source_contract(natural_key_cols: list[str]) -> SourceContract:
    rules = [ColumnExists(c) for c in natural_key_cols]
    rules += [NotNull(c) for c in natural_key_cols]
    return SourceContract(rules=rules)


def _ref_source_contract() -> SourceContract:
    return SourceContract(rules=[ColumnExists("column_1"), NotNull("column_1")])


CNPJ_SOURCE_CONTRACTS: dict[str, SourceContract] = {
    "empresa":         _scd6_source_contract(["cnpj_basico"]),
    "estabelecimento": _scd6_source_contract(["cnpj_basico", "cnpj_ordem", "cnpj_dv"]),
    "socios":          _scd6_source_contract(["cnpj_basico", "cpf_cnpj_socio", "nome_socio_razao_social"]),
    "simples":         _scd6_source_contract(["cnpj_basico"]),
    # Reference tables come in as column_1 / column_2 positional names from Parquet
    "quals":  _ref_source_contract(),
    "moti":   _ref_source_contract(),
    "natju":  _ref_source_contract(),
    "munic":  _ref_source_contract(),
    "cnae":   _ref_source_contract(),
    "pais":   _ref_source_contract(),
}

__all__ = [
    "CNPJ_CONTRACTS",
    "CNPJ_SOURCE_CONTRACTS",
    "CNPJ_REGISTRY",
]
