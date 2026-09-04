"""
observatory.py — sqldim DriftObservatory wrapper for the CNPJ pipeline.

Tracks two concerns using sqldim's dimensional observability:

1. **Schema evolution** — when a Parquet file's column set changes between
   runs, an ``ObsSchemaEvolutionFact`` row is recorded via
   :meth:`~sqldim.observability.DriftObservatory.ingest_evolution`.
2. **Quality drift** — after each table load, SCD2 churn metrics are stored
   as ``ObsQualityDriftFact`` rows via
   :meth:`~sqldim.observability.DriftObservatory.ingest_quality`.

All report objects are the canonical sqldim types — no shims or wrappers.

Usage::

    obs = PipelineObservatory.from_path("logs/observatory.duckdb")

    obs.record_load_result(
        table_name="empresa",
        layer="gold",
        run_id="2024-12-01",
        rows_inserted=100_000,
        rows_versioned=5_000,
        rows_unchanged=40_000_000,
        schema_columns=["cnpj_basico", "metadata", "row_hash"],
    )

    print(obs.observatory.breaking_change_rate().fetchdf())
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from sqldim.observability import DriftObservatory
from sqldim.contracts.engine import EvolutionReport, EvolutionChange
from sqldim.contracts.report import ContractReport, ContractViolation

_log = logging.getLogger(__name__)


class PipelineObservatory:
    """Thin facade over :class:`~sqldim.observability.DriftObservatory`
    that builds the required sqldim report objects from loading-event data.
    """

    def __init__(self, obs: DriftObservatory) -> None:
        self._obs = obs
        # Column-set snapshot per table — held in memory for the current run
        self._col_fingerprints: dict[str, frozenset[str]] = {}

    @property
    def observatory(self) -> DriftObservatory:
        """The underlying :class:`~sqldim.observability.DriftObservatory`."""
        return self._obs

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def in_memory(cls) -> "PipelineObservatory":
        """Create a transient, in-memory observatory (useful for tests)."""
        return cls(DriftObservatory.in_memory())

    @classmethod
    def from_path(cls, path: str | Path) -> "PipelineObservatory":
        """Open or create a file-backed observatory at *path*."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return cls(DriftObservatory.from_path(str(path)))

    # ------------------------------------------------------------------
    # Primary API
    # ------------------------------------------------------------------

    def record_load_result(
        self,
        *,
        table_name: str,
        layer: str,
        run_id: str,
        rows_inserted: int = 0,
        rows_versioned: int = 0,
        rows_unchanged: int = 0,
        schema_columns: Optional[list[str]] = None,
    ) -> None:
        """Record a completed table load into the observatory.

        Detects schema changes by comparing the current column set against
        the previous call's fingerprint for *table_name*.  Builds
        :class:`~sqldim.contracts.engine.EvolutionReport` and
        :class:`~sqldim.contracts.report.ContractReport` using sqldim's own
        types before ingesting them via the observatory.
        """
        total = rows_inserted + rows_versioned + rows_unchanged
        change_rate = (rows_inserted + rows_versioned) / max(total, 1)

        # -- Schema evolution (only when columns are provided) --
        if schema_columns is not None:
            new_cols = frozenset(schema_columns)
            prev_cols = self._col_fingerprints.get(table_name)

            if prev_cols is not None and new_cols != prev_cols:
                added   = new_cols - prev_cols
                removed = prev_cols - new_cols
                report = EvolutionReport(
                    safe_changes=[
                        EvolutionChange(change_type="added", column=c, detail="new column")
                        for c in sorted(added)
                    ],
                    breaking_changes=[
                        EvolutionChange(change_type="removed", column=c, detail="column removed")
                        for c in sorted(removed)
                    ],
                )
                try:
                    with self._obs.transaction():
                        self._obs.ingest_evolution(report, dataset=table_name, run_id=run_id, layer=layer)
                except Exception as exc:
                    _log.debug("[Observatory] Evolution ingest skipped: %s", exc)

                _log.info(
                    "[Observatory] Schema drift in '%s': +%d cols, -%d cols",
                    table_name, len(added), len(removed),
                )
            self._col_fingerprints[table_name] = new_cols

        # -- Quality / SCD2 churn metrics --
        violations = []
        if total == 0:
            violations.append(ContractViolation(
                rule="rows_loaded",
                severity="error",
                count=0,
                detail="No rows were loaded for this table",
            ))
        if change_rate >= 0.5:
            violations.append(ContractViolation(
                rule="scd2_churn_rate",
                severity="warning",
                count=rows_inserted + rows_versioned,
                detail=f"SCD2 churn rate {change_rate:.2%} ≥ 50% threshold",
            ))

        quality_report = ContractReport(
            violations=violations,
            view=table_name,
            elapsed_s=0.0,
        )
        try:
            with self._obs.transaction():
                self._obs.ingest_quality(quality_report, dataset=table_name, run_id=run_id, layer=layer)
        except Exception as exc:
            _log.debug("[Observatory] Quality ingest skipped: %s", exc)

        _log.info(
            "[Observatory] '%s' — %d new, %d changed, %d unchanged (churn=%.2f%%)",
            table_name, rows_inserted, rows_versioned, rows_unchanged, change_rate * 100,
        )

    def query_breaking_changes(self):
        """Return a DuckDB relation with breaking change rate per dataset."""
        try:
            return self._obs.breaking_change_rate()
        except Exception:
            return None

    def query_worst_quality_datasets(self):
        """Return a DuckDB relation with worst quality datasets."""
        try:
            return self._obs.worst_quality_datasets()
        except Exception:
            return None

    def query_drift_velocity(self):
        """Return a DuckDB relation with schema drift velocity over time."""
        try:
            return self._obs.drift_velocity()
        except Exception:
            return None

    def ingest_source_quality(
        self,
        report: ContractReport,
        *,
        table_name: str,
        run_id: str,
    ) -> None:
        """Ingest the actual ContractEngine validation report into the observatory.

        Forwards the real :class:`~sqldim.contracts.report.ContractReport`
        produced by :class:`~sqldim.contracts.ContractEngine` directly to
        :meth:`~sqldim.observability.DriftObservatory.ingest_quality`.

        This records source-layer contract validation results (column presence,
        NOT NULL checks) as ``ObsQualityDriftFact`` rows — using the genuine
        report rather than a synthetic one built from aggregated metrics.
        Called per-table right after the quality gate, before the merge.
        """
        try:
            with self._obs.transaction():
                self._obs.ingest_quality(
                    report,
                    dataset=table_name,
                    run_id=run_id,
                    layer="bronze",
                    pipeline_name="cnpj",
                    domain="receita_federal",
                )
        except Exception as exc:
            _log.debug("[Observatory] Source quality ingest skipped: %s", exc)

