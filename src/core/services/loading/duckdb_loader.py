"""
duckdb_loader.py - Zero-copy streaming loader using DuckDB as the ETL bridge.

Architecture:
    ParquetSource (sqldim)
        └── ContractEngine (quality gate — blocks on ERROR violations)
                └── DuckDB (in-process, streaming)
                        └── PostgreSQLSink (sqldim)
                                └── ColumnLineageFacet (emitted after merge)

Key advantages over asyncpg batch loading:
- Peak memory is O(1) regardless of file size — DuckDB streams in chunks internally.
- SCD2/SCD6 merge is pushed entirely to the DuckDB/PG engine.
- No Python objects materialised for rows — no List[Tuple] overhead.
- Natural keys and SCD type are inferred directly from the DimensionModel subclass.
- ParquetSource → LazySCDMetadataProcessor → PostgreSQLSink for SCD6 dimensions.
- ParquetSource → DELETE + INSERT (PostgreSQLSink.write) for lookup dimensions.
- ContractEngine validates column presence + NOT NULL on natural keys before merge.
- ColumnLineageFacet is emitted after every successful SCD6 merge.
"""

import time
from pathlib import Path
from typing import Optional, Tuple
import duckdb

from sqldim.contracts import ContractEngine, ContractViolationError
from sqldim.contracts.report import ContractReport
from sqldim.core.kimball.dimensions.scd.processors import LazySCDMetadataProcessor
from sqldim.lineage.column import ColumnLineageEntry, ColumnLineageFacet
from sqldim.sinks import PostgreSQLSink
from sqldim.sources.batch.parquet import ParquetSource

from ....setup.logging import logger
from ....database.models.business import (
    Empresa, Estabelecimento, Socios, SimplesNacional,
    Qualificacoes, MotivoCadastral, NaturezaJuridica, Municipio, Cnae, Pais,
)
from ...contracts import CNPJ_SOURCE_CONTRACTS
from ..observability.observatory import PipelineObservatory

# Model registry: table_name → DimensionModel subclass.
# Natural keys and SCD configuration are read directly from __natural_key__
# on each class — no separate static dict required.
_DIM_MODELS: dict[str, type] = {
    m.table_name(): m
    for m in (Empresa, Estabelecimento, Socios, SimplesNacional)
}

# Registry for SCD1 reference/lookup tables.  Column layout is derived from
# the model class at import time — no information_schema round-trip needed.
_REF_MODELS: dict[str, type] = {
    m.table_name(): m
    for m in (Qualificacoes, MotivoCadastral, NaturezaJuridica, Municipio, Cnae, Pais)
}

# Exposed for routing in service.py and tests — derived from the registry.
SCD2_TABLES: frozenset[str] = frozenset(_DIM_MODELS)


class DuckDBLoader:
    """
    Streams a Parquet file directly into PostgreSQL using DuckDB.

    Validation and observability flow per load():
      1. ContractEngine validates the Parquet view (column presence + NOT NULL keys).
         Any ERROR-level violation raises ContractViolationError and aborts the load.
      2. _execute_merge routes to LazySCDMetadataProcessor (SCD6) or DELETE+INSERT (ref).
      3. Observatory records the load result (rows_inserted / versioned / unchanged).
      4. For SCD6 tables a ColumnLineageFacet is built and logged.

    Usage:
        loader = DuckDBLoader(pg_dsn="postgresql://user:pass@host:5432/db")
        ok, err, rows = loader.load(parquet_path, table_name, batch_date="2024-12")
    """

    def __init__(
        self,
        pg_dsn: str,
        threads: int = 2,
        memory_limit_mb: int = 512,
        temp_dir: Optional[str] = None,
        observatory: Optional[PipelineObservatory] = None,
    ):
        """
        Args:
            pg_dsn:           PostgreSQL DSN (postgresql:// scheme).
            threads:          DuckDB worker threads (keep low to avoid RAM spikes).
            memory_limit_mb:  Hard memory cap enforced by DuckDB itself.
            temp_dir:         Directory for DuckDB spill files.  When set, DuckDB
                              can spill hash tables to disk instead of OOM-killing.
            observatory:      Optional PipelineObservatory for recording load results.
                              When None, observatory recording is skipped.
        """
        self.pg_dsn = pg_dsn
        self.threads = threads
        self.memory_limit_mb = memory_limit_mb
        self.temp_dir = temp_dir
        self._observatory = observatory

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self,
        parquet_path: Path,
        table_name: str,
        batch_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Tuple[bool, Optional[str], int]:
        """
        Stream *parquet_path* into *table_name* in PostgreSQL.

        Runs the quality gate (ContractEngine) before executing the merge.
        ContractViolationError from the gate is caught and returned as
        (False, error_message, 0) so the caller can handle it uniformly.

        Returns (success, error_message, rows_affected).
        """
        parquet_path = Path(parquet_path)
        if not parquet_path.exists():
            return False, f"Parquet file not found: {parquet_path}", 0

        batch_date_sql = f"'{batch_date}'" if batch_date else "NOW()"

        try:
            conn = self._make_connection()
            try:
                self._attach_postgres(conn)
                contract_report = self._run_quality_gate(conn, parquet_path, table_name)
                rows = self._execute_merge(conn, parquet_path, table_name, batch_date_sql, dry_run)
                self._record_observatory_ref(table_name, batch_date, parquet_path, rows)
                self._ingest_contract_quality(contract_report, table_name, batch_date)
                return True, None, rows
            finally:
                conn.close()

        except ContractViolationError as exc:
            logger.error(f"[DuckDBLoader] Contract violation for '{table_name}': {exc}")
            return False, str(exc), 0
        except Exception as exc:
            logger.error(f"[DuckDBLoader] Failed loading '{table_name}': {exc}")
            return False, str(exc), 0

    # ------------------------------------------------------------------
    # Quality gate
    # ------------------------------------------------------------------

    def _run_quality_gate(
        self,
        conn: duckdb.DuckDBPyConnection,
        parquet_path: Path,
        table_name: str,
    ) -> Optional[ContractReport]:
        """Validate the Parquet source against its contract before merging.

        Creates a transient DuckDB view over the Parquet file so
        :class:`~sqldim.contracts.ContractEngine` can run SQL-based rules
        without materialising any data in Python.

        Returns the :class:`~sqldim.contracts.report.ContractReport` so it can
        be forwarded to the observatory for drift tracking, or ``None`` when
        no source contract is registered for *table_name*.

        Raises :class:`~sqldim.contracts.ContractViolationError` when the
        report contains ERROR-level violations.
        """
        source_contract = CNPJ_SOURCE_CONTRACTS.get(table_name)
        if source_contract is None:
            logger.debug(f"[DuckDBLoader] No source contract for '{table_name}' — skipping gate")
            return None

        pq_path_str = str(parquet_path).replace("\\", "/")
        view_name = f"_cnpj_gate_{table_name}"
        conn.execute(
            f"CREATE OR REPLACE TEMP VIEW {view_name} AS "
            f"SELECT * FROM read_parquet('{pq_path_str}')"
        )

        try:
            engine = ContractEngine()
            report = engine.validate(conn, view_name, source_contract)
        finally:
            conn.execute(f"DROP VIEW IF EXISTS {view_name}")

        if report.has_errors():
            raise ContractViolationError(
                f"Source contract violations for '{table_name}':\n{report.summary()}"
            )

        if report.has_warnings():
            logger.warning(f"[DuckDBLoader] Contract warnings for '{table_name}':")
            report.log(logger)

        logger.debug(
            f"[DuckDBLoader] Quality gate passed for '{table_name}' "
            f"in {report.elapsed_s:.3f}s"
        )
        return report

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a fresh in-process DuckDB connection with resource caps."""
        import os as _os
        conn = duckdb.connect(database=":memory:")
        conn.execute(f"SET threads TO {self.threads};")
        conn.execute(f"SET memory_limit='{self.memory_limit_mb}MB';")
        conn.execute("SET enable_progress_bar = false;")
        if self.temp_dir:
            _os.makedirs(self.temp_dir, exist_ok=True)
            conn.execute(f"SET temp_directory='{self.temp_dir}';")
        return conn

    def _attach_postgres(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Install and load the postgres_scanner extension, then attach the DB."""
        conn.execute("INSTALL postgres;")
        conn.execute("LOAD postgres;")
        conn.execute(f"ATTACH '{self.pg_dsn}' AS pg (TYPE POSTGRES, READ_WRITE);")
        logger.debug("[DuckDBLoader] PostgreSQL attached via postgres_scanner.")

    def _execute_merge(
        self,
        conn: duckdb.DuckDBPyConnection,
        parquet_path: Path,
        table_name: str,
        batch_date_sql: str,
        dry_run: bool,
    ) -> int:
        """Route to sqldim LazySCDMetadataProcessor for SCD2/SCD6 tables, or standard
        upsert SQL for reference tables."""
        t0 = time.perf_counter()

        if dry_run:
            logger.info(f"[DuckDBLoader] DRY RUN for '{table_name}' — skipping execution")
            return 0

        logger.info(f"[DuckDBLoader] Starting merge for '{table_name}' from {parquet_path.name}")

        if table_name in SCD2_TABLES:
            rows = self._scd_metadata_merge(conn, parquet_path, table_name, batch_date_sql)
        else:
            rows = self._standard_upsert(conn, parquet_path, table_name)

        elapsed = time.perf_counter() - t0
        logger.info(
            f"[DuckDBLoader] '{table_name}' done: {rows:,} rows "
            f"in {elapsed:.1f}s ({rows / max(elapsed, 0.001):.0f} r/s)"
        )
        return rows

    # ------------------------------------------------------------------
    # SCD2/SCD6 merge via sqldim LazySCDMetadataProcessor
    # ------------------------------------------------------------------

    def _scd_metadata_merge(
        self,
        conn: duckdb.DuckDBPyConnection,
        parquet_path: Path,
        table_name: str,
        batch_date_sql: str,
    ) -> int:
        """Apply SCD2 (metadata-bag) merge via sqldim.

        Uses :class:`~sqldim.processors.LazySCDMetadataProcessor` backed by
        :class:`~sqldim.sinks.PostgreSQLSink`.  All source columns that are not
        part of the natural key are auto-discovered and packed into the
        ``metadata`` JSONB bag.  Change detection uses ``row_hash`` (MD5 of
        the metadata JSON) so no per-column tracking configuration is needed.

        Passes a :class:`~sqldim.sources.batch.parquet.ParquetSource` so the
        processor receives a typed source object rather than a raw path string.
        Natural keys are derived from the model's ``__natural_key__`` attribute.
        """
        from contextlib import suppress

        model = _DIM_MODELS.get(table_name)
        if model is None:
            raise ValueError(
                f"[DuckDBLoader] '{table_name}' is not a registered SCD dimension"
            )
        natural_key = list(model.__natural_key__)

        pq_path_str = str(parquet_path).replace("\\", "/")
        nk_set = frozenset(natural_key)

        pq_cols = [r[0] for r in conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{pq_path_str}')"
        ).fetchall()]
        metadata_cols = [c for c in pq_cols if c not in nk_set]

        # Re-use the already-attached DuckDB connection so both the pg ATTACH
        # and the sqldim processor share the same session.
        sink = PostgreSQLSink(dsn=self.pg_dsn, schema="public")
        sink._con = conn  # share the session

        # Extract a plain ISO string from the batch_date_sql expression if possible
        batch_date_str: Optional[str] = None
        if batch_date_sql.startswith("'") and batch_date_sql.endswith("'"):
            batch_date_str = batch_date_sql[1:-1]

        proc = LazySCDMetadataProcessor(
            natural_key=natural_key,
            metadata_columns=metadata_cols,
            sink=sink,
            con=conn,
        )

        result = proc.process(ParquetSource(pq_path_str), table_name, now=batch_date_str)

        current_count = conn.execute(
            f"SELECT COUNT(*) FROM pg.public.{table_name} WHERE is_current = TRUE"
        ).fetchone()[0]

        logger.info(
            f"[DuckDBLoader] '{table_name}' SCD merge — "
            f"{result.inserted:,} new, {result.versioned:,} changed, "
            f"{result.unchanged:,} unchanged → {current_count:,} current rows"
        )

        # Emit column-level lineage
        self._emit_column_lineage(table_name, natural_key, metadata_cols)

        # Record detailed SCD metrics to observatory
        if self._observatory is not None:
            try:
                self._observatory.record_load_result(
                    table_name=table_name,
                    layer="gold",
                    run_id=batch_date_str or "unknown",
                    rows_inserted=result.inserted,
                    rows_versioned=result.versioned,
                    rows_unchanged=result.unchanged,
                    schema_columns=pq_cols,
                )
            except Exception as obs_err:
                logger.debug(f"[Observatory] SCD record skipped: {obs_err}")

        return current_count

    def _standard_upsert(
        self,
        conn: duckdb.DuckDBPyConnection,
        parquet_path: Path,
        table_name: str,
    ) -> int:
        """
        For reference/lookup tables (qualificacoes, municipio, etc.) that are
        not SCD2 — do a simple DELETE + INSERT via DuckDB.

        Handles two common mismatches:
        - Parquet has generic column names (column_1, column_2) → mapped by position
          to the actual DB column names (looked up from pg.information_schema).
        - DB has a row_hash NOT NULL column → computed via md5 of all source columns.
        """
        pq_path_str = str(parquet_path).replace("\\", "/")
        pg_table = f"pg.{table_name}"

        # --- Discover parquet columns ---
        pq_cols = [
            row[0]
            for row in conn.execute(
                f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{pq_path_str}')) t"
            ).fetchall()
        ]

        # --- Derive actual PG table columns (excluding generated/SCD columns) ---
        # Prefer the registered model (zero DB round-trips); fall back to
        # information_schema for tables not yet in _REF_MODELS.
        _MANAGED_COLS = {"row_hash", "sk", "valid_from", "valid_to", "is_current"}
        model = _REF_MODELS.get(table_name)
        if model is not None:
            pg_cols = [
                col.name
                for col in model.__table__.columns
                if col.name not in _MANAGED_COLS
            ]
        else:
            pg_cols = [
                row[0]
                for row in conn.execute(
                    "SELECT column_name FROM pg.information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = ? "
                    "ORDER BY ordinal_position",
                    [table_name],
                ).fetchall()
                if row[0] not in _MANAGED_COLS
            ]

        # Map parquet columns → table columns by position (handles column_1/column_2 naming)
        if len(pq_cols) != len(pg_cols):
            raise ValueError(
                f"Column count mismatch for '{table_name}': "
                f"parquet has {len(pq_cols)} cols, DB has {len(pg_cols)} cols"
            )
        # Build SELECT clause using pg column names as aliases
        select_parts = [f"s.{pq} AS {pg}" for pq, pg in zip(pq_cols, pg_cols)]
        select_clause = ", ".join(select_parts)

        # Compute row_hash from all source columns via md5
        hash_parts = " || '|' || ".join(f"COALESCE(s.{c}::TEXT, '')" for c in pq_cols)
        row_hash_expr = f"md5({hash_parts}) AS row_hash"

        # Build final column list (business cols + row_hash)
        dst_cols = ", ".join(pg_cols + ["row_hash"])

        # For reference tables: delete-then-reload (they are small, < 100k rows)
        delete_sql = f"DELETE FROM {pg_table};"
        insert_sql = (
            f"INSERT INTO {pg_table} ({dst_cols}) "
            f"SELECT {select_clause}, {row_hash_expr} "
            f"FROM read_parquet('{pq_path_str}') AS s;"
        )
        conn.execute(delete_sql)
        conn.execute(insert_sql)

        count = conn.execute(f"SELECT COUNT(*) FROM {pg_table};").fetchone()[0]
        return count

    # ------------------------------------------------------------------
    # Observatory recording (reference tables only)
    # ------------------------------------------------------------------

    def _ingest_contract_quality(
        self,
        report: Optional[ContractReport],
        table_name: str,
        batch_date: Optional[str],
    ) -> None:
        """Forward the ContractEngine report to the observatory.

        Calls :meth:`~PipelineObservatory.ingest_source_quality` with the
        real :class:`~sqldim.contracts.report.ContractReport` produced by the
        quality gate, so the observatory records genuine contract violations
        rather than a synthetic report built from aggregated load metrics.

        No-op when *report* is ``None`` (no contract registered for this table)
        or when no observatory is attached.
        """
        if self._observatory is None or report is None:
            return
        try:
            self._observatory.ingest_source_quality(
                report,
                table_name=table_name,
                run_id=batch_date or "unknown",
            )
        except Exception as exc:
            logger.debug(f"[Observatory] Contract quality record skipped: {exc}")

    def _record_observatory_ref(
        self,
        table_name: str,
        batch_date: Optional[str],
        parquet_path: Path,
        rows: int,
    ) -> None:
        """Record reference-table load in the observatory (rows_inserted only).

        SCD6 tables already record detailed SCD metrics inside
        _scd_metadata_merge(); this method is a no-op for those to avoid
        double-recording.
        """
        if self._observatory is None or table_name in SCD2_TABLES:
            return

        schema_columns: Optional[list] = None
        try:
            pq_path_str = str(parquet_path).replace("\\", "/")
            schema_columns = [
                r[0] for r in duckdb.connect(":memory:").execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{pq_path_str}')"
                ).fetchall()
            ]
        except Exception:
            pass

        try:
            self._observatory.record_load_result(
                table_name=table_name,
                layer="gold",
                run_id=batch_date or "unknown",
                rows_inserted=rows,
                schema_columns=schema_columns,
            )
        except Exception as obs_err:
            logger.debug(f"[Observatory] Reference table record skipped: {obs_err}")

    # ------------------------------------------------------------------
    # Column-level lineage
    # ------------------------------------------------------------------

    def _emit_column_lineage(
        self,
        table_name: str,
        natural_key: list[str],
        metadata_cols: list[str],
    ) -> None:
        """Build and log a ColumnLineageFacet for an SCD6 merge.

        Natural-key columns map 1:1 from Parquet to the target DB column.
        All remaining Parquet columns are packed into ``metadata`` (JSONB bag).
        """
        entries: list[ColumnLineageEntry] = []

        for col in natural_key:
            entries.append(ColumnLineageEntry(
                output_column=col,
                input_columns=[col],
                transform_description="Direct mapping (natural key)",
                confidence="declared",
            ))

        if metadata_cols:
            entries.append(ColumnLineageEntry(
                output_column="metadata",
                input_columns=metadata_cols,
                transform_description="Packed into JSONB metadata bag (SCD6)",
                confidence="declared",
            ))

        facet = ColumnLineageFacet(entries=entries)
        logger.debug(
            f"[Lineage] '{table_name}' column lineage: "
            f"{len(natural_key)} direct, {len(metadata_cols)} → metadata"
        )
        logger.info(f"[Lineage] '{table_name}': {facet.to_dict()}")


# ------------------------------------------------------------------
# Factory helper consumed by FileLoadingService
# ------------------------------------------------------------------

def make_duckdb_loader(
    config,
    observatory: Optional[PipelineObservatory] = None,
) -> Optional[DuckDBLoader]:
    """
    Build a DuckDBLoader from the application config.
    Returns None if DuckDB loading is disabled or config is incomplete.

    Args:
        config:       Application ConfigLoader instance.
        observatory:  Optional PipelineObservatory injected from FileLoadingService.
    """
    try:
        loading_cfg = config.pipeline.loading
        if not getattr(loading_cfg, "use_duckdb", False):
            return None

        pg_dsn = config.pipeline.data_sink.database.get_connection_string()
        memory_mb = getattr(loading_cfg, "duckdb_memory_limit_mb", 512)
        threads = getattr(loading_cfg, "duckdb_threads", 2)
        temp_dir = getattr(loading_cfg, "duckdb_temp_directory", None)

        loader = DuckDBLoader(
            pg_dsn=pg_dsn,
            threads=threads,
            memory_limit_mb=memory_mb,
            temp_dir=temp_dir,
            observatory=observatory,
        )
        logger.info(
            f"[DuckDBLoader] Initialised — memory_limit={memory_mb}MB, threads={threads}"
        )
        return loader

    except Exception as exc:
        logger.warning(f"[DuckDBLoader] Could not initialise: {exc}")
        return None
