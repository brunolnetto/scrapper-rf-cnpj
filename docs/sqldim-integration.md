# sqldim Integration Guide

This document describes how **scrapper-rf-cnpj** integrates with the
[sqldim](https://github.com/pingu/sqldim) dimensional modelling library as
an upstream dependency. The guiding principle is **DRY** — scrapper-rf-cnpj
delegates all cross-cutting engineering concerns to sqldim and avoids
re-implementing anything sqldim already provides.

---

## Overview

| Integration point | sqldim class(es) used | File(s) in this repo |
|---|---|---|
| SCD2 merge SQL | `LazySCDMetadataProcessor`, `PostgreSQLSink` | `src/core/services/loading/duckdb_loader.py` |
| Schema-evolution & quality observability | `DriftObservatory`, `EvolutionReport`, `EvolutionChange`, `ContractReport`, `ContractViolation` | `src/core/services/observability/observatory.py` |
| Pipeline run lineage | `ConsoleLineageEmitter`, `LineageEvent`, `RunState`, `DatasetRef` | `src/core/orchestrator.py` |
| Medallion layer catalog | `MedallionRegistry`, `Layer` | `src/core/medallion.py` |
| Cumulative fact analytics | `LazyCumulativeLoader` | `src/core/services/analytics/loaders.py` |
| Activity bitmask analytics | `LazyBitmaskLoader` | `src/core/services/analytics/loaders.py` |

---

## 1. SCD2 Merge — `LazySCDMetadataProcessor`

### What it does

`DuckDBLoader._scd_metadata_merge` replaces the hand-rolled
`_scd2_merge_sql` helper with sqldim's
[`LazySCDMetadataProcessor`](../sqldim/sqldim/processors/lazy_scd_metadata.py).
The processor generates the correct `INSERT / UPDATE` SCD6 SQL (with
`metadata JSONB`, `metadata_diff JSONB`, `row_hash TEXT`, `valid_from`,
`valid_to`, `is_current`) and executes it via `PostgreSQLSink` — entirely
inside DuckDB, zero memory overhead.

### Key code

```python
# src/core/services/loading/duckdb_loader.py
from sqldim.processors import LazySCDMetadataProcessor
from sqldim.sinks import PostgreSQLSink

sink = PostgreSQLSink(dsn=self._pg_dsn)
sink._con = conn   # share the already-attached DuckDB session

processor = LazySCDMetadataProcessor(
    table_name=table_name,
    natural_keys=_NK[table_name],
    sink=sink,
    batch_date=batch_date,
)
rows = processor.process(str(parquet_path))
```

### Natural key mapping

```python
_NK: dict[str, list[str]] = {
    "empresa":        ["cnpj_basico"],
    "estabelecimento":["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
    "socios":         ["cnpj_basico", "nome_socio_razao_social"],
    "simples":        ["cnpj_basico"],
}
```

---

## 2. Observability — `DriftObservatory`

### What it does

`PipelineObservatory` (in `src/core/services/observability/observatory.py`) wraps sqldim's `DriftObservatory` and calls it after each successful load.

Two concerns are tracked:

* **Schema evolution** — when the Parquet column set changes between runs,
  an `EvolutionReport` is built with `EvolutionChange` objects and ingested
  via `DriftObservatory.ingest_evolution()`.
* **Quality drift** — SCD2 churn rate and empty-load checks produce a
  `ContractReport` with `ContractViolation` entries ingested via
  `DriftObservatory.ingest_quality()`.

### sqldim types used (no shims, no duck-typing)

```python
from sqldim.observability import DriftObservatory
from sqldim.contracts.engine import EvolutionReport, EvolutionChange
from sqldim.contracts.report import ContractReport, ContractViolation
```

| sqldim type | Attributes |
|---|---|
| `EvolutionChange` | `change_type: str`, `column: str`, `detail: str` |
| `EvolutionReport` | `safe_changes`, `additive_changes`, `breaking_changes: list[EvolutionChange]` |
| `ContractViolation` | `rule: str`, `severity: str`, `count: int`, `detail: str` |
| `ContractReport` | `violations: list[ContractViolation]`, `elapsed_s: float`, `has_errors()` |

### Persistence

The observatory persists to `logs/observatory.duckdb` (configurable via
`config.pipeline.logs_path`).  History survives pipeline restarts.

### Querying results

```python
from src.core.services.observability import PipelineObservatory

obs = PipelineObservatory.from_path("logs/observatory.duckdb")
print(obs.query_breaking_changes().fetchdf())
print(obs.query_worst_quality_datasets().fetchdf())
print(obs.query_drift_velocity().fetchdf())
```

---

## 3. Pipeline Lineage — `ConsoleLineageEmitter`

### What it does

`PipelineOrchestrator.run()` emits OpenLineage-compatible JSON events to
`stderr` at three points in a run:

| Event | When |
|---|---|
| `RunState.START` | Before `strategy.execute()` |
| `RunState.COMPLETE` | After a successful execution |
| `RunState.FAIL` | When an unhandled exception escapes |

Each event includes `inputs` and `outputs` as `DatasetRef` objects and a
`facets` dict with `year` / `month` from the pipeline configuration.

### Key code

```python
# src/core/orchestrator.py
from sqldim.lineage import ConsoleLineageEmitter, DatasetRef, LineageEvent, RunState

emitter = ConsoleLineageEmitter()
run_id  = uuid.uuid4().hex

emitter.emit(LineageEvent(
    run_id=run_id,
    job_name=self.pipeline.get_name(),
    namespace="scrapper-rf-cnpj",
    state=RunState.START,
    inputs=[DatasetRef("scrapper-rf-cnpj.bronze", "cnpj_raw_files")],
    outputs=[DatasetRef("scrapper-rf-cnpj.gold",  "cnpj_dimensions")],
    facets={"year": year, "month": month},
))
```

Output is newline-delimited JSON (OpenLineage `RunEvent` shape) — pipe to
[Marquez](https://github.com/MarquezProject/marquez) or any OpenLineage
backend by replacing `ConsoleLineageEmitter` with `OpenLineageEmitter`.

---

## 4. Medallion Layer Catalog — `MedallionRegistry`

### What it does

`src/core/medallion.py` creates a singleton `CNPJ_REGISTRY` that maps every
dataset in the pipeline to its medallion tier, using sqldim's `Layer` enum
and `MedallionRegistry`.

| Layer | Datasets |
|---|---|
| Bronze | `cnpj_raw_zip`, `cnpj_raw_csv` |
| Silver | `empresa.parquet`, `estabelecimento.parquet`, `socios.parquet`, `simples.parquet`, `quals.parquet`, `moti.parquet`, `natju.parquet`, `munic.parquet`, `cnae.parquet`, `pais.parquet` |
| Gold | `empresa`, `estabelecimento`, `socios`, `simples`, `quals`, `moti`, `natju`, `munic`, `cnae`, `pais` |

### Usage

```python
from src.core.medallion import CNPJ_REGISTRY, Layer

layer = CNPJ_REGISTRY.get_layer("empresa")          # Layer.GOLD
silver = CNPJ_REGISTRY.datasets_in(Layer.SILVER)    # list of .parquet names
```

---

## 5. Analytical Fact Builders

### 5a. Cumulative History — `LazyCumulativeLoader`

`build_simples_history()` (in `src/core/services/analytics/loaders.py`)
builds a `simples_history_cumulated` table that accumulates Simples Nacional
regime status for each CNPJ over consecutive monthly pipeline runs.

It uses sqldim's `LazyCumulativeLoader` which performs a FULL OUTER JOIN
between the existing PostgreSQL table (yesterday's state) and the new Parquet
snapshot (today's batch) — entirely inside DuckDB.

```python
from src.core.services.analytics import build_simples_history

rows = build_simples_history(
    pg_dsn="postgresql://user:pass@host/dbname",
    parquet_path="converted/2024-12/simples.parquet",
    target_period="2024-12",
)
```

The cumulated column `regime_history` is a DuckDB `LIST` of structs, each
containing `opcao_simples`, `opcao_mei`, `data_opcao_simples`,
`data_opcao_mei` for that period.

### 5b. Activity Bitmask — `LazyBitmaskLoader`

`build_simples_bitmask()` converts a per-CNPJ list of active-month dates
into a 365-bit integer activity mask using sqldim's `LazyBitmaskLoader`.

```python
from src.core.services.analytics import build_simples_bitmask

rows = build_simples_bitmask(
    pg_dsn="postgresql://user:pass@host/dbname",
    parquet_path="converted/2024-12/simples_dates.parquet",
    reference_date="2024-12-31",
    dates_column="active_months",
    window_days=365,
)
```

Results are written to `simples_activity_bitmask` in PostgreSQL.

---

## Design Principles

1. **No re-implementation** — every sqldim class is used as-is.  If sqldim
   already provides a type (`EvolutionReport`, `ContractReport`, etc.),
   this codebase imports and uses it directly.
2. **Zero-memory DuckDB path** — all heavy SQL (SCD2 merges, FULL OUTER
   JOINs, bitmask aggregation) runs inside DuckDB, not Python.
3. **Observability never crashes the pipeline** — all observatory calls are
   wrapped in `try/except`; failures are logged at `DEBUG` level only.
4. **Lineage is synchronous and zero-dependency** — `ConsoleLineageEmitter`
   requires no external services; swap in `OpenLineageEmitter` to connect
   to a lineage backend.
