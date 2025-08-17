# 🧱 Refactored Plan — Hardened + Operational Sections

---

# 1) Minimal summary

Unify CSV and Parquet ingestion into a single async pipeline that: accepts a contract (headers ± types), streams batches from source, stages per-worker in a session-local temp table, performs safe idempotent upserts inside a transaction, and exposes CLI/metrics/tests. Use `asyncpg.Pool`, per-worker connections, `copy_records_to_table`, and streaming `iter_batches()` for Parquet.

---

# 2) Files & responsibilities (unchanged; explicit)

ingestion/

* `base.py` — safe quoting, `upsert_sql`, `create_temp_table_sql`, pool helper, type mapper, small utils (identifier validator, checksum).
* `csv_ingestor.py` — batch generator: `batch_generator(path, headers, chunk_size)` → `Iterable[List[tuple]]`.
* `parquet_ingestor.py` — batch generator using `iter_batches()`.
* `uploader.py` — `async_upsert()` orchestrator, pool + workers, retries, metrics emission.
* `cli.py` — thin wrapper for orchestrator with flags.
* `tests/` — unit + integration + fault-injection + regression datasets.
* `docker/` — docker-compose for local Postgres for CI.

---

# 3) Security & safety rules (must be enforced)

1. **No raw identifier interpolation.** Validate identifiers with regex `^[A-Za-z_][A-Za-z0-9_]*$` and quote them with double-quotes. Fail fast on invalid identifiers. (implemented in `quote_ident()`)

2. **No sensitive secrets in logs.** Mask DSNs in logs and avoid printing full connection strings.

3. **SQL injection limiters.** All values must be passed as bound parameters. Only validated identifiers are interpolated.

4. **Permissions minimality.** The DB role used should have only required privileges: CREATE TEMP TABLE, INSERT/UPDATE on target tables, SELECT on manifest table. Avoid superuser.

5. **Checksum & provenance.** Store `filename`, `checksum`, `filesize`, `processed_at`, `run_id`, `status` in `processed_files` manifest to allow audits.

---

# 4) Concrete DB artifacts / example SQL

**Processed files manifest**

```sql
CREATE TABLE ingestion_manifest (
  filename TEXT PRIMARY KEY,
  checksum BYTEA,           -- e.g., sha256
  filesize BIGINT,
  processed_at TIMESTAMP WITH TIME ZONE,
  rows BIGINT,
  status TEXT,              -- pending | success | failed
  run_id TEXT,
  notes TEXT
);
```

**Create temp table helper (example)**

```
CREATE TEMP TABLE "tmp_xyz_123" (
  "col1" TEXT,
  "col2" TEXT,
  ...
) ON COMMIT DROP;
```

Create without `ON COMMIT DROP` if you need to persist across transactions in the same session; but we recommend `ON COMMIT DROP` or rely on session close to drop.

**Default `ensure_table_from_headers`** (when types not provided)

```sql
CREATE TABLE IF NOT EXISTS "target" (
  "col1" TEXT,
  "col2" TEXT,
  PRIMARY KEY ("pk_col")
);
```

---

# 5) Upsert semantics — deterministic defaults

* Default behavior: `--mode upsert` uses `ON CONFLICT` without `DISTINCT ON`. This is simpler and safer. `ON CONFLICT` is the canonical idempotent primitive.
* Optional: if per-batch dedup needed, implement `MERGE` (PG 15+) or `SELECT DISTINCT ON` **with explicit deterministic ORDER BY** (e.g., `ORDER BY pk_cols, last_updated DESC`) and document tie-breaker semantics.
* If you choose `append` mode, document that it’s non-idempotent and requires downstream dedup.

---

# 6) Resilience & recovery patterns

1. **Retries:** exponential backoff with jitter for transient DB errors. Example: attempts = 5, backoff = 2 \*\* attempt + jitter (0-0.5s).

2. **Transactions:** wrap `copy` + `upsert` + `truncate` in `conn.transaction()` per-batch.

3. **Manifest & resume:** upon success, insert manifest record; on failure, update manifest `status='failed'` and save `notes`. `--resume` checks manifest and skips files with `status='success'` and checksum match.

4. **Idempotency strategy:** two layers:

   * At DB level: `ON CONFLICT` ensures duplicate rows don't create duplicates.
   * At job level: manifest prevents reprocessing unchanged files.

5. **Backpressure:** workers should respect pool limits; cap in-flight batches per connection. If memory pressure observed, reduce `chunk_size` or increase worker count while monitoring.

---

# 7) Performance & scaling knobs (tuning guide)

* **Concurrency** = number of worker sessions; set to \~ min(cpu\_count, pg\_max\_connections/2). Use pool `max_size` = concurrency + 2.
* **Chunk size** default: 50k rows for typical wide rows; reduce to 10k–20k if rows are large or you see memory spikes.
* **Parquet**: `iter_batches(batch_size=chunk_size)` avoids large allocations; use `batch_size` tuned to memory.
* **Network**: if DB is remote, favor larger batch sizes to amortize latency; if local, smaller batches reduce lock contention.
* **Indexes**: upserts are heavier if PK index is huge — consider disabling non-essential indexes during bulk backfill, then reindex (but only if downtime/lock allowed).
* **VACUUM / bloat**: plan for VACUUM after large upserts (or use `autovacuum` config).

---

# 8) Failure modes & how handlers respond

1. **COPY succeeds but upsert fails** — transaction rollback prevents partial commit (good if you wrap). Manifest records unchanged; `max_retries` will retry.

2. **Worker crashes mid-run** — temp table dropped when connection closed; partially processed file will be re-run if manifest shows no success.

3. **Network flakiness** — retry with backoff. If persistent, mark file failed with error details.

4. **Out-of-memory while building a batch** — detect when `len(batch)` exceeds safe threshold; reduce `chunk_size` dynamically or fail fast with instruction to redo with smaller `chunk_size`.

5. **Schema mismatch** — fail fast. If `--tolerant`, optionally log missing columns and fill with `NULL` or empty string depending on `--tolerant-mode`.

---

# 9) Observability & metrics (concrete metrics to emit)

Emit metrics via a simple sink (Prometheus client, StatsD, or log structured JSON). Metric names and labels:

* `ingest.files_processed_total{file_type="csv|parquet",status="success|failed"}`
* `ingest.batches_processed_total{file_type,worker_id}`
* `ingest.rows_read_total{file_type}`
* `ingest.rows_staged_total{file_type}`
* `ingest.rows_upserted_total{table_name}`
* `ingest.batch_latency_seconds` (histogram)
* `ingest.db_errors_total{error_class}`
* `ingest.memory_peak_bytes`
* `ingest.retries_total`

Logs: structured JSON with `run_id`, `file`, `batch_idx`, `rows`, `latency_ms`, `status`, `error` for ease of searching.

---

# 10) Testing matrix (what to cover in CI)

Unit:

* `quote_ident()` rejects invalid names and accepts valid names.
* `upsert_sql()` constructs SQL with properly quoted identifiers and set clause.
* CSV header mapping: header present, header partial, header missing (positional fallback).
* CSV different delimiters and quoting (commas, semicolon, tab, pipe).
* Parquet `iter_batches()` conversion to tuple rows.

Integration (dockerized Postgres):

* End-to-end run with 1 small CSV file -> verify rows and manifest.
* Re-run same CSV -> idempotency (rows unchanged).
* Parquet file -> verify rows, type-preserve vs TEXT modes.
* Concurrent files -> no temp-table collisions and correct final counts.
* Simulated DB transient error -> ensure retries and manifest update.

Fault-injection:

* Network drop mid-batch.
* Upsert raises unique violation unexpected (simulate bad PK) -> system fails gracefully and records manifest failure.

---

# 11) CLI & config (recommended flags & defaults)

Example CLI usage:

```
uploader load \
  --dsn "postgresql://user:pw@host:5432/db" \
  --files data/*.parquet \
  --file-type parquet \
  --table my_table \
  --pk cnpj_basico \
  --headers cnpj_basico,nome,cpf \
  --chunk-size 50000 \
  --concurrency 4 \
  --mode upsert \
  --dry-run false \
  --preserve-types true \
  --max-retries 4
```

Defaults:

* `chunk_size=50_000`
* `concurrency=4`
* `max_retries=3`
* `mode=upsert`
* `preserve_types=False` (TEXT mode unless requested)

---

# 12) Developer handoff & Copilot guardrails

When handing to Copilot/LLM, supply:

* This hardened plan (copy/paste).
* Example unit tests (so generated code must pass them).
* Concrete parameterized test dataset (small CSV, small Parquet).
* A PR template requiring reviewer checklist pass items.

Guardrails to include in prompt:

* "Implement per-worker connection pooling; create temp table on the same connection; wrap staging+upsert+truncate in transaction; sanitize identifiers; use `iter_batches()` for Parquet; do not use psycopg2; add manifest table; implement retries."

---

# 13) Expanded reviewer / PR checklist (actionable)

* [ ] Library-only code contains no `psycopg2` imports (only `asyncpg`).
* [ ] `asyncpg.Pool` is used and each worker calls `pool.acquire()`; no concurrent use of same `Connection`.
* [ ] Temp table creation is on worker connection; name validated and deterministic per-worker.
* [ ] `copy_records_to_table` + `upsert_sql` executed inside `conn.transaction()` for each batch.
* [ ] Identifiers are validated via `quote_ident()` (regex + quoting).
* [ ] Parquet ingestion uses `ParquetFile.iter_batches()` and converts record batches to tuples without materializing whole row groups.
* [ ] CSV ingestion uses `csv.reader` and yields tuples that exactly match the `headers` order.
* [ ] Exponential backoff with jitter for retryable errors implemented and tested.
* [ ] Manifest table is created and used to track file processing statuses and checksums.
* [ ] Unit tests and integration tests pass (CI), including idempotency test (run twice).
* [ ] Observability metrics are emitted with consistent labels and logged run\_id for traceability.
* [ ] CLI flags include `--dry-run`, `--mode`, `--preserve-types`, `--resume`, `--max-retries`, `--chunk-size`, and `--concurrency`.
* [ ] Documentation (README.md) contains operational guidance: tuning knobs, failure recovery steps, and sample commands.

---

# 14) Operational runbook (short)

If a run fails:

1. Inspect logs for `run_id` and `file`.
2. Check `ingestion_manifest` row for filename.
3. If transient DB error, fix DB connectivity and re-run with `--resume`.
4. If schema mismatch, either update contract (headers/types) or run with `--tolerant` but capture data quality issues and create a ticket.
5. For performance issues, reduce `chunk_size` and increase concurrency until stability is reached.

---

# 15) Trade-offs & final recommendations

* Using `TEXT` everywhere is safest but loses type validation. Use `--preserve-types` only if you control the Parquet schema / header contract.
* Smaller chunk\_size reduces memory pressure but increases DB round-trips. Start conservative (50k), measure, then tune.
* `DISTINCT ON` + per-batch dedupe is expensive and non-deterministic without a tiebreaker; favor `ON CONFLICT`.
* `MERGE` is cleaner if you control PG version (15+), but `ON CONFLICT` is widely supported and battle-tested.

## Milestone Tracker

- [x] Ingestion utilities: identifier quoting, upsert SQL, temp table creation (unit tested)
- [x] CSV batch generator: yields correct batches (unit tested)
- [x] Parquet batch generator: yields correct batches (unit tested)
- [x] Async upsert orchestrator: pool, transaction, retries, metrics (integration tested)
- [x] Manifest table integration and tracking (integration tested)
- [x] CLI wrapper and operational flags (tested)
- [x] Integration and fault-injection tests (tested)
- [x] Observability metrics and logging (tested)
- [x] Documentation and operational guidance (present)
