# Audit Service Refactoring Plan

## Goal
Document and prioritize improvements for `src/core/services/audit/service.py` (the AuditService). The aim is to fix correctness bugs, tighten error handling, improve testability, and make the service easier to maintain and integrate with the refactored loading components.

This document summarizes findings, lists recommended fixes, and provides a step-by-step action plan with milestones and tests.

---

## Quick summary of findings
- Several correctness issues were found that can cause runtime failures or silently mask errors:
  - Mismatched SQL parameters vs. placeholders in INSERT/UPDATE statements (e.g. `rows_processed`, `file_manifest_id`).
  - `BatchAccumulator.add_file_event` signature and implementation don't match callers (bytes vs rows). This will lose metrics or raise TypeError.
  - Extensive use of broad `except Exception:` blocks that can hide root causes and make debugging difficult.
  - Some functions are annotated to return `str` but return `None` in failure paths; type signatures should be Optional to reflect reality.
- The class is large and handles many responsibilities (manifest creation, file integrity, batch metrics, DB operations) — opportunity to split responsibilities.

---

## High-priority fixes (apply ASAP)
1. Fix `BatchAccumulator.add_file_event` to accept (status, rows, bytes_) and update internal counters consistently. Keep it defensive: accept `AuditStatus` or status `str` and normalize.
2. Fix SQL parameter mismatches:
   - Ensure `rows_processed` is part of `INSERT` column list and parameter dict in `_insert_file_manifest`.
   - Ensure `UPDATE` statements use `:file_manifest_id` placeholder if params dict contains that key (or align keys consistently).
3. Replace broad `except Exception:` in DB/IO critical blocks with narrower exceptions where possible (e.g., `OSError`, `IOError`, `SQLAlchemyError`) and use `logger.exception(...)` to preserve stack traces.
4. Fix return type hints for methods that may return `None` (use `Optional[str]` or `Optional[uuid.UUID]` where applicable).

Small example patches (conceptual):
- `BatchAccumulator.add_file_event` should look like:

```python
def add_file_event(self, status: AuditStatus | str, rows: int = 0, bytes_: int = 0):
    # normalize status -> AuditStatus
    # update files_completed/files_failed, total_rows, total_bytes
    # update last_activity
```

- `_insert_file_manifest` must include `rows_processed` in both columns and values and pass it in params.

- `update_file_manifest` must use the same placeholder name in SQL as in params dict (for example `:file_manifest_id`).

---

## Design / refactor recommendations (medium-term)
1. Split `AuditService` into smaller components:
   - `AuditRepository` — raw DB operations (INSERT/UPDATE/SELECT). Unit-testable via injected DB engine or using an in-memory SQLite.
   - `AuditManager` — higher-level orchestration: create manifests, calculate metrics, maintain batch accumulators.
   - `IntegrityService` — file checksum and verification routines.
2. Introduce a `BatchAccumulator` class (already present) in a separate module (e.g., `audit/metrics.py`) with clear public API and unit tests.
3. Use SQLAlchemy models and sessions consistently via an injected `Database` abstraction to reduce raw SQL string duplication. Keep raw SQL only where necessary and test those queries.
4. Add small utility functions for safe JSON notes handling (safe dump/load, max length truncation).

---

## Action plan with milestones

Milestone A — Triage & small fixes (1–2 days)
- Tasks:
  - Apply the high-priority correctness fixes (BatchAccumulator, INSERT/UPDATE param alignment, return type hints).
  - Replace a few critical broad excepts with `logger.exception(...)` and specific exception types.
  - Add unit tests for these corrected paths using `pytest` and an in-memory SQLite DB for DB tests.
- Success criteria:
  - All unit tests for the audit service pass locally.
  - No silent exceptions in the fixed code paths.

Milestone B — Decomposition & repository layer (3–5 days)
- Tasks:
  - Extract `AuditRepository` with small methods: `insert_file_manifest`, `update_file_manifest`, `find_table_audit`, `insert_table_audit`, etc.
  - Replace corresponding code in `AuditService` with calls to `AuditRepository`.
  - Add unit tests for `AuditRepository` that exercise SQL (use sqlite or a SQLAlchemy engine configured for tests).
- Success criteria:
  - `AuditRepository` has focused, tested methods.
  - `AuditService` logic is clearer and smaller.

Milestone C — Metrics & concurrency hardening (2–3 days)
- Tasks:
  - Harden `BatchAccumulator` for concurrency (locks already present) and add tests for race conditions (threaded tests).
  - Add timeouts and watchdogs for long-running batch operations.
- Success criteria:
  - Accurate accumulation of metrics under simulated concurrent updates.

Milestone D — Documentation & tests (1–2 days)
- Tasks:
  - Add comprehensive unit tests covering create/insert/update flows, checksum verification, and error paths.
  - Add docstrings and examples in `docs/`.
- Success criteria:
  - Tests provide > 90% coverage for critical audit logic.

---

## Tests to add (minimum set)
- Unit tests for `BatchAccumulator`:
  - add_file_event for success/failure, bytes accumulation, timestamp updates.
  - concurrent updates (small threaded test) to confirm locking.
- Repository-level DB tests (use sqlite in-memory):
  - `_insert_file_manifest` creates a row with expected columns and values.
  - `update_file_manifest` updates expected columns and doesn't throw when row missing.
- Integration test: run small pipeline that uses `AuditService.insert_audits` with a mocked `AuditMetadata` and an in-memory DB; assert manifests created.

---

## Files to change (example)
- `src/core/services/audit/service.py` — apply fixes and refactor; split out new files below.
- `src/core/services/audit/repository.py` — new small module with DB operations.
- `src/core/services/audit/metrics.py` — move `BatchAccumulator` here and add tests.
- `tests/test_audit_service.py` — new tests.

---

## Suggested first set of commits (atomic, small)
1. `fix(audit): correct BatchAccumulator.add_file_event signature and accumulation`
2. `fix(audit): align SQL placeholders and params in file manifest insert/update`
3. `test(audit): add unit tests for BatchAccumulator and _insert_file_manifest`

---

## Implementation offer
I can implement these changes and the tests for you. Suggested immediate next steps if you want me to proceed:
- I will open a branch `fix/audit-service`.
- Implement Milestone A fixes and related unit tests using sqlite in-memory.
- Run tests and fix any issues until green.

Would you like me to proceed and apply Milestone A now? If yes, I will create the branch and start making the edits and tests. If you prefer to implement later yourself or assign to someone else, tell me and I will provide patch files you can apply manually.