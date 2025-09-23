# Code Quality Report

Generated: 2025-09-23T22:42:35.675568+00:00 UTC

## Summary

- Analyzed path: `src`

- Files analyzed: 54

- Total LOC: 11869  (SLOC: 7694, LLOC: 5969)

- Total comments: 1034  | blanks: 2080


---

## Complexity summary

| Rank | Count |
|---|---:|
| D | 3 |
| C | 27 |
| B | 57 |
| A | 390 |

---

## Files with LOC >= 400

| File | LOC | Suggestion |
|---|---:|---|
| `src/core/services/conversion/service.py` | 1163 | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |
| `src/core/services/audit/service.py` | 1053 | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |
| `src/core/services/loading/strategies.py` | 977 | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |
| `src/database/dml.py` | 777 | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |
| `src/core/etl.py` | 763 | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |
| `src/setup/config/models.py` | 661 | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |
| `src/core/strategies.py` | 528 | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |
| `src/core/services/discovery/service.py` | 442 | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |
| `src/setup/config/loader.py` | 418 | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |

---

## Functions with complexity >= 12

| # | File | Name | Line | Complexity | Rank | Suggestion |
|---:|---|---|---:|---:|---:|
| 1 | `src/utils/models.py` | `create_audit` | 56 | 23 | D | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 2 | `src/core/services/conversion/service.py` | `process_csv_with_memory` | 270 | 22 | D | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 3 | `src/core/services/conversion/service.py` | `convert_csvs_to_parquet` | 629 | 22 | D | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 4 | `src/core/etl.py` | `load_csv_files_directly` | 501 | 20 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 5 | `src/core/utils/batch_optimizer.py` | `calculate_optimal_batch_size` | 76 | 19 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 6 | `src/core/services/loading/strategies.py` | `_load_with_file_loader_batching` | 156 | 18 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 7 | `src/core/services/audit/service.py` | `create_file_manifest` | 344 | 17 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 8 | `src/core/services/loading/strategies.py` | `load_multiple_tables` | 685 | 17 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 9 | `src/core/utils/cli.py` | `validate_cli_arguments` | 10 | 17 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 10 | `src/core/strategies.py` | `execute` | 201 | 16 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 11 | `src/core/services/conversion/service.py` | `convert_table_csvs` | 411 | 16 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 12 | `src/core/services/conversion/service.py` | `process_extremely_large_table` | 948 | 16 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 13 | `src/core/services/loading/strategies.py` | `_create_manifest_entry` | 567 | 15 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 14 | `src/core/services/loading/file_loader/ingestors.py` | `batch_generator_csv` | 39 | 15 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 15 | `src/core/services/loading/file_loader/uploader.py` | `async_upsert` | 49 | 15 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 16 | `src/core/transforms.py` | `format_date_fields` | 88 | 14 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 17 | `src/core/services/loading/strategies.py` | `_load_with_batching` | 414 | 14 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 18 | `src/core/etl.py` | `_create_audit_metadata_from_csv_files` | 413 | 13 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 19 | `src/core/etl.py` | `_is_csv_file` | 707 | 13 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 20 | `src/core/services/audit/service.py` | `update_file_manifest` | 719 | 13 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 21 | `src/core/services/loading/service.py` | `_persist_table_audit_completion` | 128 | 13 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 22 | `src/database/dml.py` | `_apply_development_sampling` | 404 | 13 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 23 | `src/core/services/loading/file_loader/connection_factory.py` | `get_column_types_mapping` | 79 | 12 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 24 | `src/core/services/loading/file_loader/file_loader.py` | `_detect_format` | 24 | 12 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |
| 25 | `src/setup/config/validation.py` | `_validate_environment_specific` | 157 | 12 | C | Extract smaller functions, add focused unit tests, simplify branching (early returns), and add type hints. Consider splitting responsibilities into classes or modules. |

---

## Top files by LOC

| # | File | LOC | LLOC | SLOC | Comments | Blank |
|---:|---|---:|---:|---:|---:|---:|
| 1 | `src/core/services/conversion/service.py` | 1163 | 642 | 801 | 112 | 207 |
| 2 | `src/core/services/audit/service.py` | 1053 | 524 | 734 | 67 | 156 |
| 3 | `src/core/services/loading/strategies.py` | 977 | 487 | 635 | 98 | 178 |
| 4 | `src/database/dml.py` | 777 | 380 | 436 | 82 | 151 |
| 5 | `src/core/etl.py` | 763 | 427 | 477 | 87 | 150 |
| 6 | `src/setup/config/models.py` | 661 | 360 | 471 | 23 | 97 |
| 7 | `src/core/strategies.py` | 528 | 320 | 346 | 57 | 112 |
| 8 | `src/core/services/discovery/service.py` | 442 | 211 | 212 | 20 | 92 |
| 9 | `src/setup/config/loader.py` | 418 | 158 | 287 | 54 | 71 |
| 10 | `src/setup/config/profiles.py` | 392 | 83 | 293 | 60 | 65 |
| 11 | `src/core/transforms.py` | 354 | 185 | 191 | 25 | 73 |
| 12 | `src/database/models/audit.py` | 343 | 166 | 220 | 52 | 59 |
| 13 | `src/setup/config/validation.py` | 334 | 173 | 233 | 17 | 65 |
| 14 | `src/core/utils/development_filter.py` | 332 | 185 | 242 | 20 | 60 |
| 15 | `src/utils/models.py` | 313 | 117 | 189 | 21 | 44 |

---

## Top functions by cyclomatic complexity

| # | File | Name | Line | Complexity | Rank |
|---:|---|---|---:|---:|---:|
| 1 | `src/utils/models.py` | `create_audit` | 56 | 23 | D |
| 2 | `src/core/services/conversion/service.py` | `process_csv_with_memory` | 270 | 22 | D |
| 3 | `src/core/services/conversion/service.py` | `convert_csvs_to_parquet` | 629 | 22 | D |
| 4 | `src/core/etl.py` | `load_csv_files_directly` | 501 | 20 | C |
| 5 | `src/core/utils/batch_optimizer.py` | `calculate_optimal_batch_size` | 76 | 19 | C |
| 6 | `src/core/services/loading/strategies.py` | `_load_with_file_loader_batching` | 156 | 18 | C |
| 7 | `src/core/utils/cli.py` | `validate_cli_arguments` | 10 | 17 | C |
| 8 | `src/core/services/loading/strategies.py` | `load_multiple_tables` | 685 | 17 | C |
| 9 | `src/core/services/audit/service.py` | `create_file_manifest` | 344 | 17 | C |
| 10 | `src/core/strategies.py` | `execute` | 201 | 16 | C |
| 11 | `src/core/services/conversion/service.py` | `convert_table_csvs` | 411 | 16 | C |
| 12 | `src/core/services/conversion/service.py` | `process_extremely_large_table` | 948 | 16 | C |
| 13 | `src/core/services/loading/strategies.py` | `_create_manifest_entry` | 567 | 15 | C |
| 14 | `src/core/services/loading/file_loader/uploader.py` | `async_upsert` | 49 | 15 | C |
| 15 | `src/core/services/loading/file_loader/ingestors.py` | `batch_generator_csv` | 39 | 15 | C |
| 16 | `src/core/transforms.py` | `format_date_fields` | 88 | 14 | C |
| 17 | `src/core/services/loading/strategies.py` | `_load_with_batching` | 414 | 14 | C |
| 18 | `src/database/dml.py` | `_apply_development_sampling` | 404 | 13 | C |
| 19 | `src/core/services/loading/service.py` | `_persist_table_audit_completion` | 128 | 13 | C |
| 20 | `src/core/services/audit/service.py` | `update_file_manifest` | 719 | 13 | C |
| 21 | `src/core/etl.py` | `_create_audit_metadata_from_csv_files` | 413 | 13 | C |
| 22 | `src/core/etl.py` | `_is_csv_file` | 707 | 13 | C |
| 23 | `src/setup/config/validation.py` | `_validate_environment_specific` | 157 | 12 | C |
| 24 | `src/core/services/loading/file_loader/file_loader.py` | `_detect_format` | 24 | 12 | C |
| 25 | `src/core/services/loading/file_loader/connection_factory.py` | `get_column_types_mapping` | 79 | 12 | C |
| 26 | `src/setup/config/loader.py` | `get_environment_variables_summary` | 306 | 11 | C |
| 27 | `src/core/services/download/service.py` | `_download_zipfile` | 128 | 11 | C |
| 28 | `src/core/services/discovery/service.py` | `scrape_files_from_url` | 310 | 11 | C |
| 29 | `src/core/services/conversion/service.py` | `select_processing_strategy` | 1069 | 11 | C |
| 30 | `src/core/services/audit/service.py` | `update_manifest_notes` | 589 | 11 | C |

---

## Notes & suggested next steps

- Focus on files with large LOC and high-complexity functions (see tables above).
- Consider extracting functions/classes, adding unit tests, and reducing nesting in the highest-ranked functions.
- Re-run `radon cc -n B src` after refactors to verify improvements.
