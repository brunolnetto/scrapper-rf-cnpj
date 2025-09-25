#!/usr/bin/env python3
"""
Fast helper to run convert_csvs_to_parquet_smart and validate CSV vs Parquet row counts.

- Uses very fast `wc -l` (with Python fallback) to count CSV rows.
- Uses `pyarrow` parquet metadata (fast) to count Parquet rows.
- Falls back to `polars` for exact counting when needed.
- Parallelizes CSV counting with ThreadPoolExecutor.

Save as `scripts/run_smart_conversion_fast.py` and run from repo root with:

    PYTHONPATH=. python3 scripts/run_smart_conversion_fast.py

Notes:
- `wc -l` is POSIX; Python fallback is provided for portability.
- `pyarrow` is recommended for Parquet metadata. `polars` remains an optional dependency
  used as an exact fallback when CSVs may contain embedded newlines.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List

# Import the converter (your project must be on PYTHONPATH)
from src.core.services.conversion.service import convert_csvs_to_parquet_smart

# Optional: lightweight row counting using Polars (if installed)
try:
    import polars as pl
except Exception:
    pl = None

# Optional: use pyarrow for fast parquet metadata
try:
    import pyarrow.parquet as pq
except Exception:
    pq = None


# --------------------------
# Filename -> table inference
# --------------------------

def infer_table_from_filename(name: str) -> str | None:
    """
    Map common Receita Federal filename tokens to our table names.
    Adjust patterns if your filenames differ.
    """
    mapping = [
        ("empresa", r"EMPRE|EMPRECSV|EMPRE"),
        ("estabelecimento", r"ESTABELE|ESTABELECSV|ESTABELE"),
        ("socios", r"SOCIO|SOCIOCSV|SOCIO"),
        ("simples", r"SIMPLES|SIMPLESCSV|SIMPLES"),
        ("cnae", r"CNAE|CNAECSV"),
        ("moti", r"MOTI|MOTICSV"),
        ("munic", r"MUNIC|MUNICCSV"),
        ("natju", r"NATJU|NATJUCSV"),
        ("pais", r"PAIS|PAISCSV"),
        ("quals", r"QUALS|QUALSCSV"),
    ]
    for table, pat in mapping:
        if re.search(pat, name, re.IGNORECASE):
            return table
    return None


# --------------------------
# Audit map builder
# --------------------------

def build_audit_map(extracted_dir: Path) -> Dict[str, dict]:
    """
    Build audit_map in the shape expected by the converter:
      { table_name: {zip_id_or_source: [filename1, filename2, ...] } }
    We use a single 'source' key per file here, matching how the converter builds csv_paths.
    """
    audit_map: Dict[str, dict] = {}
    for p in sorted(extracted_dir.iterdir()):
        if not p.is_file():
            continue
        table = infer_table_from_filename(p.name)
        if not table:
            # Skip unknown files (or you can log them)
            continue
        audit_map.setdefault(table, {}).setdefault("source", []).append(p.name)
    return audit_map


# --------------------------
# Fast CSV row counting
# --------------------------

def _has_header_line(path: Path, delimiter: str) -> bool:
    """
    Heuristically detect a header line by reading up to a few lines and checking
    for the delimiter and alphabetic characters.
    """
    try:
        with path.open("r", encoding="utf8", errors="ignore") as f:
            for _ in range(5):
                line = f.readline()
                if not line:
                    return False
                line = line.strip()
                if not line:
                    continue
                if delimiter in line and any(c.isalpha() for c in line):
                    return True
                return False
    except Exception:
        return False


def count_csv_rows_fast(path: Path, delimiter: str = ";", use_wc: bool = True) -> int:
    """
    Fast CSV row count using `wc -l` when available, otherwise a Python chunked
    newline count. Subtract header line if detected.

    Falls back to Polars exact count only if the other methods fail.
    """
    header = _has_header_line(path, delimiter)

    if use_wc:
        try:
            proc = subprocess.run(["wc", "-l", str(path)], capture_output=True, text=True, check=True)
            n_str = proc.stdout.strip().split()[0]
            n = int(n_str)
            if header and n > 0:
                return n - 1
            return n
        except Exception:
            # wc not available or failed; fall through to Python count
            pass

    # Python buffered chunk count
    count = 0
    try:
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                count += chunk.count(b"\n")
        if header and count > 0:
            return count - 1
        return count
    except Exception:
        # Last resort: use Polars if present for exact parsing
        if pl is not None:
            scan = pl.scan_csv(str(path), separator=delimiter, encoding="utf8-lossy", ignore_errors=True)
            # `pl.count()` is aggregation; use `select(pl.count())` pattern
            try:
                return int(scan.select(pl.count()).collect().item())
            except Exception:
                # If polars fails, raise a descriptive error
                raise RuntimeError(f"Unable to count CSV rows for {path}")
        raise


# --------------------------
# Parquet row counting
# --------------------------

def count_parquet_rows_fast(path: Path) -> int:
    """
    Use Parquet metadata via pyarrow if available, otherwise fall back to Polars.
    """
    if pq is not None:
        try:
            parquet_file = pq.ParquetFile(str(path))
            return int(parquet_file.metadata.num_rows)
        except Exception:
            # metadata read failed; fall back
            pass

    if pl is not None:
        df = pl.read_parquet(str(path))
        return int(df.shape[0])

    raise RuntimeError("No library available to count parquet rows (install pyarrow or polars).")


# --------------------------
# Parallel counting helper
# --------------------------

def parallel_count_csvs(files: List[Path], delimiter: str = ";", max_workers: int | None = None) -> Dict[Path, int]:
    """
    Count many CSVs in parallel and return a dict {Path: rows}.
    """
    results: Dict[Path, int] = {}
    max_workers = max_workers or min(32, (os.cpu_count() or 2) * 5)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_path = {ex.submit(count_csv_rows_fast, p, delimiter): p for p in files}
        for fut in as_completed(future_to_path):
            p = future_to_path[fut]
            try:
                results[p] = fut.result()
            except Exception as e:
                results[p] = -1
                print(f"  Failed counting {p.name}: {e}")
    return results


# --------------------------
# Polars exact count helper (for verification)
# --------------------------

def count_csv_rows_polars(path: Path, delimiter: str = ";") -> int:
    if pl is None:
        raise RuntimeError("Polars is required for exact CSV counting (install polars).")
    scan = pl.scan_csv(str(path), separator=delimiter, encoding="utf8-lossy", ignore_errors=True)
    try:
        return int(scan.select(pl.count()).collect().item())
    except Exception as e:
        raise RuntimeError(f"Polars failed to count {path}: {e}")


# --------------------------
# Argument parsing / main
# --------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run smart CSV->Parquet conversion for a given extraction folder")
    p.add_argument("--extracted_dir", default="data/EXTRACTED_FILES/2025-09")
    p.add_argument("--output_dir", default="data/TMP_CONVERTED/2025-09")
    p.add_argument("--delimiter", default=";")
    p.add_argument("--tables", help="Comma-separated list of tables to process (e.g. empresa,simples)")
    p.add_argument("--preflight", action="store_true", help="Run preflight checks only and exit")
    p.add_argument("--verify-with-polars", action="store_true", help="On mismatch, re-count CSVs with Polars to verify exact rows")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    extracted_dir = Path(args.extracted_dir)
    output_dir = Path(args.output_dir)
    delimiter = args.delimiter

    if not extracted_dir.exists():
        print(f"Extraction directory not found: {extracted_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning extracted directory: {extracted_dir}")
    audit_map = build_audit_map(extracted_dir)

    # Optionally filter tables
    if args.tables:
        wanted = [t.strip() for t in args.tables.split(",") if t.strip()]
        audit_map = {k: v for k, v in audit_map.items() if k in wanted}

    print(f"Built audit_map for tables: {sorted(audit_map.keys())}")

    output_dir.mkdir(parents=True, exist_ok=True)

    if args.preflight:
        from src.core.services.conversion.service import pre_flight_check

        print("Running preflight checks...")
        pf = pre_flight_check(audit_map, extracted_dir)
        print("Preflight result:\n", pf)
        sys.exit(0)

    print(f"Starting smart conversion -> output: {output_dir}")
    try:
        convert_csvs_to_parquet_smart(audit_map, extracted_dir, output_dir, delimiter=delimiter)
    except Exception as e:
        print(f"Conversion raised an exception: {e}")
        raise

    print("Conversion finished. Validating row counts (fast mode) ...")

    # Compare per-table CSV total vs produced Parquet (parallel CSV counting)
    for table, zip_map in audit_map.items():
        csv_files = [extracted_dir / fname for files in zip_map.values() for fname in files]
        existing_files = [f for f in csv_files if f.exists()]
        missing = [f for f in csv_files if not f.exists()]
        for m in missing:
            print(f"  CSV missing: {m}")

        if not existing_files:
            print(f"Table '{table}': no CSV files found, skipping")
            continue

        counts = parallel_count_csvs(existing_files, delimiter=delimiter)
        csv_total = sum(v for v in counts.values() if v > 0)

        parquet_path = output_dir / f"{table}.parquet"
        if parquet_path.exists():
            try:
                parquet_total = count_parquet_rows_fast(parquet_path)
            except Exception as e:
                print(f"  Failed to count parquet {parquet_path.name}: {e}")
                parquet_total = 0
        else:
            parquet_total = 0

        diff = csv_total - parquet_total
        print(f"Table '{table}': CSV rows={csv_total:,}, Parquet rows={parquet_total:,} -> diff={diff:,}")

        # If user requested stricter verification or if there's a diff, optionally re-run exact counts with Polars
        if args.verify_with_polars and (diff != 0):
            if pl is None:
                print("  verify-with-polars requested but Polars not installed; skipping exact verification")
                continue
            print("  Mismatch detected — re-counting CSV files with Polars for verification...")
            exact_counts = {}
            for p in existing_files:
                try:
                    exact_counts[p] = count_csv_rows_polars(p, delimiter=delimiter)
                except Exception as e:
                    exact_counts[p] = -1
                    print(f"    Polars failed for {p.name}: {e}")
            exact_total = sum(v for v in exact_counts.values() if v > 0)
            exact_diff = exact_total - parquet_total
            print(f"  After Polars verification: CSV exact={exact_total:,}, Parquet={parquet_total:,} -> diff={exact_diff:,}")

    print("Validation complete.")
