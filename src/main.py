# Project: ETL - CNPJs da Receita Federal do Brasil
# Objective: Download, transform, and load Brazilian Federal Revenue CNPJ data
import argparse
import logging
import os
import sys
from typing import NoReturn, Optional, Sequence

# Find time bottleneck between calls
from .setup.config import get_config
from .core.orchestrator import PipelineOrchestrator
from .core.pipeline import ReceitaCNPJPipeline
from .core.strategies import StrategyFactory
from .core.utils.cli import validate_cli_arguments

from .setup.logging import logger as _logger

def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the pipeline and return the process exit code.

    Failures propagate as exceptions; :func:`run_cli` maps them to a
    non-zero exit code.
    """
    parser = argparse.ArgumentParser(
        description="Run CNPJ ETL pipeline with flexible step selection.",
        epilog="""
Examples:
  %(prog)s --download --year 2024 --month 12
    Download files only for specific period (100)
  
  %(prog)s --download
    Download files for latest available period (100)
  
  %(prog)s --download --convert --year 2024 --month 12
    Download and convert to Parquet for specific period (110)
  
  %(prog)s --convert
    Convert existing CSV files to Parquet (010)
    
  %(prog)s --load
    Load existing files to database (001)
    
  %(prog)s --convert --load
    Convert existing CSV files and load to database (011)
  
  %(prog)s --download --convert --load --year 2024 --month 12
    Full ETL pipeline for specific period (111)
    
  %(prog)s
    Full ETL pipeline for latest available period (111)
    
  %(prog)s --download --convert --load --full-refresh --year 2024 --month 12
    Full ETL with database table refresh

Valid combinations: 100, 110, 101, 010, 001, 011 or 111
Default (no flags): Full ETL (111) for latest available period
If no year/month specified: Auto-discovers latest available period from Federal Revenue
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--year", type=int, help="Year to process (if not specified, discovers latest available)")
    parser.add_argument("--month", type=int, help="Month to process (if not specified, discovers latest available)")
    
    # Pipeline step flags
    step_group = parser.add_argument_group('Pipeline steps (combine as needed)')
    step_group.add_argument(
        "--download", action="store_true", help="Download files from source"
    )
    step_group.add_argument(
        "--convert", action="store_true", help="Convert files to Parquet format"
    )
    step_group.add_argument(
        "--load", action="store_true", help="Load files to database"
    )
    
    # Database-related options (only when loading)
    db_group = parser.add_argument_group('Database options (requires --load)')
    db_group.add_argument(
        "--full-refresh", action="store_true", help="Full refresh all tables before loading"
    )
    db_group.add_argument(
        "--clear-tables", type=str, default="", help="Comma-separated tables to clear"
    )
    db_group.add_argument(
        "--force-reload", action="store_true",
        help="Delete audit entries for the target year/month before running (allows clean re-run)"
    )

    args = parser.parse_args(argv)

    # Handle default case (no flags = full ETL)
    if not (args.download or args.convert or args.load):
        args.download = True
        args.convert = True
        args.load = True

    # Validate CLI argument combinations
    validate_cli_arguments(args)

    # Create configuration and pipeline
    config_service = get_config(year=args.year, month=args.month)
    pipeline = ReceitaCNPJPipeline(config_service)

    # Create strategy based on flags
    strategy = StrategyFactory.create_strategy(
        download=args.download,
        convert=args.convert,
        load=args.load
    )

    # Create orchestrator with strategy
    orchestrator = PipelineOrchestrator(pipeline, strategy, config_service)

    # Run with parameters
    # Force-reload: wipe audit entries for the period so the run is idempotent
    if getattr(args, "force_reload", False) and args.year and args.month:
        from .database.models.audit import AuditBase
        from .setup.base import init_database
        _adb = init_database(config_service.audit.database, AuditBase)
        with _adb.engine.begin() as _conn:
            _t = __import__("sqlalchemy").text
            # Delete leaf → root: subbatch → batch → file → table
            _conn.execute(_t(
                "DELETE FROM subbatch_audit_manifest WHERE parent_batch_audit_id IN ("
                "  SELECT batch_audit_id FROM batch_audit_manifest"
                "  WHERE parent_file_audit_id IN ("
                "    SELECT file_audit_id FROM file_audit_manifest"
                "    WHERE parent_table_audit_id IN ("
                "      SELECT table_audit_id FROM table_audit_manifest"
                "      WHERE ingestion_year = :y AND ingestion_month = :m"
                "    )"
                "  )"
                ")"
            ), {"y": args.year, "m": args.month})
            _conn.execute(_t(
                "DELETE FROM batch_audit_manifest WHERE parent_file_audit_id IN ("
                "  SELECT file_audit_id FROM file_audit_manifest"
                "  WHERE parent_table_audit_id IN ("
                "    SELECT table_audit_id FROM table_audit_manifest"
                "    WHERE ingestion_year = :y AND ingestion_month = :m"
                "  )"
                ")"
            ), {"y": args.year, "m": args.month})
            _conn.execute(_t(
                "DELETE FROM file_audit_manifest WHERE parent_table_audit_id IN ("
                "  SELECT table_audit_id FROM table_audit_manifest"
                "  WHERE ingestion_year = :y AND ingestion_month = :m"
                ")"
            ), {"y": args.year, "m": args.month})
            _conn.execute(_t(
                "DELETE FROM table_audit_manifest"
                " WHERE ingestion_year = :y AND ingestion_month = :m"
            ), {"y": args.year, "m": args.month})
        _logger.info(f"[force-reload] Cleared audit entries for {args.year}-{args.month:02d}")

    orchestrator.run(
        year=args.year,
        month=args.month,
        full_refresh=args.full_refresh,
        clear_tables=args.clear_tables,
    )
    return 0


def _exit_code_from_system_exit(exc: SystemExit) -> int:
    if exc.code is None:
        return 0
    if isinstance(exc.code, int):
        return exc.code
    return 1


def _hard_exit(code: int) -> NoReturn:
    """Flush logs and streams, then terminate without waiting on threads.

    Some services (database pools, timers, executors) can leave non-daemon
    threads behind, which would keep the interpreter alive after the run.
    ``os._exit`` skips that wait, so everything that must reach disk is
    flushed explicitly first.
    """
    logging.shutdown()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
    os._exit(code)


def run_cli(argv: Optional[Sequence[str]] = None) -> NoReturn:
    """CLI entrypoint: run :func:`main` and exit with a meaningful status."""
    try:
        code = main(argv)
    except SystemExit as exc:
        code = _exit_code_from_system_exit(exc)
    except KeyboardInterrupt:
        _logger.error("Pipeline interrupted by user")
        code = 130
    except BaseException:
        _logger.exception("Pipeline failed")
        code = 1
    _hard_exit(code)


if __name__ == "__main__":
    run_cli()
