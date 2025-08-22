# Project: ETL - CNPJs da Receita Federal do Brasil
# Objective: Download, transform, and load Brazilian Federal Revenue CNPJ data
import argparse
import sys

# Find time bottleneck between calls
from .core.orchestrator import ETLOrchestrator
from .setup.config import ConfigurationService


def validate_cli_arguments(args):
    """
    Validate CLI argument combinations to prevent conflicting options.
    
    Args:
        args: Parsed command line arguments
        
    Raises:
        SystemExit: If invalid argument combinations are detected
    """
    errors = []
    
    # Check for mutually exclusive download modes
    execution_modes = [args.download_only, args.download_and_convert, args.convert_only]
    active_modes = sum(execution_modes)
    
    if active_modes > 1:
        mode_names = []
        if args.download_only:
            mode_names.append("--download-only")
        if args.download_and_convert:
            mode_names.append("--download-and-convert")
        if args.convert_only:
            mode_names.append("--convert-only")
        
        errors.append(
            f"Cannot specify multiple execution modes: {', '.join(mode_names)}. "
            "These options are mutually exclusive."
        )
    
    # Check for database-related options with download-only modes
    non_database_modes = args.download_only or args.download_and_convert or args.convert_only
    
    if non_database_modes and args.full_refresh:
        errors.append(
            "Cannot use --full-refresh with non-database modes. "
            "Database operations are not performed in download-only, download-and-convert, or convert-only modes."
        )
    
    if non_database_modes and args.clear_tables:
        errors.append(
            "Cannot use --clear-tables with non-database modes. "
            "Database operations are not performed in download-only, download-and-convert, or convert-only modes."
        )
    
    # Print errors and exit if any found
    if errors:
        print("❌ Invalid CLI argument combinations detected:")
        for i, error in enumerate(errors, 1):
            print(f"  {i}. {error}")
        print("\nUse --help for valid usage examples.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Run CNPJ ETL pipeline.",
        epilog="""
Examples:
  %(prog)s --download-only --year 2024 --month 12
    Download files only (no processing)
  
  %(prog)s --download-and-convert --year 2024 --month 12
    Download and convert to Parquet (no database loading)
  
  %(prog)s --convert-only
    Convert existing CSV files from EXTRACTED_FILES to Parquet
  
  %(prog)s --year 2024 --month 12
    Full ETL pipeline (download, convert, load to database)
  
  %(prog)s --year 2024 --month 12 --full-refresh true
    Full ETL with database table refresh

Note: --download-only, --download-and-convert, and --convert-only are mutually exclusive.
Database options (--full-refresh, --clear-tables) only work with full ETL mode.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--year", type=int, help="Year to process")
    parser.add_argument("--month", type=int, help="Month to process")
    
    # Database-related options (only for full ETL mode)
    db_group = parser.add_argument_group('Database options (full ETL mode only)')
    db_group.add_argument(
        "--full-refresh", type=bool, default=False, help="Full refresh all tables flag"
    )
    db_group.add_argument(
        "--clear-tables", type=str, default="", help="Comma-separated tables to clear"
    )
    
    # Execution mode options (mutually exclusive)
    mode_group = parser.add_argument_group('Execution modes (mutually exclusive)')
    mode_group.add_argument(
        "--download-only", action="store_true", help="Only download files without processing"
    )
    mode_group.add_argument(
        "--download-and-convert", action="store_true", help="Download files and convert to Parquet, skip database loading"
    )
    mode_group.add_argument(
        "--convert-only", action="store_true", help="Convert existing CSV files from EXTRACTED_FILES to Parquet, skip download"
    )

    args = parser.parse_args()

    # Validate CLI argument combinations
    validate_cli_arguments(args)

    # Find time bottleneck between calls
    config_service = ConfigurationService()
    orchestrator = ETLOrchestrator(config_service)

    orchestrator.run(
        year=args.year,
        month=args.month,
        full_refresh=args.full_refresh,
        clear_tables=args.clear_tables,
        download_only=args.download_only,
        download_and_convert=args.download_and_convert,
        convert_only=args.convert_only,
    )

if __name__ == "__main__":
    main()
