# Project: ETL - CNPJs da Receita Federal do Brasil
# Objective: Download, transform, and load Brazilian Federal Revenue CNPJ data
import argparse
import sys

# Find time bottleneck between calls
from .core.orchestrator import PipelineOrchestrator
from .core.etl import ReceitaCNPJPipeline
from .core.strategies import StrategyFactory
from .setup.config import ConfigurationService


def validate_cli_arguments(args):
    """
    Validate CLI argument combinations to ensure valid strategy selection.
    
    Args:
        args: Parsed command line arguments
        
    Raises:
        SystemExit: If invalid argument combinations are detected
    """
    errors = []

    # Check for valid strategy combinations

    # Regular strategy combinations
    strategy_key = (args.download, args.convert, args.load)
    valid_combinations = [
        (True, False, False),  # Download Only
        (True, True, False),   # Download and Convert  
        (False, True, False),  # Convert Only
        (False, False, True),  # Load Only
        (False, True, True),   # Convert and Load
        (True, True, True),    # Full ETL
    ]

    if strategy_key not in valid_combinations:
        errors.append(
            f"Invalid strategy combination: download={args.download}, convert={args.convert}, load={args.load}. "
            "Valid combinations: 100 (download only), 110 (download+convert), 101 (download+load), 010 (convert only), "
            "001 (load only), 011 (convert+load), 111 (full ETL)."
        )

    # Check for database-related options with non-database modes
    if not args.load and (args.full_refresh or args.clear_tables):
        errors.append(
            "Cannot use --full-refresh or --clear-tables without --load flag. "
            "Database operations require loading data to database."
        )

    are_both_not_none=args.year is not None and args.month is not None
    are_both_none=args.year is None and args.month is None
    both_or_none=not( are_both_not_none or are_both_none )
    
    if both_or_none:
        errors.append("Either both or none of year and month must be specified.")
    
    if are_both_not_none:
        valid_temporal_values=args.year <= 2000 or (args.month < 1 or args.month > 12)    
        if valid_temporal_values:
            errors.append("Invalid year or month specified.")

    # Either both or none must be specified. If none, set to current
    if not args.year and not args.month:
        from datetime import datetime
        args.year = datetime.now().year
        args.month = datetime.now().month

    # Print errors and exit if any found
    if errors:
        print("❌ Invalid CLI argument combinations detected:")
        for i, error in enumerate(errors, 1):
            print(f"  {i}. {error}")
        print("\nUse --help for valid usage examples.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Run CNPJ ETL pipeline with flexible step selection.",
        epilog="""
Examples:
  %(prog)s --download --year 2024 --month 12
    Download files only (100)
  
  %(prog)s --download --convert --year 2024 --month 12
    Download and convert to Parquet (110)
  
  %(prog)s --convert
    Convert existing CSV files to Parquet (010)
    
  %(prog)s --load
    Load existing files to database (001)
    
  %(prog)s --convert --load
    Convert existing CSV files and load to database (011)
  
  %(prog)s --download --convert --load --year 2024 --month 12
    Full ETL pipeline (111)
    
  %(prog)s --download --convert --load --full-refresh --year 2024 --month 12
    Full ETL with database table refresh

Valid combinations: 100, 110, 101, 010, 001, 011 or 111
Default (no flags): Full ETL (111)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--year", type=int, help="Year to process")
    parser.add_argument("--month", type=int, help="Month to process")
    
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

    args = parser.parse_args()

    # Handle default case (no flags = full ETL)
    if not (args.download or args.convert or args.load):
        args.download = True
        args.convert = True
        args.load = True

    # Validate CLI argument combinations
    validate_cli_arguments(args)

    # Create configuration and pipeline
    config_service = ConfigurationService(month=args.month, year=args.year)
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
    orchestrator.run(
        year=args.year,
        month=args.month,
        full_refresh=args.full_refresh,
        clear_tables=args.clear_tables,
    )

if __name__ == "__main__":
    main()
