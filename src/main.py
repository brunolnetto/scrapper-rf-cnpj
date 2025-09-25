# Project: ETL - CNPJs da Receita Federal do Brasil
# Objective: Download, transform, and load Brazilian Federal Revenue CNPJ data
import argparse
import sys

# Find time bottleneck between calls
from .setup.config import get_config
from .core.orchestrator import PipelineOrchestrator
from .core.pipeline import ReceitaCNPJPipeline
from .core.strategies import StrategyFactory
from .core.utils.cli import validate_cli_arguments

def main():
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

    args = parser.parse_args()

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
    orchestrator.run(
        year=args.year,
        month=args.month,
        full_refresh=args.full_refresh,
        clear_tables=args.clear_tables,
    )

if __name__ == "__main__":
    main()
