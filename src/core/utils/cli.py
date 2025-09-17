import sys
import logging
from src.setup.logging import logger
from .discovery import discover_latest_period

# Constants
MIN_VALID_YEAR = 2000  # Minimum year for CNPJ data processing


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
        (False, True, False),  # Convert Only
        (False, False, True),  # Load Only
        (True, True, False),   # Download and Convert 
        (True, False, True),   # Download and Load
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
        valid_temporal_values=args.year <= MIN_VALID_YEAR or (args.month < 1 or args.month > 12)    
        if valid_temporal_values:
            errors.append(f"Invalid year (must be > {MIN_VALID_YEAR}) or month (1-12) specified.")

    # Either both or none must be specified. If none, discover latest available
    if not args.year or not args.month:
        args.year, args.month = discover_latest_period()

    # Log errors and exit if any found
    if errors:
        logger.error("❌ Invalid CLI argument combinations detected:")
        for i, error in enumerate(errors, 1):
            logger.error(f"  {i}. {error}")
        logger.error("Use --help for valid usage examples.")
        sys.exit(1)