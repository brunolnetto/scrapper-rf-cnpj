#!/usr/bin/env python3
"""
Convert CLI - Command-line interface for converting specific CNPJ tables from CSV to Parquet.

Usage:
    python scripts/convert_cli.py --tables empresa estabelecimento --year 2025 --month 9
    python scripts/convert_cli.py --tables all --year 2025 --month 9 --dev-mode

This script allows selective conversion of CNPJ data tables, useful for testing or partial processing.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from setup.config import get_config
from setup.logging import logger
from core.utils.discovery import discover_audit_map
from core.services.conversion.service import convert_csvs_to_parquet_smart
from core.utils.development_filter import DevelopmentFilter


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert specific CNPJ tables from CSV to Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/convert_cli.py --tables empresa --year 2025 --month 9
  python scripts/convert_cli.py --tables all --year 2025 --month 9 --dev-mode
  python scripts/convert_cli.py --tables estabelecimento socios --year 2025 --month 9 --verbose
        """
    )

    parser.add_argument(
        "--tables",
        nargs="+",
        required=True,
        choices=["empresa", "estabelecimento", "socios", "cnae", "moti", "munic", "natju", "pais", "quals", "simples", "all"],
        help="Tables to convert (space-separated). Use 'all' for all tables."
    )

    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year of the data (e.g., 2025)"
    )

    parser.add_argument(
        "--month",
        type=int,
        required=True,
        choices=range(1, 13),
        help="Month of the data (1-12)"
    )

    parser.add_argument(
        "--dev-mode",
        action="store_true",
        help="Enable development mode with file size and table limits"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Custom output directory for Parquet files (default: auto-detected)"
    )

    return parser.parse_args()


def filter_audit_map_by_tables(audit_map: dict, desired_tables: List[str]) -> dict:
    """Filter audit map to include only desired tables."""
    if "all" in desired_tables:
        return audit_map

    filtered = {}
    for table_name, audits in audit_map.items():
        if table_name in desired_tables:
            filtered[table_name] = audits

    return filtered


def main():
    """Main CLI entry point."""
    args = parse_arguments()

    # Set logging level
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info(f"Starting conversion CLI for tables: {args.tables}, period: {args.year}-{args.month:02d}")

    try:
        # Get configuration
        config = get_config(year=args.year, month=args.month)

        # Override output dir if specified
        if args.output_dir:
            config.paths.convert_path = args.output_dir

        # Discover audit map
        unzip_dir = config.paths.extract_path / f"{args.year}-{args.month:02d}"
        audit_map = discover_audit_map(unzip_dir)

        if not audit_map:
            logger.error(f"No audit map found in {unzip_dir}")
            sys.exit(1)

        # Filter by desired tables
        filtered_audit_map = filter_audit_map_by_tables(audit_map, args.tables)

        if not filtered_audit_map:
            logger.error(f"No matching tables found for: {args.tables}")
            available = list(audit_map.keys())
            logger.info(f"Available tables: {available}")
            sys.exit(1)

        logger.info(f"Converting tables: {list(filtered_audit_map.keys())}")

        # Apply development filters if requested
        if args.dev_mode:
            logger.info("Applying development mode filters")
            dev_filter = DevelopmentFilter(config)
            for table_name, audits in filtered_audit_map.items():
                filtered_audits = dev_filter.filter_audits_by_size(audits)
                filtered_audits = dev_filter.filter_audits_by_table_limit(filtered_audits)
                filtered_audit_map[table_name] = filtered_audits

                summary = dev_filter.get_development_summary(table_name, 0, 0)
                dev_filter.log_filtering_summary([summary])

        # Perform conversion
        output_dir = config.paths.convert_path / f"{args.year}-{args.month:02d}"
        convert_csvs_to_parquet_smart(
            audit_map=filtered_audit_map,
            unzip_dir=unzip_dir,
            output_dir=output_dir
        )

        logger.info("Conversion completed successfully!")

    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()