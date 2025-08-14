# Project: ETL - CNPJs da Receita Federal do Brasil
# Objective: Download, transform, and load Brazilian Federal Revenue CNPJ data
import argparse

# Find time bottleneck between calls
from .core.orchestrator import ETLOrchestrator
from .setup.config import ConfigurationService

def main():
    parser = argparse.ArgumentParser(description="Run CNPJ ETL pipeline.")
    parser.add_argument("--year", type=int, help="Year to process")
    parser.add_argument("--month", type=int, help="Month to process")
    parser.add_argument(
        "--full-refresh", type=bool, default=False, help="Full refresh flag"
    )
    parser.add_argument(
        "--clear-tables", type=str, default="", help="Comma-separated tables to clear"
    )

    args = parser.parse_args()

    # Find time bottleneck between calls
    config_service = ConfigurationService()
    orchestrator = ETLOrchestrator(config_service)

    orchestrator.run(
        year=args.year,
        month=args.month,
        full_refresh=args.full_refresh,
        clear_tables=args.clear_tables,
    )

if __name__ == "__main__":
    main()
