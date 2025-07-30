# Project: ETL - CNPJs da Receita Federal do Brasil
# Objective: Download, transform, and load Brazilian Federal Revenue CNPJ data

import os
import time
import argparse
import sys
from datetime import datetime
import logging
from rich.logging import RichHandler

from core.orchestrator import ETLOrchestrator
from core.etl import ETLDataRetrievalError, ETLNoDataAvailableError
from setup.config import ConfigurationService

logging.basicConfig(
    level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("cnpj_etl")

def main():
    parser = argparse.ArgumentParser(description="Run CNPJ ETL pipeline.")
    parser.add_argument('--year', type=int, help='Year to process')
    parser.add_argument('--month', type=int, help='Month to process')
    args = parser.parse_args()
    
    try:
        config_service = ConfigurationService()
        orchestrator = ETLOrchestrator(config_service)
        orchestrator.run(year=args.year, month=args.month)
    except (ETLDataRetrievalError, ETLNoDataAvailableError) as e:
        log.error(f"ETL pipeline failed: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
