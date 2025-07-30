# Project: ETL - CNPJs da Receita Federal do Brasil
# Objective: Download, transform, and load Brazilian Federal Revenue CNPJ data

import os
<<<<<<< HEAD
import time
import argparse
from datetime import datetime
import logging
from rich.logging import RichHandler

from core.orchestrator import ETLOrchestrator
from setup.config import ConfigurationService

logging.basicConfig(
    level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
=======
import datetime
import argparse
from setup.base import get_sink_folder, init_database
from core.etl import CNPJ_ETL
from setup.logging import logger
from database.utils.db_admin import (
    create_database_if_not_exists,
    validate_database,
    promote_new_database,
>>>>>>> 5d1700e (feat: add promotion pipeline)
)
log = logging.getLogger("cnpj_etl")

<<<<<<< HEAD
def main():
    parser = argparse.ArgumentParser(description="Run CNPJ ETL pipeline.")
    parser.add_argument('--year', type=int, help='Year to process')
    parser.add_argument('--month', type=int, help='Month to process')
    args = parser.parse_args()
    config_service = ConfigurationService()
    orchestrator = ETLOrchestrator(config_service)
    orchestrator.run(year=args.year, month=args.month)

if __name__ == "__main__":
    main()
=======
def main(year=None, month=None):
    # Get DB credentials from environment
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = int(os.getenv('POSTGRES_PORT', '5432'))
    maintenance_db = os.getenv('POSTGRES_MAINTENANCE_DB', 'postgres')
    prod_db = os.getenv('POSTGRES_DBNAME', 'dadosrfb')
    old_db = 'dadosrfb_old'
    new_db = 'dadosrfb_new'

    # Prepare the new database (create if not exists)
    create_database_if_not_exists(user, password, host, port, maintenance_db, new_db)

    # Get year and month
    if year is None or month is None:
        today = datetime.date.today()
        ano = str(today.year)
        mes = str(today.month).zfill(2)
    else:
        ano = str(year)
        mes = str(month).zfill(2)

    print(f"[INFO] Running ETL for year={ano}, month={mes}")

    host_url = "http://200.152.38.155"
    data_url = f'{host_url}/CNPJ/dados_abertos_cnpj/{ano}-{mes}'
    layout_url = f'{host_url}/CNPJ/LAYOUT_DADOS_ABERTOS_CNPJ.pdf'

    # Data folders
    download_folder, extract_folder = get_sink_folder()

    # Database setup: temporarily override for ETL
    os.environ['POSTGRES_DBNAME'] = new_db  
    database = init_database()

    # ETL setup
    scrapper = CNPJ_ETL(
        database, data_url, layout_url, download_folder, extract_folder, 
        is_parallel=True, delete_files=True
    )

    # Run the ETL job
    try:
        scrapper.run()
        logger.info(f"ETL job for {ano}-{mes} completed successfully on '{new_db}'.")

        # Validate the new database
        if validate_database(new_db):
            logger.info("[PROMOTION] Validation passed. Promoting new database...")
            promote_new_database(
                user, password, host, port, maintenance_db, prod_db, old_db, new_db
            )
        else:
            logger.error("[PROMOTION] Validation failed. Promotion aborted. Old database remains live.")
    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        logger.error("[PROMOTION] Promotion was NOT performed. Old database remains live.")
    finally:
        # restore original for any future operations
        os.environ['POSTGRES_DBNAME'] = prod_db

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Receita Federal CNPJ ETL job.")
    parser.add_argument('--year', type=int, help='Year to process (e.g. 2024)')
    parser.add_argument('--month', type=int, help='Month to process (1-12)')
    args = parser.parse_args()
    main(args.year, args.month)
>>>>>>> 5d1700e (feat: add promotion pipeline)
