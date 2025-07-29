import os
import time
from datetime import datetime
from setup.logging import logger
from core.etl import CNPJ_ETL
from database.utils.db_admin import (
    create_database_if_not_exists,
    validate_database,
    promote_new_database,
)

class ETLOrchestrator:
    def __init__(self, config_service):
        self.config_service = config_service

    def run(self, year=None, month=None):
        start_time = time.time()

        # Get year and month from arguments or default to today
        if year is None or month is None:
            today = datetime.today()
            year = today.year
            month = today.month
        month = int(month)
        self.config_service.etl.year = int(year)
        self.config_service.etl.month = month

        # Use config_service.database for all DB credentials
        db_config = self.config_service.database
        user = db_config.user
        password = db_config.password
        host = db_config.host
        port = db_config.port
        maintenance_db = db_config.maintenance_db
        prod_db = db_config.database
        old_db = f"{prod_db}_old"
        new_db = f"{prod_db}_new"

        logger.info(f"ETL start: {datetime.now():%Y-%m-%d %H:%M:%S}")
        logger.info(f"Running ETL for year={year}, month={str(month).zfill(2)}")

        # Prepare the new database (create if not exists)
        create_database_if_not_exists(user, password, host, port, maintenance_db, new_db)

        scraper = CNPJ_ETL(config_service=self.config_service)

        # Run the ETL job
        try:
            scraper.run()
            logger.info(f"ETL job for {year}-{str(month).zfill(2)} completed successfully on '{new_db}'.")

            # Validate the new database
            if validate_database(new_db):
                logger.info("[PROMOTION] Validation passed. Starting database promotion...")
                promote_new_database(
                    user, password, host, port, 
                    maintenance_db, prod_db, old_db, new_db
                )
            else:
                logger.error("[PROMOTION] Validation failed. Promotion aborted. Old database remains live.")
        except Exception as e:
            logger.error(f"ETL job failed: {e}")
            logger.error("[PROMOTION] Promotion was NOT performed. Old database remains live.")
        finally:
            # restore original for any future operations
            os.environ['POSTGRES_DBNAME'] = prod_db