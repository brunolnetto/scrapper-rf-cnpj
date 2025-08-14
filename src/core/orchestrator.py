import time


from datetime import datetime

from ..setup.logging import logger
from ..database.utils.db_admin import (
    create_database_if_not_exists,
    truncate_tables,
    get_table_row_counts,
)

class ETLOrchestrator:
    def __init__(self, config_service):
        self.config_service = config_service

    def clear_production_tables(self, table_names=None):
        """
        Clear (truncate) production tables before upsert.

        Args:
            table_names: List of table names to clear, or None to clear all tables

        Returns:
            bool: True if successful, False otherwise
        """
        main_db_config = self.config_service.databases['main']
        prod_db = main_db_config.database_name

        logger.info(f"[CLEAR] Clearing tables in production database '{prod_db}'...")

        success = truncate_tables(
            main_db_config.user,
            main_db_config.password,
            main_db_config.host,
            main_db_config.port,
            prod_db,
            table_names,
        )

        if success:
            logger.info("[CLEAR] Tables cleared successfully.")
        else:
            logger.error("[CLEAR] Failed to clear tables.")

        return success

    def run(self, year=None, month=None, full_refresh=False, clear_tables=None):
        """
        Run the ETL process with upsert to production database.

        Args:
            year: Year for ETL processing (default: current year)
            month: Month for ETL processing (default: current month)
            full_refresh: If True, clear all tables before upsert (default: False)
            clear_tables: List of specific tables to clear, or None (only used if full_refresh=True)
        """
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
        main_db_config = self.config_service.databases['main']
        user = main_db_config.user
        password = main_db_config.password
        host = main_db_config.host
        port = main_db_config.port
        maintenance_db = main_db_config.maintenance_db
        prod_db = main_db_config.database_name

        logger.info(f"ETL start: {datetime.now():%Y-%m-%d %H:%M:%S}")
        logger.info(f"Running ETL for year={year}, month={str(month).zfill(2)}")
        logger.info(f"Target database: {prod_db}")

        # Ensure the production database exists
        create_database_if_not_exists(
            user, password, host, port, maintenance_db, prod_db
        )

        # Handle full refresh option
        if full_refresh:
            logger.info(
                "[REFRESH] Full refresh requested - clearing tables before upsert..."
            )
            if not self.clear_production_tables(clear_tables):
                logger.error("[REFRESH] Failed to clear tables. Aborting ETL process.")
                return

        # Get current row counts for validation
        logger.info("[VALIDATION] Getting current table row counts...")
        initial_row_counts = get_table_row_counts(user, password, host, port, prod_db)
        logger.info(f"[VALIDATION] Current row counts: {initial_row_counts}")

        # Initialize ETL with production database
        from .etl import CNPJ_ETL
        scrapper = CNPJ_ETL(config_service=self.config_service)

        # Run the ETL job with upsert directly to production
        try:
            logger.info(
                "[ETL] Starting data processing and upsert to production database..."
            )
            audit_metadata = scrapper.run()

            if audit_metadata:
                # Get final row counts for validation
                logger.info("[VALIDATION] Getting final table row counts...")
                final_row_counts = get_table_row_counts(
                    user, password, host, port, prod_db
                )
                logger.info(f"[VALIDATION] Final row counts: {final_row_counts}")

                logger.info(
                    f"[SUCCESS] ETL job for {year}-{str(month).zfill(2)} completed successfully."
                )
                logger.info(
                    f"[SUCCESS] Data successfully upserted to production database '{prod_db}'."
                )

                # Log row count changes
                for table_name in final_row_counts:
                    initial_count = initial_row_counts.get(table_name, 0)
                    final_count = final_row_counts[table_name]
                    change = final_count - initial_count

                    row_count_metadata = {
                        "before": initial_count,
                        "after": final_count,
                        "diff": change,
                    }
                    print(f"[METRICS] Table '{table_name}': {initial_count} -> {final_count} (Δ{change:+d})")
                    table_audit = list(
                        filter(
                            lambda x: x.audi_table_name == table_name,
                            audit_metadata.audit_list,
                        )
                    )
                    
                    if len(table_audit) != 1:
                        logger.error(f"[ERROR] Expected 1 audit entry for table '{table_name}', found {len(table_audit)}.")
                        continue
                    elif len(table_audit) == 0:
                        logger.error(f"[ERROR] No audit entry found for table '{table_name}'.")
                        continue
                    else:
                        table_audit=table_audit[0]

                    table_audit.audi_metadata = {"row_count": row_count_metadata}

                    logger.info(
                        f"[METRICS] Table '{table_name}': {initial_count} -> {final_count} (Δ{change:+d})"
                    )

                # Insert audits into the database
                scrapper.audit_service.insert_audits(audit_metadata)

        except Exception as e:
            logger.error(f"[ERROR] ETL job failed: {e}")
            logger.error(
                "[ERROR] Production database may be in inconsistent state. "
            )
            raise
        finally:
            # Calculate and log execution time
            execution_time = time.time() - start_time
            logger.info(f"[METRICS] Total execution time: {execution_time:.2f} seconds")
