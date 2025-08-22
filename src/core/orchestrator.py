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

    def run(
        self,
        year=None,
        month=None,
        full_refresh: bool = False,
        clear_tables: str = "",
        download_only: bool = False,
        download_and_convert: bool = False,
        convert_only: bool = False,
    ):
        """
        Run the ETL process with upsert to production database.

        Args:
            year: Year for ETL processing (default: current year)
            month: Month for ETL processing (default: current month)
            full_refresh: If True, clear all tables before upsert (default: False)
            clear_tables: List of specific tables to clear, or None (only used if full_refresh=True)
            download_only: If True, only download files without processing (default: False)
            download_and_convert: If True, download and convert to Parquet but skip database loading (default: False)
            convert_only: If True, convert existing CSV files to Parquet without downloading (default: False)
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
        
        # Handle download-only mode
        if download_only:
            logger.info("[DOWNLOAD-ONLY] Running in download-only mode...")
            from .etl import CNPJ_ETL
            scrapper = CNPJ_ETL(config_service=self.config_service)
            
            try:
                audits = scrapper.retrieve_data()
                if audits:
                    logger.info(f"[DOWNLOAD-ONLY] Successfully downloaded {len(audits)} files")
                    logger.info(f"[DOWNLOAD-ONLY] Files saved to: {self.config_service.paths.download_path}")
                else:
                    logger.warning("[DOWNLOAD-ONLY] No files were downloaded")
                return
            except Exception as e:
                logger.error(f"[DOWNLOAD-ONLY] Download failed: {e}")
                raise
        
        # Handle download-and-convert mode
        if download_and_convert:
            logger.info("[DOWNLOAD-CONVERT] Running in download-and-convert mode...")
            from .etl import CNPJ_ETL
            scrapper = CNPJ_ETL(config_service=self.config_service)
            
            try:
                # Download files
                audits = scrapper.retrieve_data()
                if not audits:
                    logger.warning("[DOWNLOAD-CONVERT] No files were downloaded")
                    return
                
                # Create audit metadata and convert to Parquet
                download_path = str(self.config_service.paths.download_path)
                audit_metadata = scrapper.audit_service.create_audit_metadata(audits, download_path)
                conversion_path = scrapper.convert_to_parquet(audit_metadata)
                
                logger.info(f"[DOWNLOAD-CONVERT] Successfully downloaded {len(audits)} files")
                logger.info(f"[DOWNLOAD-CONVERT] Files converted to Parquet and saved to: {conversion_path}")
                
                # Cleanup downloaded files if configured
                if self.config_service.etl.delete_files:
                    from ..utils.misc import remove_folder
                    remove_folder(download_path)
                    logger.info("[DOWNLOAD-CONVERT] Cleaned up downloaded files")
                
                return
            except Exception as e:
                logger.error(f"[DOWNLOAD-CONVERT] Process failed: {e}")
                raise

        # Handle convert-only mode
        if convert_only:
            logger.info("[CONVERT-ONLY] Running in convert-only mode...")
            from .etl import CNPJ_ETL
            scrapper = CNPJ_ETL(config_service=self.config_service)
            
            try:
                # Convert existing CSV files to Parquet
                conversion_path = scrapper.convert_existing_csvs_to_parquet()
                if conversion_path:
                    logger.info("[CONVERT-ONLY] Successfully converted existing CSV files to Parquet")
                    logger.info(f"[CONVERT-ONLY] Parquet files saved to: {conversion_path}")
                else:
                    logger.warning("[CONVERT-ONLY] No CSV files were converted")
                return
            except Exception as e:
                logger.error(f"[CONVERT-ONLY] Conversion failed: {e}")
                raise

        # Full ETL mode (default)
        logger.info("[FULL-ETL] Running full ETL pipeline...")
        logger.info(f"[FULL-ETL] Target database: {prod_db}")

        # Ensure the production database exists
        create_database_if_not_exists(
            user, password, host, port, maintenance_db, prod_db
        )

        # Handle full refresh option
        if full_refresh:
            logger.info(
                "[FULL-ETL] Full refresh requested - clearing tables before upsert..."
            )
            if not self.clear_production_tables(clear_tables):
                logger.error("[FULL-ETL] Failed to clear tables. Aborting ETL process.")
                return

        # Get current row counts for validation
        logger.info("[FULL-ETL] Getting current table row counts...")
        initial_row_counts = get_table_row_counts(user, password, host, port, prod_db)
        logger.info(f"[FULL-ETL] Current row counts: {initial_row_counts}")

        # Initialize ETL with production database
        from .etl import CNPJ_ETL
        scrapper = CNPJ_ETL(config_service=self.config_service)

        # Run the ETL job with upsert directly to production
        try:
            logger.info(
                "[FULL-ETL] Starting data processing and upsert to production database..."
            )
            audit_metadata = scrapper.run()

            if audit_metadata:
                # Get final row counts for validation
                logger.info("[FULL-ETL] Getting final table row counts...")
                final_row_counts = get_table_row_counts(
                    user, password, host, port, prod_db
                )
                logger.info(f"[FULL-ETL] Final row counts: {final_row_counts}")

                logger.info(
                    f"[FULL-ETL] ETL job for {year}-{str(month).zfill(2)} completed successfully."
                )
                logger.info(
                    f"[FULL-ETL] Data successfully upserted to production database '{prod_db}'."
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
                    print(f"[METRICS] Table '{table_name}': {initial_count} -> {final_count} (diff: {change:+d})")
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
                        f"[METRICS] Table '{table_name}': {initial_count} -> {final_count} (diff: {change:+d})"
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

