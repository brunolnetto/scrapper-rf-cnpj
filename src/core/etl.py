from typing import List, Optional
import re
from urllib import request
from bs4 import BeautifulSoup
from datetime import datetime
from functools import reduce
import pytz
from pathlib import Path
import os

from setup.logging import logger
from database.models import AuditDB, MainBase, AuditBase
from core.schemas import FileInfo, AuditMetadata
from database.dml import generate_tables_indices
from utils.misc import convert_to_bytes, remove_folder, makedir
from utils.conversion import convert_csvs_to_parquet

# Import new services
from setup.base import init_database
from setup.config import ConfigurationService
from core.constants import TABLES_INFO_DICT
from core.audit.service import AuditService
from core.loading.service import DataLoadingService
from core.loading.strategies import AutoLoadingStrategy
from core.download.service import FileDownloadService

class CNPJ_ETL:
    def __init__(
        self,
        config_service: Optional[ConfigurationService] = None
    ) -> None:
        self.config = config_service or ConfigurationService()
        
        # Initialize main database for ETL tables
        logger.info(f"Initializing main database: {self.config.database.database}")
        self.database = init_database(self.config.database, MainBase)
        self.database.create_tables()
        
        # Initialize audit database for audit records
        logger.info(f"Initializing audit database: {self.config.audit_db_config.database}")
        audit_db = init_database(self.config.audit_db_config, AuditBase)
        audit_db.create_tables()
        
        self.audit_service = AuditService(audit_db)
        self.data_loader = DataLoadingService(AutoLoadingStrategy(prefer_parquet=self.config.etl.prefer_parquet))
        self.file_downloader = FileDownloadService()

    def scrap_data(self) -> List[FileInfo]:
        """
        Scrapes the Receita Federal website to extract file information.
        Returns:
            List[FileInfo]: A list of FileInfo objects with updated date, filename, and size.
        """
        try:
            raw_html = request.urlopen(self.config.urls.get_files_url(self.config.etl.year, self.config.etl.month)).read()
        except Exception as e:
            logger.error(f"Failed to fetch URL: {self.config.urls.get_files_url(self.config.etl.year, self.config.etl.month)}, error: {e}")
            return []

        page_items = BeautifulSoup(raw_html, 'lxml')
        table_rows = page_items.find_all('tr')

        files_info = []
        for row in table_rows:
            filename_cell = row.find('a')
            date_cell = row.find('td', text=lambda text: text and re.search(r'\d{4}-\d{2}-\d{2}', text))
            size_cell = row.find('td', text=lambda text: text and any(text.endswith(size_type) for size_type in ['K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']))

            if filename_cell and date_cell and size_cell:
                filename = filename_cell.text.strip()
                if filename.endswith('.zip'):
                    updated_at = self._parse_date(
                        date_cell.text.strip(), 
                        self.config.etl.timezone,
                        filename
                    )
                    if not updated_at:
                        continue

                    file_info = FileInfo(
                        filename=filename,
                        updated_at=updated_at,
                        file_size=convert_to_bytes(size_cell.text.strip())
                    )
                    files_info.append(file_info)

        return files_info

    @staticmethod
    def _parse_date(date_str: str, timezone: str, filename: str) -> Optional[datetime]:
        try:
            updated_at = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            return pytz.timezone(timezone)\
                .localize(updated_at)\
                .replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            logger.error(f"Error parsing date for file: {filename}")
            return None    

    def fetch_data(self) -> List[AuditDB]:
        files_info = self.scrap_data()
        return self.audit_service.create_audits_from_files(files_info)

    def retrieve_data(self) -> List[AuditDB]:
        audits = self.fetch_data()

        # Development mode filtering
        if self.config.is_development_mode():
            audits = [
                audit for audit in sorted(audits, key=lambda x: x.audi_file_size_bytes)
                if audit.audi_file_size_bytes < self.config.get_file_size_limit()
            ]

        if audits:
            self.file_downloader.download_and_extract(
                audits,
                self.config.urls.get_files_url(self.config.etl.year, self.config.etl.month),
                str(self.config.paths.download_path),
                str(self.config.paths.extract_path),
                parallel=self.config.etl.is_parallel
            )
        else:
            logger.warning("No audits to retrieve.")

        return audits

    def convert_to_parquet(self, audit_metadata: AuditMetadata, max_workers: int = 4) -> Path:
        output_dir = Path(self.config.paths.extract_path) / "parquet_data"
        makedir(output_dir)
        
        audit_map = {}
        for table_name, zipfile_to_files in audit_metadata.tablename_to_zipfile_to_files.items():
            if table_name not in audit_map:
                audit_map[table_name] = {}
            for zip_filename, csv_files in zipfile_to_files.items():
                audit_map[table_name][zip_filename] = csv_files
        logger.info(f"Converting CSV files to Parquet format...")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Processing {len(audit_map)} tables with {max_workers} workers")
        convert_csvs_to_parquet(audit_map, Path(self.config.paths.extract_path), output_dir, max_workers)
        logger.info(f"Parquet conversion completed. Files saved to: {output_dir}")
        return output_dir

    def create_indices(self) -> None:
        # Build indices from TABLES_INFO_DICT
        renew_table_indices = {
            table_name: set(table_info.get('index_columns', []))
            for table_name, table_info in TABLES_INFO_DICT.items()
            if table_info.get('index_columns')
        }
        if renew_table_indices:
            generate_tables_indices(self.database.engine, renew_table_indices)


    def run(self) -> None:
        audits = self.retrieve_data()
        if audits:
            audit_metadata = self.audit_service.create_audit_metadata(
                audits, str(self.config.paths.download_path)
            )

            if self.config.etl.delete_files:
                remove_folder(str(self.config.paths.download_path))

            # Convert to Parquet if requested
            if self.config.etl.prefer_parquet:
                self.convert_to_parquet(audit_metadata)

            # Load data using the new DataLoadingService
            self.data_loader.load_data(
                self.database, 
                str(self.config.paths.extract_path), 
                audit_metadata
            )

            self.create_indices()

            self.audit_service.insert_audits(audit_metadata)
        else:
            logger.warning("No data to load!")
        if self.config.etl.delete_files:
            remove_folder(str(self.config.paths.extract_path))

