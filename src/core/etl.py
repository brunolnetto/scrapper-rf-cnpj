from typing import List, Optional
import re
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Import new services
from ..setup.logging import logger
from ..setup.config import ConfigurationService
from ..database.models import AuditDB, MainBase, AuditBase

from .constants import TABLES_INFO_DICT
from .schemas import FileInfo, AuditMetadata
from .audit.service import AuditService

class CNPJ_ETL:
    def __init__(self, config_service: ConfigurationService = None) -> None:
        from .loading.strategies import DataLoadingStrategy
        from .download.service import FileDownloadService
        
        self.config = config_service
        
        # Lazy initialization - don't connect to databases until needed
        self._database = None
        self._audit_service = None
        self._initialized = False

        # Initialize file downloader and uploader (these are fast)
        self.file_downloader = FileDownloadService()
        self.loading_strategy = DataLoadingStrategy()
        # Note: data_loader will be initialized when database is ready

    @property
    def database(self):
        """Lazy database initialization."""
        if self._database is None:
            self._init_databases()
        return self._database
    
    @property
    def audit_service(self):
        """Lazy audit service initialization.""" 
        if self._audit_service is None:
            self._init_databases()
        return self._audit_service
    
    @property
    def data_loader(self):
        """Lazy data loader initialization."""
        from .loading.service import DataLoadingService

        if not hasattr(self, '_data_loader') or self._data_loader is None:
            self._data_loader = DataLoadingService(
                self.database, self.config.paths, self.loading_strategy
            )
        return self._data_loader

    def _init_databases(self) -> None:
        """Initialize databases only when first accessed."""
        if self._initialized:
            return

        # Initialize main database for ETL tables
        from ..setup.base import init_database

        logger.info(
            f"Initializing main database: {self.config.databases['main'].database_name}"
        )
        self._database = init_database(self.config.databases["main"], MainBase)

        logger.info(
            f"Creating tables in main database: {self.config.databases['main'].database_name}"
        )
        self._database.create_tables()

        # Initialize audit database for audit records
        logger.info(
            f"Initializing audit database: {self.config.databases['audit'].database_name}"
        )
        audit_db = init_database(self.config.databases["audit"], AuditBase)

        logger.info(
            f"Creating tables in audit database: {self.config.databases['audit'].database_name}"
        )
        audit_db.create_tables()
        self._audit_service = AuditService(audit_db)
        
        self._initialized = True

    def scrap_data(self) -> List[FileInfo]:
        from ..utils.misc import convert_to_bytes

        url = self.config.urls.get_files_url(self.config.etl.year, self.config.etl.month)

        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))

        try:
            resp = session.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; CNPJ-Scraper/1.0)"},
                timeout=30,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch URL: {url}, error: {e}")
            return []

        page_items = BeautifulSoup(resp.content, "lxml")
        table_rows = page_items.find_all("tr")

        files_info = []
        for row in table_rows:
            filename_cell = row.find("a")
            date_cell = row.find("td", text=lambda t: t and re.search(r"\d{4}-\d{2}-\d{2}", t))
            size_cell = row.find(
                "td",
                text=lambda t: t and any(t.endswith(s) for s in ["K","M","G","T","P","E","Z","Y"])
            )

            if filename_cell and date_cell and size_cell:
                filename = filename_cell.text.strip()
                if filename.endswith(".zip"):
                    updated_at = self._parse_date(date_cell.text.strip(), self.config.etl.timezone, filename)
                    if not updated_at:
                        continue

                    file_info = FileInfo(
                        filename=filename,
                        updated_at=updated_at,
                        file_size=convert_to_bytes(size_cell.text.strip()),
                    )
                    files_info.append(file_info)

        return files_info

    @staticmethod
    def _parse_date(date_str: str, timezone: str, filename: str) -> Optional[datetime]:
        try:
            updated_at = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            return (
                pytz.timezone(timezone)
                .localize(updated_at)
                .replace(hour=0, minute=0, second=0, microsecond=0)
            )
        except ValueError:
            logger.error(f"Error parsing date for file: {filename}")
            return None

    def fetch_audit_data(self) -> List[AuditDB]:
        files_info = self.scrap_data()

        audits = self.audit_service.create_audits_from_files(files_info)

        # Development mode filtering
        if self.config.is_development_mode():
            audits = [
                audit
                for audit in sorted(audits, key=lambda x: x.audi_file_size_bytes)
                if audit.audi_file_size_bytes < self.config.get_file_size_limit()
            ]

        return audits

    def download_and_extract(self, audits) -> None:
        year_ = self.config.etl.year
        month_ = self.config.etl.month
        files = self.config.urls.get_files_url(year_, month_)

        self.file_downloader.download_and_extract(
            audits,
            files,
            str(self.config.paths.download_path),
            str(self.config.paths.extract_path),
            parallel=self.config.etl.is_parallel,
        )

    def retrieve_data(self) -> List[AuditDB]:
        audits = self.fetch_audit_data()

        if audits:
            self.download_and_extract(audits)
        else:
            logger.warning("No audits to retrieve.")

        return audits

    

    def convert_to_parquet(self, audit_metadata: AuditMetadata) -> Path:
        from ..utils.misc import makedir
        from ..utils.conversion import convert_csvs_to_parquet

        num_workers=self.config.etl.parallel_workers
        delimiter=self.config.etl.delimiter
        output_dir = Path(self.config.paths.conversion_path)
        
        makedir(output_dir)

        audit_map = {}
        for (
            table_name,
            zipfile_to_files,
        ) in audit_metadata.tablename_to_zipfile_to_files.items():
            if table_name not in audit_map:
                audit_map[table_name] = {}

            for zip_filename, csv_files in zipfile_to_files.items():
                audit_map[table_name][zip_filename] = csv_files

        logger.info("Converting CSV files to Parquet format...")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Processing {len(audit_map)} tables with {num_workers} workers")
        extract_path = Path(self.config.paths.extract_path)
        
        convert_csvs_to_parquet(audit_map, extract_path, output_dir, num_workers, delimiter)
        logger.info(f"Parquet conversion completed. Files saved to: {output_dir}")

        return output_dir

    def create_audit_metadata_from_existing_csvs(self) -> Optional[AuditMetadata]:
        """
        Create audit metadata from existing CSV files in EXTRACTED_FILES directory.
        
        This reuses the existing audit metadata infrastructure but creates synthetic
        entries that point to existing CSV files instead of downloaded/extracted ones.
        
        Returns:
            AuditMetadata: Compatible audit metadata for existing CSV files, or None if no files found
        """
        from pathlib import Path
        from ..core.constants import TABLES_INFO_DICT
        from ..core.schemas import AuditMetadata
        from ..database.models import AuditDBSchema
        from datetime import datetime
        from uuid import uuid4
        
        extract_path = Path(self.config.paths.extract_path)
        if not extract_path.exists():
            logger.warning(f"Extract path does not exist: {extract_path}")
            return None
        
        # Get all CSV files in extract directory
        csv_files = list(extract_path.glob("*"))
        csv_files = [f for f in csv_files if f.is_file() and ('CSV' in f.name.upper())]
        
        if not csv_files:
            logger.warning(f"No CSV files found in: {extract_path}")
            return None
        
        logger.info(f"Found {len(csv_files)} CSV files in {extract_path}")
        
        # Map CSV files to table names using expression patterns
        tablename_to_files = {}
        for table_name, table_info in TABLES_INFO_DICT.items():
            expression = table_info.get("expression")
            if not expression:
                continue
                
            # Find CSV files matching this table's expression
            matching_files = [f.name for f in csv_files if expression in f.name]
            
            if matching_files:
                tablename_to_files[table_name] = matching_files
                logger.info(f"Table '{table_name}': Found {len(matching_files)} CSV files")
        
        if not tablename_to_files:
            logger.warning("No CSV files matched any known table patterns")
            return None
        
        # Create synthetic audit metadata compatible with existing convert_to_parquet method
        # We use "synthetic_conversion" as a fake zipfile name since the existing structure expects
        # a tablename -> zipfile -> files mapping
        tablename_to_zipfile_to_files = {
            table_name: {"synthetic_conversion": files}
            for table_name, files in tablename_to_files.items()
        }
        
        # Create a minimal audit list for compatibility
        audit_list = []
        for table_name in tablename_to_files.keys():
            synthetic_audit = AuditDBSchema(
                audi_id=str(uuid4()),  # Generate a proper UUID string
                audi_table_name=table_name,
                audi_filenames=["synthetic_conversion"],  # Fake filename for compatibility
                audi_file_size_bytes=0.0,  # Not relevant for convert-only
                audi_source_updated_at=datetime.now(),
                audi_processed_at=None,
                audi_inserted_at=datetime.now(),  # Required field
                audi_metadata={"convert_only": True}
            )
            audit_list.append(synthetic_audit)
        
        logger.info(f"Successfully mapped {len(tablename_to_files)} tables from existing CSV files")
        
        return AuditMetadata(
            audit_list=audit_list,
            tablename_to_zipfile_to_files=tablename_to_zipfile_to_files
        )

    def convert_existing_csvs_to_parquet(self) -> Optional[Path]:
        """
        Convert existing CSV files from EXTRACTED_FILES to Parquet format.
        
        This method reuses the existing convert_to_parquet infrastructure by creating
        synthetic audit metadata that points to existing CSV files.
        
        Returns:
            Path: Output directory with Parquet files, or None if conversion failed
        """
        # Create synthetic audit metadata from existing CSV files
        audit_metadata = self.create_audit_metadata_from_existing_csvs()
        if not audit_metadata:
            logger.error("No existing CSV files found to convert")
            return None
        
        logger.info(f"Converting {len(audit_metadata.tablename_to_zipfile_to_files)} tables from existing CSV files to Parquet...")
        
        # Reuse the existing convert_to_parquet method
        return self.convert_to_parquet(audit_metadata)

    def create_indices(self) -> None:
        # Build indices from TABLES_INFO_DICT
        renew_table_indices = {
            table_name: set(table_info.get("index_columns", []))
            for table_name, table_info in TABLES_INFO_DICT.items()
            if table_info.get("index_columns")
        }

        if renew_table_indices:
            from ..database.dml import generate_tables_indices
            generate_tables_indices(self.database.engine, renew_table_indices)

    def run(self) -> Optional[AuditMetadata]:
        from ..utils.misc import remove_folder
        
        extract_path = str(self.config.paths.extract_path)
        download_path = str(self.config.paths.download_path)

        audits = self.retrieve_data()

        if audits:
            audit_metadata = self.audit_service.create_audit_metadata(audits, download_path)

            # Remove downloaded files
            if self.config.etl.delete_files:
                remove_folder(download_path)

            # Convert to Parquet
            self.convert_to_parquet(audit_metadata)

            # Load data using the new DataLoadingService
            self.data_loader.load_data(audit_metadata)

            # Create database indices
            self.create_indices()

            return audit_metadata
        else:
            logger.warning("No data to load!")
            return None

        if self.config.etl.delete_files:
            remove_folder(extract_path)

