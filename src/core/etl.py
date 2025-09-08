from typing import List, Optional
import re
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from uuid import UUID

# Import new services
from ..setup.logging import logger
from ..setup.config import ConfigurationService
from ..database.models import AuditDB, MainBase, AuditBase
from .interfaces import Pipeline


from .schemas import FileInfo, AuditMetadata
from .services.audit.service import AuditService
from .services.loading.strategies import DataLoadingStrategy

class ReceitaCNPJPipeline(Pipeline):
    def __init__(self, config_service: ConfigurationService = None) -> None:
        from .services.download.service import FileDownloadService

        self.config = config_service

        # Lazy initialization - don't connect to databases until needed
        self._database = None
        self._audit_service = None
        self._initialized = False

        # Batch tracking state
        self._current_batch_id: Optional[UUID] = None
        self._batch_name: Optional[str] = None

        # Initialize file downloader and uploader (these are fast)
        self.file_downloader = FileDownloadService(config=config_service)
        self.loading_strategy = DataLoadingStrategy(self.config)  # Use consolidated strategy
        # Note: data_loader will be initialized when database is ready

    @property
    def database(self):
        """Lazy database initialization."""
        if self._database is None:
            self._init_databases()
        return self._database
    
    @property
    def audit_service(self):
        """Lazy audit service initialization with manifest tracking.""" 
        if self._audit_service is None:
            self._init_databases()
        return self._audit_service
    
    @property
    def data_loader(self):
        """Lazy data loader initialization."""
        from .services.loading.service import DataLoadingService

        if not hasattr(self, '_data_loader') or self._data_loader is None:
            self._data_loader = DataLoadingService(
                self.database, 
                self.config.paths, 
                self.loading_strategy, 
                self.config,
                audit_service=self.audit_service
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
        self._audit_service = AuditService(audit_db, self.config)
        
        self._initialized = True

    def _generate_batch_name(self) -> str:
        """Generate a descriptive batch name."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"ETL_Pipeline_{self.config.etl.year}_{self.config.etl.month:02d}_{timestamp}"

    def _start_batch_tracking(self, target_table: Optional[str] = None) -> UUID:
        """Start batch tracking for the pipeline execution."""
        if not self.config.batch_config.enabled:
            return None
            
        self._batch_name = self._generate_batch_name()
        
        logger.info(f"Starting batch tracking: {self._batch_name}")
        
        # Start batch without context manager - we'll manage it manually
        batch_id = self.audit_service._start_batch(
            batch_name=self._batch_name,
            target_table=target_table or "ALL_TABLES"
        )
        self._current_batch_id = batch_id
        return batch_id

    def _complete_batch_tracking(self, success: bool = True) -> None:
        """Complete batch tracking for the pipeline execution."""
        if not self.config.batch_config.enabled or not self._current_batch_id:
            return
            
        logger.info(f"Completing batch tracking: {self._batch_name} (success={success})")
        
        # Complete the batch manually using the correct method name
        from ..database.models import BatchStatus
        status = BatchStatus.COMPLETED if success else BatchStatus.FAILED
        self.audit_service._complete_batch_with_accumulated_metrics(self._current_batch_id, status)
        self._current_batch_id = None

    @property
    def current_batch_id(self) -> Optional[UUID]:
        """Get the current batch ID for this pipeline execution."""
        return self._current_batch_id

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

        # Apply development mode filtering using centralized filter
        if self.config.is_development_mode():
            from ..core.utils.development_filter import DevelopmentFilter
            dev_filter = DevelopmentFilter(self.config)
            original_count = len(audits)

            # Filter by file size and table limits
            audits = dev_filter.filter_audits_by_size(audits)
            audits = dev_filter.filter_audits_by_table_limit(audits)

            # Log filtering summary
            dev_filter.log_filtering_summary(original_count, len(audits), "audit files")

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
        from .services.conversion.service import convert_csvs_to_parquet_smart

        num_workers = self.config.etl.parallel_workers
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
                # Development mode filtering - filter CSV files by size
                if self.config.is_development_mode():
                    from ..core.utils.development_filter import DevelopmentFilter
                    dev_filter = DevelopmentFilter(self.config)

                    # Convert string filenames to Path objects for filtering
                    csv_paths = [Path(self.config.paths.extract_path) / csv_file for csv_file in csv_files]
                    filtered_csv_paths = dev_filter.filter_csv_files_by_size(csv_paths)

                    # Convert back to string filenames
                    csv_files = [path.name for path in filtered_csv_paths]

                audit_map[table_name][zip_filename] = csv_files

        logger.info("Converting CSV files to Parquet format...")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Processing {len(audit_map)} tables with {num_workers} workers")

        # Centralized conversion summary logging
        from ..core.utils.development_filter import DevelopmentFilter
        dev_filter = DevelopmentFilter(self.config)
        dev_filter.log_conversion_summary(audit_map)

        extract_path = Path(self.config.paths.extract_path)

        convert_csvs_to_parquet_smart(audit_map, extract_path, output_dir)
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

        # Get all CSV files in extract directory using robust detection
        csv_files = self._detect_csv_files(extract_path)

        if not csv_files:
            logger.warning(f"No CSV files found in: {extract_path}")
            return None

        logger.info(f"Found {len(csv_files)} CSV files in {extract_path}")

        # Development mode filtering - filter CSV files by size
        if self.config.is_development_mode():
            from ..core.utils.development_filter import DevelopmentFilter
            dev_filter = DevelopmentFilter(self.config)
            original_count = len(csv_files)

            csv_files = dev_filter.filter_csv_files_by_size(csv_files)

            # Log filtering summary
            dev_filter.log_filtering_summary(original_count, len(csv_files), "CSV files")

        for csv_file in csv_files:
            logger.debug(f"Detected CSV file: {csv_file.name}")

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

    def validate_config(self) -> bool:
        """Validate pipeline configuration."""
        try:
            return bool(
                self.config and 
                self.config.databases and 
                self.config.etl and 
                self.config.paths
            )
        except Exception:
            return False
    
    def get_name(self) -> str:
        """Return pipeline name for logging."""
        return "ReceitaCNPJPipeline"
    
    def run(self, **kwargs) -> Optional[AuditMetadata]:
        from ..utils.misc import remove_folder
        
        # Handle orchestrator parameters
        year = kwargs.get('year')
        month = kwargs.get('month')
        if year:
            self.config.etl.year = int(year)
        if month:
            self.config.etl.month = int(month)
        
        extract_path = str(self.config.paths.extract_path)
        download_path = str(self.config.paths.download_path)

        # No longer create umbrella batch - individual table batches will be created
        try:
            audits = self.retrieve_data()

            if audits:
                audit_metadata = self.audit_service.create_audit_metadata(audits, download_path)

                # Remove downloaded files
                if self.config.etl.delete_files:
                    remove_folder(download_path)

                # Convert to Parquet
                self.convert_to_parquet(audit_metadata)

                # Load data - each table will create its own batch
                self.data_loader.load_data(audit_metadata)
                
                return audit_metadata
            else:
                logger.warning("No data to load!")
                return None

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            if self.config.etl.delete_files:
                remove_folder(extract_path)

    def _detect_csv_files(self, extract_path: Path) -> List[Path]:
        """
        Robustly detect CSV files in the extract directory.
        
        Uses multiple detection methods:
        1. File extensions (.csv, .txt)
        2. Content analysis (checks for CSV-like structure)
        3. Known table expressions as fallback
        
        Args:
            extract_path: Directory to search for CSV files
            
        Returns:
            List of detected CSV file paths
        """
        if not extract_path.exists():
            return []
        
        # Get all files in directory
        all_files = list(extract_path.glob("*"))
        candidate_files = [f for f in all_files if f.is_file()]
        
        if not candidate_files:
            return []
        
        csv_files = []
        
        for file_path in candidate_files:
            if self._is_csv_file(file_path):
                csv_files.append(file_path)
        
        return csv_files
    
    def _is_csv_file(self, file_path: Path) -> bool:
        """
        Determine if a file is a CSV file using multiple detection methods.
        
        Args:
            file_path: Path to the file to check
            
        Returns:
            True if file appears to be CSV format
        """
        # Method 1: Check file extension
        csv_extensions = {'.csv', '.txt', '.dat'}
        if file_path.suffix.lower() in csv_extensions:
            logger.debug(f"CSV detected by extension: {file_path.name}")
            return True
        
        # Method 2: Check filename for known patterns
        filename = file_path.name.upper()
        known_patterns = ['CSV', 'EMPRE', 'ESTABELE', 'SOCIO', 'SIMPLES', 'CNAE', 'MOTI', 'MUNIC', 'NATJU', 'PAIS', 'QUALS']
        if any(pattern in filename for pattern in known_patterns):
            logger.debug(f"CSV detected by pattern: {file_path.name}")
            return True
        
        # Method 3: Content analysis (check first few lines)
        try:
            with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
                # Read first few lines
                lines = []
                for i, line in enumerate(f):
                    lines.append(line.strip())
                    if i >= 4:  # Check first 5 lines
                        break
                
                if not lines:
                    return False
                
                # Check if lines contain semicolons (Brazilian CSV delimiter)
                semicolon_count = sum(line.count(';') for line in lines)
                comma_count = sum(line.count(',') for line in lines)
                
                # If semicolons are more common than commas, likely CSV
                if semicolon_count > comma_count and semicolon_count > 0:
                    logger.debug(f"CSV detected by content (semicolons): {file_path.name}")
                    return True
                
                # Check for typical CSV structure (multiple columns)
                first_line_fields = lines[0].split(';')
                if len(first_line_fields) > 5:  # At least 5 columns
                    logger.debug(f"CSV detected by structure: {file_path.name}")
                    return True
                
        except Exception as e:
            logger.debug(f"Could not analyze content of {file_path.name}: {e}")
            return False
        
        return False

