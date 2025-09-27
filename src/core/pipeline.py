from typing import List, Optional
from datetime import datetime
from pathlib import Path
from uuid import UUID

# Import new services
from ..setup.logging import logger
from ..setup.config import ConfigurationService
from ..database.models.audit import TableAuditManifest, AuditBase, AuditStatus
from ..database.models.business import MainBase
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
        self.loading_strategy = DataLoadingStrategy(self.config)

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
        from .services.loading.service import LoadingService

        self._data_loader = LoadingService(
            self.database,  
            self.loading_strategy, 
            self.config,
            audit_service=self.audit_service
        )
        return self._data_loader

    @property
    def discovery_service(self):
        """Lazy discovery service initialization for Federal Revenue data periods."""
        from .services.discovery.service import FederalRevenueDiscoveryService

        self._discovery_service = FederalRevenueDiscoveryService(self.config)
        return self._discovery_service

    def _init_databases(self) -> None:
        """Initialize databases only when first accessed."""
        if self._initialized:
            return

        # Initialize main database for ETL tables
        from ..setup.base import init_database

        logger.info(
            f"Initializing main database: {self.config.pipeline.database.database_name}"
        )
        self._database = init_database(self.config.pipeline.database, MainBase)

        logger.info(
            f"Creating tables in main database: {self.config.pipeline.database.database_name}"
        )
        self._database.create_tables()

        # Initialize audit database for audit records
        logger.info(
            f"Initializing audit database: {self.config.audit.database.database_name}"
        )
        audit_db = init_database(self.config.audit.database, AuditBase)

        logger.info(
            f"Creating tables in audit database: {self.config.audit.database.database_name}"
        )
        audit_db.create_tables()
        self._audit_service = AuditService(audit_db, self.config)
        
        # Connect the audit service to the loading strategy
        self.loading_strategy.audit_service = self._audit_service
        
        self._initialized = True

    def _generate_batch_name(self) -> str:
        """Generate a descriptive batch name."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"ETL_Pipeline_{self.config.pipeline.year}_{self.config.pipeline.month:02d}_{timestamp}"

    def _start_batch_tracking(self, target_table: Optional[str] = None) -> UUID:
        """Start batch tracking for the pipeline execution."""
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
        logger.info(f"Completing batch tracking: {self._batch_name} (success={success})")
        
        # Complete the batch manually using the correct method name
        from ..database.models.audit import AuditStatus
        status = AuditStatus.COMPLETED if success else AuditStatus.FAILED
        self.audit_service._complete_batch_with_accumulated_metrics(self._current_batch_id, status)
        self._current_batch_id = None

    @property
    def current_batch_id(self) -> Optional[UUID]:
        """Get the current batch ID for this pipeline execution."""
        return self._current_batch_id

    def scrap_data(self) -> List[FileInfo]:
        """
        Scrape data files for the configured year and month using discovery service.
        
        This method now uses the integrated discovery service instead of 
        duplicating the scraping logic.
        
        Returns:
            List of FileInfo objects for available files
        """
        try:
            # Use discovery service to scrape files for the configured period
            period_files = self.discovery_service.scrape_period_files(
                year=self.config.pipeline.year,
                month=self.config.pipeline.month
            )
            
            # Convert PeriodFileInfo to FileInfo for compatibility
            files_info = []
            for period_file in period_files:
                # Derive tablename from filename using the mapping
                tablename = self._derive_tablename_from_filename(period_file.filename)
                
                file_info = FileInfo(
                    filename=period_file.filename,
                    tablename=tablename,
                    path=period_file.download_url,
                    updated_at=period_file.updated_at,
                    file_size=period_file.file_size
                )
                files_info.append(file_info)
            
            logger.info(f"Discovery service found {len(files_info)} files for {self.config.pipeline.year}-{self.config.pipeline.month:02d}")
            return files_info
            
        except ValueError as e:
            logger.error(f"Period not available: {e}")
            return []
        except Exception as e:
            logger.error(f"Failed to scrape data using discovery service: {e}")
            return []

    def _derive_tablename_from_filename(self, filename: str) -> str:
        """
        Derive the table name from a ZIP filename based on the TABLES_INFO_DICT mapping.
        
        Args:
            filename: The ZIP filename (e.g., "Cnaes.zip", "Empresas1.zip")
            
        Returns:
            The corresponding table name (e.g., "cnae", "empresa")
        """
        from .constants import TABLES_INFO_DICT
        
        # Remove .zip extension and convert to lowercase for matching
        base_name = filename.replace('.zip', '').lower()
        
        # Find the best match based on the label patterns
        for table_name, table_info in TABLES_INFO_DICT.items():
            label = table_info["label"].lower()
            expression = table_info["expression"].lower()
            
            # Check if the filename contains the label or expression
            if label in base_name or expression in base_name:
                return table_name

        # If no match found, return a default or log warning
        logger.warning(f"Could not derive tablename from filename: {filename}")
        return "unknown"

    def fetch_audit_data(self) -> List[TableAuditManifest]:
        files_info = self.scrap_data()

        # Apply development mode filtering to files before creating audits
        logger.info(f"Development mode enabled: {self.config.is_development_mode()}")
        if self.config.is_development_mode():
            from .utils.development_filter import DevelopmentFilter
            dev_filter = DevelopmentFilter(self.config)
            original_count = len(files_info)
            
            # Filter files by size
            size_filtered_files = [f for f in files_info if dev_filter._check_file_size_limit(f)]
            logger.info(f"[DEV-MODE] Size filtering: {len(files_info)} → {len(size_filtered_files)} files (limit: {dev_filter.development.file_size_limit_mb}MB)")
            files_info = size_filtered_files
            
            # Log sample filenames for debugging
            if files_info:
                sample_names = [f.filename for f in files_info[:5]]
                logger.debug(f"[DEV-MODE] Sample filenames after size filtering: {sample_names}")
            else:
                logger.warning("[DEV-MODE] No files passed size filtering!")
            
            # Apply whole-blob filtering - limit total files across all tables
            max_files_total = dev_filter.development.max_files_per_blob  # This is the total blob limit
            
            if len(files_info) <= max_files_total:
                # No need to filter - we're under the total limit
                logger.info(f"[DEV-MODE] Blob under limit: {len(files_info)} files ≤ {max_files_total} limit")
                filtered_files = files_info
            else:
                # Need to strategically select files from the entire blob
                logger.info(f"[DEV-MODE] Blob filtering: {len(files_info)} files → {max_files_total} files (whole-blob limit)")
                
                # Group files by table for diversity, but don't limit per table
                from collections import defaultdict
                from .constants import TABLES_INFO_DICT
                
                table_groups = defaultdict(list)
                unmatched_files = []
                
                for file_info in files_info:
                    # Find which table this file belongs to
                    table_name = None
                    filename_upper = file_info.filename.upper()  # Make case-insensitive
                    
                    for tname, table_info in TABLES_INFO_DICT.items():
                        expression = table_info.get("expression")
                        if expression and expression.upper() in filename_upper:
                            table_name = tname
                            break
                    
                    if table_name:
                        table_groups[table_name].append(file_info)
                    else:
                        unmatched_files.append(file_info)
                
                # Strategic selection for table diversity within total blob limit
                filtered_files = []
                remaining_slots = max_files_total
                
                # First pass: Take 1-2 files from each table to ensure diversity
                tables_with_files = [(name, files) for name, files in table_groups.items() if files]
                files_per_table_round1 = max(1, remaining_slots // max(1, len(tables_with_files))) if tables_with_files else 0
                
                for table_name, table_files in tables_with_files:
                    # Take first few files from this table (strategic: first, middle if available)
                    take_count = min(files_per_table_round1, len(table_files), remaining_slots)
                    if take_count > 0:
                        if take_count == 1:
                            selected = [table_files[0]]
                        elif take_count == 2 and len(table_files) >= 2:
                            selected = [table_files[0], table_files[len(table_files)//2]]
                        else:
                            selected = dev_filter._select_representative_files(table_files, take_count)
                        
                        filtered_files.extend(selected)
                        remaining_slots -= len(selected)
                        logger.debug(f"[DEV-MODE] {table_name}: selected {len(selected)} files (round 1)")
                
                # Second pass: Fill remaining slots with any remaining files
                if remaining_slots > 0:
                    all_remaining_files = []
                    for table_files in table_groups.values():
                        for f in table_files:
                            if f not in filtered_files:
                                all_remaining_files.append(f)
                    
                    # Add unmatched files to remaining pool
                    all_remaining_files.extend(unmatched_files)
                    
                    if all_remaining_files and remaining_slots > 0:
                        additional = all_remaining_files[:remaining_slots]
                        filtered_files.extend(additional)
                        logger.debug(f"[DEV-MODE] Added {len(additional)} additional files (round 2)")
                
                logger.info(f"[DEV-MODE] Final selection: {len(filtered_files)} files from {len(table_groups)} tables")
            
            files_info = filtered_files
            dev_filter.log_simple_filtering(original_count, len(files_info), "discovered files")

        audits = self.audit_service.create_audits_from_files(files_info)
        logger.info(f"Created {len(audits)} audit entries from {len(files_info)} discovered files")

        return audits

    def download_and_extract(self, audits) -> None:
        # Get the period URL for downloading files
        period = self.discovery_service.find_period(
            year=self.config.pipeline.year,
            month=self.config.pipeline.month
        )
        
        if not period:
            raise ValueError(f"Period {self.config.pipeline.year}-{self.config.pipeline.month:02d} not found")
        
        # Ensure temporal directories exist before downloading
        self.config.pipeline.ensure_temporal_directories_exist(self.config.year, self.config.month)
        
        # Use the period URL as the base URL for downloading
        self.file_downloader.download_and_extract(
            audits,
            period.url,
            str(self.config.pipeline.get_temporal_download_path(self.config.year, self.config.month)),
            str(self.config.pipeline.get_temporal_extraction_path(self.config.year, self.config.month)),
            parallel=self.config.pipeline.is_parallel,
        )

    def retrieve_data(self) -> List[TableAuditManifest]:
        audits = self.fetch_audit_data()

        if audits:
            self.download_and_extract(audits)
        else:
            logger.warning("No audits to retrieve.")

        return audits

    def convert_to_parquet(self, audit_metadata: AuditMetadata) -> Path:
        from ..utils.misc import makedir
        from .services.conversion.service import convert_csvs_to_parquet_smart

        num_workers = self.config.pipeline.conversion.workers
        output_dir = self.config.pipeline.get_temporal_conversion_path(self.config.year, self.config.month)

        makedir(output_dir)

        audit_map = {}
        for (
            table_name,
            zipfile_to_files,
        ) in audit_metadata.tablename_to_zipfile_to_files.items():
            if table_name not in audit_map:
                audit_map[table_name] = {}

            for zip_filename, csv_files in zipfile_to_files.items():
                # No filtering in conversion - process all CSV files from filtered ZIP blobs
                # The filtering was already done at the ZIP blob level during download
                audit_map[table_name][zip_filename] = csv_files

        logger.info("Converting CSV files to Parquet format...")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Processing {len(audit_map)} tables with {num_workers} workers")

        # Centralized conversion summary logging
        from .utils.development_filter import DevelopmentFilter
        dev_filter = DevelopmentFilter(self.config)
        dev_filter.log_conversion_summary(audit_map)

        extract_path = self.config.pipeline.get_temporal_extraction_path(self.config.year, self.config.month)

        convert_csvs_to_parquet_smart(audit_map, extract_path, output_dir)
        logger.info(f"Parquet conversion completed. Files saved to: {output_dir}")

        # IMPROVEMENT: Update table audits to reference Parquet files instead of ZIP files
        logger.info("Updating table audits to reference converted Parquet files...")
        for table_name in audit_map.keys():
            parquet_file_path = output_dir / f"{table_name}.parquet" 
            if parquet_file_path.exists():
                processing_metadata = {
                    "conversion_method": "convert_csvs_to_parquet_smart",
                    "output_format": "parquet",
                    "output_file_size_bytes": parquet_file_path.stat().st_size  # Moved from table to file level
                }
                self.audit_service.update_table_audit_after_conversion(
                    table_name, str(parquet_file_path), processing_metadata
                )
            else:
                logger.warning(f"Expected Parquet file not found: {parquet_file_path}")


        return output_dir

    def create_audit_metadata_from_existing_parquet(self) -> Optional[AuditMetadata]:
        """
        Create audit metadata from existing files (CSV or Parquet) in extraction/conversion directories.

        This reuses the existing audit metadata infrastructure but creates synthetic
        entries that point to existing files instead of downloaded/extracted ones.
        Priority: Parquet files (converted) > CSV files (extracted)

        Returns:
            AuditMetadata: Compatible audit metadata for existing files, or None if no files found
        """
        # Check conversion directory first (Parquet files) - higher priority
        conversion_path = self.config.pipeline.get_temporal_conversion_path(self.config.year, self.config.month)
        parquet_files = []
        if conversion_path.exists():
            parquet_files = list(conversion_path.glob("*.parquet"))
            logger.info(f"Found {len(parquet_files)} Parquet files in {conversion_path}")

        # Prioritize Parquet files over CSV files
        if parquet_files:
            logger.info(f"Using {len(parquet_files)} Parquet files for loading (preferred format)")
            return self._create_audit_metadata_from_parquet_files(parquet_files, conversion_path)
        else:
            logger.warning("No files found in conversion directory")
            return None

    def create_audit_metadata_from_existing_csvs(self) -> Optional[AuditMetadata]:
        """
        Create audit metadata from existing files (CSV or Parquet) in extraction/conversion directories.

        This reuses the existing audit metadata infrastructure but creates synthetic
        entries that point to existing files instead of downloaded/extracted ones.
        Priority: Parquet files (converted) > CSV files (extracted)

        Returns:
            AuditMetadata: Compatible audit metadata for existing files, or None if no files found
        """
        # Check extraction directory for CSV files - fallback
        extract_path = self.config.pipeline.get_temporal_extraction_path(self.config.year, self.config.month)
        csv_files = []
        if extract_path.exists():
            csv_files = self._detect_csv_files(extract_path)
            logger.info(f"Found {len(csv_files)} CSV files in {extract_path}")
        else:
            logger.warning(f"Extract path does not exist: {extract_path}")

        # Prioritize Parquet files over CSV files
        if csv_files:
            logger.info(f"Using {len(csv_files)} CSV files for loading (fallback format)")
            return self._create_audit_metadata_from_csv_files(csv_files, extract_path)
        else:
            logger.warning("No files found in conversion or extraction directories")
            return None

    def _create_audit_metadata_from_parquet_files(self, parquet_files: List[Path], base_path: Path) -> Optional[AuditMetadata]:
        """Create audit metadata for Parquet files."""
        from .constants import TABLES_INFO_DICT
        from .schemas import AuditMetadata
        from ..database.models.audit import TableAuditManifestSchema, AuditStatus
        from datetime import datetime

        # Development mode filtering
        if self.config.is_development_mode():
            from .utils.development_filter import DevelopmentFilter
            dev_filter = DevelopmentFilter(self.config)
            original_count = len(parquet_files)
            parquet_files = [f for f in parquet_files if dev_filter.check_blob_size_limit(f)]
            dev_filter.log_simple_filtering(original_count, len(parquet_files), "Parquet files")

        # Map Parquet files to table names (e.g., cnae.parquet -> cnae)
        tablename_to_files = {}
        for parquet_file in parquet_files:
            table_name = parquet_file.stem  # Remove .parquet extension
            if table_name in TABLES_INFO_DICT:
                # Create synthetic zip filename for consistency
                synthetic_zip = f"{table_name.title()}.zip"
                tablename_to_files[table_name] = {synthetic_zip: [parquet_file.name]}
                logger.debug(f"Mapped Parquet file: {parquet_file.name} -> table '{table_name}'")
            else:
                logger.debug(f"Skipping unknown table: {table_name} (from {parquet_file.name})")

        if not tablename_to_files:
            logger.warning("No valid table mappings found for Parquet files")
            return None

        # Determine ingestion temporal values
        data_year = self.config.year
        data_month = self.config.month

        # Create audit metadata entries
        audit_list = []
        for table_name, zip_to_files in tablename_to_files.items():
            for file_list in zip_to_files.values():
                audit_entry = TableAuditManifestSchema(
                    entity_name=table_name,
                    source_files=file_list,  # Store actual processed files, not ZIP file
                    status=AuditStatus.PENDING,
                    ingestion_year=data_year,
                    ingestion_month=data_month,
                    created_at=datetime.now(),
                    inserted_at=None  # Will be set during loading
                )
                audit_list.append(audit_entry)

        logger.info(f"Created audit metadata for {len(audit_list)} table(s) from Parquet files")
        return AuditMetadata(audit_list=audit_list, tablename_to_zipfile_to_files=tablename_to_files)

    def _create_audit_metadata_from_csv_files(self, csv_files: List[Path], base_path: Path) -> Optional[AuditMetadata]:
        """Create audit metadata for CSV files (original implementation)."""
        from .constants import TABLES_INFO_DICT
        from .schemas import AuditMetadata  
        from ..database.models.audit import TableAuditManifestSchema
        from datetime import datetime
        from uuid import uuid4

        # Development mode filtering
        if self.config.is_development_mode():
            from .utils.development_filter import DevelopmentFilter
            dev_filter = DevelopmentFilter(self.config)
            original_count = len(csv_files)
            csv_files = [f for f in csv_files if dev_filter.check_blob_size_limit(f)]
            dev_filter.log_simple_filtering(original_count, len(csv_files), "CSV files")

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
        
        # Use configured temporal values or current time
        data_year = self.config.year
        data_month = self.config.month
        
        # Create a minimal audit list for compatibility
        audit_list = []
        for table_name in tablename_to_files.keys():
            synthetic_audit = TableAuditManifestSchema(
                table_audit_id=str(uuid4()),  # Generate a proper UUID string
                entity_name=table_name,
                source_files=["synthetic_conversion"],  # Fake filename for compatibility
                status=AuditStatus.PENDING,
                ingestion_year=data_year,
                ingestion_month=data_month,
                notes={"convert_only": True}
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

    def load_csv_files_directly(self) -> Optional[AuditMetadata]:
        """
        Load CSV files directly from EXTRACTED_FILES without conversion.
        
        This method forces CSV file usage even if Parquet files exist,
        enabling true direct CSV loading for --download --load strategy.
        
        Returns:
            AuditMetadata: Audit metadata for loaded CSV files, or None if no CSV files found
        """
        from .constants import TABLES_INFO_DICT
        from .schemas import AuditMetadata
        from ..database.models.audit import TableAuditManifestSchema
        from datetime import datetime
        from uuid import uuid4

        # Use configured temporal values or current time
        data_year = self.config.year
        data_month = self.config.month
        
        # Only check extraction directory for CSV files - ignore conversion directory
        extract_path = self.config.pipeline.get_temporal_extraction_path(self.config.year, self.config.month)
        
        if not extract_path.exists():
            logger.warning(f"Extract path does not exist: {extract_path}")
            return None

        csv_files = self._detect_csv_files(extract_path)
        
        if not csv_files:
            logger.warning(f"No CSV files found in {extract_path}")
            return None

        logger.info(f"Found {len(csv_files)} CSV files for direct loading in {extract_path}")

        # Development mode filtering
        if self.config.is_development_mode():
            from .utils.development_filter import DevelopmentFilter
            dev_filter = DevelopmentFilter(self.config)
            original_count = len(csv_files)
            csv_files = [f for f in csv_files if dev_filter.check_blob_size_limit(f)]
            dev_filter.log_simple_filtering(original_count, len(csv_files), "CSV files (direct loading)")

        # Map CSV files to table names using expression patterns
        tablename_to_files = {}
        for table_name, table_info in TABLES_INFO_DICT.items():
            expression = table_info.get("expression")
            if not expression:
                continue

            # Find CSV files matching this table's expression
            matching_files = [str(f) for f in csv_files if expression in f.name]

            if matching_files:
                tablename_to_files[table_name] = {"direct_csv_load": matching_files}
                logger.info(f"Table '{table_name}': Found {len(matching_files)} CSV files for direct loading")

        if not tablename_to_files:
            logger.warning("No CSV files matched any known table patterns for direct loading")
            return None

        # Create audit metadata for CSV loading with complete file information
        audit_list = []
        for table_name, zip_to_files in tablename_to_files.items():
            for zip_filename, file_list in zip_to_files.items():
                # Calculate total file size from CSV files
                total_size_bytes = 0
                oldest_file_time = None
                
                for file_path_str in file_list:
                    file_path = Path(file_path_str)
                    if file_path.exists():
                        # Get file size
                        total_size_bytes += file_path.stat().st_size
                        
                        # Get file modification time (oldest as source update time)
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if oldest_file_time is None or file_mtime < oldest_file_time:
                            oldest_file_time = file_mtime
                
                # Use current time for created_at and downloaded_at (since we're loading existing files)
                current_time = datetime.now()
                
                audit_entry = TableAuditManifestSchema(
                    entity_name=table_name,
                    source_files=file_list,  # Store actual CSV files
                    status=AuditStatus.PENDING,
                    ingestion_year=self.config.year if self.config.year else data_year,  # Use configured year or default
                    ingestion_month=self.config.month if self.config.month else data_month,  # Use configured month or default
                    created_at=current_time,  # When audit entry was created
                    inserted_at=None  # Will be set during loading
                )
                audit_list.append(audit_entry)

        # Create metadata with CSV files
        audit_metadata = AuditMetadata(audit_list=audit_list, tablename_to_zipfile_to_files=tablename_to_files)
        
        # Insert table audits before loading (without creating file manifests to avoid duplicates)
        self.audit_service.insert_audits(audit_metadata, create_file_manifests=False)
        logger.info("[CSV-DIRECT] Table audits inserted successfully (file manifests will be created during processing)")

        # Load data directly using the data loader with force_csv=True
        self.data_loader.load_data(audit_metadata, force_csv=True)
        logger.info("[CSV-DIRECT] Successfully loaded CSV files directly to database")
        return audit_metadata

    def validate_config(self) -> bool:
        """Validate pipeline configuration."""
        try:
            return bool(
                self.config and 
                self.config.audit and 
                self.config.pipeline and 
                self.config.pipeline.data_sink
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
            self.config._year = int(year)
        if month:
            self.config._month = int(month)
        
        # Use temporal paths for the current processing period
        download_path = str(self.config.pipeline.get_temporal_download_path(self.config.year, self.config.month))
        extract_path = str(self.config.pipeline.get_temporal_extraction_path(self.config.year, self.config.month))
        
        # Ensure temporal directories exist
        self.config.pipeline.ensure_temporal_directories_exist(self.config.year, self.config.month)

        # No longer create umbrella batch - individual table batches will be created
        try:
            audits = self.retrieve_data()

            if audits:
                audit_metadata = self.audit_service.create_audit_metadata(audits, download_path)

                # Remove downloaded files
                if self.config.pipeline.delete_files:
                    remove_folder(download_path)

                # Convert to Parquet
                self.convert_to_parquet(audit_metadata)

                # Load data - each table will create its own batch
                self.data_loader.load_data(audit_metadata)
                
                return audit_metadata
            else:
                logger.warning("No data to load!")
                return None

        except (OSError, IOError, ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            if self.config.pipeline.delete_files:
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
                
        except (OSError, IOError, UnicodeDecodeError) as e:
            logger.debug(f"Could not analyze content of {file_path.name}: {e}")
            return False
        
        return False

