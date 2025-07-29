"""
Unified data loading service for the CNPJ ETL project.

This service uses the strategy pattern to load data into the database using the selected backend (CSV, Parquet, or Auto).
"""

from typing import Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime
from database.schemas import Database
from core.schemas import AuditMetadata
from core.loading.strategies import DataLoadingStrategy, AutoLoadingStrategy
from setup.logging import logger

class DataLoadingService:
    """Unified service for data loading operations."""
    
    def __init__(self, strategy: Optional[DataLoadingStrategy] = None):
        self.strategy = strategy or AutoLoadingStrategy()
    
    def load_data(
        self,
        database: Database,
        source_folder: str,
        audit_metadata: AuditMetadata,
        **kwargs
    ) -> AuditMetadata:
        """
        Load data for all tables using the configured strategy.
        Updates audit_metadata with insertion timestamps.
        
        Args:
            database: Database connection
            source_folder: Path to extracted files or Parquet directory
            audit_metadata: Audit metadata with table-to-files mapping
            **kwargs: Additional arguments for the strategy
        
        Returns:
            Updated AuditMetadata
        """
        table_to_files = audit_metadata.tablename_to_zipfile_to_files
        source_path = Path(source_folder)
        
        # Prepare mapping: table_name -> list of files (for CSV) or just table_name (for Parquet)
        results = self.strategy.load_multiple_tables(
            database=database,
            table_to_files=table_to_files,
            source_path=source_path,
            **kwargs
        )
        
        # Update audit_metadata with insertion timestamps
        now = datetime.now()
        for audit in audit_metadata.audit_list:
            result = results.get(audit.audi_table_name)
            if result and result[0]:  # success
                audit.audi_inserted_at = now
            else:
                logger.error(f"Failed to load table {audit.audi_table_name}: {result[1] if result else 'No result'}")
        
        return audit_metadata