"""
Enhanced audit service integrating manifest tracking from refactored loader.
"""
import hashlib
from typing import List, Optional
from pathlib import Path
from datetime import datetime

from ...setup.logging import logger
from ...database.schemas import Database
from .service import AuditService


class EnhancedAuditService(AuditService):
    """Enhanced audit service with manifest integration."""
    
    def __init__(self, database: Database, config):
        super().__init__(database)
        self.config = config
        self.manifest_enabled = config.etl.manifest_tracking
        logger.info(f"Enhanced audit service initialized (manifest: {self.manifest_enabled})")
    
    def create_file_manifest(self, file_path: str, status: str, checksum: Optional[str] = None, 
                           filesize: Optional[int] = None, rows: Optional[int] = None) -> None:
        """Create manifest entry for processed file."""
        if not self.manifest_enabled:
            logger.debug(f"Manifest tracking disabled, skipping {Path(file_path).name}")
            return
        
        try:
            file_path_obj = Path(file_path)
            
            # Calculate file metadata if not provided
            if file_path_obj.exists():
                if filesize is None:
                    filesize = file_path_obj.stat().st_size
                if checksum is None:
                    checksum = self._calculate_file_checksum(file_path_obj)
            
            # Ensure manifest table exists
            self._ensure_manifest_table()
            
            # Insert manifest entry
            self._insert_manifest_entry(
                file_path=str(file_path_obj),
                filename=file_path_obj.name,
                status=status,
                checksum=checksum,
                filesize=filesize,
                rows_processed=rows,
                processed_at=datetime.now()
            )
            
            logger.info(f"Manifest entry created for {file_path_obj.name} (status: {status})")
            
        except Exception as e:
            logger.error(f"Failed to create manifest entry for {file_path}: {e}")
    
    def _calculate_file_checksum(self, file_path: Path, algorithm: str = 'sha256') -> str:
        """Calculate file checksum for integrity verification."""
        try:
            hash_func = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                # Read in chunks to handle large files
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_func.update(chunk)
            return hash_func.hexdigest()
        except Exception as e:
            logger.error(f"Failed to calculate checksum for {file_path}: {e}")
            return None
    
    def _ensure_manifest_table(self) -> None:
        """Ensure manifest table exists."""
        try:
            from sqlalchemy import text
            
            # Create manifest table if it doesn't exist
            manifest_schema = '''
            CREATE TABLE IF NOT EXISTS ingestion_manifest (
                id SERIAL PRIMARY KEY,
                file_path TEXT NOT NULL,
                filename TEXT NOT NULL,
                status TEXT NOT NULL,
                checksum TEXT,
                filesize BIGINT,
                rows_processed INTEGER,
                processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(file_path, processed_at)
            );
            
            CREATE INDEX IF NOT EXISTS idx_manifest_filename ON ingestion_manifest(filename);
            CREATE INDEX IF NOT EXISTS idx_manifest_status ON ingestion_manifest(status);
            CREATE INDEX IF NOT EXISTS idx_manifest_processed_at ON ingestion_manifest(processed_at);
            '''
            
            with self.database.engine.begin() as conn:
                conn.execute(text(manifest_schema))
                
            logger.debug("Manifest table schema ensured")
            
        except Exception as e:
            logger.warning(f"Could not create manifest table: {e}")
    
    def _insert_manifest_entry(self, file_path: str, filename: str, status: str, 
                              checksum: Optional[str], filesize: Optional[int], 
                              rows_processed: Optional[int], processed_at: datetime) -> None:
        """Insert manifest entry into database."""
        try:
            from sqlalchemy import text
            
            insert_query = '''
            INSERT INTO ingestion_manifest 
            (file_path, filename, status, checksum, filesize, rows_processed, processed_at)
            VALUES (:file_path, :filename, :status, :checksum, :filesize, :rows_processed, :processed_at)
            ON CONFLICT (file_path, processed_at) DO UPDATE SET
                status = EXCLUDED.status,
                checksum = EXCLUDED.checksum,
                filesize = EXCLUDED.filesize,
                rows_processed = EXCLUDED.rows_processed
            '''
            
            with self.database.engine.begin() as conn:
                conn.execute(text(insert_query), {
                    'file_path': file_path,
                    'filename': filename,
                    'status': status,
                    'checksum': checksum,
                    'filesize': filesize,
                    'rows_processed': rows_processed,
                    'processed_at': processed_at
                })
                
        except Exception as e:
            logger.error(f"Failed to insert manifest entry: {e}")
    
    def get_file_manifest_history(self, filename: str) -> List[dict]:
        """Get manifest history for a specific file."""
        try:
            from sqlalchemy import text
            
            query = '''
            SELECT file_path, filename, status, checksum, filesize, rows_processed, processed_at
            FROM ingestion_manifest 
            WHERE filename = :filename 
            ORDER BY processed_at DESC
            '''
            
            with self.database.engine.connect() as conn:
                result = conn.execute(text(query), {'filename': filename})
                return [dict(row._mapping) for row in result]
                
        except Exception as e:
            logger.error(f"Failed to get manifest history for {filename}: {e}")
            return []
    
    def verify_file_integrity(self, file_path: Path) -> bool:
        """Verify file integrity against stored manifest."""
        if not self.manifest_enabled:
            return True  # Skip verification if manifest disabled
        
        try:
            # Get latest manifest entry for this file
            history = self.get_file_manifest_history(file_path.name)
            if not history:
                logger.warning(f"No manifest history found for {file_path.name}")
                return False
            
            latest_entry = history[0]
            stored_checksum = latest_entry.get('checksum')
            
            if not stored_checksum:
                logger.warning(f"No checksum stored for {file_path.name}")
                return False
            
            # Calculate current checksum
            current_checksum = self._calculate_file_checksum(file_path)
            
            if current_checksum == stored_checksum:
                logger.debug(f"File integrity verified for {file_path.name}")
                return True
            else:
                logger.error(f"File integrity check failed for {file_path.name}: checksums don't match")
                return False
                
        except Exception as e:
            logger.error(f"File integrity verification failed for {file_path}: {e}")
            return False
