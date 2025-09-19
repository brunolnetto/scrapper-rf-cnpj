from typing import NamedTuple, List, Dict, Tuple, Callable, Optional
from datetime import datetime
from pydantic import BaseModel
from uuid import UUID
import enum

class AuditStatusSchema(str, enum.Enum):
    """Pydantic schema enum for unified audit status."""
    PENDING = "PENDING"
    RUNNING = "RUNNING" 
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"

class TableAuditManifestSchema(BaseModel):
    """Pydantic schema for TableAuditManifest model."""
    table_audit_id: Optional[UUID] = None
    entity_name: str
    status: AuditStatusSchema
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    description: Optional[str] = None
    error_message: Optional[str] = None
    audit_metadata: Optional[Dict] = None
    metrics: Optional[Dict] = None
    source_files: Optional[List[str]] = None
    file_size_bytes: Optional[int] = None
    source_updated_at: Optional[datetime] = None
    ingestion_year: int
    ingestion_month: int

    class Config:
        from_attributes = True

class FileInfo(BaseModel):
    """Pydantic model representing a CNPJ file."""
    filename: str
    tablename: str
    path: str

class AuditMetadata(BaseModel):
    """Represents the metadata for auditing purposes."""
    audit_list: List[TableAuditManifestSchema]
    tablename_to_zipfile_to_files: Dict[str, Dict[str, List[str]]]

class TableInfo(NamedTuple):
    """NamedTuple representing table information."""
    table_name: str
    is_simples: bool

class FileGroupInfo(BaseModel):
    """Pydantic model representing grouped file information."""
    table_name: str
    is_simples: bool
    files: List[FileInfo]
    year: int
    month: int

    @property
    def total_size_bytes(self) -> int:
        return sum(len(file.filename.encode('utf-8')) for file in self.files)

    def __repr__(self) -> str:
        return f"FileGroupInfo(table_name='{self.table_name}', files_count={len(self.files)}, year={self.year}, month={self.month})"
