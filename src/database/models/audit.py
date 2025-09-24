from sqlalchemy import (
    Column, 
    BigInteger, 
    String, 
    TIMESTAMP, 
    JSON, 
    Text,  
    Integer,
    Index, 
    ForeignKey,
    Enum,
) 
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from typing import Optional, TypeVar, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime
from uuid import uuid4
from functools import reduce
from sqlalchemy.ext.declarative import declarative_base
import enum
import uuid

# Separate bases for audit and main tables
AuditBase = declarative_base()

T = TypeVar("T", bound=BaseModel)

# Define unified status enum for all audit models
class AuditStatus(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING" 
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

# Base audit model with common fields to reduce redundancy
class BaseAuditModel(AuditBase):
    """
    Abstract base class for all audit models containing standard fields.
    This reduces redundancy across audit models.
    """
    __abstract__ = True
    
    # Standard entity identification (will be customized per level)
    entity_name = Column(String(200), nullable=False)
    
    # Standard status using unified enum
    status = Column(Enum(AuditStatus), nullable=False, default=AuditStatus.PENDING)
    
    # Optimized timestamps - removed updated_at as it's rarely used
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    started_at = Column(TIMESTAMP, nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)
    
    # Standard error handling
    description = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Standard metadata storage
    notes = Column(JSON, nullable=True)
    metrics = Column(JSON, nullable=True)

    @property
    def is_precedence_met(self) -> bool:
        """Validates timestamp precedence: created <= started <= completed"""
        previous_timestamps = [
            self.created_at,
            self.started_at,
            self.completed_at
        ]
        is_met = True
        and_map = lambda a, b: a and b
        for index, current_timestamp in enumerate(previous_timestamps):
            # Skip validation if current timestamp is None
            if current_timestamp is None:
                continue
                
            previous_t = previous_timestamps[0:index]
            if index > 0:
                # Only compare with non-None previous timestamps
                non_none_previous = [t for t in previous_t if t is not None]
                if non_none_previous:
                    greater_than_map = lambda a: a <= current_timestamp
                    this_is_met = reduce(and_map, map(greater_than_map, non_none_previous))
                    is_met = is_met and this_is_met
        return is_met

class TableAuditManifestSchema(BaseModel):
    """Pydantic schema for TableAuditManifest model."""
    table_audit_id: Optional[uuid.UUID] = None
    entity_name: str
    status: AuditStatus
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    description: Optional[str] = None
    error_message: Optional[str] = None
    notes: Optional[Dict] = None
    metrics: Optional[Dict] = None
    source_files: Optional[List[str]] = None
    ingestion_year: int
    ingestion_month: int
    source_updated_at: Optional[datetime] = None  # Temporary field for compatibility

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True
    
    def to_db_model(self) -> Any:
        """Convert TableAuditManifestSchema to TableAuditManifest model."""
        return TableAuditManifest(
            table_audit_id=self.table_audit_id,
            entity_name=self.entity_name,
            status=self.status,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            description=self.description,
            error_message=self.error_message,
            notes=self.notes,
            metrics=self.metrics,
            source_files=self.source_files,
            ingestion_year=self.ingestion_year,
            ingestion_month=self.ingestion_month,
        )

# =============================================================================
# UNIFORM AUDIT MODELS - New Schema Implementation
# =============================================================================

class TableAuditManifest(BaseAuditModel):
    """
    Uniform audit model for table-level processing metadata.
    Consistent naming: table_audit_manifest with table_audit_id primary key.
    """
    __tablename__ = "table_audit_manifest"

    # Standard primary key pattern: {entity}_audit_id
    table_audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Table-specific fields
    source_files = Column(JSON, nullable=True)  # List of source files processed
    ingestion_year = Column(Integer, nullable=False, default=datetime.now().year)
    ingestion_month = Column(Integer, nullable=False, default=datetime.now().month)
    
    # Temporary field for database compatibility - TODO: remove after schema migration
    source_updated_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    # Standard index pattern
    __table_args__ = (
        Index("idx_table_audit_status", "status"),
        Index("idx_table_audit_created_at", "created_at"),
        Index("idx_table_audit_completed_at", "completed_at"),
        Index("idx_table_audit_entity_name", "entity_name"),
    )
    
    # Relationships using new naming convention
    file_audits = relationship("FileAuditManifest", back_populates="table_audit", cascade="all, delete-orphan")
    
    def __repr__(self):
        return (
            f"TableAuditManifest(table_audit_id={self.table_audit_id}, entity_name={self.entity_name}, "
            f"status={self.status.value}, ingestion_year={self.ingestion_year}, "
            f"created_at={self.created_at}, completed_at={self.completed_at})"
        )


class FileAuditManifest(BaseAuditModel):
    """
    Uniform audit model for file-level processing metadata.
    Consistent naming: file_audit_manifest with file_audit_id primary key.
    """
    __tablename__ = "file_audit_manifest"

    # Standard primary key pattern: {entity}_audit_id
    file_audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Standard foreign key pattern: parent_{entity}_audit_id
    parent_table_audit_id = Column(UUID(as_uuid=True), ForeignKey('table_audit_manifest.table_audit_id'), nullable=False)
    
    # File-specific fields
    file_path = Column(Text, nullable=False)
    checksum = Column(Text, nullable=True)  # File integrity verification
    filesize = Column(BigInteger, nullable=True)  # File size in bytes
    
    # Standard index pattern
    __table_args__ = (
        Index("idx_file_audit_status", "status"),
        Index("idx_file_audit_created_at", "created_at"),
        Index("idx_file_audit_completed_at", "completed_at"),
        Index("idx_file_audit_entity_name", "entity_name"),
        Index("idx_file_audit_parent_id", "parent_table_audit_id"),
        Index("idx_file_audit_file_path", "file_path"),
    )
    
    # Relationships using new naming convention
    table_audit = relationship("TableAuditManifest", back_populates="file_audits", foreign_keys=[parent_table_audit_id])
    batch_audits = relationship("BatchAuditManifest", back_populates="file_audit", cascade="all, delete-orphan")

    def __repr__(self):
        return (
            f"FileAuditManifest(file_audit_id={self.file_audit_id}, entity_name={self.entity_name}, "
            f"status={self.status.value}, file_path={self.file_path}, "
            f"filesize={self.filesize}, rows_processed={self.rows_processed})"
        )


class BatchAuditManifest(AuditBase):
    """
    Optimized audit model for batch-level processing metadata.
    Removed entity_name redundancy - use target_table instead.
    """
    __tablename__ = "batch_audit_manifest"

    # Primary key
    batch_audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Foreign key
    parent_file_audit_id = Column(UUID(as_uuid=True), ForeignKey('file_audit_manifest.file_audit_id'), nullable=True)
    
    # Batch identification - no redundant entity_name
    target_table = Column(String(100), nullable=False)  # Table being processed
    
    # Standard status and timestamps (from base model but without entity_name)
    status = Column(Enum(AuditStatus), nullable=False, default=AuditStatus.PENDING)
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    started_at = Column(TIMESTAMP, nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)
    
    # Standard error handling and metadata
    description = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    notes = Column(JSON, nullable=True)
    metrics = Column(JSON, nullable=True)
    
    # Optimized index pattern
    __table_args__ = (
        Index("idx_batch_audit_status", "status"),
        Index("idx_batch_audit_created_at", "created_at"),
        Index("idx_batch_audit_completed_at", "completed_at"),
        Index("idx_batch_audit_parent_id", "parent_file_audit_id"),
        Index("idx_batch_audit_target_table", "target_table"),
    )
    
    # Relationships using new naming convention
    file_audit = relationship("FileAuditManifest", back_populates="batch_audits", foreign_keys=[parent_file_audit_id])
    subbatch_audits = relationship(
        "SubbatchAuditManifest",
        back_populates="batch_audit",
        cascade="all, delete-orphan",
        foreign_keys="SubbatchAuditManifest.parent_batch_audit_id"
    )

    def __repr__(self):
        return (
            f"BatchAuditManifest(batch_audit_id={self.batch_audit_id}, "
            f"status={self.status.value}, target_table={self.target_table}, "
            f"started_at={self.started_at}, completed_at={self.completed_at})"
        )


class SubbatchAuditManifest(AuditBase):
    """
    Optimized audit model for subbatch-level processing metadata.
    Replaced entity_name with more descriptive processing_step.
    """
    __tablename__ = "subbatch_audit_manifest"

    # Primary key
    subbatch_audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Foreign key
    parent_batch_audit_id = Column(UUID(as_uuid=True), ForeignKey('batch_audit_manifest.batch_audit_id'), nullable=False)
    
    # Subbatch identification - more descriptive than generic entity_name
    processing_step = Column(String(200), nullable=False)  # Processing step description
    table_name = Column(String(100), nullable=False)  # Table being processed
    rows_processed = Column(BigInteger, nullable=True, default=0)  # Rows processed in this step
    
    # Standard status and timestamps (from base model pattern)
    status = Column(Enum(AuditStatus), nullable=False, default=AuditStatus.PENDING)
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    started_at = Column(TIMESTAMP, nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)
    
    # Standard error handling and metadata
    description = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    notes = Column(JSON, nullable=True)
    metrics = Column(JSON, nullable=True)
    
    # Optimized index pattern
    __table_args__ = (
        Index("idx_subbatch_audit_status", "status"),
        Index("idx_subbatch_audit_created_at", "created_at"),
        Index("idx_subbatch_audit_completed_at", "completed_at"),
        Index("idx_subbatch_audit_processing_step", "processing_step"),
        Index("idx_subbatch_audit_parent_id", "parent_batch_audit_id"),
        Index("idx_subbatch_audit_table_name", "table_name"),
    )
    
    # Relationships using new naming convention
    batch_audit = relationship("BatchAuditManifest", back_populates="subbatch_audits", foreign_keys=[parent_batch_audit_id])

    def __repr__(self):
        return (
            f"SubbatchAuditManifest(subbatch_audit_id={self.subbatch_audit_id}, "
            f"processing_step={self.processing_step}, status={self.status.value}, "
            f"table_name={self.table_name}, rows_processed={self.rows_processed})"
        )