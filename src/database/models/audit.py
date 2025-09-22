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

class TableAuditManifestSchema(BaseModel):
    """Pydantic schema for TableAuditManifest model."""
    table_audit_id: Optional[uuid.UUID] = None
    entity_name: str
    status: AuditStatus
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    description: Optional[str] = None
    error_message: Optional[str] = None
    notes: Optional[Dict] = None
    metrics: Optional[Dict] = None
    source_files: Optional[List[str]] = None
    file_size_bytes: Optional[int] = None
    source_updated_at: Optional[datetime] = None
    ingestion_year: int
    ingestion_month: int

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
            updated_at=self.updated_at,
            description=self.description,
            error_message=self.error_message,
            notes=self.notes,
            metrics=self.metrics,
            source_files=self.source_files,
            file_size_bytes=self.file_size_bytes,
            source_updated_at=self.source_updated_at,
            ingestion_year=self.ingestion_year,
            ingestion_month=self.ingestion_month,
        )

# =============================================================================
# UNIFORM AUDIT MODELS - New Schema Implementation
# =============================================================================

class TableAuditManifest(AuditBase):
    """
    Uniform audit model for table-level processing metadata.
    Consistent naming: table_audit_manifest with table_audit_id primary key.
    """
    __tablename__ = "table_audit_manifest"

    # Standard primary key pattern: {entity}_audit_id
    table_audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Standard entity identification
    entity_name = Column(String(200), nullable=False)  # table_name
    
    # Standard status using unified enum
    status = Column(Enum(AuditStatus), nullable=False, default=AuditStatus.PENDING)
    
    # Standard timestamps
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    started_at = Column(TIMESTAMP, nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)
    updated_at = Column(TIMESTAMP, nullable=True, onupdate=datetime.now)
    
    # Standard error handling
    description = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Standard metadata storage
    notes = Column(JSON, nullable=True)
    metrics = Column(JSON, nullable=True)
    
    # Table-specific fields
    source_files = Column(JSON, nullable=True)
    file_size_bytes = Column(BigInteger, nullable=True)
    source_updated_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    ingestion_year = Column(Integer, nullable=False, default=datetime.now().year)
    ingestion_month = Column(Integer, nullable=False, default=datetime.now().month)
    
    # Standard index pattern
    __table_args__ = (
        Index("idx_table_audit_status", "status"),
        Index("idx_table_audit_created_at", "created_at"),
        Index("idx_table_audit_completed_at", "completed_at"),
        Index("idx_table_audit_entity_name", "entity_name"),
    )
    
    # Relationships using new naming convention
    file_audits = relationship("FileAuditManifest", back_populates="table_audit", cascade="all, delete-orphan")

    @property
    def is_precedence_met(self) -> bool:
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
    
    def __repr__(self):
        return (
            f"TableAuditManifest(table_audit_id={self.table_audit_id}, entity_name={self.entity_name}, "
            f"status={self.status.value}, ingestion_year={self.ingestion_year}, "
            f"created_at={self.created_at}, completed_at={self.completed_at})"
        )


class FileAuditManifest(AuditBase):
    """
    Uniform audit model for file-level processing metadata.
    Consistent naming: file_audit_manifest with file_audit_id primary key.
    """
    __tablename__ = "file_audit_manifest"

    # Standard primary key pattern: {entity}_audit_id
    file_audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Standard foreign key pattern: parent_{entity}_audit_id
    parent_table_audit_id = Column(UUID(as_uuid=True), ForeignKey('table_audit_manifest.table_audit_id'), nullable=False)
    
    # Standard entity identification
    entity_name = Column(String(200), nullable=False)  # file_name or file_path
    
    # Standard status using unified enum
    status = Column(Enum(AuditStatus), nullable=False, default=AuditStatus.PENDING)
    
    # Standard timestamps
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    started_at = Column(TIMESTAMP, nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)
    updated_at = Column(TIMESTAMP, nullable=True, onupdate=datetime.now)
    
    # Standard error handling
    description = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Standard metadata storage
    notes = Column(JSON, nullable=True)
    metrics = Column(JSON, nullable=True)
    
    # File-specific fields
    file_path = Column(Text, nullable=False)
    checksum = Column(Text, nullable=True)
    filesize = Column(BigInteger, nullable=True)
    rows_processed = Column(BigInteger, nullable=True)
    
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
    Uniform audit model for batch-level processing metadata.
    Consistent naming: batch_audit_manifest with batch_audit_id primary key.
    """
    __tablename__ = "batch_audit_manifest"

    # Standard primary key pattern: {entity}_audit_id
    batch_audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Standard foreign key pattern: parent_{entity}_audit_id
    parent_file_audit_id = Column(UUID(as_uuid=True), ForeignKey('file_audit_manifest.file_audit_id'), nullable=True)
    
    # Standard entity identification
    entity_name = Column(String(200), nullable=False)  # batch_name
    
    # Standard status using unified enum
    status = Column(Enum(AuditStatus), nullable=False, default=AuditStatus.PENDING)
    
    # Standard timestamps
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    started_at = Column(TIMESTAMP, nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)
    updated_at = Column(TIMESTAMP, nullable=True, onupdate=datetime.now)
    
    # Standard error handling
    description = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Standard metadata storage
    notes = Column(JSON, nullable=True)
    metrics = Column(JSON, nullable=True)
    
    # Batch-specific fields
    target_table = Column(String(100), nullable=False)  # Single table name
    
    # Standard index pattern
    __table_args__ = (
        Index("idx_batch_audit_status", "status"),
        Index("idx_batch_audit_created_at", "created_at"),
        Index("idx_batch_audit_completed_at", "completed_at"),
        Index("idx_batch_audit_entity_name", "entity_name"),
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
            f"BatchAuditManifest(batch_audit_id={self.batch_audit_id}, entity_name={self.entity_name}, "
            f"status={self.status.value}, target_table={self.target_table}, "
            f"started_at={self.started_at}, completed_at={self.completed_at})"
        )


class SubbatchAuditManifest(AuditBase):
    """
    Uniform audit model for subbatch-level processing metadata.
    Consistent naming: subbatch_audit_manifest with subbatch_audit_id primary key.
    """
    __tablename__ = "subbatch_audit_manifest"

    # Standard primary key pattern: {entity}_audit_id
    subbatch_audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    
    # Standard foreign key pattern: parent_{entity}_audit_id
    parent_batch_audit_id = Column(UUID(as_uuid=True), ForeignKey('batch_audit_manifest.batch_audit_id'), nullable=False)
    
    # Standard entity identification
    entity_name = Column(String(200), nullable=False)  # table_name or step_name
    
    # Standard status using unified enum
    status = Column(Enum(AuditStatus), nullable=False, default=AuditStatus.PENDING)
    
    # Standard timestamps
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    started_at = Column(TIMESTAMP, nullable=True)
    completed_at = Column(TIMESTAMP, nullable=True)
    updated_at = Column(TIMESTAMP, nullable=True, onupdate=datetime.now)
    
    # Standard error handling
    description = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Standard metadata storage
    notes = Column(JSON, nullable=True)
    metrics = Column(JSON, nullable=True)
    
    # Subbatch-specific fields
    table_name = Column(String(100), nullable=False)  # Processing table
    rows_processed = Column(BigInteger, nullable=True, default=0)
    
    # Standard index pattern
    __table_args__ = (
        Index("idx_subbatch_audit_status", "status"),
        Index("idx_subbatch_audit_created_at", "created_at"),
        Index("idx_subbatch_audit_completed_at", "completed_at"),
        Index("idx_subbatch_audit_entity_name", "entity_name"),
        Index("idx_subbatch_audit_parent_id", "parent_batch_audit_id"),
        Index("idx_subbatch_audit_table_name", "table_name"),
    )
    
    # Relationships using new naming convention
    batch_audit = relationship("BatchAuditManifest", back_populates="subbatch_audits", foreign_keys=[parent_batch_audit_id])

    def __repr__(self):
        return (
            f"SubbatchAuditManifest(subbatch_audit_id={self.subbatch_audit_id}, entity_name={self.entity_name}, "
            f"status={self.status.value}, table_name={self.table_name}, "
            f"rows_processed={self.rows_processed}, started_at={self.started_at})"
        )