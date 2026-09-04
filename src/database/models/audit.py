from sqlalchemy import (
    Column, 
    TIMESTAMP, 
    JSON, 
    Text,  
    Integer,
    Index, 
    Enum,
) 
from sqlalchemy.dialects.postgresql import UUID
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime
from uuid import uuid4
from functools import reduce
from sqlalchemy.orm import declarative_base
import enum
import uuid

# Separate bases for audit and main tables
AuditBase = declarative_base()

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
    source_files: Optional[List[str]] = None
    ingestion_year: int
    ingestion_month: int

    model_config = {
        "from_attributes": True,
        "arbitrary_types_allowed": True
    }
    
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
        
    # Standard index pattern
    __table_args__ = (
        Index("idx_table_audit_status", "status"),
        Index("idx_table_audit_created_at", "created_at"),
        Index("idx_table_audit_completed_at", "completed_at"),
        Index("idx_table_audit_entity_name", "entity_name"),
    )

    def __repr__(self):
        return (
            f"TableAuditManifest(table_audit_id={self.table_audit_id}, entity_name={self.entity_name}, "
            f"status={self.status.value}, ingestion_year={self.ingestion_year}, "
            f"created_at={self.created_at}, completed_at={self.completed_at})"
        )