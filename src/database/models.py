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
    Enum
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from typing import Optional, Generic, TypeVar, List, Any
from pydantic import BaseModel, Field
from datetime import datetime
from uuid import uuid4
from functools import reduce
from sqlalchemy.ext.declarative import declarative_base
import enum

# Separate bases for audit and main tables
AuditBase = declarative_base()
MainBase = declarative_base()

T = TypeVar("T", bound=BaseModel)

# Define unified status enum for all audit models
class AuditStatus(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING" 
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"

# Legacy enums - to be removed after migration
class BatchStatus(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING" 
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class SubbatchStatus(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED" 
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"


class TableIngestionManifestSchema(BaseModel, Generic[T]):
    table_manifest_id: str = Field(
        default_factory=uuid4, description="Unique identifier for the audit entry."
    )
    table_name: str = Field(
        ..., description="Table name associated with the audit entry."
    )
    source_files: Optional[List[str]] = Field(
        None, description="List of files associated to given table."
    )
    file_size_bytes: Optional[float] = Field(
        None, description="Total size of files respective to given table."
    )
    source_updated_at: Optional[datetime] = Field(
        None, description="Timestamp of the last source update."
    )
    created_at: Optional[datetime] = Field(
        None, description="Timestamp of the audit entry creation."
    )
    downloaded_at: Optional[datetime] = Field(
        None, description="Timestamp of the audit entry download."
    )
    processed_at: Optional[datetime] = Field(
        None, description="Timestamp of the audit entry processing."
    )
    inserted_at: Optional[datetime] = Field(
        ..., description="Timestamp of the audit entry insertion."
    )
    audit_metadata: Optional[dict[str, Any]] = Field(
        None, description="Metadata associated with the audit entry."
    )
    ingestion_year: int = Field(
        default_factory=lambda: datetime.now().year, description="Year of ingestion (e.g., 2024)"
    )
    ingestion_month: int = Field(
        default_factory=lambda: datetime.now().month, description="Month of ingestion (1-12)"
    )

    def to_audit_db(self) -> Any:
        """Convert TableIngestionManifestSchema to TableAuditManifest model."""
        return TableAuditManifest(
            table_audit_id=self.table_manifest_id,
            entity_name=self.table_name,
            source_files=self.source_files,
            file_size_bytes=self.file_size_bytes,
            source_updated_at=self.source_updated_at,
            created_at=self.created_at,
            downloaded_at=self.downloaded_at,
            processed_at=self.processed_at,
            inserted_at=self.inserted_at,
            audit_metadata=self.audit_metadata,
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
    audit_metadata = Column(JSON, nullable=True)  # Renamed to avoid SQLAlchemy conflict
    metrics = Column(JSON, nullable=True)  # Performance metrics (rows, bytes, duration)
    
    # Table-specific fields
    source_files = Column(JSON, nullable=True)
    file_size_bytes = Column(BigInteger, nullable=True)
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
    audit_metadata = Column(JSON, nullable=True)  # Renamed to avoid SQLAlchemy conflict
    metrics = Column(JSON, nullable=True)  # Performance metrics (rows, bytes, duration)
    
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
    audit_metadata = Column(JSON, nullable=True)  # Renamed to avoid SQLAlchemy conflict
    metrics = Column(JSON, nullable=True)  # Performance metrics (rows, bytes, duration)
    
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
    audit_metadata = Column(JSON, nullable=True)  # Renamed to avoid SQLAlchemy conflict
    metrics = Column(JSON, nullable=True)  # Performance metrics (rows, bytes, duration)
    
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


# =============================================================================
# MAIN CNPJ/SOURCE TABLES - Production Data Models
# =============================================================================

class Empresa(MainBase):
    __tablename__ = "empresa"
    cnpj_basico = Column(Text, nullable=False, primary_key=True)
    razao_social = Column(Text, nullable=True)
    natureza_juridica = Column(Text, nullable=True)
    qualificacao_responsavel = Column(Text, nullable=True)
    capital_social = Column(Text, nullable=True)
    porte_empresa = Column(Text, nullable=True)
    ente_federativo_responsavel = Column(Text, nullable=True)
    __table_args__ = (
        Index("empresa_cnpj_basico", "cnpj_basico"),
    )

    # Relationships
    estabelecimentos = relationship("Estabelecimento", back_populates="empresa")
    socios = relationship("Socios", back_populates="empresa")
    simples = relationship("SimplesNacional", back_populates="empresa")


class SimplesNacional(MainBase):
    __tablename__ = "simples"
    cnpj_basico = Column(Text, nullable=True, primary_key=True)
    opcao_pelo_simples = Column(Text, nullable=True)
    data_opcao_simples = Column(Text, nullable=True)
    data_exclusao_simples = Column(Text, nullable=True)
    opcao_mei = Column(Text, nullable=True)
    data_opcao_mei = Column(Text, nullable=True)
    data_exclusao_mei = Column(Text, nullable=True)
    __table_args__ = (
        Index("simples_cnpj_basico", "cnpj_basico"),
    )

    # Relationship to Empresa
    empresa = relationship("Empresa", back_populates="simples", primaryjoin="Empresa.cnpj_basico==SimplesNacional.cnpj_basico")


class Socios(MainBase):
    __tablename__ = "socios"
    cnpj_basico = Column(Text, nullable=False, primary_key=True)
    identificador_socio = Column(Text, nullable=True)
    nome_socio_razao_social = Column(Text, nullable=True)
    cpf_cnpj_socio = Column(Text, nullable=False, primary_key=True)
    qualificacao_socio = Column(Text, nullable=True)
    data_entrada_sociedade = Column(Text, nullable=True)
    pais = Column(Text, nullable=True)
    representante_legal = Column(Text, nullable=True)
    nome_do_representante = Column(Text, nullable=True)
    qualificacao_representante_legal = Column(Text, nullable=True)
    faixa_etaria = Column(Text, nullable=True)
    __table_args__ = (
        Index("socios_cnpj_basico", "cnpj_basico"),
    )

    # Relationship to Empresa
    empresa = relationship("Empresa", back_populates="socios", primaryjoin="Empresa.cnpj_basico==Socios.cnpj_basico")


class Estabelecimento(MainBase):
    __tablename__ = "estabelecimento"
    cnpj_basico = Column(Text, nullable=False, primary_key=True)
    cnpj_ordem = Column(Text, nullable=True, primary_key=True)
    cnpj_dv = Column(Text, nullable=True, primary_key=True)
    identificador_matriz_filial = Column(Text, nullable=True)
    nome_fantasia = Column(Text, nullable=True)
    situacao_cadastral = Column(Text, nullable=True)
    data_situacao_cadastral = Column(Text, nullable=True)
    motivo_situacao_cadastral = Column(Text, nullable=True)
    nome_cidade_exterior = Column(Text, nullable=True)
    pais = Column(Text, nullable=True)
    data_inicio_atividade = Column(Text, nullable=True)
    cnae_fiscal_principal = Column(Text, nullable=True)
    cnae_fiscal_secundaria = Column(Text, nullable=True)
    tipo_logradouro = Column(Text, nullable=True)
    logradouro = Column(Text, nullable=True)
    numero = Column(Text, nullable=True)
    complemento = Column(Text, nullable=True)
    bairro = Column(Text, nullable=True)
    cep = Column(Text, nullable=True)
    uf = Column(Text, nullable=True)
    municipio = Column(Text, nullable=True)
    ddd_1 = Column(Text, nullable=True)
    telefone_1 = Column(Text, nullable=True)
    ddd_2 = Column(Text, nullable=True)
    telefone_2 = Column(Text, nullable=True)
    ddd_fax = Column(Text, nullable=True)
    fax = Column(Text, nullable=True)
    correio_eletronico = Column(Text, nullable=True)
    situacao_especial = Column(Text, nullable=True)
    data_situacao_especial = Column(Text, nullable=True)
    __table_args__ = (
        Index("estabelecimento_cnpj_basico", "cnpj_basico"),
        Index("estabelecimento_cnpj_ordem", "cnpj_ordem"),
        Index("estabelecimento_cnpj_dv", "cnpj_dv"),
        Index("estabelecimento_cnae_principal", "cnae_fiscal_principal"),
        Index("estabelecimento_cnae_secundaria", "cnae_fiscal_secundaria"),
        Index("estabelecimento_cep", "cep"),
        Index("estabelecimento_uf", "uf"),
    )

    # Relationship to Empresa
    empresa = relationship("Empresa", back_populates="estabelecimentos", primaryjoin="Empresa.cnpj_basico==Estabelecimento.cnpj_basico")


class Qualificacoes(MainBase):
    __tablename__ = "quals"
    codigo = Column(Text, nullable=True, primary_key=True)
    descricao = Column(Text, nullable=True)


class MotivoCadastral(MainBase):
    __tablename__ = "moti"
    codigo = Column(Text, nullable=True, primary_key=True)
    descricao = Column(Text, nullable=True)


class NaturezaJuridica(MainBase):
    __tablename__ = "natju"
    codigo = Column(Text, nullable=True, primary_key=True)
    descricao = Column(Text, nullable=True)


class Municipio(MainBase):
    __tablename__ = "munic"
    codigo = Column(Text, nullable=True, primary_key=True)
    descricao = Column(Text, nullable=True)


class Cnae(MainBase):
    __tablename__ = "cnae"
    codigo = Column(Text, nullable=True, primary_key=True)
    descricao = Column(Text, nullable=True)


class Pais(MainBase):
    __tablename__ = "pais"
    codigo = Column(Text, nullable=True, primary_key=True)
    descricao = Column(Text, nullable=True)
