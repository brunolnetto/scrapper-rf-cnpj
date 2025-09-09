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

# Define status enums for batch tracking
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


class AuditDBSchema(BaseModel, Generic[T]):
    audi_id: str = Field(
        default_factory=uuid4, description="Unique identifier for the audit entry."
    )
    audi_table_name: str = Field(
        ..., description="Table name associated with the audit entry."
    )
    audi_filenames: Optional[List[str]] = Field(
        None, description="List of files associated to given table."
    )
    audi_file_size_bytes: Optional[float] = Field(
        None, description="Total size of files respective to given table."
    )
    audi_source_updated_at: Optional[datetime] = Field(
        None, description="Timestamp of the last source update."
    )
    audi_created_at: Optional[datetime] = Field(
        None, description="Timestamp of the audit entry creation."
    )
    audi_downloaded_at: Optional[datetime] = Field(
        None, description="Timestamp of the audit entry download."
    )
    audi_processed_at: Optional[datetime] = Field(
        None, description="Timestamp of the audit entry processing."
    )
    audi_inserted_at: Optional[datetime] = Field(
        ..., description="Timestamp of the audit entry insertion."
    )
    audi_metadata: Optional[dict[str, Any]] = Field(
        None, description="Metadata associated with the audit entry."
    )
    audi_ingestion_year: int = Field(
        default_factory=lambda: datetime.now().year, description="Year of ingestion (e.g., 2024)"
    )
    audi_ingestion_month: int = Field(
        default_factory=lambda: datetime.now().month, description="Month of ingestion (1-12)"
    )

    def to_audit_db(self) -> Any:
        """Convert AuditDBSchema to AuditDB model."""
        return AuditDB(
            audi_id=self.audi_id,
            audi_table_name=self.audi_table_name,
            audi_filenames=self.audi_filenames,
            audi_file_size_bytes=self.audi_file_size_bytes,
            audi_source_updated_at=self.audi_source_updated_at,
            audi_created_at=self.audi_created_at,
            audi_downloaded_at=self.audi_downloaded_at,
            audi_processed_at=self.audi_processed_at,
            audi_inserted_at=self.audi_inserted_at,
            audi_metadata=self.audi_metadata,
            audi_ingestion_year=self.audi_ingestion_year,
            audi_ingestion_month=self.audi_ingestion_month,
        )



class AuditDB(AuditBase):
    """
    SQLAlchemy model for the audit table.
    """
    __tablename__ = "table_ingestion_manifest"

    audi_id = Column(UUID(as_uuid=True), primary_key=True)
    audi_table_name = Column(String(255), nullable=False)
    audi_filenames = Column(JSON, nullable=False)
    audi_file_size_bytes = Column(BigInteger, nullable=True)
    audi_source_updated_at = Column(TIMESTAMP, nullable=True)
    audi_created_at = Column(TIMESTAMP, nullable=True)
    audi_downloaded_at = Column(TIMESTAMP, nullable=True)
    audi_processed_at = Column(TIMESTAMP, nullable=True)
    audi_inserted_at = Column(TIMESTAMP, nullable=True)
    audi_metadata = Column(JSON, nullable=True)
    audi_ingestion_year = Column(Integer, nullable=False, default=datetime.now().year)
    audi_ingestion_month = Column(Integer, nullable=False, default=datetime.now().month)
    
    # Relationship to manifest entries (one audit can have many manifest records)
    manifests = relationship(
            "AuditManifest",
            back_populates="audit",
            cascade="all, delete-orphan"
        )

    @property
    def is_precedence_met(self) -> bool:
        previous_timestamps = [
            self.audi_created_at,
            self.audi_downloaded_at,
            self.audi_processed_at,
            self.audi_inserted_at,
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

    def __get_pydantic_core_schema__(self):
        return AuditDBSchema

    def __repr__(self):
        source_updated_at = f"audi_source_updated_at={self.audi_source_updated_at}"
        created_at = f"audi_created_at={self.audi_created_at}"
        downloaded_at = f"audi_downloaded_at={self.audi_downloaded_at}"
        processed_at = f"audi_processed_at={self.audi_processed_at}"
        inserted_at = f"audi_inserted_at={self.audi_inserted_at}"
        timestamps = f"{source_updated_at}, {created_at}, {downloaded_at}, {processed_at}, {inserted_at}"
        table_name = f"audi_table_name={self.audi_table_name}"
        file_size = f"audi_file_size_bytes={self.audi_file_size_bytes}"
        filenames = f"audi_filenames={self.audi_filenames}"
        temporal = f"ingestion_year={self.audi_ingestion_year}, ingestion_month={self.audi_ingestion_month}"
        file_info = f"{table_name}, {filenames}, {file_size}, {temporal}"
        args = f"audi_id={self.audi_id}, {file_info}, {timestamps}"
        return f"AuditDB({args})"


# Manifest table for loader ingestion events
class AuditManifest(AuditBase):
    """
    SQLAlchemy model for the ingestion manifest table.
    Centralizes file-level ingestion metadata for loader/audit integration.
    """
    __tablename__ = "file_ingestion_manifest"

    file_manifest_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    audit_id = Column(UUID(as_uuid=True), ForeignKey('table_ingestion_manifest.audi_id'), nullable=False)  # FIXED: Required audit_id
    batch_id = Column(UUID(as_uuid=True), ForeignKey('batch_ingestion_manifest.batch_id'), nullable=True)
    subbatch_id = Column(UUID(as_uuid=True), ForeignKey('subbatch_ingestion_manifest.subbatch_manifest_id'), nullable=True)
    table_name = Column(String(100), nullable=True)
    file_path = Column(Text, nullable=False)
    status = Column(String(64), nullable=False)
    checksum = Column(Text, nullable=True)
    filesize = Column(BigInteger, nullable=True)
    rows_processed = Column(BigInteger, nullable=True)
    processed_at = Column(TIMESTAMP, nullable=True)
    notes = Column(Text, nullable=True)
    __table_args__ = (
        Index("idx_manifest_status", "status"),
        Index("idx_manifest_processed_at", "processed_at"),
        Index("idx_manifest_table_name", "table_name"),
        Index("idx_manifest_file_path", "file_path"),
        Index("idx_manifest_batch_id", "batch_id"),
        Index("idx_manifest_subbatch_id", "subbatch_id"),
    )

    # Foreign key relationships with explicit foreign_keys to avoid ambiguity
    audit = relationship("AuditDB", back_populates="manifests")
    batch = relationship("BatchIngestionManifest", back_populates="file_manifests", foreign_keys=[batch_id])
    subbatch = relationship("SubbatchIngestionManifest", back_populates="file_manifests", foreign_keys=[subbatch_id])

    def __get_pydantic_core_schema__(self):
        from ..core.schemas import AuditManifestSchema
        return AuditManifestSchema

    def __repr__(self):
        return (
            f"AuditManifest(manifest_id={self.manifest_id}, file_path={self.file_path}, "
            f"status={self.status}, checksum={self.checksum}, filesize={self.filesize}, "
            f"rows={self.rows_processed}, processed_at={self.processed_at}, table_name={self.table_name})"
        )


# Batch tracking models for hierarchical ETL observability
class BatchIngestionManifest(AuditBase):
    """
    SQLAlchemy model for batch-level tracking in ETL processes.
    Tracks high-level batch execution for a single target table.
    """
    __tablename__ = "batch_ingestion_manifest"

    batch_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    batch_name = Column(String(200), nullable=False)
    target_table = Column(String(100), nullable=False)  # Single table name
    file_manifest_id = Column(UUID(as_uuid=True), ForeignKey('file_ingestion_manifest.file_manifest_id'), nullable=True)  # ADDED: Reference to primary file manifest
    status = Column(Enum(BatchStatus), nullable=False, default=BatchStatus.PENDING)
    started_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    completed_at = Column(TIMESTAMP, nullable=True)
    description = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    
    __table_args__ = (
        Index("idx_batch_status", "status"),
        Index("idx_batch_target_table", "target_table"),
        Index("idx_batch_started_at", "started_at"),
        Index("idx_batch_completed_at", "completed_at"),
        Index("idx_batch_primary_file_manifest", "file_manifest_id"),  # ADDED: Index for file manifest reference
    )

    # Relationships with explicit foreign_keys to avoid ambiguity
    subbatches = relationship("SubbatchIngestionManifest", back_populates="batch", cascade="all, delete-orphan")
    file_manifests = relationship("AuditManifest", back_populates="batch", cascade="all, delete-orphan", foreign_keys="AuditManifest.batch_id")
    primary_file_manifest = relationship("AuditManifest", foreign_keys=[file_manifest_id], post_update=True)  # ADDED: Reference to primary file manifest

    def __repr__(self):
        return (
            f"BatchIngestionManifest(batch_id={self.batch_id}, batch_name={self.batch_name}, "
            f"target_table={self.target_table}, status={self.status.value}, "
            f"started_at={self.started_at}, completed_at={self.completed_at})"
        )


class SubbatchIngestionManifest(AuditBase):
    """
    SQLAlchemy model for subbatch-level tracking in ETL processes.
    Tracks individual processing steps within a batch.
    """
    __tablename__ = "subbatch_ingestion_manifest"

    subbatch_manifest_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    batch_manifest_id = Column(UUID(as_uuid=True), ForeignKey('batch_ingestion_manifest.batch_id'), nullable=False)
    table_name = Column(String(100), nullable=False)
    status = Column(Enum(SubbatchStatus), nullable=False, default=SubbatchStatus.PENDING)
    started_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    completed_at = Column(TIMESTAMP, nullable=True)
    files_processed = Column(Integer, nullable=True, default=0)
    rows_processed = Column(BigInteger, nullable=True, default=0)
    description = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
    
    __table_args__ = (
        Index("idx_subbatch_batch_manifest_id", "batch_manifest_id"),
        Index("idx_subbatch_table_name", "table_name"),
        Index("idx_subbatch_status", "status"),
        Index("idx_subbatch_started_at", "started_at"),
        Index("idx_subbatch_completed_at", "completed_at"),
    )

    # Relationship to parent batch
    batch = relationship("BatchIngestionManifest", back_populates="subbatches")
    
    # Relationship to file manifests
    file_manifests = relationship("AuditManifest", back_populates="subbatch", cascade="all, delete-orphan")

    def __repr__(self):
        return (
            f"SubbatchIngestionManifest(subbatch_id={self.subbatch_manifest_id}, batch_id={self.batch_manifest_id}, "
            f"table_name={self.table_name}, status={self.status.value}, "
            f"files_processed={self.files_processed}, rows_processed={self.rows_processed}, "
            f"started_at={self.started_at}, completed_at={self.completed_at})"
        )


# Main CNPJ/source tables use MainBase
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
