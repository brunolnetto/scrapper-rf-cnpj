from sqlalchemy import Column, BigInteger, String, TIMESTAMP, JSON, Text, Float, Index
from sqlalchemy.dialects.postgresql import UUID
from typing import Optional, Generic, TypeVar, List, Any
from pydantic import BaseModel, Field
from datetime import datetime
from uuid import uuid4
from functools import reduce
from sqlalchemy.ext.declarative import declarative_base

# Separate bases for audit and main tables
AuditBase = declarative_base()
MainBase = declarative_base()

T = TypeVar("T", bound=BaseModel)


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
            audit_metadata=self.audi_metadata,
        )


class AuditDB(AuditBase):
    """
    SQLAlchemy model for the audit table.
    """

    __tablename__ = "audit"

    audi_id = Column(UUID(as_uuid=True), primary_key=True)
    audi_table_name = Column(String(255), nullable=False)
    audi_filenames = Column(JSON, nullable=False)
    audi_file_size_bytes = Column(BigInteger, nullable=True)
    audi_source_updated_at = Column(TIMESTAMP, nullable=True)
    audi_created_at = Column(TIMESTAMP, nullable=True)
    audi_downloaded_at = Column(TIMESTAMP, nullable=True)
    audi_processed_at = Column(TIMESTAMP, nullable=True)
    audi_inserted_at = Column(TIMESTAMP, nullable=True)
    audit_metadata = Column(JSON, nullable=True)

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
            previous_t = previous_timestamps[0:index]
            if index > 0:
                greater_than_map = lambda a: a <= current_timestamp
                this_is_met = reduce(and_map, map(greater_than_map, previous_t))
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
        file_info = f"{table_name}, {filenames}, {file_size}"
        args = f"audi_id={self.audi_id}, {file_info}, {timestamps}"
        return f"AuditDB({args})"


# Main CNPJ/source tables use MainBase
class Empresa(MainBase):
    __tablename__ = "empresa"
    cnpj_basico = Column(Text, nullable=False, primary_key=True)
    razao_social = Column(Text, nullable=True)
    natureza_juridica = Column(Text, nullable=True)
    qualificacao_responsavel = Column(Text, nullable=True)
    capital_social = Column(Float(53), nullable=True)
    porte_empresa = Column(Text, nullable=True)
    ente_federativo_responsavel = Column(Text, nullable=True)
    __table_args__ = (Index("empresa_cnpj_basico", "cnpj_basico"),)


class SimplesNacional(MainBase):
    __tablename__ = "simples"
    cnpj_basico = Column(Text, nullable=True, primary_key=True)
    opcao_pelo_simples = Column(Text, nullable=True)
    data_opcao_simples = Column(Text, nullable=True)
    data_exclusao_simples = Column(Text, nullable=True)
    opcao_mei = Column(Text, nullable=True)
    data_opcao_mei = Column(Text, nullable=True)
    data_exclusao_mei = Column(Text, nullable=True)
    __table_args__ = (Index("simples_cnpj_basico", "cnpj_basico"),)


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
    __table_args__ = (Index("socios_cnpj_basico", "cnpj_basico"),)


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
