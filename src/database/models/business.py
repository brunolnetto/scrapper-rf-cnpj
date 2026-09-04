from sqlalchemy import Column, Text, Index, Boolean, TIMESTAMP, JSON, String
from sqlalchemy.orm import foreign, remote
from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlmodel import Field as SQLModelField, Relationship
from sqldim import DimensionModel, SCD2Mixin, Field as DimField

class MainBase(DimensionModel):
    """Base for all business models with Kimball support.

    Declares ``checksum`` with an explicit ``sa_column`` that remaps the DB
    column to ``row_hash`` — the name ``LazySCDMetadataProcessor`` generates
    via ``md5(cast(metadata AS varchar))``.  Using ``sa_column`` here is
    intentional: it overrides ``SCD2Mixin.checksum`` through MRO (MainBase
    precedes SCD2Mixin in every business model) and makes the remapping
    explicit rather than relying on opaque ``sa_column_kwargs`` passthrough.
    """
    checksum: Optional[str] = DimField(
        default=None,
        sa_column=Column(String, name="row_hash", index=True, nullable=True),
    )

# =============================================================================
# MAIN DIMENSIONS (Hybrid SCD Type 6 - Metadata Bag Pattern)
# =============================================================================

class Empresa(MainBase, SCD2Mixin, table=True):
    __tablename__ = "empresa"
    __scd_type__ = 6
    __natural_key__ = ["cnpj_basico"]

    sk: Optional[int] = DimField(default=None, primary_key=True)
    cnpj_basico: str = DimField(index=True)

    # DB column must stay 'metadata' — LazySCDMetadataProcessor queries it by
    # that name.  Python field is 'attributes' to avoid SQLModel's reserved
    # 'metadata' attribute on the declarative base class.
    attributes: Dict[str, Any] = SQLModelField(default_factory=dict, sa_column=Column(JSON, name="metadata", info={"scd": 2}))
    metadata_diff: Optional[Dict[str, Any]] = SQLModelField(default=None, sa_column=Column(JSON))

    estabelecimentos: List["Estabelecimento"] = Relationship(sa_relationship_kwargs={"primaryjoin": "remote(Estabelecimento.cnpj_basico) == foreign(Empresa.cnpj_basico)", "viewonly": True})
    socios: List["Socios"] = Relationship(sa_relationship_kwargs={"primaryjoin": "remote(Socios.cnpj_basico) == foreign(Empresa.cnpj_basico)", "viewonly": True})
    simples: Optional["SimplesNacional"] = Relationship(sa_relationship_kwargs={"primaryjoin": "remote(SimplesNacional.cnpj_basico) == foreign(Empresa.cnpj_basico)", "uselist": False, "viewonly": True})

class Estabelecimento(MainBase, SCD2Mixin, table=True):
    __tablename__ = "estabelecimento"
    __scd_type__ = 6
    __natural_key__ = ["cnpj_basico", "cnpj_ordem", "cnpj_dv"]

    sk: Optional[int] = DimField(default=None, primary_key=True)
    cnpj_basico: str = DimField(index=True)
    cnpj_ordem: str = DimField(index=True)
    cnpj_dv: str = DimField(index=True)

    attributes: Dict[str, Any] = SQLModelField(default_factory=dict, sa_column=Column(JSON, name="metadata", info={"scd": 2}))
    metadata_diff: Optional[Dict[str, Any]] = SQLModelField(default=None, sa_column=Column(JSON))

class Socios(MainBase, SCD2Mixin, table=True):
    __tablename__ = "socios"
    __scd_type__ = 6
    __natural_key__ = ["cnpj_basico", "cpf_cnpj_socio", "nome_socio_razao_social"]

    sk: Optional[int] = DimField(default=None, primary_key=True)
    cnpj_basico: str = DimField(index=True)
    cpf_cnpj_socio: str = DimField(index=True)
    nome_socio_razao_social: str = DimField(index=True)

    attributes: Dict[str, Any] = SQLModelField(default_factory=dict, sa_column=Column(JSON, name="metadata", info={"scd": 2}))
    metadata_diff: Optional[Dict[str, Any]] = SQLModelField(default=None, sa_column=Column(JSON))

class SimplesNacional(MainBase, SCD2Mixin, table=True):
    __tablename__ = "simples"
    __scd_type__ = 6
    __natural_key__ = ["cnpj_basico"]

    sk: Optional[int] = DimField(default=None, primary_key=True)
    cnpj_basico: str = DimField(index=True)

    attributes: Dict[str, Any] = SQLModelField(default_factory=dict, sa_column=Column(JSON, name="metadata", info={"scd": 2}))
    metadata_diff: Optional[Dict[str, Any]] = SQLModelField(default=None, sa_column=Column(JSON))

# =============================================================================
# LOOKUP DIMENSIONS (SCD Type 1)
# =============================================================================

class Qualificacoes(MainBase, table=True):
    __tablename__ = "quals"
    codigo: str = DimField(primary_key=True)
    descricao: Optional[str] = DimField(default=None)

class MotivoCadastral(MainBase, table=True):
    __tablename__ = "moti"
    codigo: str = DimField(primary_key=True)
    descricao: Optional[str] = DimField(default=None)

class NaturezaJuridica(MainBase, table=True):
    __tablename__ = "natju"
    codigo: str = DimField(primary_key=True)
    descricao: Optional[str] = DimField(default=None)

class Municipio(MainBase, table=True):
    __tablename__ = "munic"
    codigo: str = DimField(primary_key=True)
    descricao: Optional[str] = DimField(default=None)

class Cnae(MainBase, table=True):
    __tablename__ = "cnae"
    codigo: str = DimField(primary_key=True)
    descricao: Optional[str] = DimField(default=None)

class Pais(MainBase, table=True):
    __tablename__ = "pais"
    codigo: str = DimField(primary_key=True)
    descricao: Optional[str] = DimField(default=None)
