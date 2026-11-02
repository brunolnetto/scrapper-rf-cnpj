from sqlalchemy import Column, JSON, String
from typing import Optional, List, Dict, Any
from sqlmodel import Field as SQLModelField, Relationship
from sqldim import DimensionModel, SCD2Mixin, Field as DimField

def _row_hash_field():
    """Return a fresh SQLAlchemy column for each SCD2 table."""
    return SQLModelField(
        default=None,
        sa_column=Column(String, name="row_hash", index=True, nullable=True),
    )


class MainBase(DimensionModel):
    """Shared base for the CNPJ business dimensions."""

# =============================================================================
# MAIN DIMENSIONS (Hybrid SCD Type 6 - Metadata Bag Pattern)
# =============================================================================

class Empresa(MainBase, SCD2Mixin, table=True):
    checksum: Optional[str] = _row_hash_field()
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
    checksum: Optional[str] = _row_hash_field()
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
    checksum: Optional[str] = _row_hash_field()
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
    checksum: Optional[str] = _row_hash_field()
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
