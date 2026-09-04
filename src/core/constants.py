"""
Core constants for the project.
"""

from typing import Dict, Any
from enum import Enum

from .transforms import (
    codigo_transform_map,
    empresa_transform_map,
    socios_transform_map,
    estabelecimento_transform_map,
    simples_transform_map,
)

# Constants
class Encoding(Enum):
    LATIN1 = "latin-1" 
    UTF8 = "utf-8"


# Common table encoding
DEFAULT_ENCODING = Encoding.LATIN1

TABLES_INFO_DICT: Dict[str, Dict[str, Any]] = {
    "empresa": {
        "label": "Empresa",
        "group": "empresas",
        "expression": "EMPRE",
        "columns": [
            "cnpj_basico", "razao_social", "natureza_juridica",
            "qualificacao_responsavel", "capital_social",
            "porte_empresa", "ente_federativo_responsavel",
        ],
        "transform_map": empresa_transform_map,
        "encoding": DEFAULT_ENCODING,
        "scd2_columns": ["row_hash", "is_current", "valid_from", "valid_to", "metadata", "metadata_diff"]
    },
    "estabelecimento": {
        "label": "Estabelecimento",
        "group": "estabelecimentos",
        "expression": "ESTABELE",
        "columns": [
            "cnpj_basico", "cnpj_ordem", "cnpj_dv",
            "identificador_matriz_filial", "nome_fantasia",
            "situacao_cadastral", "data_situacao_cadastral", "motivo_situacao_cadastral",
            "nome_cidade_exterior", "pais", "data_inicio_atividade",
            "cnae_fiscal_principal", "cnae_fiscal_secundaria",
            "tipo_logradouro", "logradouro", "numero", "complemento",
            "bairro", "cep", "uf", "municipio",
            "ddd_1", "telefone_1", "ddd_2", "telefone_2",
            "ddd_fax", "fax", "correio_eletronico",
            "situacao_especial", "data_situacao_especial",
        ],
        "transform_map": estabelecimento_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "socios": {
        "label": "Socios",
        "group": "socios",
        "expression": "SOCIO",
        "columns": [
            "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
            "cpf_cnpj_socio", "qualificacao_socio", "data_entrada_sociedade",
            "pais", "representante_legal", "nome_representante",
            "qualificacao_representante_legal", "faixa_etaria",
        ],
        "transform_map": socios_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "simples": {
        "label": "Simples",
        "group": "simples",
        "expression": "SIMPLES",
        "columns": [
            "cnpj_basico", "opcao_pelo_simples", "data_opcao_simples",
            "data_exclusao_simples", "opcao_pelo_mei",
            "data_opcao_mei", "data_exclusao_mei",
        ],
        "transform_map": simples_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "cnae": {
        "label": "CNAEs",
        "group": "cnaes",
        "expression": "CNAE",
        "columns": ["codigo", "descricao"],
        "transform_map": codigo_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "moti": {
        "label": "Motivos",
        "group": "motivos",
        "expression": "MOTI",
        "columns": ["codigo", "descricao"],
        "transform_map": codigo_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "munic": {
        "label": "Municipios",
        "group": "municipios",
        "expression": "MUNIC",
        "columns": ["codigo", "descricao"],
        "transform_map": codigo_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "natju": {
        "label": "Naturezas",
        "group": "naturezas",
        "expression": "NATJU",
        "columns": ["codigo", "descricao"],
        "transform_map": codigo_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "pais": {
        "label": "Paises",
        "group": "paises",
        "expression": "PAIS",
        "columns": ["codigo", "descricao"],
        "transform_map": codigo_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "quals": {
        "label": "Qualificacoes",
        "group": "qualificacoes",
        "expression": "QUALS",
        "columns": ["codigo", "descricao"],
        "transform_map": codigo_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
}
