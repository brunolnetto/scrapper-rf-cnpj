"""
Core constants for the project.
"""

from typing import Dict, Any
from enum import Enum

from ..setup.logging import logger

# Constants
class Encoding(Enum):
    LATIN1 = "latin-1"
    UTF8 = "utf-8"


def empresa_transform_map(artifact: Any) -> Any:
    """
    Transform map for 'empresa' artifact.

    Args:
        artifact: The artifact to transform.

    Returns:
        The transformed artifact.
    """
    try:
        comma_to_period = lambda x: x.replace(",", ".")
        artifact["capital_social"] = artifact["capital_social"].apply(comma_to_period)
        artifact["capital_social"] = artifact["capital_social"].astype(float)
    except Exception as e:
        logger.error(f"Error transforming 'empresa' artifact: {e}")
        raise ValueError(f"Error transforming 'empresa' artifact: {e}")
    return artifact


# Default transform function (no-op)
def default_transform_map(artifact: Any) -> Any:
    return artifact


# Common table encoding
DEFAULT_ENCODING = Encoding.LATIN1

TABLES_INFO_DICT: Dict[str, Dict[str, Any]] = {
    "empresa": {
        "label": "Empresa",
        "group": "empresas",
        "index_columns": ["cnpj_basico"],
        "columns": [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo_responsavel",
        ],
        "expression": "EMPRE",
        "transform_map": empresa_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "estabelecimento": {
        "label": "Estabelecimento",
        "group": "estabelecimentos",
        "index_columns": ["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
        "columns": [
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "identificador_matriz_filial",
            "nome_fantasia",
            "situacao_cadastral",
            "data_situacao_cadastral",
            "motivo_situacao_cadastral",
            "nome_cidade_exterior",
            "pais",
            "data_inicio_atividade",
            "cnae_fiscal_principal",
            "cnae_fiscal_secundaria",
            "tipo_logradouro",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "uf",
            "municipio",
            "ddd_1",
            "telefone_1",
            "ddd_2",
            "telefone_2",
            "ddd_fax",
            "fax",
            "correio_eletronico",
            "situacao_especial",
            "data_situacao_especial",
        ],
        "expression": "ESTABELE",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "socios": {
        "label": "Socios",
        "group": "socios",
        "index_columns": ["cnpj_basico"],
        "columns": [
            "cnpj_basico",
            "identificador_socio",
            "nome_socio_razao_social",
            "cpf_cnpj_socio",
            "qualificacao_socio",
            "data_entrada_sociedade",
            "pais",
            "representante_legal",
            "nome_do_representante",
            "qualificacao_representante_legal",
            "faixa_etaria",
        ],
        "expression": "SOCIO",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING        
    },
    "simples": {
        "label": "Simples",
        "group": "simples",
        "columns": [
            "cnpj_basico",
            "opcao_pelo_simples",
            "data_opcao_simples",
            "data_exclusao_simples",
            "opcao_mei",
            "data_opcao_mei",
            "data_exclusao_mei",
        ],
        "expression": "SIMPLES",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "cnae": {
        "label": "CNAEs",
        "group": "cnaes",
        "columns": ["codigo", "descricao"],
        "expression": "CNAE",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "moti": {
        "label": "Motivos",
        "group": "motivos",
        "columns": ["codigo", "descricao"],
        "expression": "MOTI",
        "encoding": DEFAULT_ENCODING,
    },
    "munic": {
        "label": "Municipios",
        "group": "municipios",
        "columns": ["codigo", "descricao"],
        "expression": "MUNIC",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "natju": {
        "label": "Naturezas",
        "group": "naturezas",
        "columns": ["codigo", "descricao"],
        "expression": "NATJU",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "pais": {
        "label": "Paises",
        "group": "paises",
        "columns": ["codigo", "descricao"],
        "expression": "PAIS",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "quals": {
        "label": "Qualificacoes",
        "group": "qualificacoes",
        "columns": ["codigo", "descricao"],
        "expression": "QUALS",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
}
