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
        "transform_map": empresa_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "estabelecimento": {
        "label": "Estabelecimento",
        "group": "estabelecimentos",
        "expression": "ESTABELE",
        "transform_map": estabelecimento_transform_map,  # Comprehensive estabelecimento transforms
        "encoding": DEFAULT_ENCODING,
    },
    "socios": {
        "label": "Socios",
        "group": "socios",
        "expression": "SOCIO",
        "transform_map": socios_transform_map,  # NULL handling
        "encoding": DEFAULT_ENCODING        
    },
    "simples": {
        "label": "Simples",
        "group": "simples",
        "expression": "SIMPLES",
        "transform_map": simples_transform_map,  # CNPJ formatting + date formatting
        "encoding": DEFAULT_ENCODING,
    },
    "cnae": {
        "label": "CNAEs",
        "group": "cnaes",
        "expression": "CNAE",
        "transform_map": codigo_transform_map,  # NULL handling + codigo cleanup
        "encoding": DEFAULT_ENCODING,
    },
    "moti": {
        "label": "Motivos",
        "group": "motivos",
        "expression": "MOTI",
        "transform_map": codigo_transform_map,  # NULL handling + codigo cleanup
        "encoding": DEFAULT_ENCODING,
    },
    "munic": {
        "label": "Municipios",
        "group": "municipios",
        "expression": "MUNIC",
        "transform_map": codigo_transform_map,  # NULL handling + codigo cleanup
        "encoding": DEFAULT_ENCODING,
    },
    "natju": {
        "label": "Naturezas",
        "group": "naturezas",
        "expression": "NATJU",
        "transform_map": codigo_transform_map,  # NULL handling + codigo cleanup
        "encoding": DEFAULT_ENCODING,
    },
    "pais": {
        "label": "Paises",
        "group": "paises",
        "expression": "PAIS",
        "transform_map": codigo_transform_map,  # NULL handling + codigo cleanup
        "encoding": DEFAULT_ENCODING,
    },
    "quals": {
        "label": "Qualificacoes",
        "group": "qualificacoes",
        "expression": "QUALS",
        "transform_map": codigo_transform_map,  # NULL handling + codigo cleanup
        "encoding": DEFAULT_ENCODING,
    },
}
