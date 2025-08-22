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
        "expression": "EMPRE",
        "transform_map": empresa_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "estabelecimento": {
        "label": "Estabelecimento",
        "group": "estabelecimentos",
        "expression": "ESTABELE",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "socios": {
        "label": "Socios",
        "group": "socios",
        "expression": "SOCIO",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING        
    },
    "simples": {
        "label": "Simples",
        "group": "simples",
        "expression": "SIMPLES",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "cnae": {
        "label": "CNAEs",
        "group": "cnaes",
        "expression": "CNAE",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "moti": {
        "label": "Motivos",
        "group": "motivos",
        "expression": "MOTI",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "munic": {
        "label": "Municipios",
        "group": "municipios",
        "expression": "MUNIC",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "natju": {
        "label": "Naturezas",
        "group": "naturezas",
        "expression": "NATJU",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "pais": {
        "label": "Paises",
        "group": "paises",
        "expression": "PAIS",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
    "quals": {
        "label": "Qualificacoes",
        "group": "qualificacoes",
        "expression": "QUALS",
        "transform_map": default_transform_map,
        "encoding": DEFAULT_ENCODING,
    },
}
