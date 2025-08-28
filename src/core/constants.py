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

def empresa_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """
    Transform map for 'empresa' row data.
    Pure Python implementation - converts capital_social from Brazilian format to standard format.

    Handles:
    - Simple comma decimal: "1000,50" → "1000.50"
    - Thousands separators: "1.234.567,89" → "1234567.89"
    - Edge cases: empty, invalid, etc.

    Args:
        row_dict: Dictionary representing a single row

    Returns:
        Transformed row dictionary
    """
    try:
        if "capital_social" in row_dict and row_dict["capital_social"]:
            original_value = row_dict["capital_social"].strip()

            if not original_value:
                return row_dict

            # Brazilian number format conversion
            # Pattern: "1.234.567,89" → "1234567.89"
            if "," in original_value:
                # Split on comma (decimal separator in Brazilian format)
                parts = original_value.split(",")
                if len(parts) == 2:
                    # Remove dots (thousands separators) from integer part
                    integer_part = parts[0].replace(".", "")
                    decimal_part = parts[1]

                    # Reconstruct as standard format
                    converted_value = f"{integer_part}.{decimal_part}"

                    try:
                        # Validate it's a valid number and convert to float
                        numeric_value = float(converted_value)
                        row_dict["capital_social"] = converted_value  # Keep as formatted string
                    except ValueError:
                        # Keep original value if conversion fails
                        logger.warning(f"Could not convert capital_social: {original_value}")
                else:
                    # Multiple commas - invalid format, keep original
                    logger.warning(f"Invalid capital_social format (multiple commas): {original_value}")
            # If no comma, try to convert to float anyway
            else:
                try:
                    numeric_value = float(original_value.replace(".", "").replace(",", "."))
                    formatted_value = f"{numeric_value:.2f}"  # Format as string with 2 decimal places
                    row_dict["capital_social"] = formatted_value
                except ValueError:
                    logger.warning(f"Could not convert capital_social: {original_value}")

    except Exception as e:
        logger.warning(f"Transform error for empresa row: {e}")

    return row_dict

# Default transform function (no-op)
def default_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Default no-op transform that returns row unchanged."""
    return row_dict


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
