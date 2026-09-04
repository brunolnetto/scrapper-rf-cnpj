"""
Transform functions for data cleaning during ETL processing.
Consolidates all non-key attributes into a 'metadata' bag for Hybrid SCD.
"""

from typing import Dict, List, Callable, Any
from ..setup.logging import logger

# Constants for Brazilian Federal Revenue data formats
BRAZILIAN_NULL_DATE = "00000000"  # Standard null date format in RF data


def normalize_null_values(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize NULL values across all fields in a row."""
    try:
        for key, value in row_dict.items():
            if value is None or value in ("NULL", "null", "None"):
                row_dict[key] = ""
            elif isinstance(value, str):
                row_dict[key] = value.strip()
            elif isinstance(value, (int, float)):
                row_dict[key] = str(value)
            else:
                row_dict[key] = str(value) if value is not None else ""
    except Exception as e:
        logger.warning(f"Error normalizing NULL values: {e}")
    return row_dict


def clean_leading_zeros_from_fields(row_dict: Dict[str, Any], field_names: List[str]) -> Dict[str, Any]:
    """Clean leading zeros from specified fields."""
    try:
        for field in field_names:
            if field in row_dict:
                raw_value = row_dict[field]
                if raw_value is None:
                    value = ""
                elif isinstance(raw_value, (int, float)):
                    value = str(raw_value)
                else:
                    value = str(raw_value).strip()
                
                if value and value.isdigit():
                    cleaned_value = value.lstrip('0') or '0'
                    row_dict[field] = cleaned_value
                elif field == "codigo" and not value:
                    row_dict[field] = '0'
                else:
                    row_dict[field] = value
    except Exception as e:
        logger.warning(f"Error cleaning leading zeros: {e}")
    return row_dict


def format_cnpj_fields(row_dict: Dict[str, Any], field_specs: Dict[str, int]) -> Dict[str, Any]:
    """Format CNPJ fields to specified digit lengths with zero padding."""
    try:
        for field, length in field_specs.items():
            if field in row_dict and row_dict[field]:
                value = str(row_dict[field]).strip()
                if value and value.isdigit():
                    row_dict[field] = value.zfill(length)
    except Exception as e:
        logger.warning(f"Error formatting CNPJ fields: {e}")
    return row_dict


def format_date_fields(row_dict: Dict[str, Any], field_names: List[str], null_date_value: str = None) -> Dict[str, Any]:
    """Format date fields from YYYYMMDD to YYYY-MM-DD."""
    try:
        for field in field_names:
            if field in row_dict and row_dict[field]:
                date_value = str(row_dict[field]).strip()
                if null_date_value and date_value == null_date_value:
                    row_dict[field] = ""
                    continue
                if date_value and len(date_value) == 8 and date_value.isdigit():
                    year = date_value[:4]
                    month = date_value[4:6]
                    day = date_value[6:8]
                    if 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                        row_dict[field] = f"{year}-{month}-{day}"
    except Exception as e:
        logger.warning(f"Error formatting date fields: {e}")
    return row_dict


def convert_brazilian_currency(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Brazilian currency format to standard float string format."""
    try:
        if "capital_social" in row_dict and row_dict["capital_social"]:
            val = str(row_dict["capital_social"]).strip().replace(".", "").replace(",", ".")
            if val:
                row_dict["capital_social"] = f"{float(val):.2f}"
    except Exception:
        pass
    return row_dict


def pack_metadata_transform(keys: List[str]) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """Packs all non-key columns into an 'attributes' dictionary."""
    def transform(row_dict: Dict[str, Any]) -> Dict[str, Any]:
        metadata = {}
        result = {}
        for k, v in row_dict.items():
            if k in keys:
                result[k] = v
            elif k not in ('sk', 'valid_from', 'valid_to', 'is_current', 'row_hash', 'metadata_diff', 'checksum'):
                metadata[k] = v
        result['attributes'] = metadata
        return result
    return transform


def compose_transforms(*transforms: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """Compose multiple transform functions into a single transform."""
    def composed_transform(row_dict: Dict[str, Any]) -> Dict[str, Any]:
        for transform in transforms:
            row_dict = transform(row_dict)
        return row_dict
    return composed_transform


# =============================================================================
# COMPOSITE TRANSFORM FUNCTIONS
# =============================================================================

def default_transform_map(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    return normalize_null_values(row_dict)


def codigo_transform_map(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """SCD1 Lookup transform (keep original columns)."""
    return compose_transforms(
        normalize_null_values,
        lambda d: clean_leading_zeros_from_fields(d, ["codigo"])
    )(row_dict)


def empresa_transform_map(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Hybrid SCD transform for 'empresa'."""
    return compose_transforms(
        normalize_null_values,
        lambda d: format_cnpj_fields(d, {"cnpj_basico": 8}),
        convert_brazilian_currency,
        pack_metadata_transform(["cnpj_basico"])
    )(row_dict)


def socios_transform_map(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Hybrid SCD transform for 'socios'."""
    return compose_transforms(
        normalize_null_values,
        lambda d: format_cnpj_fields(d, {"cnpj_basico": 8}),
        lambda d: format_date_fields(d, ["data_entrada_sociedade"]),
        lambda d: clean_leading_zeros_from_fields(d, ["qualificacao_socio", "qualificacao_representante_legal"]),
        pack_metadata_transform(["cnpj_basico", "cpf_cnpj_socio", "nome_socio_razao_social"])
    )(row_dict)


def estabelecimento_transform_map(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Hybrid SCD transform for 'estabelecimento'."""
    return compose_transforms(
        normalize_null_values,
        lambda d: format_cnpj_fields(d, {"cnpj_basico": 8, "cnpj_ordem": 4, "cnpj_dv": 2}),
        lambda d: format_date_fields(d, ["data_inicio_atividade", "data_situacao_cadastral"]),
        lambda d: clean_leading_zeros_from_fields(d, ["motivo_situacao_cadastral", "cnae_fiscal_principal"]),
        pack_metadata_transform(["cnpj_basico", "cnpj_ordem", "cnpj_dv"])
    )(row_dict)


def simples_transform_map(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Hybrid SCD transform for 'simples'."""
    return compose_transforms(
        normalize_null_values,
        lambda d: format_cnpj_fields(d, {"cnpj_basico": 8}),
        lambda d: format_date_fields(d, ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"], null_date_value=BRAZILIAN_NULL_DATE),
        pack_metadata_transform(["cnpj_basico"])
    )(row_dict)
