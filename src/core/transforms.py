"""
Transform functions for data cleaning during ETL processing.
"""

from typing import Dict, List, Callable
from ..setup.logging import logger

# Constants for Brazilian Federal Revenue data formats
BRAZILIAN_NULL_DATE = "00000000"  # Standard null date format in RF data


def normalize_null_values(row_dict: Dict[str, str]) -> Dict[str, str]:
    """
    Normalize NULL values across all fields in a row.
    
    Converts None, "NULL", "null", "None", empty strings to empty string "".
    Also strips whitespace from all string values and converts other types to strings.
    
    Args:
        row_dict: Dictionary representing a single row
        
    Returns:
        Row dictionary with normalized NULL values
    """
    try:
        for key, value in row_dict.items():
            if value is None or value in ("NULL", "null", "None"):
                row_dict[key] = ""
            elif isinstance(value, str):
                row_dict[key] = value.strip()
            elif isinstance(value, (int, float)):
                # Convert numbers to strings for consistent processing
                row_dict[key] = str(value)
            else:
                # Convert other types to strings
                row_dict[key] = str(value) if value is not None else ""
                
    except (AttributeError, KeyError, TypeError) as e:
        logger.warning(f"Error normalizing NULL values: {e}")
    
    return row_dict


def clean_leading_zeros_from_fields(row_dict: Dict[str, str], field_names: List[str]) -> Dict[str, str]:
    """
    Clean leading zeros from specified fields.
    
    Args:
        row_dict: Dictionary representing a single row
        field_names: List of field names to clean
        
    Returns:
        Row dictionary with cleaned fields
    """
    try:
        for field in field_names:
            if field in row_dict:
                # Convert to string first, handling None, integers, and strings
                raw_value = row_dict[field]
                if raw_value is None:
                    value = ""
                elif isinstance(raw_value, (int, float)):
                    value = str(raw_value)
                else:
                    value = str(raw_value).strip()
                
                if value and value.isdigit():
                    # Remove leading zeros but keep at least one digit
                    cleaned_value = value.lstrip('0') or '0'
                    row_dict[field] = cleaned_value
                elif field == "codigo" and not value:  # Special case for empty codigo
                    row_dict[field] = '0'
                else:
                    # Keep the converted string value
                    row_dict[field] = value
    except (AttributeError, KeyError, TypeError) as e:
        logger.warning(f"Error cleaning leading zeros from fields {field_names}: {e}")
    
    return row_dict


def format_cnpj_fields(row_dict: Dict[str, str], field_specs: Dict[str, int]) -> Dict[str, str]:
    """
    Format CNPJ fields to specified digit lengths with zero padding.
    
    Args:
        row_dict: Dictionary representing a single row
        field_specs: Dict mapping field names to required digit lengths
                    e.g., {"cnpj_basico": 8, "cnpj_ordem": 4, "cnpj_dv": 2}
        
    Returns:
        Row dictionary with formatted CNPJ fields
    """
    try:
        for field, length in field_specs.items():
            if field in row_dict and row_dict[field]:
                value = row_dict[field].strip()
                if value and value.isdigit():
                    row_dict[field] = value.zfill(length)
    except (AttributeError, KeyError, ValueError, TypeError) as e:
        logger.warning(f"Error formatting CNPJ fields {field_specs}: {e}")
    
    return row_dict


def format_date_fields(row_dict: Dict[str, str], field_names: List[str], null_date_value: str = None) -> Dict[str, str]:
    """
    Format date fields from YYYYMMDD to YYYY-MM-DD.
    
    Args:
        row_dict: Dictionary representing a single row
        field_names: List of date field names to format
        null_date_value: Value to convert to empty string (e.g., "00000000")
        
    Returns:
        Row dictionary with formatted date fields
    """
    try:
        for field in field_names:
            if field in row_dict and row_dict[field]:
                date_value = row_dict[field].strip()
                
                # Handle special null date value
                if null_date_value and date_value == null_date_value:
                    row_dict[field] = ""
                    continue
                
                # Check if it's 8 digits (YYYYMMDD format)
                if date_value and len(date_value) == 8 and date_value.isdigit():
                    # Convert YYYYMMDD to YYYY-MM-DD
                    year = date_value[:4]
                    month = date_value[4:6]
                    day = date_value[6:8]
                    
                    # Basic validation
                    if 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                        formatted_date = f"{year}-{month}-{day}"
                        row_dict[field] = formatted_date
                    else:
                        logger.warning(f"Invalid date components in {field}: {date_value}")
                elif date_value and date_value != "0":
                    logger.warning(f"Invalid date format in {field}: {date_value}")
                    
    except (AttributeError, KeyError, ValueError, TypeError) as e:
        logger.warning(f"Error formatting date fields {field_names}: {e}")
    
    return row_dict


def convert_brazilian_currency(row_dict: Dict[str, str]) -> Dict[str, str]:
    """
    Convert Brazilian currency format to standard format.
    
    Handles capital_social field conversion:
    - "1.234.567,89" → "1234567.89"
    - "1000,50" → "1000.50"
    
    Args:
        row_dict: Dictionary representing a single row
        
    Returns:
        Row dictionary with converted currency values
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

    except (ValueError, TypeError, AttributeError) as e:
        logger.warning(f"Error converting Brazilian currency: {e}")

    return row_dict


def compose_transforms(*transforms: Callable[[Dict[str, str]], Dict[str, str]]) -> Callable[[Dict[str, str]], Dict[str, str]]:
    """
    Compose multiple transform functions into a single transform.
    
    Args:
        *transforms: Variable number of transform functions
        
    Returns:
        Composed transform function
    """
    def composed_transform(row_dict: Dict[str, str]) -> Dict[str, str]:
        result = row_dict.copy()
        for transform in transforms:
            result = transform(result)
        return result
    
    return composed_transform


# =============================================================================
# SPECIALIZED TRANSFORM BUILDERS
# =============================================================================

def build_cnpj_only_transform() -> Callable[[Dict[str, str]], Dict[str, str]]:
    """Build transform for tables that only need cnpj_basico formatting."""
    def cnpj_only_transform(row_dict: Dict[str, str]) -> Dict[str, str]:
        return format_cnpj_fields(row_dict, {"cnpj_basico": 8})
    return cnpj_only_transform


def build_codigo_cleanup_transform() -> Callable[[Dict[str, str]], Dict[str, str]]:
    """Build transform for tables with codigo field needing cleanup."""
    def codigo_cleanup_transform(row_dict: Dict[str, str]) -> Dict[str, str]:
        return clean_leading_zeros_from_fields(row_dict, ["codigo"])
    return codigo_cleanup_transform


def build_qualificacao_cleanup_transform() -> Callable[[Dict[str, str]], Dict[str, str]]:
    """Build transform for qualificacao fields cleanup."""
    def qualificacao_cleanup_transform(row_dict: Dict[str, str]) -> Dict[str, str]:
        return clean_leading_zeros_from_fields(
            row_dict, ["qualificacao_socio", "qualificacao_representante_legal"]
        )
    return qualificacao_cleanup_transform


def build_estabelecimento_date_transform() -> Callable[[Dict[str, str]], Dict[str, str]]:
    """Build transform for estabelecimento date fields."""
    def estabelecimento_date_transform(row_dict: Dict[str, str]) -> Dict[str, str]:
        return format_date_fields(
            row_dict, ["data_inicio_atividade", "data_situacao_cadastral"]
        )
    return estabelecimento_date_transform


def build_simples_date_transform() -> Callable[[Dict[str, str]], Dict[str, str]]:
    """Build transform for simples date fields."""
    def simples_date_transform(row_dict: Dict[str, str]) -> Dict[str, str]:
        return format_date_fields(
            row_dict, 
            ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"],
            null_date_value=BRAZILIAN_NULL_DATE
        )
    return simples_date_transform


def build_socios_date_transform() -> Callable[[Dict[str, str]], Dict[str, str]]:
    """Build transform for socios date fields."""
    def socios_date_transform(row_dict: Dict[str, str]) -> Dict[str, str]:
        return format_date_fields(row_dict, ["data_entrada_sociedade"])
    return socios_date_transform


def build_estabelecimento_cnpj_transform() -> Callable[[Dict[str, str]], Dict[str, str]]:
    """Build transform for estabelecimento CNPJ components."""
    def estabelecimento_cnpj_transform(row_dict: Dict[str, str]) -> Dict[str, str]:
        return format_cnpj_fields(
            row_dict, {"cnpj_basico": 8, "cnpj_ordem": 4, "cnpj_dv": 2}
        )
    return estabelecimento_cnpj_transform


def build_reference_codes_transform() -> Callable[[Dict[str, str]], Dict[str, str]]:
    """Build transform for reference code validation."""
    def reference_codes_transform(row_dict: Dict[str, str]) -> Dict[str, str]:
        return clean_leading_zeros_from_fields(
            row_dict, ["motivo_situacao_cadastral", "cnae_fiscal_principal"]
        )
    return reference_codes_transform


# =============================================================================
# COMPOSITE TRANSFORM FUNCTIONS
# =============================================================================

def default_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Default transform that normalizes NULL values and strips whitespace."""
    return normalize_null_values(row_dict)


def codigo_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Transform for tables with 'codigo' field requiring cleanup."""
    try:
        return compose_transforms(
            normalize_null_values,
            build_codigo_cleanup_transform()
        )(row_dict)
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"Transform error for codigo row: {e}")
        return row_dict


def empresa_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Transform for 'empresa' table data."""
    try:
        return compose_transforms(
            normalize_null_values,
            build_cnpj_only_transform(),
            convert_brazilian_currency
        )(row_dict)
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"Transform error for empresa row: {e}")
        return row_dict


def socios_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Transform for 'socios' table data."""
    try:
        return compose_transforms(
            normalize_null_values,
            build_cnpj_only_transform(),
            build_socios_date_transform(),
            build_qualificacao_cleanup_transform()
        )(row_dict)
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"Transform error for socios row: {e}")
        return row_dict


def estabelecimento_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Transform for 'estabelecimento' table data."""
    try:
        return compose_transforms(
            normalize_null_values,
            build_estabelecimento_cnpj_transform(),
            build_estabelecimento_date_transform(),
            build_reference_codes_transform()
        )(row_dict)
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"Transform error for estabelecimento row: {e}")
        return row_dict


def simples_transform_map(row_dict: Dict[str, str]) -> Dict[str, str]:
    """Transform for 'simples' table data."""
    try:
        return compose_transforms(
            normalize_null_values,
            build_cnpj_only_transform(),
            build_simples_date_transform()
        )(row_dict)
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"Transform error for simples row: {e}")
        return row_dict
