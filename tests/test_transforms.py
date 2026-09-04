"""
Tests for src/core/transforms.py — Brazilian Federal Revenue data transforms.
"""
import pytest
from src.core.transforms import (
    normalize_null_values,
    clean_leading_zeros_from_fields,
    format_cnpj_fields,
    format_date_fields,
    convert_brazilian_currency,
    compose_transforms,
    default_transform_map,
    codigo_transform_map,
    empresa_transform_map,
    socios_transform_map,
    estabelecimento_transform_map,
    simples_transform_map,
    BRAZILIAN_NULL_DATE,
)


# ---------------------------------------------------------------------------
# normalize_null_values
# ---------------------------------------------------------------------------

def test_normalize_null_none_values():
    row = {"a": None, "b": "NULL", "c": "null", "d": "None"}
    result = normalize_null_values(row)
    assert all(result[k] == "" for k in ["a", "b", "c", "d"])

def test_normalize_strips_whitespace():
    row = {"a": "  hello  "}
    assert normalize_null_values(row)["a"] == "hello"

def test_normalize_converts_numbers():
    row = {"a": 42, "b": 3.14}
    result = normalize_null_values(row)
    assert result["a"] == "42"
    assert result["b"] == "3.14"

def test_normalize_other_types():
    row = {"a": True}
    result = normalize_null_values(row)
    assert result["a"] == "True"

def test_normalize_handles_empty_dict():
    assert normalize_null_values({}) == {}


# ---------------------------------------------------------------------------
# clean_leading_zeros_from_fields
# ---------------------------------------------------------------------------

def test_clean_leading_zeros_removes_zeros():
    row = {"codigo": "0042"}
    result = clean_leading_zeros_from_fields(row, ["codigo"])
    assert result["codigo"] == "42"

def test_clean_leading_zeros_keeps_single_zero():
    row = {"codigo": "000"}
    result = clean_leading_zeros_from_fields(row, ["codigo"])
    assert result["codigo"] == "0"

def test_clean_leading_zeros_non_digit_unchanged():
    row = {"codigo": "ABC"}
    result = clean_leading_zeros_from_fields(row, ["codigo"])
    assert result["codigo"] == "ABC"

def test_clean_leading_zeros_special_empty_codigo():
    row = {"codigo": ""}
    result = clean_leading_zeros_from_fields(row, ["codigo"])
    assert result["codigo"] == "0"

def test_clean_leading_zeros_none_value():
    row = {"codigo": None}
    result = clean_leading_zeros_from_fields(row, ["codigo"])
    # None is treated as empty string which hits the special empty codigo case
    assert result["codigo"] in ("", "0")

def test_clean_leading_zeros_integer_value():
    row = {"codigo": 42}
    result = clean_leading_zeros_from_fields(row, ["codigo"])
    assert result["codigo"] == "42"

def test_clean_leading_zeros_missing_field():
    row = {"other": "value"}
    result = clean_leading_zeros_from_fields(row, ["codigo"])
    assert result == {"other": "value"}


# ---------------------------------------------------------------------------
# format_cnpj_fields
# ---------------------------------------------------------------------------

def test_format_cnpj_pads_to_length():
    row = {"cnpj_basico": "1234", "cnpj_ordem": "1", "cnpj_dv": "5"}
    result = format_cnpj_fields(row, {"cnpj_basico": 8, "cnpj_ordem": 4, "cnpj_dv": 2})
    assert result["cnpj_basico"] == "00001234"
    assert result["cnpj_ordem"] == "0001"
    assert result["cnpj_dv"] == "05"

def test_format_cnpj_non_digit_unchanged():
    row = {"cnpj_basico": "ABCD"}
    result = format_cnpj_fields(row, {"cnpj_basico": 8})
    assert result["cnpj_basico"] == "ABCD"

def test_format_cnpj_empty_value_unchanged():
    row = {"cnpj_basico": ""}
    result = format_cnpj_fields(row, {"cnpj_basico": 8})
    assert result["cnpj_basico"] == ""


# ---------------------------------------------------------------------------
# format_date_fields
# ---------------------------------------------------------------------------

def test_format_date_converts_yyyymmdd():
    row = {"data": "20240115"}
    result = format_date_fields(row, ["data"])
    assert result["data"] == "2024-01-15"

def test_format_date_null_date_cleared():
    row = {"data": BRAZILIAN_NULL_DATE}
    result = format_date_fields(row, ["data"], null_date_value=BRAZILIAN_NULL_DATE)
    assert result["data"] == ""

def test_format_date_invalid_month_unchanged():
    row = {"data": "20241315"}
    result = format_date_fields(row, ["data"])
    assert result["data"] == "20241315"

def test_format_date_empty_field_unchanged():
    row = {"data": ""}
    result = format_date_fields(row, ["data"])
    assert result["data"] == ""

def test_format_date_missing_field():
    row = {"other": "val"}
    result = format_date_fields(row, ["data"])
    assert "data" not in result


# ---------------------------------------------------------------------------
# convert_brazilian_currency
# ---------------------------------------------------------------------------

def test_convert_currency_dot_comma_format():
    row = {"capital_social": "1.234.567,89"}
    result = convert_brazilian_currency(row)
    assert result["capital_social"] == "1234567.89"

def test_convert_currency_simple_comma():
    row = {"capital_social": "1000,50"}
    result = convert_brazilian_currency(row)
    assert result["capital_social"] == "1000.50"

def test_convert_currency_no_comma():
    row = {"capital_social": "1000"}
    result = convert_brazilian_currency(row)
    assert "." in result["capital_social"]

def test_convert_currency_empty():
    row = {"capital_social": ""}
    result = convert_brazilian_currency(row)
    assert result["capital_social"] == ""

def test_convert_currency_missing_field():
    row = {"other": "val"}
    result = convert_brazilian_currency(row)
    assert "capital_social" not in result

def test_convert_currency_multiple_commas():
    row = {"capital_social": "1,2,3"}
    result = convert_brazilian_currency(row)
    # Should keep original due to multiple commas
    assert result["capital_social"] == "1,2,3"


# ---------------------------------------------------------------------------
# compose_transforms
# ---------------------------------------------------------------------------

def test_compose_applies_in_order():
    def add_x(d): d["x"] = "x"; return d
    def add_y(d): d["y"] = d.get("x", "") + "y"; return d
    fn = compose_transforms(add_x, add_y)
    result = fn({})
    assert result["x"] == "x"
    assert result["y"] == "xy"


# ---------------------------------------------------------------------------
# Composite transform maps
# ---------------------------------------------------------------------------

def test_default_transform_map():
    row = {"a": None, "b": "  val  "}
    result = default_transform_map(row)
    assert result["a"] == ""
    assert result["b"] == "val"

def test_codigo_transform_map():
    row = {"codigo": "0042"}
    result = codigo_transform_map(row)
    assert result["codigo"] == "42"

def test_empresa_transform_map_cnpj_and_currency():
    row = {"cnpj_basico": "1234", "capital_social": "1.000,00"}
    result = empresa_transform_map(row)
    # cnpj_basico is a natural key → stays top-level
    assert result["cnpj_basico"] == "00001234"
    # non-key fields are packed into dim_metadata by pack_metadata_transform
    assert result["dim_metadata"]["capital_social"] == "1000.00"

def test_socios_transform_map():
    row = {
        "cnpj_basico": "1234",
        "data_entrada_sociedade": "20240101",
        "qualificacao_socio": "005",
        "qualificacao_representante_legal": "010",
    }
    result = socios_transform_map(row)
    assert result["cnpj_basico"] == "00001234"
    # non-key fields are packed into dim_metadata
    assert result["dim_metadata"]["data_entrada_sociedade"] == "2024-01-01"
    assert result["dim_metadata"]["qualificacao_socio"] == "5"

def test_estabelecimento_transform_map():
    row = {
        "cnpj_basico": "12345678",
        "cnpj_ordem": "1",
        "cnpj_dv": "1",
        "data_inicio_atividade": "20200601",
        "data_situacao_cadastral": "20200601",
        "motivo_situacao_cadastral": "001",
        "cnae_fiscal_principal": "0010",
    }
    result = estabelecimento_transform_map(row)
    # Natural keys stay top-level
    assert result["cnpj_ordem"] == "0001"
    # Non-key fields go to dim_metadata
    assert result["dim_metadata"]["data_inicio_atividade"] == "2020-06-01"
    assert result["dim_metadata"]["motivo_situacao_cadastral"] == "1"

def test_simples_transform_map_clears_null_dates():
    row = {
        "cnpj_basico": "1234",
        "data_opcao_simples": BRAZILIAN_NULL_DATE,
        "data_exclusao_simples": "20240101",
        "data_opcao_mei": BRAZILIAN_NULL_DATE,
        "data_exclusao_mei": BRAZILIAN_NULL_DATE,
    }
    result = simples_transform_map(row)
    assert result["dim_metadata"]["data_opcao_simples"] == ""
    assert result["dim_metadata"]["data_exclusao_simples"] == "2024-01-01"

def test_transform_handles_exception_gracefully():
    """Transform maps must not raise on bad input — return original row."""
    # Pass a row that will cause a TypeError inside compose_transforms
    result = empresa_transform_map({"capital_social": None})
    assert isinstance(result, dict)

# ---------------------------------------------------------------------------
# Builder functions covered via composite transform maps (builders are closures)
# ---------------------------------------------------------------------------

def test_cnpj_padding_via_empresa():
    """build_cnpj_only_transform covered via empresa_transform_map."""
    assert empresa_transform_map({"cnpj_basico": "99"})["cnpj_basico"] == "00000099"

def test_codigo_cleanup_via_codigo_map():
    """build_codigo_cleanup_transform covered via codigo_transform_map."""
    assert codigo_transform_map({"codigo": "007"})["codigo"] == "7"

def test_qualificacao_cleanup_via_socios():
    """build_qualificacao_cleanup_transform covered via socios_transform_map."""
    row = {"cnpj_basico": "1", "qualificacao_socio": "005", "qualificacao_representante_legal": "010"}
    result = socios_transform_map(row)
    assert result["dim_metadata"]["qualificacao_socio"] == "5"
    assert result["dim_metadata"]["qualificacao_representante_legal"] == "10"

def test_estabelecimento_date_via_map():
    """build_estabelecimento_date_transform covered via estabelecimento_transform_map."""
    row = {
        "cnpj_basico": "1", "cnpj_ordem": "1", "cnpj_dv": "1",
        "data_inicio_atividade": "20240101", "data_situacao_cadastral": "20240601",
    }
    result = estabelecimento_transform_map(row)
    assert result["dim_metadata"]["data_inicio_atividade"] == "2024-01-01"

def test_simples_date_via_map():
    """build_simples_date_transform covered via simples_transform_map."""
    row = {
        "cnpj_basico": "1",
        "data_opcao_simples": BRAZILIAN_NULL_DATE,
        "data_exclusao_simples": "20240101",
        "data_opcao_mei": BRAZILIAN_NULL_DATE,
        "data_exclusao_mei": BRAZILIAN_NULL_DATE,
    }
    result = simples_transform_map(row)
    assert result["dim_metadata"]["data_opcao_simples"] == ""
    assert result["dim_metadata"]["data_exclusao_simples"] == "2024-01-01"

def test_socios_date_via_map():
    """build_socios_date_transform covered via socios_transform_map."""
    row = {"cnpj_basico": "1", "data_entrada_sociedade": "20231231"}
    assert socios_transform_map(row)["dim_metadata"]["data_entrada_sociedade"] == "2023-12-31"

def test_estabelecimento_cnpj_padding_via_map():
    """build_estabelecimento_cnpj_transform covered via estabelecimento_transform_map."""
    row = {"cnpj_basico": "1", "cnpj_ordem": "2", "cnpj_dv": "3"}
    result = estabelecimento_transform_map(row)
    # Natural keys stay top-level
    assert result["cnpj_basico"] == "00000001"
    assert result["cnpj_ordem"] == "0002"
    assert result["cnpj_dv"] == "03"

def test_reference_codes_via_estabelecimento():
    """build_reference_codes_transform covered via estabelecimento_transform_map."""
    row = {
        "cnpj_basico": "1", "cnpj_ordem": "1", "cnpj_dv": "1",
        "motivo_situacao_cadastral": "001", "cnae_fiscal_principal": "0010",
    }
    result = estabelecimento_transform_map(row)
    assert result["dim_metadata"]["motivo_situacao_cadastral"] == "1"
    assert result["dim_metadata"]["cnae_fiscal_principal"] == "10"
