"""
Tests for src/database/utils.py — SQL generation utilities.
"""
import pytest
from src.database.utils import (
    quote_ident,
    map_types,
    create_temp_table_sql,
    ensure_table_sql,
    upsert_from_temp_sql,
    scd2_upsert_from_temp_sql,
    apply_transforms_to_batch,
    get_primary_key_columns,
    get_table_columns,
    get_table_index_columns,
    get_tables_to_indices,
    extract_primary_keys,
    get_column_types_mapping,
    validate_table_info,
    safe_get_primary_keys,
    safe_get_column_types,
)
from src.database.schemas import TableInfo


# ---------------------------------------------------------------------------
# quote_ident
# ---------------------------------------------------------------------------

def test_quote_ident_valid():
    assert quote_ident("my_table") == '"my_table"'

def test_quote_ident_invalid_raises():
    with pytest.raises(ValueError):
        quote_ident("invalid-name")

def test_quote_ident_with_numbers():
    assert quote_ident("col1") == '"col1"'


# ---------------------------------------------------------------------------
# map_types
# ---------------------------------------------------------------------------

def test_map_types_with_mapping():
    result = map_types(["a", "b"], {"a": "BIGINT"})
    assert result["a"] == "BIGINT"
    assert result["b"] == "TEXT"

def test_map_types_empty_mapping():
    result = map_types(["a", "b"], {})
    assert result == {"a": "TEXT", "b": "TEXT"}

def test_map_types_none_mapping():
    result = map_types(["x"], None)
    assert result == {"x": "TEXT"}


# ---------------------------------------------------------------------------
# create_temp_table_sql
# ---------------------------------------------------------------------------

def test_create_temp_table_sql():
    sql = create_temp_table_sql("tmp_test", ["col1", "col2"], {"col1": "TEXT", "col2": "BIGINT"})
    assert "CREATE TEMP TABLE IF NOT EXISTS" in sql
    assert '"tmp_test"' in sql
    assert "BIGINT" in sql


# ---------------------------------------------------------------------------
# ensure_table_sql
# ---------------------------------------------------------------------------

def test_ensure_table_sql_with_pk():
    sql = ensure_table_sql("my_table", ["id", "name"], {"id": "BIGINT", "name": "TEXT"}, ["id"])
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "PRIMARY KEY" in sql
    assert '"id"' in sql

def test_ensure_table_sql_without_pk():
    sql = ensure_table_sql("my_table", ["name"], {"name": "TEXT"}, [])
    assert "PRIMARY KEY" not in sql


# ---------------------------------------------------------------------------
# upsert_from_temp_sql
# ---------------------------------------------------------------------------

def test_upsert_from_temp_sql_generates_insert_on_conflict():
    sql = upsert_from_temp_sql("empresa", "tmp_empresa", ["cnpj_basico", "razao_social"], ["cnpj_basico"])
    assert "INSERT INTO" in sql
    assert "ON CONFLICT" in sql
    assert "ROW_NUMBER()" in sql

def test_upsert_from_temp_sql_no_pks_raises():
    with pytest.raises(ValueError):
        upsert_from_temp_sql("empresa", "tmp_empresa", ["cnpj_basico"], [])


# ---------------------------------------------------------------------------
# scd2_upsert_from_temp_sql
# ---------------------------------------------------------------------------

def test_scd2_upsert_generates_expire_and_insert():
    sql = scd2_upsert_from_temp_sql("empresa", "tmp_empresa", ["cnpj_basico", "row_hash"], ["cnpj_basico"], "2024-01-01")
    assert "UPDATE" in sql
    assert "valid_to" in sql
    assert "is_current" in sql
    assert "INSERT INTO" in sql
    assert "2024-01-01" in sql

def test_scd2_upsert_default_batch_date():
    sql = scd2_upsert_from_temp_sql("empresa", "tmp", ["cnpj_basico"], ["cnpj_basico"])
    assert "NOW()" in sql

def test_scd2_upsert_no_pks_raises():
    with pytest.raises(ValueError):
        scd2_upsert_from_temp_sql("empresa", "tmp", ["cnpj_basico"], [])


# ---------------------------------------------------------------------------
# apply_transforms_to_batch
# ---------------------------------------------------------------------------

def test_apply_transforms_no_transform():
    """If transform_map is the default, batch is returned as-is."""
    from src.core.transforms import default_transform_map
    ti = type("TI", (), {"table_name": "empresa", "transform_map": default_transform_map})()
    batch = [("12345678", "Empresa A")]
    result = apply_transforms_to_batch(ti, batch, ["cnpj_basico", "razao_social"])
    assert result == batch

def test_apply_transforms_with_custom_transform():
    def my_transform(row):
        row["cnpj_basico"] = "TRANSFORMED"
        return row
    ti = type("TI", (), {"table_name": "empresa", "transform_map": staticmethod(my_transform)})()
    # transform_map must differ from default_transform_map to be applied
    from src.core.transforms import default_transform_map
    assert ti.transform_map is not default_transform_map
    batch = [("12345678", "Empresa A")]
    result = apply_transforms_to_batch(ti, batch, ["cnpj_basico", "razao_social"])
    # The transform replaces cnpj_basico in the dict, which is mapped back to tuple position 0
    assert result[0][0] == "TRANSFORMED"


# ---------------------------------------------------------------------------
# SQLAlchemy model introspection
# ---------------------------------------------------------------------------

def test_get_table_columns_empresa():
    cols = get_table_columns("empresa")
    assert "cnpj_basico" in cols
    # SCD2 columns
    assert "is_current" in cols
    assert "row_hash" in cols

def test_get_table_columns_unknown_raises():
    with pytest.raises(ValueError):
        get_table_columns("nonexistent_table")

def test_get_table_index_columns_empresa():
    idx_cols = get_table_index_columns("empresa")
    assert "cnpj_basico" in idx_cols

def test_get_tables_to_indices():
    result = get_tables_to_indices()
    assert "empresa" in result
    assert isinstance(result["empresa"], set)

def test_get_primary_key_columns_empresa():
    from src.database.models.business import Empresa
    # Pass model directly to avoid base registry traversal (SQLModel incompatibility)
    pks = get_primary_key_columns("empresa", table_model=Empresa)
    # SCD2 models use surrogate key 'sk' as the physical PK
    assert "sk" in pks

def test_extract_primary_keys_empresa():
    ti = type("TI", (), {"table_name": "empresa"})()
    pks = extract_primary_keys(ti)
    assert "sk" in pks

def test_get_column_types_mapping_empresa():
    ti = type("TI", (), {"table_name": "empresa", "columns": ["cnpj_basico"]})()
    result = get_column_types_mapping(ti)
    assert "cnpj_basico" in result


# ---------------------------------------------------------------------------
# safe_get_primary_keys / safe_get_column_types / validate_table_info
# ---------------------------------------------------------------------------

def test_safe_get_primary_keys_from_attribute():
    ti = type("TI", (), {"table_name": "empresa", "primary_keys": ["cnpj_basico"], "columns": ["cnpj_basico"]})()
    assert safe_get_primary_keys(ti) == ["cnpj_basico"]

def test_safe_get_primary_keys_falls_back_to_model():
    ti = type("TI", (), {"table_name": "empresa", "columns": ["cnpj_basico"]})()
    result = safe_get_primary_keys(ti)
    # SCD2 models use surrogate key 'sk' as the physical PK
    assert "sk" in result

def test_safe_get_column_types_from_attribute():
    ti = type("TI", (), {"table_name": "empresa", "types": {"cnpj_basico": "TEXT"}, "columns": ["cnpj_basico"]})()
    assert safe_get_column_types(ti)["cnpj_basico"] == "TEXT"

def test_safe_get_column_types_falls_back_to_model():
    ti = type("TI", (), {"table_name": "empresa", "columns": ["cnpj_basico"]})()
    result = safe_get_column_types(ti)
    assert "cnpj_basico" in result

def test_validate_table_info_valid():
    ti = type("TI", (), {"table_name": "empresa", "columns": ["cnpj_basico"]})()
    assert validate_table_info(ti) is True

def test_validate_table_info_missing_table_name():
    ti = type("TI", (), {"columns": ["cnpj_basico"]})()
    assert validate_table_info(ti) is False

def test_validate_table_info_empty_columns():
    ti = type("TI", (), {"table_name": "empresa", "columns": []})()
    assert validate_table_info(ti) is False
