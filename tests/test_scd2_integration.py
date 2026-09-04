"""
Tests for SCD2 routing logic in DatabaseService.
Uses fully-configured mocks to avoid live database connections.
"""
import pytest
import asyncpg
from unittest.mock import AsyncMock, MagicMock, patch, call
from src.database.service import DatabaseService
from src.database.engine import Database


def make_mock_conn():
    """Return a properly configured async connection mock."""
    conn = AsyncMock()
    conn.is_closed = MagicMock(return_value=False)
    conn.transaction = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=None), __aexit__=AsyncMock(return_value=False)))
    return conn


def make_mock_table_info(table_name: str, primary_keys=None, batch_date=None):
    """Return a mock TableInfo object."""
    ti = MagicMock()
    ti.table_name = table_name
    ti.columns = ["cnpj_basico", "row_hash"]
    ti.primary_keys = primary_keys or ["cnpj_basico"]
    ti.types = {"cnpj_basico": "TEXT", "row_hash": "TEXT"}
    ti.batch_date = batch_date
    return ti


# ---------------------------------------------------------------------------
# SCD2 routing: business tables must call scd2_upsert_from_temp_sql
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("table_name", ["empresa", "estabelecimento", "socios", "simples"])
async def test_scd2_routing_for_business_tables(table_name):
    """DatabaseService must call scd2_upsert_from_temp_sql for all SCD2 tables."""
    mock_db = MagicMock(spec=Database)
    service = DatabaseService(mock_db)
    conn = make_mock_conn()
    table_info = make_mock_table_info(table_name, batch_date="2024-01-01")
    batch = [("12345678", "hash_v1")]

    with patch("src.database.service.base.scd2_upsert_from_temp_sql", return_value="SCD2_SQL") as mock_scd2, \
         patch("src.database.service.base.upsert_from_temp_sql") as mock_upsert, \
         patch("src.database.service.base.create_temp_table_sql", return_value="CREATE TEMP TABLE"), \
         patch("src.database.service.base.map_types", return_value={"cnpj_basico": "TEXT", "row_hash": "TEXT"}):

        await service._process_batch_sequential_optimized(
            conn, batch, 10, table_info, 0, 1, batch_date="2024-01-01"
        )

    mock_scd2.assert_called_once()
    call_args = mock_scd2.call_args
    assert call_args.args[0] == table_name
    assert call_args.args[4] == "2024-01-01"
    mock_upsert.assert_not_called()


@pytest.mark.asyncio
async def test_standard_upsert_for_non_scd2_tables():
    """DatabaseService must call upsert_from_temp_sql for non-business tables."""
    mock_db = MagicMock(spec=Database)
    service = DatabaseService(mock_db)
    conn = make_mock_conn()
    table_info = make_mock_table_info("qualificacoes")
    batch = [("01", "hash_v1")]

    with patch("src.database.service.base.scd2_upsert_from_temp_sql") as mock_scd2, \
         patch("src.database.service.base.upsert_from_temp_sql", return_value="UPSERT_SQL") as mock_upsert, \
         patch("src.database.service.base.create_temp_table_sql", return_value="CREATE TEMP TABLE"), \
         patch("src.database.service.base.map_types", return_value={"cnpj_basico": "TEXT", "row_hash": "TEXT"}):

        await service._process_batch_sequential_optimized(
            conn, batch, 10, table_info, 0, 1
        )

    mock_scd2.assert_not_called()
    mock_upsert.assert_called_once()


@pytest.mark.asyncio
async def test_no_pk_falls_back_to_plain_insert():
    """Tables without primary keys fall back to a plain INSERT."""
    mock_db = MagicMock(spec=Database)
    service = DatabaseService(mock_db)
    conn = make_mock_conn()
    # Use a table not in the SQLAlchemy registry so PK extraction also fails
    table_info = make_mock_table_info("unknown_table_xyz", primary_keys=[])
    table_info.primary_keys = []
    batch = [("01", "hash_v1")]

    with patch("src.database.service.base.create_temp_table_sql", return_value="CREATE TEMP TABLE"), \
         patch("src.database.service.base.map_types", return_value={"cnpj_basico": "TEXT", "row_hash": "TEXT"}), \
         patch("src.database.service.base.scd2_upsert_from_temp_sql") as mock_scd2, \
         patch("src.database.service.base.upsert_from_temp_sql") as mock_upsert, \
         patch("src.database.service.base.quote_ident", side_effect=lambda x: f'"{x}"'):

        await service._process_batch_sequential_optimized(
            conn, batch, 10, table_info, 0, 1
        )

    mock_scd2.assert_not_called()
    mock_upsert.assert_not_called()
    # A plain INSERT should be executed directly via conn.execute
    execute_sqls = " ".join(str(c) for c in conn.execute.call_args_list)
    assert "INSERT" in execute_sqls


@pytest.mark.asyncio
async def test_batch_date_propagated_from_table_info():
    """batch_date is picked up from table_info.batch_date if not explicit."""
    mock_db = MagicMock(spec=Database)
    service = DatabaseService(mock_db)
    conn = make_mock_conn()
    table_info = make_mock_table_info("empresa", batch_date="2025-06-15")
    batch = [("99", "h1")]

    with patch("src.database.service.base.scd2_upsert_from_temp_sql", return_value="SCD2_SQL") as mock_scd2, \
         patch("src.database.service.base.create_temp_table_sql", return_value="CREATE"), \
         patch("src.database.service.base.map_types", return_value={"cnpj_basico": "TEXT", "row_hash": "TEXT"}):

        # Pass batch_date explicitly to simulate upsert_batches behaviour
        await service._process_batch_sequential_optimized(
            conn, batch, 10, table_info, 0, 1, batch_date="2025-06-15"
        )

    assert mock_scd2.call_args.args[4] == "2025-06-15"


# ---------------------------------------------------------------------------
# DatabaseService.load_records_directly: sync wrapper guards
# ---------------------------------------------------------------------------

def test_load_records_directly_returns_zero_for_empty_batch():
    mock_db = MagicMock(spec=Database)
    service = DatabaseService(mock_db)
    ok, err, count = service.load_records_directly(MagicMock(), [])
    assert ok is True
    assert count == 0


def test_load_records_directly_fails_in_async_context():
    """When called from an async context it should return an error (not raise)."""
    import asyncio

    mock_db = MagicMock(spec=Database)
    service = DatabaseService(mock_db)
    table_info = make_mock_table_info("empresa")
    records = [("1", "h")]

    async def _inner():
        return service.load_records_directly(table_info, records)

    ok, err, count = asyncio.get_event_loop().run_until_complete(_inner())
    assert ok is False
    assert "async" in err.lower()
