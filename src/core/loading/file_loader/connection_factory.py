"""
connection_factory.py
Simple factory to create asyncpg pools from SQLAlchemy database connections.
Provides clean separation between SQLAlchemy ORM and high-performance asyncpg loading.
"""
import asyncpg
from ....database.schemas import Database
from ....setup.config import ConfigurationService
from ....setup.logging import logger


async def create_asyncpg_pool_from_sqlalchemy(
    database: Database, config: ConfigurationService
) -> asyncpg.Pool:
    """
    Create asyncpg connection pool from SQLAlchemy database configuration.
    
    Args:
        database: SQLAlchemy database instance
        config: Configuration service with async pool settings
        
    Returns:
        asyncpg.Pool: Ready-to-use connection pool
    """
    # Extract connection details from SQLAlchemy engine URL
    url = database.engine.url
    
    # Build asyncpg DSN from SQLAlchemy URL components
    if url.password:
        dsn = f"postgresql://{url.username}:{url.password}@{url.host}:{url.port or 5432}/{url.database}"
    else:
        dsn = f"postgresql://{url.username}@{url.host}:{url.port or 5432}/{url.database}"
    
    # Get pool configuration from ETL config
    min_size = getattr(config.etl, 'async_pool_min_size', 2)
    max_size = getattr(config.etl, 'async_pool_max_size', 10)
    
    logger.info(f"[ConnectionFactory] Creating asyncpg pool (min: {min_size}, max: {max_size})")
    logger.debug(f"[ConnectionFactory] DSN: {dsn.split('@')[0]}@***")  # Hide credentials in logs

    try:
        pool = await asyncpg.create_pool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            command_timeout=60,  # 60 second timeout for commands
            server_settings={
                'application_name': 'receita_cnpj_loader'
            }
        )
        logger.info("[ConnectionFactory] AsyncPG pool created successfully")
        return pool
    except Exception as e:
        logger.error(f"[ConnectionFactory] Failed to create asyncpg pool: {e}")
        raise


def extract_primary_keys(table_info) -> list[str]:
    """
    Extract primary key columns from table info.
    
    Args:
        table_info: TableInfo object with table metadata
        
    Returns:
        List of primary key column names
    """
    try:
        # Try to get primary keys from SQLAlchemy model
        from ....utils.model_utils import get_model_by_table_name
        model = get_model_by_table_name(table_info.table_name)
        pk_columns = [col.name for col in model.__table__.primary_key.columns]
        if pk_columns:
            return pk_columns
    except Exception:
        pass


def get_column_types_mapping(table_info) -> dict[str, str]:
    """
    Get PostgreSQL column type mapping from table info.
    
    Args:
        table_info: TableInfo object with table metadata
        
    Returns:
        Dictionary mapping column names to PostgreSQL types
    """
    try:
        from ....utils.model_utils import get_model_by_table_name
        model = get_model_by_table_name(table_info.table_name)
        
        types = {}
        for column in model.__table__.columns:
            # Map SQLAlchemy types to PostgreSQL types
            col_type = str(column.type).upper()
            if any(t in col_type for t in ['VARCHAR', 'TEXT', 'STRING']):
                types[column.name] = 'TEXT'
            elif any(t in col_type for t in ['FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL']):
                types[column.name] = 'DOUBLE PRECISION'
            elif any(t in col_type for t in ['INTEGER', 'BIGINT', 'INT']):
                types[column.name] = 'BIGINT'
            elif 'TIMESTAMP' in col_type:
                types[column.name] = 'TIMESTAMP WITH TIME ZONE'
            elif 'DATE' in col_type:
                types[column.name] = 'DATE'
            else:
                types[column.name] = 'TEXT'  # Safe default
        
        return types
        
    except Exception as e:
        logger.warning(f"Could not get column types for {table_info.table_name}: {e}")
        # Fallback to all TEXT
        return {col: 'TEXT' for col in table_info.columns}