import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Union
from psycopg2 import OperationalError
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import text

from .config import DatabaseConfig
from ..database.engine import create_database_instance as create_db_engine, Database
from .logging import logger

def load_environment_variables(env_file: str = ".env") -> None:
    """Load environment variables from a file."""
    env_path = Path.cwd() / env_file
    load_dotenv(env_path)


def init_database(database_config: DatabaseConfig, base) -> Union[Database, None]:
    """
    Connects to a PostgreSQL database using environment variables for connection details.
    Returns a Database object with engine, session_maker, and base.
    Table creation should be done via Database.create_tables().

    Args:
        database_config: Database configuration object.
        base: The SQLAlchemy declarative base (e.g., MainBase or AuditBase).

    Returns:
        Database: A Database object for the connection.
        None: If there was an error connecting to the database.
    """
    db_uri = database_config.get_connection_string()
    database_obj = create_db_engine(db_uri, base)

    try:
        if not database_exists(db_uri):
            create_database(db_uri)
    except OperationalError as e:
        logger.error(f"Error creating database: {e}")
        return None
    try:
        with database_obj.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info(
                f'Connection to database "{database_config.database_name}" established!'
            )
    except OperationalError as e:
        logger.error(f"Error connecting to the database: {e}")
        return None
    # Do not create tables here; use Database.create_tables() after instantiation
    return Database(database_obj.engine, database_obj.session_maker, base)
