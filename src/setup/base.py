import os
from dotenv import load_dotenv
from typing import Tuple, Union
from psycopg2 import OperationalError
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import text

from setup.logging import logger
from utils.misc import makedir
from setup.config import DatabaseConfig
from database.schemas import Database
from database.engine import create_database_instance as create_db_engine

def load_environment_variables(env_file: str = '.env') -> None:
    """Load environment variables from a file."""
    env_path = os.path.join(os.getcwd(), env_file)
    load_dotenv(env_path)

def get_sink_folder() -> Tuple[str, str]:
    """
    Get the output and extracted file paths based on the environment variables or default paths.

    Returns:
        Tuple[str, str]: A tuple containing the output file path and the extracted file path.
    """
    load_environment_variables()
    
    root_path = os.path.join(os.getcwd(), 'data')
    default_output_file_path = os.path.join(root_path, 'DOWNLOAD_FILES')
    default_input_file_path = os.path.join(root_path, 'EXTRACTED_FILES')
    
    output_route = os.getenv('DOWNLOAD_PATH', default_output_file_path)
    extract_route = os.getenv('EXTRACT_PATH', default_input_file_path)
    
    output_folder = os.path.join(root_path, output_route)
    extract_folder = os.path.join(root_path, extract_route)
        
    makedir(output_folder)
    makedir(extract_folder)
    
    return output_folder, extract_folder

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
            logger.info(f'Connection to database "{database_config.database}" established!')
    except OperationalError as e:
        logger.error(f"Error connecting to the database: {e}")
        return None
    # Do not create tables here; use Database.create_tables() after instantiation
    return Database(database_obj.engine, database_obj.session_maker, base)
