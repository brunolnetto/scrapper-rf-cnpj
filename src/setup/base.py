import os
from dotenv import load_dotenv
from typing import Tuple, Union
from psycopg2 import OperationalError
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from setup.logging import logger
from utils.misc import makedir
from database.models import Base
from database.schemas import Database
<<<<<<< HEAD
from database.engine import create_database as create_db_engine
=======
from database.engine import create_database_instance
>>>>>>> ace4e8e (refactor: review names and fix methods)
from utils.docker import get_postgres_host

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

def get_db_uri(db_name: str) -> str:
    """Construct the database URI from environment variables."""
    load_environment_variables()
    
    host = get_postgres_host() if os.getenv('ENVIRONMENT') == 'docker' else os.getenv('POSTGRES_HOST', 'localhost')
    port = int(os.getenv('POSTGRES_PORT', '5432'))
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    
    return f'postgresql://{user}:{password}@{host}:{port}/{db_name}'

def init_database(db_name: str) -> Union[Database, None]:
    """
    Connects to a PostgreSQL database using environment variables for connection details.

    Returns:
        Database: A NamedTuple with engine and conn attributes for the database connection.
        None: If there was an error connecting to the database.
    """
    db_uri = get_db_uri(db_name)
    database_obj = create_db_engine(db_uri)

    try:
        if not database_exists(db_uri):
            create_database(db_uri)
    except OperationalError as e:
        logger.error(f"Error creating database: {e}")
        return None
    
    try:
        with database_obj.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info('Connection to the database established!')
    except OperationalError as e:
        logger.error(f"Error connecting to the database: {e}")
        return None
    
    try:
        Base.metadata.create_all(database_obj.engine)
    except Exception as e:
        logger.error(f"Error creating tables in the database: {e}")
        return None
        
    return database_obj
