from os import path, getcwd
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import numpy as np

from utils.misc import delete_var, update_progress, get_line_count
from core.constants import TABLES_INFO_DICT
from core.schemas import TableInfo
from database.schemas import Database
from setup.logging import logger

MAX_RETRIES=3

##########################################################################
## LOAD AND TRANSFORM
##########################################################################
# Chunk size for download and extraction 
READ_CHUNK_SIZE = 10000
def populate_table_with_filename(
    database: Database, table_info: TableInfo, source_folder: str, filename: str
): 
    """
    Populates a table in the database with data from a file.

    Args:
        database (Database): The database object.
        table_info (TableInfo): The table information object.
        to_folder (str): The folder path where the file is located.
        filename (str): The name of the file.

    Returns:
        None
    """
    dtypes = { column: str for column in table_info.columns }
    extracted_file_path = path.join(getcwd(), source_folder, filename)
    
    csv_read_props = {
        "filepath_or_buffer": extracted_file_path,
        "sep": ';', 
        "skiprows": 0,
        "chunksize": READ_CHUNK_SIZE, 
        "dtype": dtypes,
        "header": None, 
        "encoding": table_info.encoding,
        "low_memory": False,
        "memory_map": True,
        "on_bad_lines": 'warn',
        "keep_default_na": False,
        "encoding_errors": 'replace'
    }

    row_count = get_line_count(extracted_file_path)
    chunk_count = np.ceil(row_count/READ_CHUNK_SIZE)
    for index, df_chunk in enumerate(pd.read_csv(**csv_read_props)):
        for retry_count in range(MAX_RETRIES):
            try:
                # Transform chunk
                df_chunk = df_chunk.reset_index()

                # Remove index column
                del df_chunk['index']

                # Cast for string
                for column in df_chunk.columns:
                    df_chunk[column] = df_chunk[column].astype(str)

                # Rename columns
                df_chunk.columns = table_info.columns
                df_chunk = table_info.transform_map(df_chunk)

                update_progress(index*READ_CHUNK_SIZE, row_count, filename)

                # Query arguments
                query_args = {
                    "name": table_info.table_name,
                    "if_exists": 'append',
                    "con": database.engine,
                    "index": index,
                    "chunksize": 1000,
                }

                # Break the dataframe into chunks
                df_chunk.to_sql(**query_args)
                break

            except Exception as e:
                progress=f"({retry_count+1}/{MAX_RETRIES})"
                summary=f"Failed to insert chunk {index} of file {extracted_file_path}"
                logger.error(f'{progress} {summary}: {e}.')

            if(retry_count == MAX_RETRIES-1):
                raise Exception(f'Failed to insert chunk {index} of file {extracted_file_path}.')

        if(index == chunk_count-1):
            break
    
    update_progress(row_count, row_count, filename)
    print()
    
    logger.info('File ' + filename + ' inserted with success on database!')

    delete_var(df_chunk)

def populate_table_with_filenames(
    database: Database, table_info: TableInfo, source_folder: str, filenames: list
):
    """
    Populates a table in the database with data from multiple files.

    Args:
        database (Database): The database object.
        table_info (TableInfo): The table information object.
        source_folder (str): The folder path where the files are located.
        filenames (list): A list of file names.

    Returns:
        None
    """
    title=f'Table files {table_info.label.upper()}:'
    logger.info(title)
    
    # Drop table (if exists)
    with database.engine.begin() as conn:
        query=text(f"DROP TABLE IF EXISTS {table_info.table_name};")
        
        # Execute the compiled SQL string
        try:
            conn.execute(query)
        except OperationalError as e:
            logger.error(f"Failed to erase table {table_info.table_name}: {e}")
    
    # Inserir dados
    for filename in filenames:
        logger.info('Current file: ' + filename + '')
        try:
            for retry_count in range(MAX_RETRIES):
                try:
                    file_path=path.join(getcwd(), source_folder, filename)
                    populate_table_with_filename(database, table_info, source_folder, filename)
                    break

                except Exception as e:
                    logger.error(f'({retry_count+1}/{MAX_RETRIES}) Failed to insert file {file_path}: {e}')            
            
            if(retry_count == MAX_RETRIES-1):
                raise Exception(f'Failed to insert file {file_path}.')

        except Exception as e:
            summary=f'Failed to save file {file_path} on table {table_info.table_name}'
            logger.error(f'{summary}: {e}')
    
    logger.info(f'Finished files of table {table_info.label}!')


def table_name_to_table_info(table_name: str) -> TableInfo:
    table_info_dict = TABLES_INFO_DICT[table_name]
    
    # Get table info
    label = table_info_dict['label']
    zip_group = table_info_dict['group']
    columns = table_info_dict['columns']
    encoding = table_info_dict['encoding']
    transform_map = table_info_dict.get('transform_map', lambda x: x)
    expression = table_info_dict['expression']

    # Create table info object
    return TableInfo(label, zip_group, table_name, columns, encoding, transform_map, expression)


def populate_table(
    database: Database, table_name: str, from_folder: str, table_files: list
):
    """
    Populates a table in the database with data from multiple files.

    Args:
        database (Database): The database object.
        table_name (str): The name of the table.
        from_folder (str): The folder path where the files are located.
        table_files (list): A list of file names.

    Returns:
        None
    """
    table_info = table_name_to_table_info(table_name)

    populate_table_with_filenames(database, table_info, from_folder, table_files)


def generate_tables_indices(engine, tables_to_indices):
    """
    Generates indices for the database tables.

    Args:
        engine: The database engine.

    Returns:
        None
    """
    # Criar índices na base de dados:
    logger.info(f"Generating indices on database tables {list(tables_to_indices.keys())}")

    # Index metadata
    fields_list = [
        (table_name, column_name, f'{table_name}_{column_name}') 
        for table_name, columns  in tables_to_indices.items()
        for column_name in columns
    ]
    mask="create index {index_name} on {table_name} using btree(\"{column_name}\"); commit;"
    
    # Execute index queries
    try:
        with engine.connect() as conn:
            for table_name_, column_name_, index_name_ in fields_list:
                # Compile a SQL string
                query=text(
                    mask.format(
                        table_name=table_name_, 
                        column_name=column_name_, 
                        index_name=index_name_, 
                    )
                )

                # Execute the compiled SQL string
                try:
                    conn.execute(query)
                    print(f"Index {index_name_} created on column {column_name_} of table {table_name_}.")

                except Exception as e:
                    msg=f"Error generating index {index_name_} on column `{column_name_}` for table {table_name_}"
                    logger.error(f"{msg}: {e}")

                message = f"Index {index_name_} generated on column `{column_name_}` for table {table_name_}"
                logger.info(message)

        message = f"Index created on tables {list(tables_to_indices.keys())}"
        logger.info(message)

    except Exception as e:
        logger.error(f"Failed to generate indices: {e}") 

