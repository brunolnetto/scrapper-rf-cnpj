from os import path, getcwd
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from utils.dataframe import to_sql
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
    database: Database, 
    table_info: TableInfo,
    source_folder: str,
    filename: str
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
    len_cols=len(table_info.columns)
    data = {
        str(col): []
        for col in range(0, len_cols)
    }
    
    df = pd.DataFrame(data)
    dtypes = { column: str for column in table_info.columns }
    extracted_file_path = path.join(getcwd(), source_folder, filename)
    
    csv_read_props = {
        "filepath_or_buffer": extracted_file_path,
        "sep": ';', 
        "skiprows": 0,
        "chunksize": READ_CHUNK_SIZE, 
        "header": None, 
        "dtype": dtypes,
        "encoding": table_info.encoding,
        "low_memory": False,
        "memory_map": True,
        "on_bad_lines": 'skip'
    }
    
    row_count_estimation = get_line_count(extracted_file_path)
    
    for index, df_chunk in enumerate(pd.read_csv(**csv_read_props)):
        for retry_count in range(MAX_RETRIES):
            try:
                # Tratamento do arquivo antes de inserir na base:
                df_chunk = df_chunk.reset_index()
                del df_chunk['index']
                
                # Renomear colunas
                df_chunk.columns = table_info.columns
                df_chunk = table_info.transform_map(df_chunk)

                update_progress(index * READ_CHUNK_SIZE, row_count_estimation, filename)
                
                # Gravar dados no banco
                to_sql(
                    df_chunk, 
                    filename=extracted_file_path,
                    tablename=table_info.table_name, 
                    conn=database.engine, 
                    if_exists='append', 
                    index=False,
                    verbose=False
                )
                break

            except:
                logger.error(f'Failed to insert chunk {index} of file {extracted_file_path}. Attempt {retry_count+1}/{MAX_RETRIES}')

        if(retry_count == MAX_RETRIES-1):
                raise Exception(f'Failed to insert chunk {index} of arquivo {extracted_file_path}.')
    
    update_progress(row_count_estimation, row_count_estimation, filename)
    print()
    
    logger.info('File ' + filename + ' inserted with success on databae!')

    delete_var(df)

def populate_table_with_filenames(
    database: Database, 
    table_info: TableInfo, 
    source_folder: str,
    filenames: list
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
    with database.engine.connect() as conn:
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

                except:
                    logger.error(f'Failed to insert file {file_path}. Attempt {retry_count+1}/{MAX_RETRIES}')            
            
            if(retry_count == MAX_RETRIES-1):
                raise Exception(f'Failed to insert file file {file_path}.')

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
    return TableInfo(
        label, zip_group, table_name, columns, encoding, transform_map, expression
    )


def populate_table(
    database: Database, 
    table_name: str, 
    from_folder: str, 
    table_files: list
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


def generate_tables_indices(engine, tables):
    """
    Generates indices for the database tables.

    Args:
        engine: The database engine.

    Returns:
        None
    """
    # Criar índices na base de dados:
    logger.info(f"Generating indices on database tables {tables}")

    # Index metadata
    fields_tables = [([f'{table}_cnpj'], table) for table in tables]
    mask="create index {field} on {table}(cnpj_basico) using btree(\"cnpj_basico\"); commit;"
    queries = [ 
        text(mask.format(field=field_, table=table_))
        for field_, table_ in fields_tables 
    ]
    
    # Execute index queries
    try:
        with engine.connect() as conn:
            for field_, table_ in fields_tables:
                # Compile a SQL string
                query=text(mask.format(field=field_, table=table_))
                
                # Execute the compiled SQL string
                try:
                    conn.execute(query)
                except Exception as e:
                    logger.error(f"Error generating indices for table {table_}: {e}")
                
                message = f"Index {field_} generated on table {table_}, for column `cnpj_basico`"
                logger.info(message)
        
        message = f"Index {field_} created on tables {tables}"
        logger.info(message)
    
    except Exception as e:
        logger.error(f"Failed to generate indices: {e}") 

