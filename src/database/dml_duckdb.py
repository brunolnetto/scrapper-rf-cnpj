"""
DuckDB-based data loading utilities for CNPJ ETL.

This module provides efficient data loading using DuckDB for Parquet files,
replacing the pandas-based CSV loading approach for better performance.
"""

import duckdb
import subprocess
import tempfile
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from setup.logging import logger
from core.constants import TABLES_INFO_DICT
from core.schemas import TableInfo
from database.schemas import Database
from utils.misc import update_progress

class DuckDBLoader:
    """Efficient data loader using DuckDB for Parquet files."""
    
    def __init__(self, database: Database):
        self.database = database
        self.duckdb_available = self._check_duckdb_availability()
        
    def _check_duckdb_availability(self) -> bool:
        """Check if DuckDB is available for use."""
        try:
            import duckdb
            return True
        except ImportError:
            logger.warning("DuckDB not available. Install with: pip install duckdb")
            return False
    
    def _get_psql_connection_params(self) -> Dict[str, str]:
        """Extract PostgreSQL connection parameters from database URI."""
        uri = self.database.engine.url
        
        return {
            'host': uri.host or 'localhost',
            'port': str(uri.port or 5432),
            'user': uri.username or 'postgres',
            'password': uri.password or '',
            'database': uri.database or 'postgres'
        }
    
    def _create_table_schema(self, table_info: TableInfo) -> str:
        """Create table schema SQL for the given table info."""
        columns = []
        for col in table_info.columns:
            # Use TEXT for all columns initially, let PostgreSQL handle type inference
            columns.append(f'"{col}" TEXT')
        
        return f"""
        CREATE TABLE IF NOT EXISTS {table_info.table_name} (
            {', '.join(columns)}
        );
        """
    
    def _drop_table_if_exists(self, table_name: str) -> None:
        """Drop table if it exists."""
        conn_params = self._get_psql_connection_params()
        
        drop_cmd = [
            'psql', '-h', conn_params['host'], '-p', conn_params['port'],
            '-U', conn_params['user'], '-d', conn_params['database'],
            '-c', f'DROP TABLE IF EXISTS {table_name};'
        ]
        
        try:
            subprocess.run(
                drop_cmd, 
                check=True, 
                env={'PGPASSWORD': conn_params['password']},
                capture_output=True
            )
            logger.info(f"Dropped existing table: {table_name}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to drop table {table_name}: {e}")
            raise
    
    def _create_table(self, table_info: TableInfo) -> None:
        """Create table with proper schema."""
        conn_params = self._get_psql_connection_params()
        create_sql = self._create_table_schema(table_info)
        
        create_cmd = [
            'psql', '-h', conn_params['host'], '-p', conn_params['port'],
            '-U', conn_params['user'], '-d', conn_params['database'],
            '-c', create_sql
        ]
        
        try:
            subprocess.run(
                create_cmd, 
                check=True, 
                env={'PGPASSWORD': conn_params['password']},
                capture_output=True
            )
            logger.info(f"Created table: {table_info.table_name}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create table {table_info.table_name}: {e}")
            raise
    
    def load_parquet_file(
        self, 
        table_info: TableInfo, 
        parquet_file: Path,
        show_progress: bool = True
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load a single Parquet file into PostgreSQL using DuckDB.
        
        Args:
            table_info: Table information
            parquet_file: Path to Parquet file
            show_progress: Whether to show progress updates
            
        Returns:
            Tuple of (success, error_message, rows_loaded)
        """
        if not self.duckdb_available:
            return False, "DuckDB not available", 0
        
        if not parquet_file.exists():
            return False, f"Parquet file not found: {parquet_file}", 0
        
        try:
            # Step 1: Drop and recreate table
            self._drop_table_if_exists(table_info.table_name)
            self._create_table(table_info)
            
            # Step 2: Use DuckDB to read Parquet and convert to CSV
            logger.info(f"Loading Parquet file: {parquet_file}")
            
            # Connect to DuckDB and read Parquet
            con = duckdb.connect()
            
            # Get row count for progress tracking
            row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')").fetchone()[0]
            
            if show_progress:
                logger.info(f"Processing {row_count:,} rows from {parquet_file.name}")
            
            # Convert Parquet to CSV string using DuckDB
            csv_data = con.execute(f"SELECT * FROM read_parquet('{parquet_file}')").df().to_csv(
                index=False, header=False
            )
            con.close()
            
            # Step 3: Write CSV data to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_csv:
                temp_csv.write(csv_data)
                temp_csv_path = temp_csv.name
            
            try:
                # Step 4: Use psql \copy to load data efficiently
                conn_params = self._get_psql_connection_params()
                
                copy_cmd = [
                    'psql', '-h', conn_params['host'], '-p', conn_params['port'],
                    '-U', conn_params['user'], '-d', conn_params['database'],
                    '-c', f"\\copy {table_info.table_name} FROM '{temp_csv_path}' WITH (FORMAT CSV, DELIMITER ',');"
                ]
                
                result = subprocess.run(
                    copy_cmd,
                    check=True,
                    env={'PGPASSWORD': conn_params['password']},
                    capture_output=True,
                    text=True
                )
                
                if show_progress:
                    update_progress(row_count, row_count, parquet_file.name)
                    print()  # New line after progress
                
                logger.info(f"Successfully loaded {row_count:,} rows into {table_info.table_name}")
                return True, None, row_count
                
            finally:
                # Clean up temporary file
                os.unlink(temp_csv_path)
                
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to load data: {e.stderr if e.stderr else str(e)}"
            logger.error(error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Unexpected error loading {parquet_file}: {e}"
            logger.error(error_msg)
            return False, error_msg, 0
    
    def load_table_from_parquet_files(
        self, 
        table_info: TableInfo, 
        parquet_files: List[Path],
        show_progress: bool = True
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load multiple Parquet files for a single table.
        
        Args:
            table_info: Table information
            parquet_files: List of Parquet file paths
            show_progress: Whether to show progress updates
            
        Returns:
            Tuple of (success, error_message, total_rows_loaded)
        """
        if not parquet_files:
            return False, "No Parquet files provided", 0
        
        logger.info(f"Loading {len(parquet_files)} Parquet files for table: {table_info.table_name}")
        
        total_rows = 0
        successful_files = 0
        
        for i, parquet_file in enumerate(parquet_files):
            logger.info(f"Processing file {i+1}/{len(parquet_files)}: {parquet_file.name}")
            
            success, error, rows = self.load_parquet_file(
                table_info, parquet_file, show_progress
            )
            
            if success:
                total_rows += rows
                successful_files += 1
            else:
                logger.error(f"Failed to load {parquet_file}: {error}")
                # Continue with other files instead of failing completely
        
        if successful_files == len(parquet_files):
            logger.info(f"Successfully loaded all {len(parquet_files)} files for {table_info.table_name}")
            return True, None, total_rows
        elif successful_files > 0:
            logger.warning(f"Partially loaded {successful_files}/{len(parquet_files)} files for {table_info.table_name}")
            return True, f"Partial load: {successful_files}/{len(parquet_files)} files", total_rows
        else:
            return False, "Failed to load any files", 0

def populate_table_duckdb(
    database: Database, 
    table_name: str, 
    parquet_dir: Path,
    table_files: List[str] = None
) -> Tuple[bool, Optional[str], int]:
    """
    Populate a table using DuckDB and Parquet files.
    
    Args:
        database: Database connection
        table_name: Name of the table to populate
        parquet_dir: Directory containing Parquet files
        table_files: List of CSV file names (for compatibility, not used with Parquet)
        
    Returns:
        Tuple of (success, error_message, rows_loaded)
    """
    from database.dml import table_name_to_table_info
    
    # Get table info
    table_info = table_name_to_table_info(table_name)
    
    # Find Parquet file for this table
    parquet_file = parquet_dir / f"{table_name}.parquet"
    
    if not parquet_file.exists():
        return False, f"No Parquet file found for table {table_name}: {parquet_file}", 0
    
    # Create loader and load data
    loader = DuckDBLoader(database)
    return loader.load_parquet_file(table_info, parquet_file)

def populate_table_with_filenames_duckdb(
    database: Database, 
    table_info: TableInfo, 
    parquet_dir: Path,
    filenames: List[str] = None
) -> Tuple[bool, Optional[str], int]:
    """
    Populate a table with multiple Parquet files using DuckDB.
    
    Args:
        database: Database connection
        table_info: Table information
        parquet_dir: Directory containing Parquet files
        filenames: List of file names (for compatibility, not used with Parquet)
        
    Returns:
        Tuple of (success, error_message, total_rows_loaded)
    """
    # Find Parquet file for this table
    parquet_file = parquet_dir / f"{table_info.table_name}.parquet"
    
    if not parquet_file.exists():
        return False, f"No Parquet file found for table {table_info.table_name}: {parquet_file}", 0
    
    # Create loader and load data
    loader = DuckDBLoader(database)
    return loader.load_parquet_file(table_info, parquet_file)