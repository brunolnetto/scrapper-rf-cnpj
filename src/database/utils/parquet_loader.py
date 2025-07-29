"""
Parquet to PostgreSQL loading utilities.

This module provides efficient methods for loading Parquet files into PostgreSQL
using various approaches: SQLAlchemy + pandas, DuckDB + COPY, and direct streaming.
"""

import os
import tempfile
import subprocess
import time
import psutil
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple, Union
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from setup.logging import logger
from database.schemas import Database
from core.schemas import TableInfo
from core.constants import TABLES_INFO_DICT

# Performance tracking
@dataclass
class LoadMetrics:
    """Metrics for data loading operations"""
    method: str
    file_path: Path
    table_name: str
    rows_loaded: int
    columns: int
    load_time_seconds: float
    memory_peak_mb: float
    throughput_mb_per_second: float
    throughput_rows_per_second: float
    success: bool
    error_message: Optional[str] = None

class PerformanceTracker:
    """Track memory usage and timing during operations"""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.start_memory = None
        self.peak_memory = 0
    
    def start(self):
        """Start tracking"""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / (1024 * 1024)  # MB
        self.peak_memory = self.start_memory
    
    def update_peak_memory(self):
        """Update peak memory usage"""
        current_memory = self.process.memory_info().rss / (1024 * 1024)  # MB
        self.peak_memory = max(self.peak_memory, current_memory)
    
    def stop(self) -> tuple[float, float]:
        """Stop tracking and return elapsed time and peak memory"""
        elapsed_time = time.time() - self.start_time
        self.update_peak_memory()
        return elapsed_time, self.peak_memory - self.start_memory

class ParquetLoader:
    """
    Efficient Parquet to PostgreSQL loader with multiple loading strategies.
    
    Supports:
    - SQLAlchemy + pandas (good for small-medium files)
    - DuckDB + COPY (best for large files)
    - Direct streaming (memory efficient)
    """
    
    def __init__(self, database: Database, connection_string: Optional[str] = None):
        """
        Initialize the ParquetLoader.
        
        Args:
            database: Database connection object
            connection_string: Optional SQLAlchemy connection string for direct operations
        """
        self.database = database
        self.connection_string = connection_string or str(database.engine.url)
        self.duckdb_available = self._check_duckdb_availability()
        self._parse_connection_string()
    
    def _check_duckdb_availability(self) -> bool:
        """Check if DuckDB is available for fast loading"""
        try:
            import duckdb
            return True
        except ImportError:
            logger.warning("DuckDB not available. Install with: pip install duckdb")
            return False
    
    def _parse_connection_string(self):
        """Parse PostgreSQL connection string into components"""
        if self.connection_string.startswith('postgresql://'):
            parts = self.connection_string.replace('postgresql://', '').split('@')
            if len(parts) != 2:
                raise ValueError("Invalid PostgreSQL connection string format")
            
            user_pass = parts[0].split(':')
            host_port_db = parts[1].split('/')
            
            if len(user_pass) != 2 or len(host_port_db) != 2:
                raise ValueError("Invalid PostgreSQL connection string format")
            
            self.user, self.password = user_pass
            host_port, self.database_name = host_port_db
            
            if ':' in host_port:
                self.host, self.port = host_port.split(':')
            else:
                self.host, self.port = host_port, '5432'
        else:
            raise ValueError("Only SQLAlchemy connection string format supported")
    
    def load_parquet_sqlalchemy(
        self, 
        parquet_file: Path, 
        table_name: str, 
        table_info: Optional[TableInfo] = None,
        if_exists: str = 'replace',
        chunk_size: int = 10000
    ) -> LoadMetrics:
        """
        Load Parquet file using SQLAlchemy + pandas.
        
        Args:
            parquet_file: Path to Parquet file
            table_name: Target table name
            table_info: Optional table info for schema validation
            if_exists: Action if table exists ('replace', 'append', 'fail')
            chunk_size: Chunk size for loading
            
        Returns:
            LoadMetrics with performance data
        """
        tracker = PerformanceTracker()
        tracker.start()
        
        try:
            # Read Parquet with pandas
            logger.info(f"Reading Parquet file: {parquet_file}")
            df = pd.read_parquet(parquet_file)
            
            tracker.update_peak_memory()
            
            # Apply table info transformations if provided
            if table_info:
                df = self._apply_table_info(df, table_info)
            
            logger.info(f"Loading {len(df)} rows into table '{table_name}'")
            
            # Load to PostgreSQL using pandas to_sql
            df.to_sql(
                name=table_name,
                con=self.database.engine,
                if_exists=if_exists,
                index=False,
                method='multi',  # Use PostgreSQL COPY
                chunksize=chunk_size
            )
            
            elapsed_time, memory_used = tracker.stop()
            file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
            
            return LoadMetrics(
                method="sqlalchemy",
                file_path=parquet_file,
                table_name=table_name,
                rows_loaded=len(df),
                columns=len(df.columns),
                load_time_seconds=elapsed_time,
                memory_peak_mb=memory_used,
                throughput_mb_per_second=file_size_mb / elapsed_time if elapsed_time > 0 else 0,
                throughput_rows_per_second=len(df) / elapsed_time if elapsed_time > 0 else 0,
                success=True
            )
            
        except Exception as e:
            elapsed_time, memory_used = tracker.stop()
            error_msg = f"Error loading Parquet via SQLAlchemy: {e}"
            logger.error(error_msg)
            
            return LoadMetrics(
                method="sqlalchemy",
                file_path=parquet_file,
                table_name=table_name,
                rows_loaded=0,
                columns=0,
                load_time_seconds=elapsed_time,
                memory_peak_mb=memory_used,
                throughput_mb_per_second=0,
                throughput_rows_per_second=0,
                success=False,
                error_message=error_msg
            )
    
    def load_parquet_duckdb(
        self, 
        parquet_file: Path, 
        table_name: str, 
        table_info: Optional[TableInfo] = None,
        if_exists: str = 'replace'
    ) -> LoadMetrics:
        """
        Load Parquet file using DuckDB + PostgreSQL COPY (fastest method).
        
        Args:
            parquet_file: Path to Parquet file
            table_name: Target table name
            table_info: Optional table info for schema validation
            if_exists: Action if table exists ('replace', 'append', 'fail')
            
        Returns:
            LoadMetrics with performance data
        """
        if not self.duckdb_available:
            error_msg = "DuckDB not available. Install with: pip install duckdb"
            logger.error(error_msg)
            return LoadMetrics(
                method="duckdb",
                file_path=parquet_file,
                table_name=table_name,
                rows_loaded=0,
                columns=0,
                load_time_seconds=0,
                memory_peak_mb=0,
                throughput_mb_per_second=0,
                throughput_rows_per_second=0,
                success=False,
                error_message=error_msg
            )
        
        tracker = PerformanceTracker()
        tracker.start()
        
        try:
            import duckdb
            
            # Handle table existence
            if if_exists == 'replace':
                self._drop_table_if_exists(table_name)
                logger.info(f"Dropped existing table '{table_name}'")
            
            # Create table structure
            self._create_table_from_parquet(parquet_file, table_name, table_info)
            
            # Copy data using DuckDB Python API -> CSV -> PostgreSQL COPY
            logger.info(f"Starting DuckDB -> PostgreSQL COPY for table '{table_name}'")
            
            # Use DuckDB Python API to read Parquet and output to CSV string
            con = duckdb.connect()
            csv_data = con.execute(f"SELECT * FROM read_parquet('{parquet_file.absolute()}')").df().to_csv(index=False, header=False)
            con.close()
            
            # Write CSV data to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_csv:
                temp_csv.write(csv_data)
                temp_csv_path = temp_csv.name
            
            try:
                # Use psql \copy (client-side) instead of COPY (server-side)
                copy_cmd = [
                    'psql', '-h', self.host, '-p', self.port, '-U', self.user, 
                    '-d', self.database_name, '-c', 
                    f'\\copy {table_name} FROM \'{temp_csv_path}\' WITH (FORMAT CSV);'
                ]
                subprocess.run(copy_cmd, check=True, env={'PGPASSWORD': self.password})
                
                elapsed_time, memory_used = tracker.stop()
                file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
                
                # Get row count for metrics
                row_count = self._get_table_row_count(table_name)
                
                return LoadMetrics(
                    method="duckdb",
                    file_path=parquet_file,
                    table_name=table_name,
                    rows_loaded=row_count,
                    columns=self._get_table_column_count(table_name),
                    load_time_seconds=elapsed_time,
                    memory_peak_mb=memory_used,
                    throughput_mb_per_second=file_size_mb / elapsed_time if elapsed_time > 0 else 0,
                    throughput_rows_per_second=row_count / elapsed_time if elapsed_time > 0 else 0,
                    success=True
                )
                
            finally:
                # Clean up temporary file
                os.unlink(temp_csv_path)
            
        except Exception as e:
            elapsed_time, memory_used = tracker.stop()
            error_msg = f"Error loading Parquet via DuckDB: {e}"
            logger.error(error_msg)
            
            return LoadMetrics(
                method="duckdb",
                file_path=parquet_file,
                table_name=table_name,
                rows_loaded=0,
                columns=0,
                load_time_seconds=elapsed_time,
                memory_peak_mb=memory_used,
                throughput_mb_per_second=0,
                throughput_rows_per_second=0,
                success=False,
                error_message=error_msg
            )
    
    def load_parquet_streaming(
        self, 
        parquet_file: Path, 
        table_name: str, 
        table_info: Optional[TableInfo] = None,
        if_exists: str = 'replace',
        batch_size: int = 10000
    ) -> LoadMetrics:
        """
        Load Parquet file using streaming approach (memory efficient).
        
        Args:
            parquet_file: Path to Parquet file
            table_name: Target table name
            table_info: Optional table info for schema validation
            if_exists: Action if table exists ('replace', 'append', 'fail')
            batch_size: Batch size for streaming
            
        Returns:
            LoadMetrics with performance data
        """
        tracker = PerformanceTracker()
        tracker.start()
        
        try:
            # Read Parquet with polars in streaming mode
            logger.info(f"Streaming Parquet file: {parquet_file}")
            
            # Create table if needed
            if if_exists == 'replace':
                self._drop_table_if_exists(table_name)
                self._create_table_from_parquet(parquet_file, table_name, table_info)
            
            # Stream data in batches
            total_rows = 0
            with pl.scan_parquet(parquet_file) as lazy_df:
                for batch in lazy_df.iter_chunks(chunk_size=batch_size):
                    df_batch = pl.DataFrame(batch)
                    
                    # Apply table info transformations if provided
                    if table_info:
                        df_batch = self._apply_table_info_polars(df_batch, table_info)
                    
                    # Convert to pandas and load
                    df_pandas = df_batch.to_pandas()
                    df_pandas.to_sql(
                        name=table_name,
                        con=self.database.engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=batch_size
                    )
                    
                    total_rows += len(df_batch)
                    tracker.update_peak_memory()
            
            elapsed_time, memory_used = tracker.stop()
            file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
            
            return LoadMetrics(
                method="streaming",
                file_path=parquet_file,
                table_name=table_name,
                rows_loaded=total_rows,
                columns=self._get_table_column_count(table_name),
                load_time_seconds=elapsed_time,
                memory_peak_mb=memory_used,
                throughput_mb_per_second=file_size_mb / elapsed_time if elapsed_time > 0 else 0,
                throughput_rows_per_second=total_rows / elapsed_time if elapsed_time > 0 else 0,
                success=True
            )
            
        except Exception as e:
            elapsed_time, memory_used = tracker.stop()
            error_msg = f"Error loading Parquet via streaming: {e}"
            logger.error(error_msg)
            
            return LoadMetrics(
                method="streaming",
                file_path=parquet_file,
                table_name=table_name,
                rows_loaded=0,
                columns=0,
                load_time_seconds=elapsed_time,
                memory_peak_mb=memory_used,
                throughput_mb_per_second=0,
                throughput_rows_per_second=0,
                success=False,
                error_message=error_msg
            )
    
    def load_table_from_parquet(
        self, 
        table_name: str, 
        parquet_file: Path, 
        method: str = 'auto',
        **kwargs
    ) -> LoadMetrics:
        """
        Load a table from Parquet file using the specified method.
        
        Args:
            table_name: Target table name
            parquet_file: Path to Parquet file
            method: Loading method ('auto', 'sqlalchemy', 'duckdb', 'streaming')
            **kwargs: Additional arguments for the loading method
            
        Returns:
            LoadMetrics with performance data
        """
        # Get table info if available
        table_info = self._get_table_info(table_name)
        
        # Auto-select best method based on file size
        if method == 'auto':
            file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
            if file_size_mb < 100:  # Small files
                method = 'sqlalchemy'
            elif self.duckdb_available:  # Large files with DuckDB available
                method = 'duckdb'
            else:  # Large files without DuckDB
                method = 'streaming'
        
        logger.info(f"Loading table '{table_name}' from {parquet_file} using method: {method}")
        
        if method == 'sqlalchemy':
            return self.load_parquet_sqlalchemy(parquet_file, table_name, table_info, **kwargs)
        elif method == 'duckdb':
            return self.load_parquet_duckdb(parquet_file, table_name, table_info, **kwargs)
        elif method == 'streaming':
            return self.load_parquet_streaming(parquet_file, table_name, table_info, **kwargs)
        else:
            raise ValueError(f"Unknown loading method: {method}")
    
    def load_multiple_tables(
        self, 
        table_files: Dict[str, Path], 
        method: str = 'auto',
        max_workers: int = 4,
        **kwargs
    ) -> List[LoadMetrics]:
        """
        Load multiple tables from Parquet files in parallel.
        
        Args:
            table_files: Dict mapping table names to Parquet file paths
            method: Loading method ('auto', 'sqlalchemy', 'duckdb', 'streaming')
            max_workers: Maximum number of parallel workers
            **kwargs: Additional arguments for the loading methods
            
        Returns:
            List of LoadMetrics for each table
        """
        metrics = []
        
        if max_workers == 1:
            # Sequential loading
            for table_name, parquet_file in table_files.items():
                metric = self.load_table_from_parquet(table_name, parquet_file, method, **kwargs)
                metrics.append(metric)
        else:
            # Parallel loading
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        self.load_table_from_parquet, 
                        table_name, 
                        parquet_file, 
                        method, 
                        **kwargs
                    ): table_name 
                    for table_name, parquet_file in table_files.items()
                }
                
                for future in as_completed(futures):
                    table_name = futures[future]
                    try:
                        metric = future.result()
                        metrics.append(metric)
                        logger.info(f"Completed loading table '{table_name}': {metric.success}")
                    except Exception as e:
                        logger.error(f"Error loading table '{table_name}': {e}")
                        metrics.append(LoadMetrics(
                            method=method,
                            file_path=table_files[table_name],
                            table_name=table_name,
                            rows_loaded=0,
                            columns=0,
                            load_time_seconds=0,
                            memory_peak_mb=0,
                            throughput_mb_per_second=0,
                            throughput_rows_per_second=0,
                            success=False,
                            error_message=str(e)
                        ))
        
        return metrics
    
    def _apply_table_info(self, df: pd.DataFrame, table_info: TableInfo) -> pd.DataFrame:
        """Apply table info transformations to DataFrame"""
        # Rename columns if needed
        if len(df.columns) == len(table_info.columns):
            df.columns = table_info.columns
        
        # Apply transform function
        df = table_info.transform_map(df)
        
        return df
    
    def _apply_table_info_polars(self, df: pl.DataFrame, table_info: TableInfo) -> pl.DataFrame:
        """Apply table info transformations to Polars DataFrame"""
        # Rename columns if needed
        if len(df.columns) == len(table_info.columns):
            df = df.select(pl.all().name.map(dict(zip(df.columns, table_info.columns))))
        
        # Convert to pandas for transform function, then back to polars
        df_pandas = df.to_pandas()
        df_pandas = table_info.transform_map(df_pandas)
        return pl.from_pandas(df_pandas)
    
    def _get_table_info(self, table_name: str) -> Optional[TableInfo]:
        """Get table info from constants"""
        if table_name in TABLES_INFO_DICT:
            table_info_dict = TABLES_INFO_DICT[table_name]
            return TableInfo(
                label=table_info_dict['label'],
                zip_group=table_info_dict['group'],
                table_name=table_name,
                columns=table_info_dict['columns'],
                encoding=table_info_dict['encoding'],
                transform_map=table_info_dict.get('transform_map', lambda x: x),
                expression=table_info_dict['expression']
            )
        return None
    
    def _drop_table_if_exists(self, table_name: str):
        """Drop table if it exists"""
        with self.database.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
    
    def _create_table_from_parquet(self, parquet_file: Path, table_name: str, table_info: Optional[TableInfo] = None):
        """Create table structure from Parquet file"""
        if table_info:
            # Use table info to create proper schema
            columns = [f"{col} TEXT" for col in table_info.columns]
            create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        else:
            # Infer schema from Parquet file
            if self.duckdb_available:
                import duckdb
                con = duckdb.connect()
                sample_df = con.execute(f"SELECT * FROM read_parquet('{parquet_file.absolute()}') LIMIT 1").df()
                con.close()
                column_count = len(sample_df.columns)
                columns = [f"col_{i} TEXT" for i in range(column_count)]
                create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
            else:
                # Fallback to generic schema
                create_sql = f"CREATE TABLE {table_name} (data TEXT);"
        
        with self.database.engine.begin() as conn:
            conn.execute(text(create_sql))
    
    def _get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table"""
        try:
            with self.database.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
                return result.scalar()
        except Exception:
            return 0
    
    def _get_table_column_count(self, table_name: str) -> int:
        """Get column count for a table"""
        try:
            with self.database.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}';
                """))
                return result.scalar()
        except Exception:
            return 0

# Convenience functions for backward compatibility
def load_parquet_to_postgres(
    database: Database,
    parquet_file: Path,
    table_name: str,
    method: str = 'auto',
    **kwargs
) -> LoadMetrics:
    """
    Convenience function to load a single Parquet file to PostgreSQL.
    
    Args:
        database: Database connection
        parquet_file: Path to Parquet file
        table_name: Target table name
        method: Loading method ('auto', 'sqlalchemy', 'duckdb', 'streaming')
        **kwargs: Additional arguments
        
    Returns:
        LoadMetrics with performance data
    """
    loader = ParquetLoader(database)
    return loader.load_table_from_parquet(table_name, parquet_file, method, **kwargs)

def load_multiple_parquet_files(
    database: Database,
    table_files: Dict[str, Path],
    method: str = 'auto',
    max_workers: int = 4,
    **kwargs
) -> List[LoadMetrics]:
    """
    Convenience function to load multiple Parquet files to PostgreSQL.
    
    Args:
        database: Database connection
        table_files: Dict mapping table names to Parquet file paths
        method: Loading method ('auto', 'sqlalchemy', 'duckdb', 'streaming')
        max_workers: Maximum number of parallel workers
        **kwargs: Additional arguments
        
    Returns:
        List of LoadMetrics for each table
    """
    loader = ParquetLoader(database)
    return loader.load_multiple_tables(table_files, method, max_workers, **kwargs)