"""
Unified data loading utilities for CNPJ ETL.

This module provides efficient data loading for both CSV and Parquet files,
with SQLAlchemy upsert operations for better performance and data integrity.
Supports both pandas and polars engines based on file format and preferences.
"""

import polars as pl
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import Table, MetaData, text

from ..setup.logging import logger
from ..core.constants import TABLES_INFO_DICT
from ..core.schemas import TableInfo
from ..utils.misc import update_progress, get_line_count, delete_var
from ..utils.model_utils import get_table_columns
from .schemas import Database

def table_name_to_table_info(table_name: str) -> TableInfo:
    """Convert table name to TableInfo object."""
    table_info_dict = TABLES_INFO_DICT[table_name]

    # Get table info
    label = table_info_dict["label"]
    zip_group = table_info_dict["group"]
    columns = get_table_columns(table_name)  # Derive from SQLAlchemy model
    encoding = table_info_dict["encoding"]
    transform_map = table_info_dict.get("transform_map", lambda x: x)
    expression = table_info_dict["expression"]

    # Create table info object
    return TableInfo(
        label, zip_group, table_name, columns, encoding, transform_map, expression
    )


def generate_tables_indices(engine, tables_to_indices):
    """
    Generates indices for the database tables.

    Args:
        engine: The database engine.
        tables_to_indices: Dict mapping table names to lists of column names for indexing.

    Returns:
        None
    """

    # Criar índices na base de dados:
    logger.info(
        f"Generating indices on database tables {list(tables_to_indices.keys())}"
    )

    # Index metadata
    fields_list = [
        (table_name, column_name, f"{table_name}_{column_name}")
        for table_name, columns in tables_to_indices.items()
        for column_name in columns
    ]
    mask = 'create index if not exists {index_name} on {table_name} using btree("{column_name}");'

    # Execute index queries
    try:
        with engine.connect() as conn:
            for table_name_, column_name_, index_name_ in fields_list:
                # Compile a SQL string
                query_str = mask.format(
                    table_name=table_name_,
                    column_name=column_name_,
                    index_name=index_name_,
                )
                query = text(query_str)
                print(query_str)
                # Execute the compiled SQL string
                try:
                    conn.execute(query)
                    print(
                        f"Index {index_name_} created on column {column_name_} of table {table_name_}."
                    )

                except Exception as e:
                    msg = f"Error generating index {index_name_} on column `{column_name_}` for table {table_name_}"
                    logger.error(f"{msg}: {e}")

                message = f"Index {index_name_} generated on column `{column_name_}` for table {table_name_}"
                logger.info(message)

        # Commit all index creations at once
        conn.commit()
        message = f"Index created on tables {list(tables_to_indices.keys())}"
        logger.info(message)

    except Exception as e:
        logger.error(f"Failed to generate indices: {e}")


class UnifiedLoader:
    """Unified data loader supporting both CSV and Parquet files."""

    def __init__(self, database: Database):
        self.database = database

    def _get_table_metadata(
        self, table_info: TableInfo
    ) -> Tuple[Table, List[str], List[str]]:
        """Get SQLAlchemy table metadata and key information."""
        metadata = MetaData()
        table = Table(
            table_info.table_name, metadata, autoload_with=self.database.engine
        )
        primary_keys = [col.name for col in table.primary_key.columns]
        update_columns = [col for col in table_info.columns if col not in primary_keys]
        return table, primary_keys, update_columns

    def _upsert_chunk(
        self,
        table: Table,
        rows: List[Dict],
        primary_keys: List[str],
        update_columns: List[str],
    ) -> None:
        """Perform upsert operation for a chunk of data."""
        with self.database.engine.begin() as conn:
            if primary_keys:
                stmt = pg_insert(table).values(rows)
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=primary_keys,
                    set_={col: getattr(stmt.excluded, col) for col in update_columns},
                )
                conn.execute(upsert_stmt)
            else:
                # Fallback: regular insert
                conn.execute(table.insert(), rows)

    def load_csv_file(
        self,
        table_info: TableInfo,
        csv_file: Union[str, Path],
        chunk_size: int = 50000,
        max_retries: int = 3,
        show_progress: bool = True,
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load a CSV file into PostgreSQL using pandas with SQLAlchemy upsert.

        Args:
            table_info: Table information
            csv_file: Path to CSV file
            chunk_size: Number of rows to process in each chunk
            max_retries: Maximum number of retry attempts per chunk
            show_progress: Whether to show progress updates

        Returns:
            Tuple of (success, error_message, rows_loaded)
        """
        csv_file = Path(csv_file)
        if not csv_file.exists():
            return False, f"CSV file not found: {csv_file}", 0

        try:
            logger.info(f"Loading CSV file: {csv_file}")

            # Get row count for progress tracking
            row_count = get_line_count(str(csv_file))
            if row_count is None or row_count == 0:
                logger.warning(
                    f"File {csv_file} is empty or could not be read. Skipping."
                )
                return True, None, 0

            if show_progress:
                logger.info(f"Processing {row_count:,} rows from {csv_file.name}")

            # Setup CSV reading parameters
            dtypes = {column: str for column in table_info.columns}
            csv_read_props = {
                "filepath_or_buffer": str(csv_file),
                "sep": ";",
                "skiprows": 0,
                "chunksize": chunk_size,
                "dtype": dtypes,
                "header": None,
                "encoding": table_info.encoding,
                "low_memory": False,
                "memory_map": True,
                "on_bad_lines": "warn",
                "keep_default_na": False,
                "encoding_errors": "replace",
            }

            # Get table metadata
            table, primary_keys, update_columns = self._get_table_metadata(table_info)

            chunk_count = np.ceil(row_count / chunk_size)
            processed_rows = 0

            # Process chunks
            for index, df_chunk in enumerate(pd.read_csv(**csv_read_props)):
                for retry_count in range(max_retries):
                    try:
                        # Transform chunk
                        df_chunk = df_chunk.reset_index(drop=True)
                        for column in df_chunk.columns:
                            df_chunk[column] = df_chunk[column].astype(str)
                        df_chunk.columns = table_info.columns
                        df_chunk = table_info.transform_map(df_chunk)

                        # Upsert data
                        rows = df_chunk.to_dict(orient="records")
                        self._upsert_chunk(table, rows, primary_keys, update_columns)

                        processed_rows += len(df_chunk)
                        if show_progress:
                            update_progress(processed_rows, row_count, csv_file.name)
                        break

                    except Exception as e:
                        progress = f"({retry_count + 1}/{max_retries})"
                        summary = f"Failed to upsert chunk {index} of file {csv_file}"
                        logger.error(f"{progress} {summary}: {e}.")

                        if retry_count == max_retries - 1:
                            raise Exception(
                                f"Failed to upsert chunk {index} of file {csv_file}."
                            )

                if index == chunk_count - 1:
                    break

            if show_progress:
                update_progress(row_count, row_count, csv_file.name)
                print()

            logger.info(
                f"File {csv_file.name} upserted with success on database {self.database}!"
            )
            delete_var(df_chunk)
            return True, None, processed_rows

        except Exception as e:
            error_msg = f"Unexpected error loading {csv_file}: {e}"
            logger.error(error_msg)
            return False, error_msg, 0

    def load_parquet_file(
        self,
        table_info: TableInfo,
        parquet_file: Union[str, Path],
        chunk_size: int = 50000,
        max_retries: int = 3,
        show_progress: bool = True,
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load a Parquet file into PostgreSQL using polars or pandas with SQLAlchemy upsert.

        Args:
            table_info: Table information
            parquet_file: Path to Parquet file
            chunk_size: Number of rows to process in each chunk
            max_retries: Maximum number of retry attempts per chunk
            show_progress: Whether to show progress updates

        Returns:
            Tuple of (success, error_message, rows_loaded)
        """
        parquet_file = Path(parquet_file)
        if not parquet_file.exists():
            return False, f"Parquet file not found: {parquet_file}", 0

        try:
            logger.info(f"Loading Parquet file: {parquet_file}")

            # Read parquet file
            df = pl.read_parquet(str(parquet_file)).to_pandas()

            row_count = len(df)
            if show_progress:
                logger.info(f"Processing {row_count:,} rows from {parquet_file.name}")

            # Get table metadata
            table, primary_keys, update_columns = self._get_table_metadata(table_info)

            # Process in chunks
            processed_rows = 0
            for index in range(0, row_count, chunk_size):
                df_chunk = df.iloc[index : index + chunk_size].copy()

                for retry_count in range(max_retries):
                    try:
                        # Transform chunk
                        for column in df_chunk.columns:
                            df_chunk[column] = df_chunk[column].astype(str)
                        df_chunk.columns = table_info.columns
                        df_chunk = table_info.transform_map(df_chunk)

                        # Upsert data
                        rows = df_chunk.to_dict(orient="records")
                        self._upsert_chunk(table, rows, primary_keys, update_columns)

                        processed_rows += len(df_chunk)
                        if show_progress:
                            update_progress(
                                processed_rows, row_count, parquet_file.name
                            )
                        break

                    except Exception as e:
                        progress = f"({retry_count + 1}/{max_retries})"
                        summary = (
                            f"Failed to upsert chunk {index} of file {parquet_file}"
                        )
                        logger.error(f"{progress} {summary}: {e}.")

                        if retry_count == max_retries - 1:
                            raise Exception(
                                f"Failed to upsert chunk {index} of file {parquet_file}."
                            )

            if show_progress:
                print()

            logger.info(
                f"File {parquet_file.name} upserted with success on database {self.database}!"
            )
            return True, None, processed_rows

        except Exception as e:
            error_msg = f"Unexpected error loading {parquet_file}: {e}"
            logger.error(error_msg)
            return False, error_msg, 0

    def load_file(
        self,
        table_info: TableInfo,
        file_path: Union[str, Path],
        chunk_size: int = 50000,
        max_retries: int = 3,
        show_progress: bool = True,
    ) -> Tuple[bool, Optional[str], int]:
        """
        Auto-detect file format and load accordingly using enhanced robust detection.

        Args:
            table_info: Table information
            file_path: Path to file (CSV or Parquet)
            chunk_size: Number of rows to process in each chunk
            max_retries: Maximum number of retry attempts per chunk
            show_progress: Whether to show progress updates

        Returns:
            Tuple of (success, error_message, rows_loaded)
        """
        file_path = Path(file_path)

        # Use enhanced file type detection with fallback
        try:
            from ..utils.file_detection import detect_file_type_enhanced
            file_type = detect_file_type_enhanced(file_path)
            logger.debug(f"Enhanced detection for {file_path.name}: {file_type}")
        except ImportError:
            # Fallback to basic detection if enhanced utils not available
            logger.warning("Enhanced file detection not available, using basic detection")
            if file_path.suffix.lower() == ".parquet":
                file_type = "parquet"
            elif (
                file_path.suffix.lower() in [".csv", ".txt"]
                or not file_path.suffix
                or "CSV" in file_path.suffix.upper()  # Handle .CNAECSV, .MOTICSV etc.
                or "csv" in file_path.name.lower()
            ):  # Handle files with csv in name
                file_type = "csv"
            else:
                file_type = "unknown"
        except Exception as e:
            logger.error(f"Enhanced file detection failed for {file_path}: {e}")
            # Ultimate fallback
            file_type = "csv"

        logger.debug(f"File {file_path.name} detected as type: {file_type}")

        if file_type == "parquet":
            return self.load_parquet_file(
                table_info, file_path, chunk_size, max_retries, show_progress
            )
        elif file_type == "csv":
            return self.load_csv_file(
                table_info, file_path, chunk_size, max_retries, show_progress
            )
        else:
            return (
                False,
                f"Unsupported or unrecognized file format: {file_path.name} (detected: {file_type})",
                0,
            )
