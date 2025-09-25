"""
ingestors.py
Batch generators for CSV and Parquet files.
Supports configurable encoding for CSV files.
"""
import pyarrow.parquet as pq
import csv
from typing import Iterable, List, Tuple

def batch_generator_parquet(path: str, headers: List[str], chunk_size: int = 20_000) -> Iterable[List[Tuple]]:
    """
    Generate batches from Parquet files.
    
    Args:
        path: Path to the Parquet file
        headers: Expected column headers
        chunk_size: Number of rows per batch
        
    Yields:
        List of tuples representing rows in the batch
    """
    pf = pq.ParquetFile(path)
    for record_batch in pf.iter_batches(batch_size=chunk_size):
        name_to_idx = {record_batch.schema.field(i).name: i for i in range(record_batch.num_columns)}

        rows = []
        for i in range(record_batch.num_rows):
            row = []
            for j, h in enumerate(headers):
                if h in name_to_idx:
                    # Standard case: column name matches
                    val = record_batch.column(name_to_idx[h])[i].as_py()
                    # normalize: str or None
                    row.append(str(val) if val is not None else None)
                elif str(j) in name_to_idx:
                    # Fallback: try numeric column name (for files with '0', '1', etc.)
                    val = record_batch.column(name_to_idx[str(j)])[i].as_py()
                    row.append(str(val) if val is not None else None)
                else:
                    row.append(None)
            rows.append(tuple(row))
        yield rows

def batch_generator_csv(path: str, headers: List[str], chunk_size: int = 20_000, 
                       encoding: str = 'utf-8') -> Iterable[List[Tuple]]:
    """
    Generate batches from CSV files with configurable encoding.
    
    Args:
        path: Path to the CSV file
        headers: Expected column headers
        chunk_size: Number of rows per batch
        encoding: File encoding (e.g., 'utf-8', 'latin-1')
        
    Yields:
        List of tuples representing rows in the batch
    """
    with open(path, 'r', newline='', encoding=encoding, errors='replace') as f:
        # Dialect detection
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            # Default to semicolon delimiter for Brazilian data
            dialect = csv.excel()
            dialect.delimiter = ';'
        
        # More robust header detection
        reader = csv.reader(f, dialect=dialect)
        first_row = next(reader, None)
        
        # Check if first row contains header-like strings (non-numeric, matches expected headers)
        skip_header = False
        if first_row:
            # If any expected header is found in first row, treat it as header
            if any(header in first_row for header in headers):
                skip_header = True
            # Or if first row looks like text headers (no numbers)
            elif all(not cell.isdigit() for cell in first_row if cell):
                skip_header = True
        
        # If we decided not to skip header, reset and include first row as data
        if not skip_header and first_row:
            f.seek(0)
            reader = csv.reader(f, dialect=dialect)
        
        # Batch processing
        batch = []
        for row in reader:
            # Ensure row has correct number of columns
            while len(row) < len(headers):
                row.append(None)
            
            # Truncate if row has too many columns
            if len(row) > len(headers):
                row = row[:len(headers)]
            
            batch.append(tuple(row))
            
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        
        # Yield remaining rows
        if batch:
            yield batch

def memory_aware_batch_generator_parquet(path: str, headers: List[str], chunk_size: int = 20_000, 
                                        memory_monitor=None) -> Iterable[List[Tuple]]:
    """
    Memory-aware Parquet batch generator.
    """
    import pyarrow.parquet as pq
    import gc
    
    try:
        pf = pq.ParquetFile(path)
        name_to_idx = None
        batch_count = 0
        
        for record_batch in pf.iter_batches(batch_size=chunk_size):
            # Build column mapping only once
            if name_to_idx is None:
                name_to_idx = {record_batch.schema.field(i).name: i for i in range(record_batch.num_columns)}
            
            # Memory check before processing batch
            if memory_monitor and memory_monitor.should_prevent_processing():
                raise MemoryError("Memory limit exceeded during Parquet processing")
            
            batch_rows = []
            for i in range(record_batch.num_rows):
                row = []
                for h in headers:
                    if h in name_to_idx:
                        val = record_batch.column(name_to_idx[h])[i].as_py()
                        row.append(str(val) if val is not None else None)
                    else:
                        row.append(None)
                batch_rows.append(tuple(row))
            
            yield batch_rows
            batch_count += 1
            
            # Clean up references
            del batch_rows
            del record_batch
            
            # Aggressive cleanup every few batches
            if batch_count % 5 == 0:
                gc.collect()
                if memory_monitor and memory_monitor.is_memory_pressure_high():
                    memory_monitor.perform_aggressive_cleanup()
                    
    finally:
        # Final cleanup
        gc.collect()


def memory_aware_batch_generator_csv(
    path: str, headers: List[str], chunk_size: int = 20_000, 
    encoding: str = 'utf-8', memory_monitor=None) -> Iterable[List[Tuple]]:
    """
    Memory-aware CSV batch generator.
    """
    import csv
    import gc
    
    with open(path, 'r', newline='', encoding=encoding, errors='replace') as f:
        # Use smaller sample for dialect detection
        sample = f.read(min(4096, chunk_size * 50))
        f.seek(0)
        
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel()
            dialect.delimiter = ';'
        
        reader = csv.reader(f, dialect=dialect)
        first_row = next(reader, None)
        
        # Header detection
        skip_header = False
        if first_row:
            if any(header in first_row for header in headers):
                skip_header = True
            elif all(not cell.isdigit() for cell in first_row if cell):
                skip_header = True
        
        if not skip_header and first_row:
            f.seek(0)
            reader = csv.reader(f, dialect=dialect)
        
        # Process with memory monitoring
        batch = []
        batch_count = 0
        row_count = 0
        
        for row in reader:
            # Memory check periodically
            if row_count % 1000 == 0 and memory_monitor:
                if memory_monitor.should_prevent_processing():
                    raise MemoryError(f"Memory limit exceeded at row {row_count}")
            
            # Process row
            while len(row) < len(headers):
                row.append(None)
            if len(row) > len(headers):
                row = row[:len(headers)]
            
            batch.append(tuple(row))
            row_count += 1
            
            # Yield batch when size reached
            if len(batch) >= chunk_size:
                yield batch
                batch = []
                batch_count += 1
                
                # Cleanup every few batches
                if batch_count % 5 == 0:
                    gc.collect()
                    if memory_monitor and memory_monitor.is_memory_pressure_high():
                        memory_monitor.perform_aggressive_cleanup()
        
        # Yield remaining rows
        if batch:
            yield batch
        
        # Final cleanup
        gc.collect()