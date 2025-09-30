"""
Updated convert_csvs_to_parquet that uses the new multi-file strategies.
Replace your existing function with this version.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import psutil
import time

from ....setup.config.models import ConversionConfig
from ....setup.logging import logger
from ..memory.service import MemoryMonitor
from .models import LargeDatasetConfig, UltraConservativeConfig
from .utils import read_cgroup_memory_limit_bytes
from .converters import convert_table_csvs_multifile


def convert_csvs_to_parquet(
    audit_map: dict,
    unzip_dir: Path,
    output_dir: Path,
    config: Optional[ConversionConfig] = None,
    delimiter: str = ";"
):
    """
    Enhanced conversion with multi-file optimization.
    Key improvement: Processes all files for a table together instead of one-by-one.
    """
    if config is None:
        config = ConversionConfig()

    output_dir.mkdir(exist_ok=True)

    if not audit_map:
        logger.warning("No tables to convert")
        return

    logger.info(f"Starting conversion of {len(audit_map)} tables")

    # Enhanced system info logging
    try:
        memory = psutil.virtual_memory()
        logger.info(f"System Memory: {memory.total/(1024**3):.2f}GB total, "
                    f"{memory.available/(1024**3):.2f}GB available")
    except:
        pass

    # Create global memory monitor
    global_monitor = MemoryMonitor(config)

    # Prepare work queue sorted by total size (all files per table)
    table_work = []
    for table_name, zip_map in audit_map.items():
        csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
        valid_paths = [p for p in csv_paths if p.exists()]

        if not valid_paths:
            logger.warning(f"No valid files for '{table_name}'")
            continue

        total_bytes = sum(p.stat().st_size for p in valid_paths)
        table_work.append((table_name, zip_map, valid_paths, total_bytes))

    # Sort by size (largest first can sometimes be better for packing)
    table_work.sort(key=lambda x: x[3], reverse=True)

    logger.info(f"Processing {len(table_work)} tables with valid data")

    # Determine processing approach
    total_dataset_gb = sum(x[3] for x in table_work) / (1024**3)
    largest_table_gb = max(x[3] for x in table_work) / (1024**3)
    
    # Use sequential for very large datasets
    use_parallel = total_dataset_gb < 50 and largest_table_gb < 20
    
    if use_parallel:
        logger.info("Using parallel processing (dataset size permits)")
    else:
        logger.info("Using sequential processing (large dataset detected)")

    from rich.progress import (
        Progress, SpinnerColumn, BarColumn, TextColumn,
        TimeElapsedColumn, MofNCompleteColumn, TimeRemainingColumn
    )
    from ....database.utils import get_table_columns

    success_count = 0
    error_count = 0
    total_bytes_processed = 0

    if use_parallel:
        # Parallel processing with limited workers
        max_workers = min(config.workers, 2)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold green]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            ) as progress:

                main_task = progress.add_task("Converting tables", total=len(table_work))
                tasks = {}
                
                for table_name, zip_map, csv_paths, total_bytes in table_work:
                    expected_columns = get_table_columns(table_name)

                    # Use new multi-file function
                    task = executor.submit(
                        convert_table_csvs_multifile,
                        table_name,
                        csv_paths,
                        output_dir,
                        delimiter,
                        expected_columns,
                        config,
                        global_monitor
                    )
                    tasks[task] = (table_name, total_bytes, len(csv_paths))

                completed = 0
                for future in as_completed(tasks):
                    table_name, table_bytes, file_count = tasks[future]

                    try:
                        result = future.result()
                        if "[OK]" in result:
                            logger.info(f"✅ {result}")
                            success_count += 1
                            total_bytes_processed += table_bytes
                        else:
                            logger.error(f"❌ {result}")
                            error_count += 1
                    except Exception as e:
                        logger.error(f"❌ Exception in '{table_name}': {e}")
                        error_count += 1

                    completed += 1
                    progress.update(main_task, completed=completed)

                    # Inter-task cleanup
                    global_monitor.perform_aggressive_cleanup()
    else:
        # Sequential processing for large datasets
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        ) as progress:

            main_task = progress.add_task("Converting tables", total=len(table_work))
            
            for i, (table_name, zip_map, csv_paths, total_bytes) in enumerate(table_work):
                logger.info(f"Processing table {i+1}/{len(table_work)}: "
                           f"{table_name} ({total_bytes:,} bytes, {len(csv_paths)} files)")
                
                # Aggressive cleanup before each table
                if i > 0:
                    logger.info("Performing inter-table cleanup...")
                    global_monitor.perform_aggressive_cleanup()
                    time.sleep(1.0)
                
                try:
                    expected_columns = get_table_columns(table_name)
                    
                    # Use new multi-file function
                    result = convert_table_csvs_multifile(
                        table_name,
                        csv_paths,
                        output_dir,
                        delimiter,
                        expected_columns,
                        config,
                        global_monitor
                    )
                    
                    if "[OK]" in result:
                        logger.info(f"✅ {result}")
                        success_count += 1
                        total_bytes_processed += total_bytes
                    else:
                        logger.error(f"❌ {result}")
                        error_count += 1
                        
                except Exception as e:
                    logger.error(f"❌ Exception in '{table_name}': {e}")
                    error_count += 1

                progress.update(main_task, completed=i+1)

    # Final summary
    final_status = global_monitor.get_status_report()
    total_mb = total_bytes_processed / (1024 * 1024)
    
    logger.info("=" * 60)
    logger.info("CONVERSION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"   ✅ Successful: {success_count} tables")
    logger.info(f"   ❌ Failed: {error_count} tables")
    logger.info(f"   📊 Total processed: {total_bytes_processed:,} bytes ({total_mb:.1f}MB)")
    logger.info(f"   🧠 Final memory usage: {final_status['usage_above_baseline_mb']:.1f}MB above baseline")
    logger.info(f"   📁 Output directory: {output_dir}")
    logger.info("=" * 60)


# Helper function to add to your existing file
def add_multifile_imports():
    """
    Add these imports to the top of your conversion service file:
    """
    return """
# Add these imports at the top of your file:
from typing import Iterator
import itertools

# The ChunkIterator class and helper functions are in the artifacts above
# Copy them into your conversion service file
"""



# Drop-in replacement for your convert_csvs_to_parquet_smart
def convert_csvs_to_parquet_smart(
    audit_map: dict,
    unzip_dir: Path,
    output_dir: Path,
    delimiter: str = ";"
):
    """
    Smart conversion with automatic strategy selection.
    Now includes multi-file optimization.
    """
    # Auto-detect if we're in a constrained environment
    try:
        mem = psutil.virtual_memory()
        total_gb = mem.total / (1024**3)
        available_gb = mem.available / (1024**3)
        
        # Check for cgroup limits (Docker/K8s)
        cgroup_limit = read_cgroup_memory_limit_bytes()
        
        if cgroup_limit:
            effective_limit_gb = cgroup_limit / (1024**3)
            logger.info(f"Detected cgroup memory limit: {effective_limit_gb:.2f}GB")
            
            if effective_limit_gb < 4:
                logger.info("Using ultra-conservative configuration for constrained environment")
                config = UltraConservativeConfig()
            else:
                config = LargeDatasetConfig()
        elif total_gb < 8:
            logger.info(f"Small system detected ({total_gb:.1f}GB), using conservative config")
            config = LargeDatasetConfig()
        else:
            logger.info(f"Standard system ({total_gb:.1f}GB), using standard config")
            config = ConversionConfig()
    except:
        config = ConversionConfig()
    
    # Use enhanced conversion
    convert_csvs_to_parquet(
        audit_map,
        unzip_dir,
        output_dir,
        config,
        delimiter
    )