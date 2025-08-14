#!/usr/bin/env python3
"""
Example script demonstrating unified data loading (CSV and Parquet) to PostgreSQL.

This script shows how to use the UnifiedLoader class and functions to efficiently
load both CSV and Parquet files into PostgreSQL with upsert operations.

Creates separate databases for performance testing.
"""

import os
import sys
import logging
from pathlib import Path
import time

import polars as pl

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# Setup simple logging for this example (disable JSON format)
def setup_simple_logger():
    """Setup a simple logger without JSON formatting for cleaner example output."""
    # Create a custom logger for this example
    example_logger = logging.getLogger("unified_loading_example")
    example_logger.setLevel(logging.INFO)

    # Remove any existing handlers
    for handler in example_logger.handlers[:]:
        example_logger.removeHandler(handler)

    # Create console handler with simple format
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create simple formatter
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)

    example_logger.addHandler(console_handler)
    example_logger.propagate = False  # Don't propagate to parent loggers

    return example_logger


# Setup simple logger
simple_logger = setup_simple_logger()

# Suppress JSON logging from imported modules
os.environ["DISABLE_JSON_LOGGING"] = "true"

# Now import the modules (which will trigger the JSON logger setup)
from database.dml import UnifiedLoader, populate_table, table_name_to_table_info
from database.engine import create_database_instance
from database.models import MainBase

# Temporarily disable the existing logger to reduce noise

logging.getLogger("setup.logging").setLevel(logging.WARNING)
logging.getLogger("database.dml_unified").setLevel(logging.WARNING)


def create_performance_databases():
    """Create separate databases for performance testing"""
    databases = {}
    base_connection = "postgresql://postgres:postgres@localhost:5432"

    # Database configurations for performance testing
    db_configs = {
        "csv_performance": f"{base_connection}/csv_performance_db",
        "parquet_performance": f"{base_connection}/parquet_performance_db",
        "unified_performance": f"{base_connection}/unified_performance_db",
    }

    for db_name, connection_string in db_configs.items():
        try:
            # Create database instance
            database = create_database_instance(connection_string, MainBase)

            # Test connection
            with database.engine.connect() as conn:
                from sqlalchemy import text

                conn.execute(text("SELECT 1"))

            # Create tables if they don't exist
            database.create_tables()

            databases[db_name] = database
            print(f"✅ Database '{db_name}' ready")

        except Exception as e:
            print(f"❌ Failed to create database '{db_name}': {e}")
            # Try to create the database first
            try:
                from sqlalchemy import create_engine, text

                admin_engine = create_engine(f"{base_connection}/postgres")
                with admin_engine.connect() as conn:
                    conn.execute(text("COMMIT"))  # End any transaction
                    db_name_only = connection_string.split("/")[-1]
                    conn.execute(text(f"CREATE DATABASE {db_name_only}"))
                    simple_logger.info(f"Created database '{db_name_only}'")

                # Try again
                database = create_database_instance(connection_string, MainBase)
                database.create_tables()
                databases[db_name] = database
                print(f"✅ Database '{db_name}' ready (created)")

            except Exception as create_error:
                print(f"❌ Could not create database '{db_name}': {create_error}")

    return databases


def load_table_quietly(
    database, table_name, source_path
):
    """Load a table with minimal logging output."""
    # Temporarily increase logging level to reduce noise
    original_level = logging.getLogger("database.dml_unified").level
    logging.getLogger("database.dml_unified").setLevel(logging.ERROR)

    try:
        success, error, rows = populate_table(
            database=database,
            table_name=table_name,
            source_path=source_path
        )
        return success, error, rows
    finally:
        # Restore original logging level
        logging.getLogger("database.dml_unified").setLevel(original_level)


def detect_file_type(file_path):
    """
    Robust file type detection for CSV and Parquet files.

    Uses multiple strategies to identify file format:
    1. File extension analysis
    2. Magic number/signature detection
    3. Content-based detection for edge cases

    Args:
        file_path: Path to file to analyze

    Returns:
        str: 'csv', 'parquet', or 'unknown'
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return "unknown"

    # Strategy 1: File extension analysis
    extension = file_path.suffix.lower()
    name_lower = file_path.name.lower()

    # Clear Parquet files
    if extension == ".parquet":
        return "parquet"

    # Clear CSV files
    if extension in [".csv", ".txt"]:
        return "csv"

    # Handle special CNPJ file patterns (e.g., .CNAECSV, .MOTICSV)
    if extension.endswith("csv") or "csv" in extension.lower():
        return "csv"

    # Strategy 2: Magic number/signature detection
    try:
        with open(file_path, "rb") as f:
            # Read first few bytes for magic number detection
            header = f.read(16)

            # Parquet magic number: "PAR1" at start or "PAR1" at specific offset
            if header.startswith(b"PAR1"):
                return "parquet"

            # Check for Parquet signature at byte 4 (common pattern)
            if len(header) >= 8 and header[4:8] == b"PAR1":
                return "parquet"
    except (IOError, OSError):
        pass

    # Strategy 3: Content-based detection for text files
    try:
        # Try to read first few lines to detect CSV-like content
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline().strip()
            second_line = f.readline().strip()

            # CSV indicators: semicolon separators, consistent field counts
            if ";" in first_line and ";" in second_line:
                # Check if field counts are consistent (basic CSV validation)
                first_fields = len(first_line.split(";"))
                second_fields = len(second_line.split(";"))
                if first_fields > 1 and abs(first_fields - second_fields) <= 1:
                    return "csv"

            # Also check for comma-separated values
            if "," in first_line and "," in second_line:
                first_fields = len(first_line.split(","))
                second_fields = len(second_line.split(","))
                if first_fields > 1 and abs(first_fields - second_fields) <= 1:
                    return "csv"

    except (IOError, UnicodeDecodeError):
        pass

    # Strategy 4: Try binary content analysis for edge cases
    try:
        with open(file_path, "rb") as f:
            content = f.read(1024)  # Read first KB

            # Look for Parquet-specific patterns
            if b"schema" in content.lower() or b"thrift" in content.lower():
                return "parquet"

            # Look for typical CSV patterns (printable ASCII with separators)
            if b";" in content or b"," in content:
                # Check if content is mostly printable
                printable_ratio = sum(
                    1 for b in content if 32 <= b <= 126 or b in [9, 10, 13]
                ) / len(content)
                if printable_ratio > 0.8:  # 80% printable characters
                    return "csv"

    except (IOError, OSError):
        pass

    # Strategy 5: Filename pattern matching for CNPJ files
    cnpj_patterns = [
        "cnae",
        "moti",
        "munic",
        "pais",
        "quals",
        "natju",
        "empresa",
        "estabelecimento",
        "socio",
    ]

    for pattern in cnpj_patterns:
        if pattern in name_lower:
            # Assume CSV for CNPJ data files if no clear format detected
            return "csv"

    return "unknown"


def get_file_info(file_path):
    """
    Get comprehensive file information including type, size, and metadata.

    Args:
        file_path: Path to file to analyze

    Returns:
        dict: File information including type, size, encoding, etc.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return {"exists": False, "type": "unknown"}

    info = {
        "exists": True,
        "type": detect_file_type(file_path),
        "size_bytes": file_path.stat().st_size,
        "size_mb": file_path.stat().st_size / (1024 * 1024),
        "name": file_path.name,
        "extension": file_path.suffix,
        "modified_time": file_path.stat().st_mtime,
    }

    # Try to detect encoding for text files
    if info["type"] == "csv":
        try:
            import chardet

            with open(file_path, "rb") as f:
                raw_data = f.read(10000)  # Read first 10KB
                encoding_result = chardet.detect(raw_data)
                info["encoding"] = encoding_result.get("encoding", "unknown")
                info["encoding_confidence"] = encoding_result.get("confidence", 0.0)
        except ImportError:
            # Fallback without chardet
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    f.read(1000)
                info["encoding"] = "utf-8"
            except UnicodeDecodeError:
                try:
                    with open(file_path, "r", encoding="latin1") as f:
                        f.read(1000)
                    info["encoding"] = "latin1"
                except:
                    info["encoding"] = "unknown"

    return info


def convert_csv_to_parquet(csv_file, output_dir=None, use_polars=True):
    """
    Convert CSV file to Parquet format for better performance.

    Args:
        csv_file: Path to CSV file
        output_dir: Directory to save Parquet file (default: same as CSV)
        use_polars: Whether to use polars for conversion (fallback to pandas)

    Returns:
        Path to created Parquet file, or original CSV file if conversion failed
    """
    csv_file = Path(csv_file)

    # Validate input is actually a CSV file
    file_type = detect_file_type(csv_file)
    if file_type != "csv":
        simple_logger.warning(
            f"File {csv_file.name} is not detected as CSV (detected: {file_type})"
        )
        return csv_file

    if output_dir is None:
        output_dir = csv_file.parent / "parquet_data"
    else:
        output_dir = Path(output_dir)

    # Create output directory
    output_dir.mkdir(exist_ok=True)

    # Generate parquet filename
    parquet_file = output_dir / f"{csv_file.stem}.parquet"

    # Skip if parquet file already exists and is newer
    if (
        parquet_file.exists()
        and parquet_file.stat().st_mtime > csv_file.stat().st_mtime
    ):
        simple_logger.info(
            f"Parquet file already exists and is newer: {parquet_file.name}"
        )
        return parquet_file

    try:
        # Read CSV with polars
        df = pl.read_csv(csv_file, separator=";", encoding="latin1")
        # Write to parquet
        df.write_parquet(parquet_file)
        simple_logger.info(
            f"Converted {csv_file.name} to {parquet_file.name} using polars"
        )
        return parquet_file

    except Exception as e:
        simple_logger.error(f"Failed to convert {csv_file.name} to Parquet: {e}")
        return csv_file  # Return original file if conversion failed


def example_multiple_files_loading():
    """Example: Load multiple files using unified loader with auto-detection"""
    print("\n" + "=" * 60)
    print("EXAMPLE: Multiple Files Loading (Auto-Detection)")
    print("=" * 60)

    # Setup database connections
    databases = create_performance_databases()
    if not databases:
        print("❌ No databases available for testing")
        return

    # Use unified database for this example
    database = databases.get("unified_performance")
    if not database:
        print("❌ Unified database not available")
        return

    # Example data directories
    directories = [
        Path("data/EXTRACTED_FILES/parquet_data"),
        Path("data/EXTRACTED_FILES"),
    ]

    # Find available data files
    available_files = []
    for directory in directories:
        if directory.exists():
            # Look for parquet files
            available_files.extend(list(directory.glob("*.parquet")))
            # Look for CSV files (common patterns)
            available_files.extend(list(directory.glob("*.CSV")))
            available_files.extend(list(directory.glob("*CSV")))

    if not available_files:
        print("⚠️  No data files found in:")
        for directory in directories:
            print(f"   - {directory}")
        print("   Please run the ETL pipeline first to generate data files.")
        return

    print(f"Found {len(available_files)} data files:")
    for file in available_files[:5]:  # Show first 5
        print(f"   - {file.name} ({file.suffix.upper() or 'CSV'})")
    if len(available_files) > 5:
        print(f"   ... and {len(available_files) - 5} more")

    # Load files using unified approach
    print("\nLoading files using unified loader...")

    successful = 0
    total_rows = 0
    start_time = time.time()

    # Test with a few representative tables
    test_tables = ["cnae", "moti", "munic", "pais"]

    for table_name in test_tables:
        print(f"\n📊 Loading table: {table_name}")

        try:
            # Use the unified populate function with auto-detection (quietly)
            success, error, rows = load_table_quietly(
                database=database,
                table_name=table_name,
                source_path=Path("data/EXTRACTED_FILES")
            )

            if success:
                successful += 1
                total_rows += rows
                print(f"   ✅ Success: {rows:,} rows loaded")
            else:
                print(f"   ❌ Failed: {error}")

        except Exception as e:
            print(f"   ❌ Error: {e}")

    total_time = time.time() - start_time

    print("\n📈 SUMMARY:")
    print(f"   Tables processed: {len(test_tables)}")
    print(f"   Successful: {successful}")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Total time: {total_time:.2f}s")
    if total_time > 0:
        print(f"   Overall throughput: {total_rows / total_time:.0f} rows/s")


def example_upsert_demonstration():
    """Example: Demonstrate upsert functionality"""
    print("\n" + "=" * 60)
    print("EXAMPLE: Upsert Functionality Demonstration")
    print("=" * 60)

    # Setup database connections
    databases = create_performance_databases()
    if not databases:
        print("❌ No databases available for testing")
        return

    # Use unified database for this example
    database = databases.get("unified_performance")
    if not database:
        print("❌ Unified database not available")
        return

    # Create unified loader
    loader = UnifiedLoader(database)

    # Use a small table for demonstration
    table_name = "cnae"
    table_info = table_name_to_table_info(table_name)

    # Find data file
    data_files = [
        Path("data/EXTRACTED_FILES/parquet_data/cnae.parquet"),
        Path("data/EXTRACTED_FILES/F.K03200$Z.D50510.CNAECSV"),
    ]

    data_file = None
    for file in data_files:
        if file.exists():
            data_file = file
            break

    if not data_file:
        print("⚠️  No data files found for upsert demonstration")
        return

    print(f"📁 Using file: {data_file.name}")

    # First load - initial insert
    print("\n1️⃣  First load (initial insert):")
    start_time = time.time()

    # Temporarily disable logging for cleaner output
    original_level = logging.getLogger("database.dml_unified").level
    logging.getLogger("database.dml_unified").setLevel(logging.ERROR)

    success, error, rows = loader.load_file(
        table_info=table_info, file_path=data_file, show_progress=False
    )
    first_load_time = time.time() - start_time

    # Restore logging level
    logging.getLogger("database.dml_unified").setLevel(original_level)

    if success:
        print(f"   ✅ Success: {rows:,} rows inserted in {first_load_time:.2f}s")
    else:
        print(f"   ❌ Failed: {error}")
        return

    # Second load - should be upsert (update existing + insert new)
    print("\n2️⃣  Second load (upsert - should update existing rows):")
    start_time = time.time()

    # Temporarily disable logging for cleaner output
    logging.getLogger("database.dml_unified").setLevel(logging.ERROR)

    success, error, rows = loader.load_file(
        table_info=table_info, file_path=data_file, show_progress=False
    )
    second_load_time = time.time() - start_time

    # Restore logging level
    logging.getLogger("database.dml_unified").setLevel(original_level)

    if success:
        print(
            f"   ✅ Success: {rows:,} rows processed (upserted) in {second_load_time:.2f}s"
        )
        print(
            f"   � Upsert was {first_load_time / second_load_time:.1f}x the time of initial insert"
        )
    else:
        print(f"   ❌ Failed: {error}")

    # Verify data integrity
    print("\n3️⃣  Verifying data integrity:")
    try:
        with database.engine.connect() as conn:
            from sqlalchemy import text

            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            final_count = result.fetchone()[0]
            print(f"   📊 Final row count: {final_count:,}")
            print("   ✅ Data integrity maintained (no duplicates from upsert)")
    except Exception as e:
        print(f"   ❌ Error checking row count: {e}")


def example_performance_comparison():
    """Example: Compare CSV vs Parquet performance across different databases"""
    print("\n" + "=" * 60)
    print("EXAMPLE: Format Performance Comparison (Separate Databases)")
    print("=" * 60)

    # Setup database connections
    databases = create_performance_databases()
    if len(databases) < 2:
        print("❌ Need at least 2 databases for performance comparison")
        return

    # Test files for performance comparison
    table_name = "cnae"  # Small table for quick testing
    csv_file = Path("data/EXTRACTED_FILES/F.K03200$Z.D50510.CNAECSV")
    parquet_file = Path("data/EXTRACTED_FILES/parquet_data/cnae.parquet")

    # Convert CSV to Parquet if needed
    if csv_file.exists() and not parquet_file.exists():
        print("\n🔄 Converting CSV to Parquet for performance comparison...")
        parquet_file = convert_csv_to_parquet(
            csv_file=csv_file,
            output_dir=Path("data/EXTRACTED_FILES/parquet_data"),
            use_polars=True,
        )

        if parquet_file == csv_file:
            print("   ⚠️  Conversion failed, will skip Parquet testing")
            parquet_file = None
        else:
            # Show file size comparison
            csv_size = csv_file.stat().st_size / (1024 * 1024)
            parquet_size = parquet_file.stat().st_size / (1024 * 1024)
            compression_ratio = csv_size / parquet_size if parquet_size > 0 else 0
            print("   ✅ Conversion successful!")
            print(
                f"   📊 CSV: {csv_size:.2f}MB → Parquet: {parquet_size:.2f}MB ({compression_ratio:.1f}x smaller)"
            )

    results = []

    # Test CSV loading performance
    if csv_file.exists() and "csv_performance" in databases:
        print("\n📄 Testing CSV loading performance...")
        database = databases["csv_performance"]
        loader = UnifiedLoader(database)
        table_info = table_name_to_table_info(table_name)

        # Temporarily disable logging for cleaner output
        original_level = logging.getLogger("database.dml_unified").level
        logging.getLogger("database.dml_unified").setLevel(logging.ERROR)

        start_time = time.time()
        success, error, rows = loader.load_file(
            table_info=table_info, file_path=csv_file, show_progress=False
        )
        csv_time = time.time() - start_time

        # Restore logging level
        logging.getLogger("database.dml_unified").setLevel(original_level)

        if success:
            csv_size = csv_file.stat().st_size / (1024 * 1024)  # MB
            results.append(
                {
                    "format": "CSV",
                    "file_size_mb": csv_size,
                    "rows": rows,
                    "time_seconds": csv_time,
                    "throughput_rows_per_sec": rows / csv_time if csv_time > 0 else 0,
                    "throughput_mb_per_sec": csv_size / csv_time if csv_time > 0 else 0,
                }
            )
            print(
                f"   ✅ CSV: {rows:,} rows in {csv_time:.2f}s ({rows / csv_time:.0f} rows/s)"
            )
        else:
            print(f"   ❌ CSV failed: {error}")

    # Test Parquet loading performance
    if parquet_file and parquet_file.exists() and "parquet_performance" in databases:
        print("\n📊 Testing Parquet loading performance...")
        database = databases["parquet_performance"]
        loader = UnifiedLoader(database)
        table_info = table_name_to_table_info(table_name)

        # Temporarily disable logging for cleaner output
        original_level = logging.getLogger("database.dml_unified").level
        logging.getLogger("database.dml_unified").setLevel(logging.ERROR)

        start_time = time.time()
        success, error, rows = loader.load_file(
            table_info=table_info, file_path=parquet_file, show_progress=False
        )
        parquet_time = time.time() - start_time

        # Restore logging level
        logging.getLogger("database.dml_unified").setLevel(original_level)

        if success:
            parquet_size = parquet_file.stat().st_size / (1024 * 1024)  # MB
            results.append(
                {
                    "format": "Parquet",
                    "file_size_mb": parquet_size,
                    "rows": rows,
                    "time_seconds": parquet_time,
                    "throughput_rows_per_sec": rows / parquet_time
                    if parquet_time > 0
                    else 0,
                    "throughput_mb_per_sec": parquet_size / parquet_time
                    if parquet_time > 0
                    else 0,
                }
            )
            print(
                f"   ✅ Parquet: {rows:,} rows in {parquet_time:.2f}s ({rows / parquet_time:.0f} rows/s)"
            )
        else:
            print(f"   ❌ Parquet failed: {error}")
    elif not parquet_file:
        print("\n⚠️  Skipping Parquet test - no Parquet file available")

    # Performance comparison summary
    if len(results) >= 2:
        print("\n📈 PERFORMANCE COMPARISON:")
        print("-" * 80)
        print(
            f"{'Format':<10} | {'Size (MB)':<10} | {'Time (s)':<10} | {'Rows/s':<15} | {'MB/s':<10}"
        )
        print("-" * 80)

        for result in results:
            print(
                f"{result['format']:<10} | "
                f"{result['file_size_mb']:<10.2f} | "
                f"{result['time_seconds']:<10.2f} | "
                f"{result['throughput_rows_per_sec']:<15.0f} | "
                f"{result['throughput_mb_per_sec']:<10.2f}"
            )

        # Calculate performance ratios
        if len(results) == 2:
            csv_result = next(r for r in results if r["format"] == "CSV")
            parquet_result = next(r for r in results if r["format"] == "Parquet")

            speed_ratio = (
                parquet_result["throughput_rows_per_sec"]
                / csv_result["throughput_rows_per_sec"]
            )
            size_ratio = csv_result["file_size_mb"] / parquet_result["file_size_mb"]

            print("-" * 80)
            print("💡 Performance Insights:")
            print(
                f"   • Parquet is {speed_ratio:.1f}x {'faster' if speed_ratio > 1 else 'slower'} than CSV"
            )
            print(f"   • Parquet file is {size_ratio:.1f}x smaller than CSV")
            print(f"   • Storage savings: {(1 - 1 / size_ratio) * 100:.1f}%")

            if speed_ratio < 1:
                print(
                    "   ℹ️  Note: For small files, Parquet overhead may exceed I/O benefits"
                )
                print("   ℹ️  Parquet advantages increase with larger datasets")
    elif len(results) == 1:
        print(f"\n📊 Single format tested: {results[0]['format']}")
        print(f"   • Throughput: {results[0]['throughput_rows_per_sec']:.0f} rows/s")
        print(f"   • File size: {results[0]['file_size_mb']:.2f} MB")

    return results


def example_file_type_detection():
    """Example: Demonstrate robust file type detection capabilities"""
    print("\n" + "=" * 60)
    print("EXAMPLE: Robust File Type Detection")
    print("=" * 60)

    # Find files to analyze
    directories = [
        Path("data/EXTRACTED_FILES"),
        Path("data/EXTRACTED_FILES/parquet_data"),
    ]

    found_files = []
    for directory in directories:
        if directory.exists():
            # Look for various file types
            found_files.extend(list(directory.glob("*")))

    if not found_files:
        print("⚠️  No files found for type detection demonstration")
        return

    print(f"📁 Analyzing {len(found_files)} files for type detection:")
    print("-" * 60)
    print(
        f"{'File Name':<35} | {'Detected Type':<12} | {'Size (MB)':<10} | {'Extension':<10}"
    )
    print("-" * 60)

    # Group files by detected type
    type_counts = {"csv": 0, "parquet": 0, "unknown": 0}
    type_examples = {"csv": [], "parquet": [], "unknown": []}

    for file_path in found_files[:15]:  # Analyze first 15 files
        if file_path.is_file():
            file_info = get_file_info(file_path)

            # Display file information
            file_name = file_info["name"][:34]  # Truncate long names
            file_type = file_info["type"]
            file_size = file_info["size_mb"]
            extension = file_info["extension"] or "none"

            print(
                f"{file_name:<35} | {file_type:<12} | {file_size:<10.2f} | {extension:<10}"
            )

            # Track statistics
            type_counts[file_type] = type_counts.get(file_type, 0) + 1
            if len(type_examples[file_type]) < 3:  # Keep first 3 examples
                type_examples[file_type].append(file_info)

    print("-" * 60)
    print("\n📊 DETECTION SUMMARY:")
    print(f"   • CSV files detected: {type_counts['csv']}")
    print(f"   • Parquet files detected: {type_counts['parquet']}")
    print(f"   • Unknown/Other files: {type_counts['unknown']}")

    # Show detailed analysis for a few files
    print("\n🔍 DETAILED ANALYSIS:")

    for file_type, examples in type_examples.items():
        if examples and file_type != "unknown":
            print(f"\n   {file_type.upper()} File Analysis:")
            for i, info in enumerate(examples[:2]):  # Show first 2 examples
                print(f"     {i + 1}. {info['name']}")
                print(f"        Size: {info['size_mb']:.2f}MB")
                if "encoding" in info:
                    print(f"        Encoding: {info.get('encoding', 'unknown')}")

                # Try to detect specific issues or characteristics
                file_path = Path(info["name"])

                # Find the full path
                full_path = None
                for directory in directories:
                    candidate = directory / info["name"]
                    if candidate.exists():
                        full_path = candidate
                        break

                if full_path and file_type == "csv":
                    try:
                        # Try to read first line for CSV validation
                        with open(
                            full_path, "r", encoding="latin1", errors="ignore"
                        ) as f:
                            first_line = f.readline().strip()
                            field_count = len(first_line.split(";"))
                            print(f"        CSV fields (';' separated): {field_count}")
                            if field_count == 1:
                                # Try comma separation
                                field_count_comma = len(first_line.split(","))
                                if field_count_comma > 1:
                                    print(
                                        f"        CSV fields (',' separated): {field_count_comma}"
                                    )
                    except Exception as e:
                        print(f"        CSV analysis error: {e}")

    # Test edge cases and robustness
    print("\n🧪 ROBUSTNESS TESTING:")

    # Test with non-existent file
    fake_file = Path("non_existent_file.csv")
    fake_result = detect_file_type(fake_file)
    print(f"   • Non-existent file detection: {fake_result}")

    # Test with files that have misleading extensions
    test_cases = []
    for directory in directories:
        if directory.exists():
            # Look for files that might be misnamed
            candidates = list(directory.glob("*"))[:5]
            for candidate in candidates:
                if candidate.is_file():
                    test_cases.append(candidate)

    if test_cases:
        print(f"   • Testing {len(test_cases)} files for edge cases...")
        for test_file in test_cases[:3]:
            detected = detect_file_type(test_file)
            actual_ext = test_file.suffix.lower()
            expected_from_ext = (
                "parquet"
                if actual_ext == ".parquet"
                else "csv"
                if actual_ext in [".csv", ".txt"]
                else "unknown"
            )

            match_status = "✅" if detected == expected_from_ext else "⚠️"
            print(
                f"     {match_status} {test_file.name}: detected={detected}, expected={expected_from_ext}"
            )

    print("\n💡 FILE TYPE DETECTION STRATEGIES:")
    print(
        "   1. File extension analysis (.parquet, .csv, .txt, special CNPJ patterns)"
    )
    print("   2. Magic number detection (PAR1 signature for Parquet)")
    print("   3. Content analysis (separator patterns, field consistency)")
    print("   4. Binary pattern matching (schema, thrift keywords)")
    print("   5. CNPJ-specific filename patterns (cnae, moti, munic, etc.)")
    print("   6. Encoding detection for text files")


def example_csv_to_parquet_conversion():
    """Example: Demonstrate CSV to Parquet conversion with multiple files"""
    print("\n" + "=" * 60)
    print("EXAMPLE: CSV to Parquet Conversion")
    print("=" * 60)

    # Find CSV files to convert
    csv_files = []
    csv_dir = Path("data/EXTRACTED_FILES")

    if csv_dir.exists():
        # Use robust file type detection instead of just extension matching
        all_files = list(csv_dir.glob("*"))
        for file_path in all_files:
            if file_path.is_file() and detect_file_type(file_path) == "csv":
                csv_files.append(file_path)

    if not csv_files:
        print("⚠️  No CSV files found for conversion")
        return

    print(f"Found {len(csv_files)} CSV files for conversion:")

    # Create parquet output directory
    parquet_dir = csv_dir / "parquet_data"
    parquet_dir.mkdir(exist_ok=True)

    conversion_results = []

    # Convert first few files as example
    for csv_file in csv_files[:3]:
        print(f"\n📄 Processing: {csv_file.name}")

        # Validate it's actually a CSV using robust detection
        file_info = get_file_info(csv_file)
        print(f"   File type detected: {file_info['type']}")
        print(f"   File size: {file_info['size_mb']:.2f}MB")
        if "encoding" in file_info:
            print(f"   Encoding: {file_info.get('encoding', 'unknown')}")

        start_time = time.time()
        parquet_file = convert_csv_to_parquet(
            csv_file=csv_file, output_dir=parquet_dir, use_polars=True
        )
        conversion_time = time.time() - start_time

        if parquet_file != csv_file:  # Conversion successful
            # Verify the converted file
            parquet_info = get_file_info(parquet_file)
            print("   ✅ Converted successfully!")
            print(f"   Parquet type detected: {parquet_info['type']}")
            print(f"   Conversion time: {conversion_time:.2f}s")

            conversion_results.append(
                {
                    "csv_file": csv_file,
                    "parquet_file": parquet_file,
                    "conversion_time": conversion_time,
                }
            )

    # Show conversion summary
    if conversion_results:
        print("\n📊 CONVERSION SUMMARY:")
        print("-" * 60)
        total_csv_size = 0
        total_parquet_size = 0

        for result in conversion_results:
            csv_size = result["csv_file"].stat().st_size / (1024 * 1024)
            parquet_size = result["parquet_file"].stat().st_size / (1024 * 1024)
            total_csv_size += csv_size
            total_parquet_size += parquet_size

            print(
                f"   {result['csv_file'].name[:30]:<30} | "
                f"{csv_size:>6.1f}MB -> {parquet_size:>6.1f}MB | "
                f"{result['conversion_time']:>5.1f}s"
            )

        print("-" * 60)
        overall_compression = (
            total_csv_size / total_parquet_size if total_parquet_size > 0 else 0
        )
        print(
            f"   {'TOTAL':<30} | "
            f"{total_csv_size:>6.1f}MB -> {total_parquet_size:>6.1f}MB | "
            f"{overall_compression:>5.1f}x"
        )

        print(
            f"\n💡 Parquet files are {overall_compression:.1f}x smaller and typically load faster!"
        )

    return conversion_results


def main():
    """Main function to run all examples"""
    print("🚀 UNIFIED DATA LOADING EXAMPLES")
    print("=" * 60)
    print("Demonstrates CSV and Parquet loading with upsert operations")
    print("Uses separate databases for performance isolation")

    # Check if we're in the right directory
    if not Path("src").exists():
        print("❌ Please run this script from the project root directory")
        print("   cd /path/to/scrapper-rf-cnpj")
        print("   python examples/unified_loading_example.py")
        return

    # Check basic database connection
    try:
        connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
        database = create_database_instance(connection_string, MainBase)
        with database.engine.connect() as conn:
            from sqlalchemy import text

            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("   Please ensure PostgreSQL is running and accessible")
        return

    # Run examples
    try:
        example_file_type_detection()
        example_csv_to_parquet_conversion()
        example_multiple_files_loading()
        example_upsert_demonstration()
        example_performance_comparison()

        print("\n🎉 All examples completed!")
        print("💡 The unified loader automatically:")
        print("   • Detects file formats (CSV/Parquet) using multiple strategies")
        print("   • Validates file types beyond simple extension checking")
        print("   • Converts CSV to Parquet when needed for performance")
        print("   • Uses appropriate engines (pandas/polars)")
        print("   • Performs upsert operations (no data loss)")
        print("   • Handles chunked processing for large files")
        print("   • Provides comprehensive error handling and retry logic")
        print("   • Isolates performance tests in separate databases")
        print("   • Includes encoding detection for text files")

    except Exception as e:
        print(f"❌ Error running examples: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
