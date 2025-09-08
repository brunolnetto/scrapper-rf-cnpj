"""
Example usage of the new ETL configuration system.
Demonstrates how to use the enhanced configurations and batch optimization.
"""

from pathlib import Path
from src.setup.etl_config import get_etl_config
from src.core.utils.batch_optimizer import BatchSizeOptimizer
from src.core.utils.development_filter import EnhancedDevelopmentFilter

def main():
    """Example of using the new ETL configuration system."""
    
    # Get unified ETL configuration
    etl_config = get_etl_config(year=2024, month=12)
    
    print("=== ETL Configuration Overview ===")
    print(f"Year: {etl_config.year}, Month: {etl_config.month}")
    print(f"Development Mode: {etl_config.development.enabled}")
    print()
    
    # Download configuration
    print("Download Configuration:")
    print(f"  Workers: {etl_config.download.workers}")
    print(f"  Max Retries: {etl_config.download.max_retries}")
    print(f"  Verify Checksums: {etl_config.download.verify_checksums}")
    print()
    
    # Conversion configuration
    print("Conversion Configuration:")
    print(f"  Chunk Size: {etl_config.conversion.chunk_size:,}")
    print(f"  Max Memory: {etl_config.conversion.max_memory_mb}MB")
    print(f"  Compression: {etl_config.conversion.compression}")
    print()
    
    # Loading configuration
    print("Loading Configuration:")
    print(f"  Batch Size: {etl_config.loading.batch_size:,}")
    print(f"  Max Batch Size: {etl_config.loading.max_batch_size:,}")
    print(f"  Min Batch Size: {etl_config.loading.min_batch_size:,}")
    print(f"  Parallel Workers: {etl_config.loading.parallel_workers}")
    print()
    
    # Development configuration
    if etl_config.development.enabled:
        print("Development Configuration:")
        print(f"  Max Files per Table: {etl_config.development.max_files_per_table}")
        print(f"  Row Limit Percent: {etl_config.development.row_limit_percent:.1%}")
        print(f"  Max Blob Size: {etl_config.development.max_blob_size_mb}MB")
        print()
    
    # Example: Batch Size Optimization
    print("=== Batch Size Optimization Example ===")
    optimizer = BatchSizeOptimizer(etl_config)
    
    # Simulate file configurations
    example_files = {
        "empresa": Path("data/CONVERTED_FILES/empresa.parquet"),
        "estabelecimento": Path("data/CONVERTED_FILES/estabelecimento.parquet"),
        "cnae": Path("data/CONVERTED_FILES/cnae.parquet")
    }
    
    # Get batch configurations (would use real files in practice)
    for table_name, file_path in example_files.items():
        if file_path.exists():
            config = optimizer.get_batch_configuration_for_file(file_path, table_name)
            print(f"{table_name}:")
            print(f"  Batch Size: {config.batch_size:,}")
            print(f"  Workers: {config.parallel_workers}")
            print(f"  Streaming: {config.use_streaming}")
            print(f"  Estimated Batches: {config.estimated_batches}")
            print(f"  Reason: {config.optimization_reason}")
            print()
    
    # Example: Development Mode Filtering
    if etl_config.development.enabled:
        print("=== Development Mode Filtering Example ===")
        dev_filter = EnhancedDevelopmentFilter(etl_config)
        
        # Example file list
        example_file_list = [
            Path(f"data/EXTRACTED_FILES/empresa_{i}.csv") for i in range(10)
        ]
        
        filtered_files = dev_filter.filter_files_by_blob_limit(example_file_list, "empresa")
        print(f"Original files: {len(example_file_list)}")
        print(f"Filtered files: {len(filtered_files)}")
        
        # Get sampling configuration
        sampling_config = dev_filter.get_sampling_configuration("empresa")
        print(f"Sampling Config: {sampling_config}")
        print()
    
    print("=== Stage-Specific Configuration Access ===")
    # Example of accessing stage-specific configurations
    download_config = etl_config.get_stage_config("download")
    conversion_config = etl_config.get_stage_config("conversion")
    loading_config = etl_config.get_stage_config("loading")
    
    print(f"Download enabled: {download_config.enabled}")
    print(f"Conversion enabled: {conversion_config.enabled}")
    print(f"Loading enabled: {loading_config.enabled}")
    print()
    
    print("Configuration system ready for use!")

if __name__ == "__main__":
    main()
