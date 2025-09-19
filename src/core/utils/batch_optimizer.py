"""
Dynamic batch size optimization for ETL operations.
Calculates optimal batch sizes based on file characteristics, memory availability, and table types.
"""

from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
import math

from ...setup.config import AppConfig
from ...setup.logging import logger

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None


@dataclass
class BatchConfiguration:
    """Complete batch configuration for processing a file."""
    batch_size: int
    file_size_mb: float
    memory_available_mb: float
    use_streaming: bool
    parallel_workers: int
    estimated_batches: int
    table_name: str
    optimization_reason: str


class BatchSizeOptimizer:
    """Calculates optimal batch sizes for different scenarios."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.loading_config = config.pipeline.loading
        
        # Table-specific adjustment factors
        self.table_adjustments = {
            "estabelecimento": 0.5,  # Complex data with many columns, reduce batch size
            "empresa": 0.7,          # Moderately complex
            "socios": 0.8,           # Moderate complexity
            "cnae": 2.0,             # Simple reference data, increase batch size
            "municipio": 2.0,        # Simple reference data
            "pais": 3.0,             # Very simple reference data
            "natureza": 2.5,         # Simple reference data
            "qualificacao": 2.5,     # Simple reference data
            "motivo": 2.0,           # Simple reference data
            "simples": 1.2           # Moderate complexity
        }
    
    def get_memory_info(self) -> Dict[str, float]:
        """Get current memory information."""
        if PSUTIL_AVAILABLE and psutil:
            try:
                memory = psutil.virtual_memory()
                return {
                    "available_mb": memory.available / (1024 * 1024),
                    "total_mb": memory.total / (1024 * 1024),
                    "used_percent": memory.percent
                }
            except Exception as e:
                logger.warning(f"Failed to get memory info: {e}")
        
        # Fallback values
        return {
            "available_mb": 4000.0,
            "total_mb": 8000.0,
            "used_percent": 50.0
        }
    
    def calculate_optimal_batch_size(
        self, 
        file_size_mb: float, 
        table_name: str,
        memory_info: Optional[Dict[str, float]] = None
    ) -> BatchConfiguration:
        """
        Calculate optimal batch size based on file and system characteristics.
        
        Args:
            file_size_mb: Size of the file in MB
            table_name: Name of the table being processed
            memory_info: Optional memory information
            
        Returns:
            BatchConfiguration: Complete batch configuration
        """
        if memory_info is None:
            memory_info = self.get_memory_info()
        
        memory_available_mb = memory_info["available_mb"]
        memory_used_percent = memory_info["used_percent"]
        
        base_batch_size = self.loading_config.batch_size
        max_batch_size = self.loading_config.max_batch_size
        min_batch_size = self.loading_config.min_batch_size
        
        optimization_reasons = []
        
        # Start with base batch size
        optimal_size = base_batch_size
        
        # 1. File size adjustments
        if file_size_mb > 5000:  # Very large files (>5GB)
            optimal_size = min_batch_size
            optimization_reasons.append(f"very_large_file_{file_size_mb:.0f}MB")
        elif file_size_mb > 2000:  # Large files (>2GB)
            optimal_size = min(base_batch_size // 2, 25000)
            optimization_reasons.append(f"large_file_{file_size_mb:.0f}MB")
        elif file_size_mb > 1000:  # Medium-large files (>1GB)
            optimal_size = int(base_batch_size * 0.7)
            optimization_reasons.append(f"medium_large_file_{file_size_mb:.0f}MB")
        elif file_size_mb < 10:  # Small files (<10MB)
            optimal_size = min(max_batch_size, base_batch_size * 2)
            optimization_reasons.append(f"small_file_{file_size_mb:.1f}MB")
        elif file_size_mb < 50:  # Medium-small files (<50MB)
            optimal_size = min(max_batch_size, int(base_batch_size * 1.5))
            optimization_reasons.append(f"medium_small_file_{file_size_mb:.1f}MB")
        
        # 2. Table-specific adjustments
        if table_name in self.table_adjustments:
            factor = self.table_adjustments[table_name]
            optimal_size = int(optimal_size * factor)
            optimization_reasons.append(f"table_factor_{factor}")
        
        # 3. Memory-based adjustments
        if memory_used_percent > 90:  # Critical memory usage
            optimal_size = min(optimal_size, min_batch_size)
            optimization_reasons.append("critical_memory")
        elif memory_available_mb < 500:  # Very low memory
            optimal_size = min(optimal_size, min_batch_size * 2)
            optimization_reasons.append("very_low_memory")
        elif memory_available_mb < 1000:  # Low memory
            optimal_size = min(optimal_size, int(base_batch_size * 0.5))
            optimization_reasons.append("low_memory")
        elif memory_available_mb > 8000 and memory_used_percent < 50:  # High memory available
            optimal_size = min(int(optimal_size * 1.5), max_batch_size)
            optimization_reasons.append("high_memory_available")
        
        # 4. Development mode override
        if self.config.development.enabled:
            dev_limit = max(
                min_batch_size, 
                int(optimal_size * self.config.development.row_limit_percent)
            )
            optimal_size = min(optimal_size, dev_limit)
            optimization_reasons.append(f"dev_mode_{self.config.development.row_limit_percent}")
        
        # 5. Ensure within absolute bounds
        optimal_size = max(min_batch_size, min(optimal_size, max_batch_size))
        
        # Calculate additional configuration
        use_streaming = file_size_mb > (self.loading_config.batch_size_mb * 5)
        
        # Determine parallel workers based on memory and file size
        if memory_available_mb > 4000 and file_size_mb > 500:
            parallel_workers = min(
                self.loading_config.parallel_workers,
                max(1, int(memory_available_mb / 1500))  # ~1.5GB per worker
            )
        elif memory_available_mb < 2000 or file_size_mb < 100:
            parallel_workers = 1
        else:
            parallel_workers = min(2, self.loading_config.parallel_workers)
        
        # Estimate number of batches
        estimated_rows = file_size_mb * self.config.pipeline.conversion.row_estimation_factor
        estimated_batches = max(1, math.ceil(estimated_rows / optimal_size))
        
        optimization_reason = " + ".join(optimization_reasons) if optimization_reasons else "default"
        
        batch_config = BatchConfiguration(
            batch_size=int(optimal_size),
            file_size_mb=file_size_mb,
            memory_available_mb=memory_available_mb,
            use_streaming=use_streaming,
            parallel_workers=parallel_workers,
            estimated_batches=estimated_batches,
            table_name=table_name,
            optimization_reason=optimization_reason
        )
        
        logger.debug(
            f"Batch optimization for {table_name}: "
            f"size={batch_config.batch_size}, "
            f"workers={batch_config.parallel_workers}, "
            f"streaming={batch_config.use_streaming}, "
            f"reason='{batch_config.optimization_reason}'"
        )
        
        return batch_config
    
    def get_batch_configuration_for_file(self, file_path: Path, table_name: str) -> BatchConfiguration:
        """Get complete batch configuration for a specific file."""
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        return self.calculate_optimal_batch_size(file_size_mb, table_name)
    
    def get_batch_configurations_for_files(
        self, 
        file_paths: Dict[str, Path]
    ) -> Dict[str, BatchConfiguration]:
        """Get batch configurations for multiple files."""
        memory_info = self.get_memory_info()
        configurations = {}
        
        for table_name, file_path in file_paths.items():
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            config = self.calculate_optimal_batch_size(
                file_size_mb, table_name, memory_info
            )
            configurations[table_name] = config
        
        return configurations
    
    def log_optimization_summary(self, configurations: Dict[str, BatchConfiguration]) -> None:
        """Log a summary of batch optimizations."""
        logger.info("Batch size optimization summary:")
        
        for table_name, config in configurations.items():
            logger.info(
                f"  {table_name}: batch_size={config.batch_size:,}, "
                f"file_size={config.file_size_mb:.1f}MB, "
                f"workers={config.parallel_workers}, "
                f"estimated_batches={config.estimated_batches}"
            )
        
        total_files = len(configurations)
        avg_batch_size = sum(c.batch_size for c in configurations.values()) / total_files
        total_size_mb = sum(c.file_size_mb for c in configurations.values())
        
        logger.info(
            f"Optimization totals: {total_files} files, "
            f"avg_batch_size={avg_batch_size:,.0f}, "
            f"total_size={total_size_mb:.1f}MB"
        )
