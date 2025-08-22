# Comprehensive Refactoring Plan: Enhanced File Loading Integration

## Executive Summary

This document outlines a comprehensive plan to replace the current ETL loading logic in `src/core/loading/` with the enhanced, robust file loading system developed in `lab/refactored_fileloader/`. The refactoring will improve reliability, performance, and maintainability while preserving backward compatibility.

## Current State Analysis

### Current Architecture (`src/core/loading/`)
```
ETLOrchestrator → CNPJ_ETL → DataLoadingService → DataLoadingStrategy → UnifiedLoader
```

**Strengths:**
- Well-structured strategy pattern
- Lazy loading with proper configuration management
- Dual database architecture (main/audit)
- Parquet-first approach with CSV fallback

**Limitations:**
- Basic file detection (extension-based only)
- Pandas-only processing (memory intensive)
- No internal parallelism
- Limited error recovery
- Basic audit trail

### Enhanced Architecture (`lab/refactored_fileloader/`)
```
FileLoader → Ingestors (CSV/Parquet) → AsyncUploader → Database
```

**Improvements:**
- 4-layer robust file detection with content validation
- Specialized ingestors with dialect detection
- Async processing with internal parallelism
- Complete manifest audit trail with checksums
- Better error handling and recovery

## Refactoring Strategy

### Phase 1: Foundation and Detection Enhancement
**Duration:** 2-3 hours  
**Risk:** Low  
**Priority:** High

#### 1.1 Configuration Enhancement
**File:** `src/setup/config.py`

Add enhanced loading configuration to `ETLConfig`:
```python
@dataclass
class ETLConfig:
    # Existing fields...
    
    # Enhanced loading features
    enhanced_loading_enabled: bool = False
    robust_file_detection: bool = True
    enable_parallel_processing: bool = False
    internal_concurrency: int = 3
    manifest_tracking: bool = False
    
    # Performance tuning
    sub_batch_size: int = 5000
    async_pool_min_size: int = 1
    async_pool_max_size: int = 10
```

Environment variables:
```env
# Enhanced Loading Configuration
ENHANCED_LOADING_ENABLED=true
ROBUST_FILE_DETECTION=true
ENABLE_PARALLEL_PROCESSING=false
INTERNAL_CONCURRENCY=3
MANIFEST_TRACKING=true
SUB_BATCH_SIZE=5000
ASYNC_POOL_MIN_SIZE=1
ASYNC_POOL_MAX_SIZE=10
```

#### 1.2 Enhanced File Detection Integration
**File:** `src/utils/file_detection.py` (new)

Create wrapper for robust detection:
```python
"""
Enhanced file detection module integrating refactored file loader.
"""
from pathlib import Path
from typing import Optional
from ..setup.logging import logger

def detect_file_type_enhanced(file_path: Path) -> str:
    """
    Enhanced file type detection using refactored file loader.
    Falls back to basic detection if enhanced loader is not available.
    """
    try:
        from lab.refactored_fileloader.src.file_loader import FileLoader
        return FileLoader.detect_file_format(str(file_path))
    except ImportError as e:
        logger.warning(f"Enhanced file detection not available: {e}")
        return _basic_file_detection(file_path)
    except Exception as e:
        logger.error(f"Enhanced detection failed for {file_path}: {e}")
        return _basic_file_detection(file_path)

def _basic_file_detection(file_path: Path) -> str:
    """Fallback basic detection (current implementation)."""
    # Current logic from UnifiedLoader.load_file()
    pass
```

#### 1.3 Update UnifiedLoader Detection
**File:** `src/database/dml.py`

Replace basic detection in `UnifiedLoader.load_file()`:
```python
def load_file(self, table_info: TableInfo, file_path: Union[str, Path], ...):
    file_path = Path(file_path)
    
    # Use enhanced detection
    try:
        from utils.file_detection import detect_file_type_enhanced
        file_type = detect_file_type_enhanced(file_path)
    except ImportError:
        # Keep existing fallback logic
        logger.warning("Enhanced file detection not available, using basic detection")
        # ... existing basic detection code
```

### Phase 2: Enhanced Strategy Implementation
**Duration:** 4-5 hours  
**Risk:** Medium  
**Priority:** High

#### 2.1 Create Enhanced Loading Strategy
**File:** `src/core/loading/enhanced_strategies.py` (new)

```python
"""
Enhanced data loading strategies using refactored file loader components.
"""
from abc import ABC
from typing import Dict, List, Tuple, Optional
import asyncio
from pathlib import Path

from ...database.schemas import Database
from ...setup.logging import logger
from ...setup.config import PathConfig
from .strategies import BaseDataLoadingStrategy
from ...database.dml import table_name_to_table_info

class EnhancedDataLoadingStrategy(BaseDataLoadingStrategy):
    """Enhanced loading strategy with robust detection and optional async processing."""
    
    def __init__(self, config):
        self.config = config
        self.enhanced_enabled = config.etl.enhanced_loading_enabled
        self.parallel_enabled = config.etl.enable_parallel_processing
        self.internal_concurrency = config.etl.internal_concurrency
        
    def load_table(self, database: Database, table_name: str, path_config: PathConfig, 
                   table_files: Optional[List[str]] = None) -> Tuple[bool, Optional[str], int]:
        """Load table with enhanced detection and optional async processing."""
        
        if not self.enhanced_enabled:
            # Fallback to standard strategy
            from .strategies import DataLoadingStrategy
            standard_strategy = DataLoadingStrategy()
            return standard_strategy.load_table(database, table_name, path_config, table_files)
        
        logger.info(f"[Enhanced] Loading table '{table_name}' with enhanced strategy")
        
        try:
            table_info = table_name_to_table_info(table_name)
            
            # Check Parquet first (maintain existing priority)
            parquet_file = path_config.conversion_path / f"{table_name}.parquet"
            if parquet_file.exists():
                return self._load_parquet_enhanced(database, table_info, parquet_file)
            elif table_files:
                return self._load_csv_files_enhanced(database, table_info, path_config, table_files)
            else:
                return False, f"No files found for table {table_name}", 0
                
        except Exception as e:
            logger.error(f"[Enhanced] Failed to load table '{table_name}': {e}")
            # Fallback to standard loading
            return self._fallback_to_standard(database, table_name, path_config, table_files)
    
    def _load_parquet_enhanced(self, database, table_info, parquet_file):
        """Load Parquet file with enhanced validation."""
        try:
            from lab.refactored_fileloader.src.file_loader import FileLoader
            
            # Robust detection and validation
            loader = FileLoader(str(parquet_file))
            if loader.get_format() != 'parquet':
                raise ValueError(f"File validation failed: expected parquet, got {loader.get_format()}")
            
            logger.info(f"[Enhanced] Validated Parquet file: {parquet_file.name}")
            
            # Use enhanced loading if parallel processing is enabled
            if self.parallel_enabled and self._should_use_async(table_info.table_name):
                return self._load_parquet_async(database, table_info, parquet_file, loader)
            else:
                # Use existing UnifiedLoader with enhanced confidence
                from ...database.dml import UnifiedLoader
                return UnifiedLoader(database).load_parquet_file(table_info, parquet_file)
                
        except Exception as e:
            logger.warning(f"[Enhanced] Parquet enhanced loading failed: {e}, falling back")
            return self._fallback_parquet_loading(database, table_info, parquet_file)
    
    def _load_csv_files_enhanced(self, database, table_info, path_config, table_files):
        """Load CSV files with enhanced detection and processing."""
        total_rows = 0
        
        for filename in table_files:
            csv_file = path_config.extract_path / filename
            
            try:
                from lab.refactored_fileloader.src.file_loader import FileLoader
                
                # Robust detection and validation
                loader = FileLoader(str(csv_file))
                if loader.get_format() != 'csv':
                    logger.warning(f"File {filename} detected as {loader.get_format()}, treating as CSV")
                
                # Use enhanced loading if parallel processing is enabled
                if self.parallel_enabled and self._should_use_async(table_info.table_name):
                    success, error, rows = self._load_csv_async(database, table_info, csv_file, loader)
                else:
                    # Use existing UnifiedLoader with enhanced confidence
                    from ...database.dml import UnifiedLoader
                    success, error, rows = UnifiedLoader(database).load_csv_file(table_info, csv_file)
                
                if not success:
                    return success, error, total_rows
                total_rows += rows
                
            except Exception as e:
                logger.warning(f"[Enhanced] CSV enhanced loading failed for {filename}: {e}")
                # Fallback for this file
                success, error, rows = self._fallback_csv_loading(database, table_info, csv_file)
                if not success:
                    return success, error, total_rows
                total_rows += rows
        
        return True, None, total_rows
    
    def _should_use_async(self, table_name: str) -> bool:
        """Determine if async processing should be used for this table."""
        # Use async for large tables
        large_tables = {'empresa', 'estabelecimento', 'socios', 'simples'}
        return table_name in large_tables
    
    def _load_parquet_async(self, database, table_info, parquet_file, loader):
        """Load Parquet file using async processing."""
        # Implementation for async Parquet loading
        # This would integrate with the async uploader
        logger.info(f"[Enhanced] Using async processing for Parquet: {parquet_file.name}")
        # TODO: Implement async integration
        return self._fallback_parquet_loading(database, table_info, parquet_file)
    
    def _load_csv_async(self, database, table_info, csv_file, loader):
        """Load CSV file using async processing."""
        # Implementation for async CSV loading
        logger.info(f"[Enhanced] Using async processing for CSV: {csv_file.name}")
        # TODO: Implement async integration
        return self._fallback_csv_loading(database, table_info, csv_file)
    
    def _fallback_to_standard(self, database, table_name, path_config, table_files):
        """Fallback to standard loading strategy."""
        logger.info(f"[Enhanced] Falling back to standard loading for table '{table_name}'")
        from .strategies import DataLoadingStrategy
        standard_strategy = DataLoadingStrategy()
        return standard_strategy.load_table(database, table_name, path_config, table_files)
    
    def _fallback_parquet_loading(self, database, table_info, parquet_file):
        """Fallback Parquet loading using current UnifiedLoader."""
        from ...database.dml import UnifiedLoader
        return UnifiedLoader(database).load_parquet_file(table_info, parquet_file)
    
    def _fallback_csv_loading(self, database, table_info, csv_file):
        """Fallback CSV loading using current UnifiedLoader."""
        from ...database.dml import UnifiedLoader
        return UnifiedLoader(database).load_csv_file(table_info, csv_file)

    def load_multiple_tables(self, database, table_to_files, path_config: PathConfig):
        """Load multiple tables with enhanced processing."""
        results = {}
        for table_name, zipfile_to_files in table_to_files.items():
            # Flatten the nested structure
            all_files = []
            for zip_filename, csv_files in zipfile_to_files.items():
                all_files.extend(csv_files)
            
            if not all_files:
                logger.warning(f"[Enhanced] No files found for table '{table_name}'")
                results[table_name] = (False, "No files found", 0)
                continue
                
            results[table_name] = self.load_table(database, table_name, path_config, all_files)
        return results
```

#### 2.2 Update DataLoadingService
**File:** `src/core/loading/service.py`

Add support for enhanced strategy:
```python
def __init__(self, database: Database, path_config: PathConfig, strategy: BaseDataLoadingStrategy, config=None):
    self.database = database
    self.path_config = path_config
    
    # Auto-select enhanced strategy if enabled
    if config and config.etl.enhanced_loading_enabled:
        try:
            from .enhanced_strategies import EnhancedDataLoadingStrategy
            self.strategy = EnhancedDataLoadingStrategy(config)
            logger.info("Using enhanced loading strategy")
        except ImportError:
            logger.warning("Enhanced strategy not available, using provided strategy")
            self.strategy = strategy
    else:
        self.strategy = strategy
```

### Phase 3: Async Integration Layer
**Duration:** 6-8 hours  
**Risk:** High  
**Priority:** Medium

#### 3.1 Async Database Bridge
**File:** `src/core/loading/async_bridge.py` (new)

```python
"""
Bridge between SQLAlchemy (sync) and asyncpg (async) for enhanced loading.
"""
import asyncio
import asyncpg
from typing import List, Dict, Tuple, Optional
from ...setup.logging import logger
from ...database.schemas import Database

class AsyncDatabaseBridge:
    """Bridge for integrating async processing with existing SQLAlchemy setup."""
    
    def __init__(self, database: Database, config):
        self.database = database
        self.config = config
        self._pool = None
    
    async def get_async_pool(self) -> asyncpg.Pool:
        """Get or create async connection pool."""
        if self._pool is None:
            # Extract connection details from SQLAlchemy engine
            url = self.database.engine.url
            dsn = f"postgresql://{url.username}:{url.password}@{url.host}:{url.port}/{url.database}"
            
            self._pool = await asyncpg.create_pool(
                dsn,
                min_size=self.config.etl.async_pool_min_size,
                max_size=self.config.etl.async_pool_max_size
            )
        return self._pool
    
    async def load_file_async(self, table_info, file_path, loader):
        """Load file using async uploader with SQLAlchemy compatibility."""
        try:
            from lab.refactored_fileloader.src.uploader import async_upsert
            
            pool = await self.get_async_pool()
            
            # Convert table_info to async uploader format
            headers = table_info.columns
            table_name = table_info.table_name
            primary_keys = self._get_primary_keys(table_info)
            
            # Use refactored batch generator
            batch_gen = loader._batch_generator
            
            rows_processed = await async_upsert(
                pool=pool,
                file_path=str(file_path),
                headers=headers,
                table=table_name,
                primary_keys=primary_keys,
                batch_gen=batch_gen,
                chunk_size=self.config.etl.chunk_size,
                sub_batch_size=self.config.etl.sub_batch_size,
                enable_internal_parallelism=self.config.etl.enable_parallel_processing,
                internal_concurrency=self.config.etl.internal_concurrency
            )
            
            return True, None, rows_processed
            
        except Exception as e:
            logger.error(f"Async loading failed: {e}")
            return False, str(e), 0
    
    def _get_primary_keys(self, table_info) -> List[str]:
        """Extract primary keys from table_info."""
        # Get from constants or SQLAlchemy metadata
        from ...core.constants import TABLES_INFO_DICT
        table_dict = TABLES_INFO_DICT.get(table_info.table_name, {})
        return table_dict.get('index_columns', ['cnpj_basico'])
    
    async def close(self):
        """Close async pool."""
        if self._pool:
            await self._pool.close()
```

#### 3.2 Async Integration in Enhanced Strategy
Update `_load_parquet_async` and `_load_csv_async` in enhanced strategy:

```python
def _load_parquet_async(self, database, table_info, parquet_file, loader):
    """Load Parquet file using async processing."""
    try:
        import asyncio
        from .async_bridge import AsyncDatabaseBridge
        
        bridge = AsyncDatabaseBridge(database, self.config)
        
        # Run async operation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                bridge.load_file_async(table_info, parquet_file, loader)
            )
            loop.run_until_complete(bridge.close())
            return result
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Async Parquet loading failed: {e}")
        return self._fallback_parquet_loading(database, table_info, parquet_file)
```

### Phase 4: Manifest and Audit Enhancement
**Duration:** 3-4 hours  
**Risk:** Low  
**Priority:** Medium

#### 4.1 Enhanced Audit Integration
**File:** `src/core/audit/enhanced_service.py` (new)

```python
"""
Enhanced audit service integrating manifest tracking from refactored loader.
"""
from typing import List, Optional
from ...setup.logging import logger
from ...database.schemas import Database
from .service import AuditService

class EnhancedAuditService(AuditService):
    """Enhanced audit service with manifest integration."""
    
    def __init__(self, database: Database, config):
        super().__init__(database)
        self.config = config
        self.manifest_enabled = config.etl.manifest_tracking
    
    def create_file_manifest(self, file_path: str, status: str, checksum: Optional[bytes] = None, 
                           filesize: Optional[int] = None, rows: Optional[int] = None):
        """Create manifest entry for processed file."""
        if not self.manifest_enabled:
            return
        
        try:
            # Insert into manifest table (create if needed)
            self._ensure_manifest_table()
            self._insert_manifest_entry(file_path, status, checksum, filesize, rows)
        except Exception as e:
            logger.error(f"Failed to create manifest entry: {e}")
    
    def _ensure_manifest_table(self):
        """Ensure manifest table exists."""
        try:
            from lab.refactored_fileloader.src.base import INGESTION_MANIFEST_SCHEMA
            with self.database.engine.begin() as conn:
                conn.execute(text(INGESTION_MANIFEST_SCHEMA))
        except Exception as e:
            logger.warning(f"Could not create manifest table: {e}")
    
    def _insert_manifest_entry(self, file_path, status, checksum, filesize, rows):
        """Insert manifest entry."""
        # Implementation to insert into ingestion_manifest table
        pass
```

#### 4.2 Update CNPJ_ETL for Enhanced Audit
**File:** `src/core/etl.py`

Add enhanced audit service:
```python
@property
def enhanced_audit_service(self):
    """Lazy enhanced audit service initialization."""
    if not hasattr(self, '_enhanced_audit_service') or self._enhanced_audit_service is None:
        if self.config.etl.enhanced_loading_enabled:
            from .audit.enhanced_service import EnhancedAuditService
            self._enhanced_audit_service = EnhancedAuditService(self.database, self.config)
        else:
            self._enhanced_audit_service = self.audit_service
    return self._enhanced_audit_service
```

### Phase 5: Testing and Validation
**Duration:** 4-6 hours  
**Risk:** Low  
**Priority:** High

#### 5.1 Integration Tests
**File:** `tests/integration/test_enhanced_loading.py` (new)

```python
"""
Integration tests for enhanced loading functionality.
"""
import pytest
from pathlib import Path
from src.setup.config import ConfigurationService
from src.core.loading.enhanced_strategies import EnhancedDataLoadingStrategy

class TestEnhancedLoading:
    
    def test_enhanced_detection(self):
        """Test enhanced file detection works correctly."""
        pass
    
    def test_parquet_loading_enhanced(self):
        """Test enhanced Parquet loading."""
        pass
    
    def test_csv_loading_enhanced(self):
        """Test enhanced CSV loading."""
        pass
    
    def test_fallback_behavior(self):
        """Test fallback to standard loading."""
        pass
    
    def test_async_processing_integration(self):
        """Test async processing integration."""
        pass
```

#### 5.2 Performance Benchmarks
**File:** `tests/performance/benchmark_enhanced_vs_standard.py` (new)

```python
"""
Performance benchmarks comparing enhanced vs standard loading.
"""
import time
from src.core.loading.strategies import DataLoadingStrategy
from src.core.loading.enhanced_strategies import EnhancedDataLoadingStrategy

def benchmark_loading_strategies():
    """Benchmark different loading strategies."""
    # Implementation for performance comparison
    pass
```

### Phase 6: Migration and Deployment
**Duration:** 2-3 hours  
**Risk:** Low  
**Priority:** High

#### 6.1 Feature Flag Implementation
**File:** `src/core/etl.py`

Add feature flag support:
```python
def _init_loading_strategy(self):
    """Initialize appropriate loading strategy based on configuration."""
    if self.config.etl.enhanced_loading_enabled:
        try:
            from .loading.enhanced_strategies import EnhancedDataLoadingStrategy
            self.loading_strategy = EnhancedDataLoadingStrategy(self.config)
            logger.info("Enhanced loading strategy enabled")
        except ImportError as e:
            logger.warning(f"Enhanced loading not available: {e}")
            from .loading.strategies import DataLoadingStrategy
            self.loading_strategy = DataLoadingStrategy()
    else:
        from .loading.strategies import DataLoadingStrategy
        self.loading_strategy = DataLoadingStrategy()
```

#### 6.2 Documentation Updates
**File:** `docs/ENHANCED_LOADING.md` (new)

```markdown
# Enhanced Loading System

## Overview
The enhanced loading system provides improved reliability, performance, and audit capabilities.

## Configuration
Add to your `.env` file:
```env
ENHANCED_LOADING_ENABLED=true
ROBUST_FILE_DETECTION=true
ENABLE_PARALLEL_PROCESSING=false
```

## Features
- 4-layer robust file detection
- Internal parallelism for large files
- Complete manifest audit trail
- Graceful fallback to standard loading

## Migration Guide
1. Enable enhanced loading: `ENHANCED_LOADING_ENABLED=true`
2. Test with small datasets first
3. Monitor performance and logs
4. Enable parallel processing for large tables if needed
```

## Implementation Timeline

| Phase | Duration | Prerequisites | Risk Level |
|-------|----------|---------------|------------|
| Phase 1: Foundation | 2-3 hours | None | Low |
| Phase 2: Enhanced Strategy | 4-5 hours | Phase 1 | Medium |
| Phase 3: Async Integration | 6-8 hours | Phase 2 | High |
| Phase 4: Audit Enhancement | 3-4 hours | Phase 1 | Low |
| Phase 5: Testing | 4-6 hours | Phases 1-4 | Low |
| Phase 6: Migration | 2-3 hours | Phase 5 | Low |
| **Total** | **21-29 hours** | | |

## Risk Mitigation

### High-Risk Areas
1. **Async Integration**: Complex interaction between SQLAlchemy and asyncpg
   - *Mitigation*: Implement bridge pattern with fallback
   - *Testing*: Extensive integration tests

2. **Performance Regression**: Enhanced detection might be slower
   - *Mitigation*: Feature flags and performance benchmarks
   - *Testing*: A/B testing with production-like data

### Low-Risk Areas
1. **File Detection Enhancement**: Additive functionality with fallback
2. **Configuration Changes**: Backward compatible with defaults
3. **Audit Integration**: Optional feature with existing audit system

## Success Criteria

### Phase 1 Success
- [ ] Enhanced file detection works correctly
- [ ] Fallback to basic detection functions
- [ ] No regression in existing functionality

### Phase 2 Success
- [ ] Enhanced strategy loads Parquet and CSV files
- [ ] Graceful fallback to standard strategy
- [ ] Configuration-driven feature activation

### Phase 3 Success
- [ ] Async processing works for large tables
- [ ] No memory leaks in async operations
- [ ] Performance improvement measurable

### Phase 4 Success
- [ ] Manifest table created and populated
- [ ] Enhanced audit entries match files processed
- [ ] Backward compatibility maintained

### Phase 5 Success
- [ ] All integration tests pass
- [ ] Performance benchmarks show improvement
- [ ] Error scenarios handled gracefully

### Phase 6 Success
- [ ] Feature flags control functionality
- [ ] Documentation complete and accurate
- [ ] Migration path validated

## Rollback Plan

1. **Immediate Rollback**: Set `ENHANCED_LOADING_ENABLED=false`
2. **Partial Rollback**: Disable specific features (async, manifest)
3. **Complete Rollback**: Revert to commit before enhanced strategy
4. **Data Integrity**: Enhanced system maintains same data validation

## Monitoring and Metrics

### Key Metrics to Track
- Loading performance (time per table)
- Memory usage during processing
- File detection accuracy
- Error rates and fallback frequency
- Manifest completeness

### Alerting Thresholds
- Loading time increase > 50%
- Memory usage increase > 30%
- Error rate > 5%
- Fallback usage > 10%

## Post-Implementation Tasks

1. **Performance Tuning**: Optimize based on production metrics
2. **Feature Expansion**: Add more async tables based on performance
3. **Monitoring Dashboard**: Create visualization for enhanced loading metrics
4. **Documentation**: Update user guides and troubleshooting docs
5. **Training**: Team training on new features and monitoring

---

**Note**: This refactoring plan maintains backward compatibility and provides multiple safety mechanisms. The enhanced system is designed to improve reliability and performance while ensuring the existing ETL pipeline continues to function without interruption.
