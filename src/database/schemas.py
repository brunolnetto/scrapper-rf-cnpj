from typing import List, Dict, Optional, Callable, Any
from dataclasses import dataclass, field
from pathlib import Path

from ..setup.logging import logger


@dataclass
class TableInfo:
    """
    Represents comprehensive information about a table with dynamic metadata support.
    
    This replaces the rigid NamedTuple implementation with a more flexible dataclass
    that can be enhanced with additional attributes as needed.
    """
    
    # Core required attributes (from original NamedTuple)
    label: str
    zip_group: str
    table_name: str
    columns: List[str]
    encoding: str
    transform_map: Callable
    expression: str
    
    # Optional attributes for enhanced functionality
    table_model: Optional[object] = None
    primary_keys: Optional[List[str]] = None
    types: Optional[Dict[str, str]] = None
    
    # Additional metadata for loading optimization
    estimated_rows: Optional[int] = None
    file_size_mb: Optional[float] = None
    processing_hints: Dict[str, Any] = field(default_factory=dict)
    
    # Audit and tracking information
    creation_metadata: Dict[str, Any] = field(default_factory=dict)
    validation_status: Optional[str] = None
    last_enhanced_at: Optional[str] = None
    
    def __post_init__(self):
        """
        Post-initialization to ensure consistent state and auto-enhance metadata.
        """
        # Auto-enhance with missing metadata if possible
        if not self.primary_keys or not self.types:
            try:
                self._auto_enhance_metadata()
            except Exception as e:
                logger.debug(f"Auto-enhancement failed for {self.table_name}: {e}")
        
        # Validate required attributes
        self._validate_core_attributes()
        
        # Set creation metadata
        from datetime import datetime
        self.creation_metadata.update({
            'created_at': datetime.now().isoformat(),
            'version': '2.0',
            'auto_enhanced': bool(self.primary_keys or self.types)
        })
    
    def _validate_core_attributes(self):
        """Validate that core attributes are properly set."""
        if not self.table_name:
            raise ValueError("table_name cannot be empty")
        
        if not self.columns:
            raise ValueError(f"columns cannot be empty for table {self.table_name}")
        
        if not isinstance(self.columns, list):
            raise TypeError(f"columns must be a list for table {self.table_name}")
    
    def _auto_enhance_metadata(self):
        """
        Automatically enhance the TableInfo with missing metadata.
        """
        try:
            # Try to get primary keys if missing
            if not self.primary_keys and self.table_model:
                self.primary_keys = self._extract_primary_keys_from_model()
            
            # Try to get column types if missing
            if not self.types and self.table_model:
                self.types = self._extract_types_from_model()
            
            # Set enhancement timestamp
            from datetime import datetime
            self.last_enhanced_at = datetime.now().isoformat()
            
        except Exception as e:
            logger.debug(f"Auto-enhancement partial failure for {self.table_name}: {e}")
    
    def _extract_primary_keys_from_model(self) -> List[str]:
        """Extract primary keys from SQLAlchemy model."""
        if not self.table_model:
            return []
        
        try:
            from sqlalchemy import inspect
            inspector = inspect(self.table_model)
            pk_columns = []
            for column in inspector.columns:
                if column.primary_key:
                    pk_columns.append(column.name)
            return pk_columns
        except Exception as e:
            logger.debug(f"Primary key extraction failed: {e}")
            return []
    
    def _extract_types_from_model(self) -> Dict[str, str]:
        """Extract PostgreSQL column types from SQLAlchemy model."""
        if not self.table_model:
            return {}
        
        try:
            types = {}
            for column in self.table_model.__table__.columns:
                # Map SQLAlchemy types to PostgreSQL types
                col_type = str(column.type).upper()
                if any(t in col_type for t in ['VARCHAR', 'TEXT', 'STRING']):
                    types[column.name] = 'TEXT'
                elif any(t in col_type for t in ['FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL']):
                    types[column.name] = 'DOUBLE PRECISION'
                elif any(t in col_type for t in ['INTEGER', 'BIGINT', 'INT']):
                    types[column.name] = 'BIGINT'
                elif 'TIMESTAMP' in col_type:
                    types[column.name] = 'TIMESTAMP WITH TIME ZONE'
                elif 'DATE' in col_type:
                    types[column.name] = 'DATE'
                else:
                    types[column.name] = 'TEXT'  # Safe default
            return types
        except Exception as e:
            logger.debug(f"Type extraction failed: {e}")
            return {}
    
    def ensure_primary_keys(self) -> List[str]:
        """
        Ensure primary keys are available with fallback strategies.
        """
        if self.primary_keys:
            return self.primary_keys
        
        # Strategy 1: Extract from model
        if self.table_model:
            self.primary_keys = self._extract_primary_keys_from_model()
            if self.primary_keys:
                return self.primary_keys
        
        # Strategy 2: Common patterns
        common_patterns = ['id', f'{self.table_name}_id', 'pk', 'key']
        for pattern in common_patterns:
            if pattern in self.columns:
                logger.info(f"Inferred primary key '{pattern}' for {self.table_name}")
                self.primary_keys = [pattern]
                return self.primary_keys
        
        # Strategy 3: Use first column as fallback
        if self.columns:
            logger.warning(f"Using first column '{self.columns[0]}' as primary key for {self.table_name}")
            self.primary_keys = [self.columns[0]]
            return self.primary_keys
        
        self.primary_keys = []
        return self.primary_keys
    
    def ensure_types(self) -> Dict[str, str]:
        """
        Ensure column types are available with fallback strategies.
        """
        if self.types:
            return self.types
        
        # Strategy 1: Extract from model
        if self.table_model:
            self.types = self._extract_types_from_model()
            if self.types:
                return self.types
        
        # Strategy 2: Infer from column names
        self.types = self._infer_types_from_names()
        return self.types
    
    def _infer_types_from_names(self) -> Dict[str, str]:
        """Infer PostgreSQL types from column names using common patterns."""
        type_patterns = {
            'id': 'BIGINT',
            '_id': 'BIGINT', 
            'count': 'BIGINT',
            'number': 'BIGINT',
            'num_': 'BIGINT',
            'amount': 'DOUBLE PRECISION',
            'value': 'DOUBLE PRECISION',
            'price': 'DOUBLE PRECISION',
            'rate': 'DOUBLE PRECISION',
            'percent': 'DOUBLE PRECISION',
            'date': 'DATE',
            'created_at': 'TIMESTAMP WITH TIME ZONE',
            'updated_at': 'TIMESTAMP WITH TIME ZONE',
            'timestamp': 'TIMESTAMP WITH TIME ZONE',
            'time': 'TIMESTAMP WITH TIME ZONE',
            'is_': 'BOOLEAN',
            'has_': 'BOOLEAN',
            'active': 'BOOLEAN',
            'enabled': 'BOOLEAN'
        }
        
        inferred_types = {}
        for col in self.columns:
            col_lower = col.lower()
            inferred_type = 'TEXT'  # Default
            
            for pattern, pg_type in type_patterns.items():
                if pattern in col_lower:
                    inferred_type = pg_type
                    break
            
            inferred_types[col] = inferred_type
        
        return inferred_types
    
    def add_processing_hint(self, key: str, value: Any):
        """Add a processing hint for optimization."""
        self.processing_hints[key] = value
    
    def get_processing_hint(self, key: str, default: Any = None) -> Any:
        """Get a processing hint value."""
        return self.processing_hints.get(key, default)
    
    def estimate_memory_usage(self) -> float:
        """
        Estimate memory usage in MB for loading this table.
        """
        if not self.estimated_rows:
            return 0.0
        
        # Rough estimation: average 100 bytes per row + overhead
        base_size = (self.estimated_rows * len(self.columns) * 100) / (1024 * 1024)
        
        # Add overhead for processing
        overhead_multiplier = 2.5  # Accounts for temp tables, transforms, etc.
        
        estimated_mb = base_size * overhead_multiplier
        return round(estimated_mb, 2)
    
    def set_file_metadata(self, file_path: Path):
        """Set file-related metadata from actual file."""
        try:
            if file_path.exists():
                stat = file_path.stat()
                self.file_size_mb = round(stat.st_size / (1024 * 1024), 2)
                
                # Rough estimation of rows based on file size
                # Assumes average 200 bytes per row (very rough)
                if not self.estimated_rows:
                    self.estimated_rows = int(stat.st_size / 200)
                
                self.add_processing_hint('source_file', str(file_path))
                self.add_processing_hint('file_format', file_path.suffix.lstrip('.'))
        except Exception as e:
            logger.debug(f"Could not set file metadata for {file_path}: {e}")
    
    def validate_for_loading(self) -> bool:
        """
        Validate that TableInfo has all necessary attributes for loading.
        """
        issues = []
        
        # Check core attributes
        if not self.table_name:
            issues.append("Missing table_name")
        
        if not self.columns:
            issues.append("Missing columns")
        
        # Check enhanced attributes
        primary_keys = self.ensure_primary_keys()
        if not primary_keys:
            issues.append("No primary keys available")
        
        types = self.ensure_types()
        if len(types) != len(self.columns):
            issues.append("Column types incomplete")
        
        # Set validation status
        if issues:
            self.validation_status = f"FAILED: {'; '.join(issues)}"
            logger.error(f"TableInfo validation failed for {self.table_name}: {self.validation_status}")
            return False
        else:
            self.validation_status = "PASSED"
            return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert TableInfo to dictionary for serialization."""
        return {
            'label': self.label,
            'zip_group': self.zip_group,
            'table_name': self.table_name,
            'columns': self.columns,
            'encoding': self.encoding,
            'expression': self.expression,
            'primary_keys': self.primary_keys,
            'types': self.types,
            'estimated_rows': self.estimated_rows,
            'file_size_mb': self.file_size_mb,
            'processing_hints': self.processing_hints,
            'creation_metadata': self.creation_metadata,
            'validation_status': self.validation_status,
            'last_enhanced_at': self.last_enhanced_at
        }
    
    def __repr__(self) -> str:
        """Enhanced string representation."""
        pk_info = f", pks={len(self.primary_keys or [])}" if self.primary_keys else ", pks=auto"
        type_info = f", types={len(self.types or {})}" if self.types else ", types=auto"
        size_info = f", ~{self.file_size_mb}MB" if self.file_size_mb else ""
        
        return (f"TableInfo(name='{self.table_name}', "
                f"cols={len(self.columns)}{pk_info}{type_info}{size_info})")


# Factory functions for creating enhanced TableInfo objects

def create_table_info_from_dict(table_name: str, config_dict: Dict[str, Any]) -> TableInfo:
    """
    Create TableInfo from configuration dictionary (replaces the old factory pattern).
    """
    try:
        # Get table model if available
        table_model = None
        try:
            from ..database.utils import get_model_by_table_name
            table_model = get_model_by_table_name(table_name)
        except Exception as e:
            logger.debug(f"Could not get table model for {table_name}: {e}")
        
        # Get columns from model or config
        columns = config_dict.get('columns', [])
        if not columns and table_model:
            try:
                columns = [col.name for col in table_model.__table__.columns]
            except Exception:
                pass
        
        # Create TableInfo instance
        table_info = TableInfo(
            label=config_dict.get('label', table_name),
            zip_group=config_dict.get('group', 'default'),
            table_name=table_name,
            columns=columns,
            encoding=config_dict.get('encoding', 'utf-8'),
            transform_map=config_dict.get('transform_map', lambda x: x),
            expression=config_dict.get('expression', ''),
            table_model=table_model
        )
        
        # Validate the created TableInfo
        if not table_info.validate_for_loading():
            logger.warning(f"TableInfo validation issues for {table_name}")
        
        return table_info
        
    except Exception as e:
        logger.error(f"Failed to create TableInfo for {table_name}: {e}")
        raise


def enhanced_table_name_to_table_info(table_name: str) -> TableInfo:
    """
    Enhanced replacement for the original table_name_to_table_info function.
    Use this in your LoadingService instead of the old function.
    """
    try:
        # Import the original constants
        from ..core.constants import TABLES_INFO_DICT
        
        if table_name not in TABLES_INFO_DICT:
            raise ValueError(f"Table '{table_name}' not found in configuration")
        
        config_dict = TABLES_INFO_DICT[table_name]
        table_info = create_table_info_from_dict(table_name, config_dict)
        
        logger.debug(f"Created enhanced TableInfo: {table_info}")
        return table_info
        
    except Exception as e:
        logger.error(f"Failed to create enhanced TableInfo for {table_name}: {e}")
        raise


# Compatibility function to ensure smooth transition
def table_info_to_legacy_format(table_info: TableInfo) -> tuple:
    """
    Convert new TableInfo back to old NamedTuple format if needed for compatibility.
    """
    try:
        from collections import namedtuple
        LegacyTableInfo = namedtuple('TableInfo', [
            'label', 'zip_group', 'table_name', 'columns', 
            'encoding', 'transform_map', 'expression', 'table_model'
        ])
        
        return LegacyTableInfo(
            label=table_info.label,
            zip_group=table_info.zip_group,
            table_name=table_info.table_name,
            columns=table_info.columns,
            encoding=table_info.encoding,
            transform_map=table_info.transform_map,
            expression=table_info.expression,
            table_model=table_info.table_model
        )
    except Exception as e:
        logger.error(f"Legacy format conversion failed: {e}")
        raise