"""
Development mode filtering with bin-packing logic.
Ensures we maximize file count within a specific size budget (MB).
"""
from pathlib import Path
from typing import List, Dict, Any, TypeVar
import re
import polars as pl
from ...setup.config.models import DevelopmentConfig
from ...setup.logging import logger

T = TypeVar("T")

class DevelopmentFilter:
    """
    Bin-packing development filter.
    Maximizes number of files processed while staying strictly under size/count limits.
    """

    def __init__(self, config: DevelopmentConfig):
        self.config = config
        self.is_enabled = config.enabled

    def _bin_pack_items(self, items: List[T], get_size_mb: callable, table_name: str) -> List[T]:
        """
        Greedy bin-packing: Find maximum items that fit within the size and count limits.
        """
        if not self.is_enabled:
            return items

        limit_mb = self.config.file_size_limit_mb
        max_count = self.config.max_files_per_table
        
        packed = []
        current_size_mb = 0
        
        for item in items:
            size_mb = get_size_mb(item)
            
            # Always include the first file even if it exceeds the budget alone
            if not packed:
                packed.append(item)
                current_size_mb += size_mb
            elif (current_size_mb + size_mb) <= limit_mb and len(packed) < max_count:
                packed.append(item)
                current_size_mb += size_mb
            else:
                # Stop as soon as we hit the boundary (First Fit logic)
                break
                
        if len(packed) < len(items):
            logger.info(
                f"[DEV-MODE] {table_name}: Truncated to {len(packed)} files "
                f"({current_size_mb:.1f}MB / {limit_mb}MB limit)"
            )
            
        return packed

    def filter_files_by_blob_limit(self, file_paths: List[Path], table_name: str) -> List[Path]:
        """
        Apply bin-packing to a list of local file paths.
        """
        def get_size(p: Path) -> float:
            return p.stat().st_size / (1024 * 1024)
            
        return self._bin_pack_items(file_paths, get_size, table_name)

    def filter_files_by_blob_size_limit_with_file_info(self, file_infos: List[Any], group_by_table: bool = True) -> List[Any]:
        """
        Apply bin-packing to discovered FileInfo objects.
        Groups by table and packs each group individually.
        """
        if not self.is_enabled:
            return file_infos

        # Group by table
        table_groups = {}
        for info in file_infos:
            table_name = self._extract_table_name(info.filename)
            if table_name not in table_groups:
                table_groups[table_name] = []
            table_groups[table_name].append(info)

        filtered_results = []
        for table_name, group in table_groups.items():
            def get_size(info) -> float:
                return getattr(info, "file_size", 0) / (1024 * 1024)
                
            packed_group = self._bin_pack_items(group, get_size, table_name)
            filtered_results.extend(packed_group)

        return filtered_results

    def filter_dataframe_by_percentage(self, df: Any, table_name: str) -> Any:
        """Sample dataframe by configured percentage in development mode."""
        if not self.is_enabled:
            return df

        original_rows = len(df)
        row_limit_percent = self.config.row_limit_percent
        target_rows = max(1, int(original_rows * row_limit_percent))

        if target_rows >= original_rows:
            return df

        if hasattr(df, 'sample'):  # polars DataFrame
            sampled_df = df.sample(n=target_rows, seed=42)
        elif hasattr(df, 'head'):  # polars LazyFrame
            step = max(1, original_rows // target_rows)
            if hasattr(df, 'with_row_index'):
                 sampled_df = (df.with_row_index()
                             .filter(pl.col("index") % step == 0)
                             .head(target_rows)
                             .drop("index"))
            else:
                sampled_df = df.head(target_rows)
        else:
            raise TypeError(f"Unsupported DataFrame type: {type(df)}")

        logger.info(f"[DEV-MODE] {table_name}: Sampled {target_rows:,} rows ({row_limit_percent:.1%})")
        return sampled_df

    def _extract_table_name(self, filename: str) -> str:
        name = filename.lower()
        name = name.replace('.zip', '').replace('.csv', '').replace('.parquet', '')
        base_name = re.sub(r'\d+$', '', name)
        
        mapping = {
            'estabelecimento': 'estabelecimentos',
            'empresa': 'empresas',
            'socio': 'socios',
            'cnae': 'cnaes'
        }
        return mapping.get(base_name, base_name)

    def log_simple_filtering(self, original_count: int, filtered_count: int, item_type: str) -> None:
        if self.is_enabled and original_count != filtered_count:
            reduction = (1 - filtered_count / original_count) * 100
            logger.info(f"[DEV-MODE] {item_type.capitalize()} packed: {original_count} -> {filtered_count} ({reduction:.1f}% reduction)")

    def log_conversion_summary(self, audit_map: Dict[str, Dict[str, List[str]]]) -> None:
        if self.is_enabled:
            tables_count = len(audit_map)
            total_files = sum(sum(len(csvs) for csvs in zips.values()) for zips in audit_map.values())
            logger.info(f"[DEV-MODE] Conversion packing: {tables_count} tables, {total_files} files total")
