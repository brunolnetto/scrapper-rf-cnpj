"""Metric collection for every audit level and the overall audit summary.

Mixin of :class:`AuditService`; relies on attributes set in its ``__init__``.
"""

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import text

from ....setup.logging import logger


class AuditMetricsMixin:
    """Metric collection for every audit level and the overall audit summary."""

    def collect_comprehensive_column_metrics(self, table_name: str, parquet_file_path: str) -> Dict[str, Any]:
        """
        Collect comprehensive column-level metrics from a Parquet file.
        
        Args:
            table_name: Name of the table
            parquet_file_path: Path to the Parquet file
            
        Returns:
            Dictionary containing comprehensive column metrics
        """
        try:
            from pathlib import Path

            import polars as pl
            
            file_path = Path(parquet_file_path)
            if not file_path.exists():
                logger.warning(f"Parquet file not found: {parquet_file_path}")
                return {}
                
            logger.info(f"Collecting comprehensive metrics for {table_name} from {file_path.name}")
            
            # Read parquet file with Polars for efficient analysis
            df = pl.read_parquet(str(file_path))
            
            total_rows = len(df)
            if total_rows == 0:
                return {
                    "table_name": table_name,
                    "total_rows": 0,
                    "total_columns": len(df.columns),
                    "file_size_bytes": file_path.stat().st_size,
                    "column_metrics": {},
                    "data_quality": {"empty_file": True}
                }
            
            # Calculate comprehensive metrics
            metrics = {
                "table_name": table_name,
                "total_rows": total_rows,
                "total_columns": len(df.columns),
                "file_size_bytes": file_path.stat().st_size,
                "collection_timestamp": datetime.now().isoformat(),
                "column_metrics": {},
                "data_quality": {
                    "empty_file": False,
                    "total_null_values": 0,
                    "completeness_percentage": 0.0
                }
            }
            
            total_null_count = 0
            
            # Analyze each column
            for column in df.columns:
                try:
                    col_series = df[column]
                    
                    # Basic statistics
                    null_count = col_series.null_count()
                    non_null_count = total_rows - null_count
                    completeness = (non_null_count / total_rows * 100) if total_rows > 0 else 0
                    total_null_count += null_count
                    
                    # Column data type
                    col_dtype = str(col_series.dtype)
                    
                    # Unique value analysis
                    unique_count = col_series.n_unique()
                    cardinality_ratio = (unique_count / total_rows * 100) if total_rows > 0 else 0
                    
                    column_metrics = {
                        "data_type": col_dtype,
                        "total_values": total_rows,
                        "non_null_count": non_null_count,
                        "null_count": null_count,
                        "null_percentage": (null_count / total_rows * 100) if total_rows > 0 else 0,
                        "completeness_percentage": completeness,
                        "unique_count": unique_count,
                        "cardinality_ratio": cardinality_ratio,
                        "is_unique": unique_count == non_null_count
                    }
                    
                    # Type-specific analysis
                    if col_dtype in ['Utf8', 'String']:
                        # String column analysis
                        non_null_series = col_series.drop_nulls()
                        if len(non_null_series) > 0:
                            lengths = non_null_series.str.len_chars()
                            column_metrics.update({
                                "min_length": lengths.min(),
                                "max_length": lengths.max(),
                                "avg_length": float(lengths.mean()) if lengths.mean() is not None else 0,
                                "empty_strings": (non_null_series.str.len_chars() == 0).sum(),
                                "pattern_analysis": {
                                    "contains_digits": (non_null_series.str.contains(r'\d')).sum(),
                                    "contains_letters": (non_null_series.str.contains(r'[a-zA-Z]')).sum(),
                                    "contains_special": (non_null_series.str.contains(r'[^a-zA-Z0-9\s]')).sum()
                                }
                            })
                        
                    elif col_dtype in ['Int64', 'Int32', 'Float64', 'Float32']:
                        # Numeric column analysis
                        non_null_series = col_series.drop_nulls()
                        if len(non_null_series) > 0:
                            column_metrics.update({
                                "min_value": float(non_null_series.min()),
                                "max_value": float(non_null_series.max()),
                                "mean_value": float(non_null_series.mean()),
                                "median_value": float(non_null_series.median()),
                                "zero_count": (non_null_series == 0).sum(),
                                "negative_count": (non_null_series < 0).sum() if col_dtype.startswith('Int') or col_dtype.startswith('Float') else 0
                            })
                    
                    # Sample values for reference (first 5 non-null unique values)
                    sample_values = col_series.drop_nulls().unique().head(5).to_list()
                    column_metrics["sample_values"] = sample_values
                    
                    metrics["column_metrics"][column] = column_metrics
                    
                except Exception as col_error:
                    logger.warning(f"Failed to analyze column {column}: {col_error}")
                    metrics["column_metrics"][column] = {
                        "error": str(col_error),
                        "data_type": "unknown"
                    }
            
            # Overall data quality metrics
            total_cells = total_rows * len(df.columns)
            overall_completeness = ((total_cells - total_null_count) / total_cells * 100) if total_cells > 0 else 0
            
            metrics["data_quality"].update({
                "total_null_values": total_null_count,
                "total_cells": total_cells,
                "completeness_percentage": overall_completeness,
                "columns_with_nulls": sum(1 for col_metrics in metrics["column_metrics"].values() 
                                        if col_metrics.get("null_count", 0) > 0),
                "fully_populated_columns": sum(1 for col_metrics in metrics["column_metrics"].values() 
                                             if col_metrics.get("null_count", 0) == 0)
            })
            
            logger.info(f"Collected metrics for {table_name}: {total_rows:,} rows, {len(df.columns)} columns, {overall_completeness:.1f}% complete")
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to collect comprehensive metrics for {table_name}: {e}")
            return {
                "table_name": table_name,
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }

    def collect_file_audit_metrics(self, file_path: str, processing_stats: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Collect comprehensive metrics for file-level audit tracking.
        
        Args:
            file_path: Path to the processed file
            processing_stats: Optional processing statistics (rows, duration, etc.)
            
        Returns:
            Dictionary containing file-level metrics
        """
        try:
            from pathlib import Path
            
            file_obj = Path(file_path)
            
            # Basic file information
            file_metrics = {
                "file_name": file_obj.name,
                "file_path": str(file_obj),
                "file_extension": file_obj.suffix.lower(),
                "collection_timestamp": datetime.now().isoformat()
            }
            
            # File size and existence check
            if file_obj.exists():
                stat_info = file_obj.stat()
                file_metrics.update({
                    "file_size_bytes": stat_info.st_size,
                    "file_size_mb": round(stat_info.st_size / (1024 * 1024), 2),
                    "last_modified": datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                    "file_exists": True
                })
                
                # File format specific analysis
                if file_obj.suffix.lower() == '.parquet':
                    try:
                        import polars as pl
                        df = pl.read_parquet(str(file_obj))
                        file_metrics.update({
                            "format_type": "parquet",
                            "row_count": len(df),
                            "column_count": len(df.columns),
                            "column_names": df.columns,
                            "estimated_memory_mb": round(df.estimated_size() / (1024 * 1024), 2) if hasattr(df, 'estimated_size') else None,
                            "data_completeness": {
                                "total_cells": len(df) * len(df.columns),
                                "null_cells": sum(df[col].null_count() for col in df.columns) if len(df) > 0 else 0
                            }
                        })
                    except Exception as parquet_error:
                        file_metrics["parquet_analysis_error"] = str(parquet_error)
                        
                elif file_obj.suffix.lower() == '.csv':
                    try:
                        # Quick CSV analysis without loading full file
                        with open(file_obj, 'r', encoding='utf-8') as f:
                            first_line = f.readline().strip()
                            f.seek(0, 2)  # Go to end
                            estimated_lines = f.tell() / len(first_line) if len(first_line) > 0 else 0
                            
                        file_metrics.update({
                            "format_type": "csv",
                            "estimated_rows": int(estimated_lines),
                            "sample_first_line": first_line[:100],  # First 100 chars
                            "delimiter_detected": ";" if ";" in first_line else "," if "," in first_line else "unknown"
                        })
                    except Exception as csv_error:
                        file_metrics["csv_analysis_error"] = str(csv_error)
            else:
                file_metrics.update({
                    "file_size_bytes": 0,
                    "file_exists": False
                })
            
            # Processing statistics if provided
            if processing_stats:
                file_metrics["processing_stats"] = {
                    "rows_processed": processing_stats.get("rows_processed", 0),
                    "processing_duration_seconds": processing_stats.get("duration_seconds", 0),
                    "processing_throughput_rows_per_sec": (
                        processing_stats.get("rows_processed", 0) / max(processing_stats.get("duration_seconds", 1), 1)
                    ),
                    "errors_encountered": processing_stats.get("error_count", 0),
                    "warnings_encountered": processing_stats.get("warning_count", 0),
                    "processing_status": processing_stats.get("status", "unknown")
                }
            
            # Performance classifications
            if file_metrics.get("file_size_bytes", 0) > 0:
                size_bytes = file_metrics["file_size_bytes"]
                if size_bytes < 1024 * 1024:  # < 1MB
                    size_category = "small"
                elif size_bytes < 100 * 1024 * 1024:  # < 100MB
                    size_category = "medium"
                elif size_bytes < 1024 * 1024 * 1024:  # < 1GB
                    size_category = "large"
                else:
                    size_category = "very_large"
                    
                file_metrics["performance_classification"] = {
                    "size_category": size_category,
                    "complexity_estimate": "low" if file_metrics.get("column_count", 0) < 10 else "medium" if file_metrics.get("column_count", 0) < 50 else "high"
                }
            
            return file_metrics
            
        except Exception as e:
            logger.error(f"Failed to collect file audit metrics for {file_path}: {e}")
            return {
                "file_path": file_path,
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }

    def collect_batch_audit_metrics(self, batch_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Collect comprehensive metrics for batch-level audit tracking.
        
        Args:
            batch_stats: Batch processing statistics
            
        Returns:
            Dictionary containing batch-level metrics
        """
        try:
            import time
            # Allow callers to pass None explicitly; treat None as missing and
            # fall back to current time to avoid TypeErrors when subtracting.
            st = batch_stats.get("start_time", None)
            et = batch_stats.get("end_time", None)
            start_time = st if (isinstance(st, (int, float)) and st is not None) else time.perf_counter()
            end_time = et if (isinstance(et, (int, float)) and et is not None) else time.perf_counter()
            duration = max(end_time - start_time, 0)
            
            batch_metrics = {
                "batch_summary": {
                    "total_files_processed": batch_stats.get("files_processed", 0),
                    "total_files_failed": batch_stats.get("files_failed", 0),
                    "total_rows_processed": batch_stats.get("total_rows", 0),
                    "total_bytes_processed": batch_stats.get("total_bytes", 0),
                    "processing_duration_seconds": duration,
                    "success_rate_percentage": (
                        (batch_stats.get("files_processed", 0) / max(batch_stats.get("files_processed", 0) + batch_stats.get("files_failed", 0), 1)) * 100
                    )
                },
                "performance_metrics": {
                    "avg_rows_per_second": batch_stats.get("total_rows", 0) / max(duration, 1),
                    "avg_bytes_per_second": batch_stats.get("total_bytes", 0) / max(duration, 1),
                    "avg_files_per_minute": (batch_stats.get("files_processed", 0) + batch_stats.get("files_failed", 0)) / max(duration / 60, 1),
                    "memory_peak_mb": batch_stats.get("memory_peak_mb", 0),
                    "memory_efficiency": batch_stats.get("memory_efficiency_score", "unknown")
                },
                "quality_metrics": {
                    "data_quality_issues": batch_stats.get("data_quality_issues", 0),
                    "schema_validation_failures": batch_stats.get("schema_failures", 0),
                    "data_completeness_average": batch_stats.get("avg_completeness", 0),
                    "duplicate_records_found": batch_stats.get("duplicates_found", 0)
                },
                "resource_utilization": {
                    "cpu_usage_peak_percent": batch_stats.get("cpu_peak_percent", 0),
                    "io_operations": batch_stats.get("io_operations", 0),
                    "network_bytes_transferred": batch_stats.get("network_bytes", 0),
                    "temp_storage_used_mb": batch_stats.get("temp_storage_mb", 0)
                },
                "collection_timestamp": datetime.now().isoformat()
            }
            
            # Add efficiency ratings
            throughput_rating = "high" if batch_metrics["performance_metrics"]["avg_rows_per_second"] > 1000 else \
                               "medium" if batch_metrics["performance_metrics"]["avg_rows_per_second"] > 100 else "low"
                               
            batch_metrics["efficiency_rating"] = {
                "throughput_rating": throughput_rating,
                "success_rating": "excellent" if batch_metrics["batch_summary"]["success_rate_percentage"] > 95 else \
                                 "good" if batch_metrics["batch_summary"]["success_rate_percentage"] > 85 else "needs_improvement",
                "overall_score": min(100, max(0, 
                    (batch_metrics["batch_summary"]["success_rate_percentage"] * 0.6) + 
                    (min(batch_metrics["performance_metrics"]["avg_rows_per_second"] / 1000 * 40, 40))
                ))
            }
            
            return batch_metrics
            
        except Exception as e:
            logger.error(f"Failed to collect batch audit metrics: {e}")
            return {
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }

    def collect_subbatch_audit_metrics(self, subbatch_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Collect comprehensive metrics for subbatch-level audit tracking.
        
        Args:
            subbatch_stats: Subbatch processing statistics
            
        Returns:
            Dictionary containing subbatch-level metrics
        """
        try:
            import time
            st = subbatch_stats.get("start_time", None)
            et = subbatch_stats.get("end_time", None)
            start_time = st if (isinstance(st, (int, float)) and st is not None) else time.perf_counter()
            end_time = et if (isinstance(et, (int, float)) and et is not None) else time.perf_counter()
            duration = max(end_time - start_time, 0)
            
            subbatch_metrics = {
                "processing_summary": {
                    "table_name": subbatch_stats.get("table_name", "unknown"),
                    "rows_processed": subbatch_stats.get("rows_processed", 0),
                    "processing_duration_seconds": duration,
                    "processing_stage": subbatch_stats.get("processing_stage", "unknown"),
                    "operation_type": subbatch_stats.get("operation_type", "upsert")  # insert, update, upsert, delete
                },
                "data_operation_metrics": {
                    "rows_inserted": subbatch_stats.get("rows_inserted", 0),
                    "rows_updated": subbatch_stats.get("rows_updated", 0),
                    "rows_skipped": subbatch_stats.get("rows_skipped", 0),
                    "rows_failed": subbatch_stats.get("rows_failed", 0),
                    "duplicate_rows_handled": subbatch_stats.get("duplicates_handled", 0),
                    "constraint_violations": subbatch_stats.get("constraint_violations", 0)
                },
                "performance_details": {
                    "rows_per_second": subbatch_stats.get("rows_processed", 0) / max(duration, 1),
                    "avg_row_size_bytes": subbatch_stats.get("avg_row_size", 0),
                    "database_connection_time_ms": subbatch_stats.get("db_connection_time_ms", 0),
                    "query_execution_time_ms": subbatch_stats.get("query_execution_time_ms", 0),
                    "data_transformation_time_ms": subbatch_stats.get("transformation_time_ms", 0)
                },
                "data_quality": {
                    "null_value_count": subbatch_stats.get("null_values", 0),
                    "data_type_conversions": subbatch_stats.get("type_conversions", 0),
                    "validation_errors": subbatch_stats.get("validation_errors", 0),
                    "data_cleansing_applied": subbatch_stats.get("cleansing_operations", []),
                    "completeness_percentage": subbatch_stats.get("completeness_percent", 0)
                },
                "error_analysis": {
                    "error_types": subbatch_stats.get("error_types", {}),
                    "warning_count": subbatch_stats.get("warning_count", 0),
                    "retry_attempts": subbatch_stats.get("retry_attempts", 0),
                    "recovery_actions": subbatch_stats.get("recovery_actions", [])
                },
                "collection_timestamp": datetime.now().isoformat()
            }
            
            # Calculate efficiency scores
            if subbatch_metrics["processing_summary"]["rows_processed"] > 0:
                success_rate = (
                    (subbatch_metrics["data_operation_metrics"]["rows_inserted"] + 
                     subbatch_metrics["data_operation_metrics"]["rows_updated"]) / 
                    subbatch_metrics["processing_summary"]["rows_processed"] * 100
                )
                
                subbatch_metrics["quality_score"] = {
                    "processing_success_rate": success_rate,
                    "data_quality_score": max(0, 100 - subbatch_metrics["data_quality"]["validation_errors"] * 5),
                    "performance_score": min(100, subbatch_metrics["performance_details"]["rows_per_second"] / 10),
                    "overall_health": "excellent" if success_rate > 98 and subbatch_metrics["data_quality"]["validation_errors"] == 0 else \
                                   "good" if success_rate > 90 else "needs_attention"
                }
            
            return subbatch_metrics
            
        except Exception as e:
            logger.error(f"Failed to collect subbatch audit metrics: {e}")
            return {
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }

    def update_table_audit_with_metrics(
        self, table_name: str, metrics: Dict[str, Any]
    ) -> None:
        """
        Update table audit record with comprehensive column metrics.
        
        Args:
            table_name: Name of the table
            comprehensive_metrics: Detailed metrics collected from the data
        """
        try:
            import json
            
            # Find the most recent table audit for this table
            table_audit_id = self._find_table_audit(table_name)
            if not table_audit_id:
                logger.warning(f"No table audit found for {table_name} - cannot update with metrics")
                return
            
            # Update the table audit with comprehensive metrics
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE table_audit_manifest 
                    SET metrics = :metrics,
                        notes = COALESCE(notes, '{}')::jsonb || (:additional_notes)::jsonb
                    WHERE table_audit_id = :table_audit_id
                '''), {
                    'metrics': json.dumps(metrics),
                    'additional_notes': json.dumps({
                        "comprehensive_metrics_updated": True,
                        "metrics_collection_timestamp": metrics.get("collection_timestamp"),
                        "data_quality_score": metrics.get("data_quality", {}).get("completeness_percentage", 0)
                    }),
                    'table_audit_id': str(table_audit_id)
                })
                
            logger.info(f"Updated table audit for {table_name} with comprehensive metrics")
            
        except Exception as e:
            logger.error(f"Failed to update table audit with comprehensive metrics for {table_name}: {e}")

    def update_file_audit_with_metrics(
        self, file_audit_id: str, file_metrics: Dict[str, Any]
    ) -> None:
        """Update file audit record with comprehensive metrics."""
        try:
            import json
            
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE file_audit_manifest 
                    SET metrics = :metrics,
                        notes = COALESCE(notes, '{}')::jsonb || (:additional_notes)::jsonb
                    WHERE file_audit_id = :file_audit_id
                '''), {
                    'metrics': json.dumps(file_metrics),
                    'additional_notes': json.dumps({
                        "file_metrics_updated": True,
                        "metrics_collection_timestamp": file_metrics.get("collection_timestamp"),
                        "file_size_category": file_metrics.get("performance_classification", {}).get("size_category", "unknown")
                    }),
                    'file_audit_id': file_audit_id
                })
                
            logger.info(f"Updated file audit {file_audit_id} with comprehensive metrics")
            
        except Exception as e:
            logger.error(f"Failed to update file audit {file_audit_id} with metrics: {e}")

    def update_batch_audit_with_metrics(
        self, batch_audit_id: str, batch_metrics: Dict[str, Any]) -> None:
        """Update batch audit record with comprehensive metrics."""
        try:
            import json
            
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE batch_audit_manifest 
                    SET metrics = :metrics,
                        notes = COALESCE(notes, '{}')::jsonb || (:additional_notes)::jsonb
                    WHERE batch_audit_id = :batch_audit_id
                '''), {
                    'metrics': json.dumps(batch_metrics),
                    'additional_notes': json.dumps({
                        "batch_metrics_updated": True,
                        "metrics_collection_timestamp": batch_metrics.get("collection_timestamp"),
                        "success_rate": batch_metrics.get("batch_summary", {}).get("success_rate_percentage", 0),
                        "efficiency_rating": batch_metrics.get("efficiency_rating", {}).get("overall_score", 0)
                    }),
                    'updated_at': datetime.now(),
                    'batch_audit_id': batch_audit_id
                })
                
            logger.info(f"Updated batch audit {batch_audit_id} with comprehensive metrics")
            
        except Exception as e:
            logger.error(f"Failed to update batch audit {batch_audit_id} with metrics: {e}")

    def update_subbatch_audit_with_metrics(
        self, subbatch_audit_id: str, subbatch_metrics: Dict[str, Any]) -> None:
        """Update subbatch audit record with comprehensive metrics."""
        try:
            import json
            
            with self.database.engine.begin() as conn:
                conn.execute(text('''
                    UPDATE subbatch_audit_manifest 
                    SET metrics = :metrics,
                        notes = COALESCE(notes, '{}')::jsonb || (:additional_notes)::jsonb
                    WHERE subbatch_audit_id = :subbatch_audit_id
                '''), {
                    'metrics': json.dumps(subbatch_metrics),
                    'additional_notes': json.dumps({
                        "subbatch_metrics_updated": True,
                        "metrics_collection_timestamp": subbatch_metrics.get("collection_timestamp"),
                        "processing_health": subbatch_metrics.get("quality_score", {}).get("overall_health", "unknown"),
                        "rows_per_second": subbatch_metrics.get("performance_details", {}).get("rows_per_second", 0)
                    }),
                    'updated_at': datetime.now(),
                    'subbatch_audit_id': subbatch_audit_id
                })
                
            logger.info(f"Updated subbatch audit {subbatch_audit_id} with comprehensive metrics")
            
        except Exception as e:
            logger.error(f"Failed to update subbatch audit {subbatch_audit_id} with metrics: {e}")

    def get_comprehensive_audit_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of all audit metrics across all manifest levels.
        
        Returns:
            Dictionary containing cross-level audit summary
        """
        try:
            with self.database.engine.connect() as conn:
                # Table-level metrics
                table_result = conn.execute(text('''
                    SELECT 
                        COUNT(*) as total_tables,
                        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_tables,
                        COUNT(CASE WHEN metrics IS NOT NULL AND metrics != '{}' THEN 1 END) as tables_with_metrics,
                        AVG(CASE 
                            WHEN metrics IS NOT NULL 
                            AND JSON_EXTRACT_PATH_TEXT(metrics, 'data_quality', 'completeness_percentage') IS NOT NULL
                            THEN CAST(JSON_EXTRACT_PATH_TEXT(metrics, 'data_quality', 'completeness_percentage') AS FLOAT)
                            ELSE NULL 
                        END) as avg_data_completeness
                    FROM table_audit_manifest
                    WHERE ingestion_year = :year AND ingestion_month = :month
                '''), {'year': self.config.year, 'month': self.config.month})
                
                table_summary = table_result.fetchone()
                
                # File-level metrics
                file_result = conn.execute(text('''
                    SELECT 
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_files,
                        COUNT(CASE WHEN metrics IS NOT NULL AND metrics != '{}' THEN 1 END) as files_with_metrics,
                        SUM(CASE 
                            WHEN metrics IS NOT NULL 
                            AND JSON_EXTRACT_PATH_TEXT(metrics, 'file_size_bytes') IS NOT NULL
                            THEN CAST(JSON_EXTRACT_PATH_TEXT(metrics, 'file_size_bytes') AS BIGINT)
                            ELSE 0 
                        END) as total_file_size_bytes
                    FROM file_audit_manifest f
                    JOIN table_audit_manifest t ON f.parent_table_audit_id = t.table_audit_id
                    WHERE t.ingestion_year = :year AND t.ingestion_month = :month
                '''), {'year': self.config.year, 'month': self.config.month})
                
                file_summary = file_result.fetchone()
                
                # Batch-level metrics
                batch_result = conn.execute(text('''
                    SELECT 
                        COUNT(*) as total_batches,
                        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_batches,
                        COUNT(CASE WHEN metrics IS NOT NULL AND metrics != '{}' THEN 1 END) as batches_with_metrics,
                        AVG(CASE 
                            WHEN metrics IS NOT NULL 
                            AND JSON_EXTRACT_PATH_TEXT(metrics, 'batch_summary', 'success_rate_percentage') IS NOT NULL
                            THEN CAST(JSON_EXTRACT_PATH_TEXT(metrics, 'batch_summary', 'success_rate_percentage') AS FLOAT)
                            ELSE NULL 
                        END) as avg_success_rate
                    FROM batch_audit_manifest b
                    JOIN file_audit_manifest f ON b.parent_file_audit_id = f.file_audit_id
                    JOIN table_audit_manifest t ON f.parent_table_audit_id = t.table_audit_id
                    WHERE t.ingestion_year = :year AND t.ingestion_month = :month
                '''), {'year': self.config.year, 'month': self.config.month})
                
                batch_summary = batch_result.fetchone()
                
                # Subbatch-level metrics
                subbatch_result = conn.execute(text('''
                    SELECT 
                        COUNT(*) as total_subbatches,
                        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_subbatches,
                        COUNT(CASE WHEN metrics IS NOT NULL AND metrics != '{}' THEN 1 END) as subbatches_with_metrics,
                        SUM(CASE WHEN rows_processed IS NOT NULL THEN rows_processed ELSE 0 END) as total_rows_processed
                    FROM subbatch_audit_manifest s
                    JOIN batch_audit_manifest b ON s.parent_batch_audit_id = b.batch_audit_id
                    JOIN file_audit_manifest f ON b.parent_file_audit_id = f.file_audit_id
                    JOIN table_audit_manifest t ON f.parent_table_audit_id = t.table_audit_id
                    WHERE t.ingestion_year = :year AND t.ingestion_month = :month
                '''), {'year': self.config.year, 'month': self.config.month})
                
                subbatch_summary = subbatch_result.fetchone()
                
                comprehensive_summary = {
                    "reporting_period": f"{self.config.year}-{self.config.month:02d}",
                    "collection_timestamp": datetime.now().isoformat(),
                    "table_level_summary": {
                        "total_tables": table_summary[0] if table_summary else 0,
                        "completed_tables": table_summary[1] if table_summary else 0,
                        "tables_with_metrics": table_summary[2] if table_summary else 0,
                        "avg_data_completeness": round(table_summary[3], 2) if table_summary and table_summary[3] else 0
                    },
                    "file_level_summary": {
                        "total_files": file_summary[0] if file_summary else 0,
                        "completed_files": file_summary[1] if file_summary else 0,
                        "files_with_metrics": file_summary[2] if file_summary else 0,
                        "total_file_size_gb": round((file_summary[3] if file_summary else 0) / (1024**3), 2)
                    },
                    "batch_level_summary": {
                        "total_batches": batch_summary[0] if batch_summary else 0,
                        "completed_batches": batch_summary[1] if batch_summary else 0,
                        "batches_with_metrics": batch_summary[2] if batch_summary else 0,
                        "avg_success_rate": round(batch_summary[3], 2) if batch_summary and batch_summary[3] else 0
                    },
                    "subbatch_level_summary": {
                        "total_subbatches": subbatch_summary[0] if subbatch_summary else 0,
                        "completed_subbatches": subbatch_summary[1] if subbatch_summary else 0,
                        "subbatches_with_metrics": subbatch_summary[2] if subbatch_summary else 0,
                        "total_rows_processed": subbatch_summary[3] if subbatch_summary else 0
                    }
                }
                
                # Calculate overall health metrics
                total_operations = (comprehensive_summary["table_level_summary"]["total_tables"] + 
                                  comprehensive_summary["file_level_summary"]["total_files"] +
                                  comprehensive_summary["batch_level_summary"]["total_batches"] +
                                  comprehensive_summary["subbatch_level_summary"]["total_subbatches"])
                
                completed_operations = (comprehensive_summary["table_level_summary"]["completed_tables"] + 
                                      comprehensive_summary["file_level_summary"]["completed_files"] +
                                      comprehensive_summary["batch_level_summary"]["completed_batches"] +
                                      comprehensive_summary["subbatch_level_summary"]["completed_subbatches"])
                
                comprehensive_summary["overall_health"] = {
                    "total_operations": total_operations,
                    "completed_operations": completed_operations,
                    "overall_success_rate": round((completed_operations / max(total_operations, 1)) * 100, 2),
                    "metrics_coverage": {
                        "tables": round((comprehensive_summary["table_level_summary"]["tables_with_metrics"] / 
                                       max(comprehensive_summary["table_level_summary"]["total_tables"], 1)) * 100, 2),
                        "files": round((comprehensive_summary["file_level_summary"]["files_with_metrics"] / 
                                      max(comprehensive_summary["file_level_summary"]["total_files"], 1)) * 100, 2),
                        "batches": round((comprehensive_summary["batch_level_summary"]["batches_with_metrics"] / 
                                        max(comprehensive_summary["batch_level_summary"]["total_batches"], 1)) * 100, 2),
                        "subbatches": round((comprehensive_summary["subbatch_level_summary"]["subbatches_with_metrics"] / 
                                           max(comprehensive_summary["subbatch_level_summary"]["total_subbatches"], 1)) * 100, 2)
                    }
                }
                
                return comprehensive_summary
                
        except Exception as e:
            logger.error(f"Failed to generate comprehensive audit summary: {e}")
            return {
                "error": str(e),
                "collection_timestamp": datetime.now().isoformat()
            }
