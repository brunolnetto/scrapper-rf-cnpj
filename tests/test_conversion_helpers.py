"""
Unit tests for conversion service helper functions.
"""
import pytest
import tempfile
import pathlib
from unittest.mock import Mock, patch
from src.core.services.conversion.service import (
    _derive_schema_from_sample,
    _try_native_streaming,
    _process_with_manual_chunking
)


class TestConversionHelpers:
    """Test suite for conversion helper functions."""

    def test_derive_schema_from_sample_basic(self):
        """Test basic schema derivation from sample data."""
        # Mock inputs
        csv_path = Mock()
        csv_path.stat.return_value.st_size = 1000

        expected_columns = ["col1", "col2", "col3"]
        delimiter = ";"

        # Mock the file reading and schema derivation
        with patch('src.core.services.conversion.service.pl.scan_csv') as mock_scan:
            mock_lazy_frame = Mock()
            mock_scan.return_value = mock_lazy_frame

            mock_schema = Mock()
            mock_schema.names.return_value = ["column_1", "column_2", "column_3"]
            mock_lazy_frame.collect_schema.return_value = mock_schema

            with patch('src.core.services.conversion.service.pl') as mock_pl:
                mock_pa_schema = Mock()
                mock_pl.Struct.return_value = mock_pa_schema

                result = _derive_schema_from_sample(csv_path, expected_columns, delimiter)

                assert result == mock_pa_schema
                mock_scan.assert_called_once()

    def test_try_native_streaming_success(self):
        """Test successful native streaming."""
        # Mock inputs
        csv_path = Mock()
        tmp_output = Mock()
        tmp_output.__str__ = Mock(return_value="/tmp/test.parquet")

        expected_columns = ["col1", "col2"]
        delimiter = ";"
        config = Mock()

        with patch('src.core.services.conversion.service.pl.scan_csv') as mock_scan, \
             patch('src.core.services.conversion.service.pq.ParquetFile') as mock_pq_file:

            mock_lazy_frame = Mock()
            mock_scan.return_value = mock_lazy_frame

            # Mock successful sink_parquet
            mock_lazy_frame.sink_parquet.return_value = None

            # Mock metadata
            mock_meta = Mock()
            mock_row_group = Mock()
            mock_row_group.num_rows = 100
            mock_meta.row_group.side_effect = lambda i: mock_row_group
            mock_meta.num_row_groups = 1

            mock_pq_file.return_value.metadata = mock_meta

            result = _try_native_streaming(csv_path, tmp_output, delimiter, expected_columns, config)

            assert result is True
            mock_lazy_frame.sink_parquet.assert_called_once()

    def test_try_native_streaming_failure(self):
        """Test native streaming failure falls back gracefully."""
        csv_path = Mock()
        tmp_output = Mock()
        expected_columns = ["col1", "col2"]
        delimiter = ";"
        config = Mock()

        with patch('src.core.services.conversion.service.pl.scan_csv') as mock_scan:
            mock_lazy_frame = Mock()
            mock_scan.return_value = mock_lazy_frame

            # Mock sink_parquet failure
            mock_lazy_frame.sink_parquet.side_effect = Exception("Sink failed")

            result = _try_native_streaming(csv_path, tmp_output, delimiter, expected_columns, config)

            assert result is False

    def test_process_with_manual_chunking(self):
        """Test manual chunking processing."""
        # This would be more complex to test fully, so we'll do a basic smoke test
        csv_path = Mock()
        tmp_output = Mock()
        tmp_output.__str__ = Mock(return_value="/tmp/test.parquet")

        pa_schema = Mock()
        expected_columns = ["col1", "col2"]
        delimiter = ";"
        config = Mock()
        memory_monitor = Mock()

        # Mock all the complex internals
        with patch('src.core.services.conversion.service.pl.scan_csv') as mock_scan, \
             patch('src.core.services.conversion.service.pq.ParquetWriter') as mock_writer, \
             patch('src.core.services.conversion.service.time.time') as mock_time:

            mock_lazy_frame = Mock()
            mock_scan.return_value = mock_lazy_frame

            mock_batch_df = Mock()
            mock_batch_df.height = 0  # End immediately
            mock_batch_df.to_arrow.return_value = Mock()

            mock_chunk_lf = Mock()
            mock_chunk_lf.collect.return_value = mock_batch_df
            mock_lazy_frame.slice.return_value = mock_chunk_lf

            mock_time.return_value = 1000

            result = _process_with_manual_chunking(
                csv_path, tmp_output, pa_schema, delimiter, expected_columns, config, memory_monitor
            )

            assert result == 0  # No rows processed in this test