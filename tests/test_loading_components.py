"""
Unit tests for the loading service components.
"""
import unittest
from unittest.mock import Mock, MagicMock
from pathlib import Path

from src.core.services.loading_new.memory_manager import MemoryManager
from src.core.services.loading_new.batch_processor import BatchProcessor
from src.core.services.loading_new.file_processor import FileProcessor


class TestMemoryManager(unittest.TestCase):
    """Test cases for MemoryManager."""

    def test_init_without_monitor(self):
        """Test initialization without memory monitor."""
        manager = MemoryManager()
        self.assertIsNone(manager.memory_monitor)

    def test_init_with_monitor(self):
        """Test initialization with memory monitor."""
        mock_monitor = Mock()
        manager = MemoryManager(mock_monitor)
        self.assertEqual(manager.memory_monitor, mock_monitor)

    def test_get_status_report_no_monitor(self):
        """Test get_status_report when no monitor is available."""
        manager = MemoryManager()
        result = manager.get_status_report()
        self.assertIsNone(result)

    def test_get_status_report_with_monitor(self):
        """Test get_status_report with monitor."""
        mock_monitor = Mock()
        mock_status = {"usage": 100}
        mock_monitor.get_status_report.return_value = mock_status

        manager = MemoryManager(mock_monitor)
        result = manager.get_status_report()
        self.assertEqual(result, mock_status)
        mock_monitor.get_status_report.assert_called_once()

    def test_should_prevent_processing_no_monitor(self):
        """Test should_prevent_processing when no monitor is available."""
        manager = MemoryManager()
        result = manager.should_prevent_processing()
        self.assertFalse(result)

    def test_should_prevent_processing_with_monitor(self):
        """Test should_prevent_processing with monitor."""
        mock_monitor = Mock()
        mock_monitor.should_prevent_processing.return_value = True

        manager = MemoryManager(mock_monitor)
        result = manager.should_prevent_processing()
        self.assertTrue(result)
        mock_monitor.should_prevent_processing.assert_called_once()

    def test_estimate_memory_requirements(self):
        """Test memory requirements estimation."""
        mock_monitor = Mock()
        manager = MemoryManager(mock_monitor)

        # Create mock file paths
        file_paths = [Mock(spec=Path), Mock(spec=Path)]
        file_paths[0].stat.return_value.st_size = 1024 * 1024  # 1MB
        file_paths[1].stat.return_value.st_size = 2 * 1024 * 1024  # 2MB

        result = manager.estimate_memory_requirements(file_paths)

        # Should estimate ~4MB total (2x file sizes)
        self.assertEqual(result["total_estimated_mb"], 4.0)
        self.assertEqual(result["estimated_memory_per_chunk_mb"], 2.0)


class TestBatchProcessor(unittest.TestCase):
    """Test cases for BatchProcessor."""

    def test_init(self):
        """Test initialization."""
        mock_audit = Mock()
        mock_config = Mock()
        processor = BatchProcessor(mock_audit, mock_config)
        self.assertEqual(processor.audit_service, mock_audit)
        self.assertEqual(processor.config, mock_config)

    def test_split_into_subbatches(self):
        """Test splitting batches into subbatches."""
        processor = BatchProcessor()
        batch = [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')]

        result = processor.split_into_subbatches(batch, 2)
        expected = [[(1, 'a'), (2, 'b')], [(3, 'c'), (4, 'd')], [(5, 'e')]]

        self.assertEqual(result, expected)

    def test_batch_context_no_audit_service(self):
        """Test batch context when no audit service is available."""
        processor = BatchProcessor()
        with processor.batch_context("test_table", "test_batch") as batch_id:
            self.assertIsNone(batch_id)

    def test_subbatch_context_no_audit_service(self):
        """Test subbatch context when no audit service is available."""
        processor = BatchProcessor()
        mock_batch_id = Mock()
        with processor.subbatch_context(mock_batch_id, "test_table", "test_desc") as subbatch_id:
            self.assertIsNone(subbatch_id)


class TestFileProcessor(unittest.TestCase):
    """Test cases for FileProcessor."""

    def test_init(self):
        """Test initialization."""
        mock_config = Mock()
        mock_monitor = Mock()
        processor = FileProcessor(mock_config, mock_monitor)
        self.assertEqual(processor.config, mock_config)
        self.assertEqual(processor.memory_monitor, mock_monitor)

    def test_get_file_size(self):
        """Test getting file size."""
        mock_config = Mock()
        processor = FileProcessor(mock_config)

        mock_path = Mock(spec=Path)
        mock_path.stat.return_value.st_size = 1024

        result = processor.get_file_size(mock_path)
        self.assertEqual(result, 1024)
        mock_path.stat.assert_called_once()

    def test_get_file_size_error(self):
        """Test getting file size when error occurs."""
        mock_config = Mock()
        processor = FileProcessor(mock_config)

        mock_path = Mock(spec=Path)
        mock_path.stat.side_effect = Exception("File not found")

        result = processor.get_file_size(mock_path)
        self.assertEqual(result, 0)

    def test_optimize_processing_order(self):
        """Test optimizing processing order by file size."""
        mock_config = Mock()
        processor = FileProcessor(mock_config)

        # Create mock paths with different sizes
        mock_path1 = Mock(spec=Path)
        mock_path1.stat.return_value.st_size = 2000
        mock_path1.name = "large.txt"

        mock_path2 = Mock(spec=Path)
        mock_path2.stat.return_value.st_size = 1000
        mock_path2.name = "small.txt"

        file_paths = [mock_path1, mock_path2]
        result = processor.optimize_processing_order(file_paths)

        # Should return small file first
        self.assertEqual(result, [mock_path2, mock_path1])