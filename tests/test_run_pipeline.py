"""
Tests for run_pipeline.py module
"""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from run_pipeline import run_pipeline


@pytest.mark.integration
class TestRunPipeline:
    """Test suite for run_pipeline orchestration."""

    @patch('data_collection.download_queue_report')
    @patch('parse_queue.main')
    @patch('analyze_queue.main')
    def test_run_pipeline_success(self, mock_analyze, mock_parse, mock_download):
        """Test that run_pipeline executes all steps successfully."""
        # Setup mocks
        mock_download.return_value = 'raw/publicqueuereport.xlsx'
        mock_parse.return_value = None
        mock_analyze.return_value = None

        # Run pipeline
        run_pipeline()

        # Verify all steps were called
        mock_download.assert_called_once()
        mock_parse.assert_called_once()
        mock_analyze.assert_called_once()

    @patch('data_collection.download_queue_report')
    @patch('parse_queue.main')
    @patch('analyze_queue.main')
    def test_run_pipeline_download_failure(self, mock_analyze, mock_parse, mock_download):
        """Test that pipeline exits if download fails."""
        # Setup mock to raise exception
        mock_download.side_effect = Exception("Network error")

        # Run pipeline and expect SystemExit
        with pytest.raises(SystemExit) as exc_info:
            run_pipeline()

        assert exc_info.value.code == 1
        # Parse and analyze should not be called
        mock_parse.assert_not_called()
        mock_analyze.assert_not_called()

    @patch('data_collection.download_queue_report')
    @patch('parse_queue.main')
    @patch('analyze_queue.main')
    def test_run_pipeline_parse_failure(self, mock_analyze, mock_parse, mock_download):
        """Test that pipeline exits if parsing fails."""
        # Setup mocks
        mock_download.return_value = 'raw/publicqueuereport.xlsx'
        mock_parse.side_effect = Exception("Parse error")

        # Run pipeline and expect SystemExit
        with pytest.raises(SystemExit) as exc_info:
            run_pipeline()

        assert exc_info.value.code == 1
        # Download should be called, but analyze should not
        mock_download.assert_called_once()
        mock_analyze.assert_not_called()

    @patch('data_collection.download_queue_report')
    @patch('parse_queue.main')
    @patch('analyze_queue.main')
    def test_run_pipeline_analyze_failure(self, mock_analyze, mock_parse, mock_download):
        """Test that pipeline exits if analysis fails."""
        # Setup mocks
        mock_download.return_value = 'raw/publicqueuereport.xlsx'
        mock_parse.return_value = None
        mock_analyze.side_effect = Exception("Analysis error")

        # Run pipeline and expect SystemExit
        with pytest.raises(SystemExit) as exc_info:
            run_pipeline()

        assert exc_info.value.code == 1
        # Download and parse should be called
        mock_download.assert_called_once()
        mock_parse.assert_called_once()

    @patch('data_collection.download_queue_report')
    @patch('parse_queue.main')
    @patch('analyze_queue.main')
    def test_run_pipeline_prints_progress(self, mock_analyze, mock_parse, mock_download, capsys):
        """Test that pipeline prints progress messages."""
        # Setup mocks
        mock_download.return_value = 'raw/publicqueuereport.xlsx'
        mock_parse.return_value = None
        mock_analyze.return_value = None

        # Run pipeline
        run_pipeline()

        # Capture output
        captured = capsys.readouterr()

        # Verify progress messages
        assert 'Starting CAISO Queue pipeline' in captured.out
        assert 'Downloading latest queue report' in captured.out
        assert 'Parsing and loading data' in captured.out
        assert 'Analyzing data and generating reports' in captured.out
        assert 'Pipeline completed successfully' in captured.out

    @patch('data_collection.download_queue_report')
    @patch('parse_queue.main')
    @patch('analyze_queue.main')
    def test_run_pipeline_execution_order(self, mock_analyze, mock_parse, mock_download):
        """Test that pipeline steps execute in correct order."""
        # Track execution order
        execution_order = []

        mock_download.side_effect = lambda: execution_order.append('download') or 'raw/file.xlsx'
        mock_parse.side_effect = lambda: execution_order.append('parse')
        mock_analyze.side_effect = lambda: execution_order.append('analyze')

        # Run pipeline
        run_pipeline()

        # Verify execution order
        assert execution_order == ['download', 'parse', 'analyze']

    @patch('data_collection.download_queue_report')
    @patch('parse_queue.main')
    @patch('analyze_queue.main')
    def test_run_pipeline_handles_different_exceptions(self, mock_analyze, mock_parse, mock_download):
        """Test pipeline handles different exception types."""
        # Test with different exception types
        exceptions_to_test = [
            ValueError("Invalid value"),
            IOError("File error"),
            RuntimeError("Runtime error"),
        ]

        for exception in exceptions_to_test:
            # Reset mocks
            mock_download.reset_mock()
            mock_parse.reset_mock()
            mock_analyze.reset_mock()

            # Set download to raise the exception
            mock_download.side_effect = exception

            # Should exit with code 1 for any exception
            with pytest.raises(SystemExit) as exc_info:
                run_pipeline()

            assert exc_info.value.code == 1


@pytest.mark.unit
class TestPipelineComponents:
    """Test individual pipeline components and imports."""

    def test_pipeline_imports(self):
        """Test that pipeline can import required modules."""
        # This test verifies imports work
        try:
            from run_pipeline import run_pipeline
            assert run_pipeline is not None
        except ImportError as e:
            pytest.fail(f"Failed to import run_pipeline: {e}")

    @patch('data_collection.download_queue_report')
    @patch('parse_queue.main')
    @patch('analyze_queue.main')
    def test_pipeline_function_exists(self, mock_analyze, mock_parse, mock_download):
        """Test that run_pipeline function is callable."""
        mock_download.return_value = 'test.xlsx'

        # Should be callable without errors
        assert callable(run_pipeline)
        run_pipeline()

    @patch('builtins.print')
    @patch('data_collection.download_queue_report')
    @patch('parse_queue.main')
    @patch('analyze_queue.main')
    def test_pipeline_timestamps(self, mock_analyze, mock_parse, mock_download, mock_print):
        """Test that pipeline logs timestamps."""
        mock_download.return_value = 'test.xlsx'

        run_pipeline()

        # Check that print was called with messages about start and completion
        print_calls = [str(call) for call in mock_print.call_args_list]
        all_output = ' '.join(print_calls)

        assert any('Starting CAISO Queue pipeline' in call for call in print_calls)
        assert any('completed successfully' in call for call in print_calls)
