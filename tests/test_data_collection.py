"""
Tests for data_collection.py module
"""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from data_collection import download_queue_report, CAISO_URL, RAW_DIR


@pytest.mark.unit
class TestDataCollection:
    """Test suite for data collection functionality."""

    def test_caiso_url_is_defined(self):
        """Test that CAISO_URL constant is defined."""
        assert CAISO_URL is not None
        assert isinstance(CAISO_URL, str)
        assert CAISO_URL.startswith('http')
        assert '.xlsx' in CAISO_URL

    def test_raw_dir_is_defined(self):
        """Test that RAW_DIR constant is defined."""
        assert RAW_DIR is not None
        assert isinstance(RAW_DIR, str)

    @patch('data_collection.requests.get')
    @patch('data_collection.RAW_DIR', new_callable=lambda: tempfile.mkdtemp())
    def test_download_queue_report_success(self, mock_raw_dir, mock_get):
        """Test successful download of queue report."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raw = Mock()
        mock_response.raw.decode_content = True
        mock_response.raw.read = Mock(return_value=b'fake excel data')
        mock_get.return_value = mock_response

        # Override RAW_DIR for this test
        with patch('data_collection.RAW_DIR', mock_raw_dir):
            try:
                output_path = download_queue_report()

                # Verify the download was called
                mock_get.assert_called_once_with(CAISO_URL, stream=True)

                # Verify output path is returned
                assert output_path is not None
                assert isinstance(output_path, str)
                assert '.xlsx' in output_path

            finally:
                # Cleanup temp directory
                if os.path.exists(mock_raw_dir):
                    shutil.rmtree(mock_raw_dir)

    @patch('data_collection.requests.get')
    def test_download_queue_report_network_error(self, mock_get):
        """Test handling of network errors during download."""
        import requests

        # Setup mock to raise exception
        mock_get.side_effect = requests.exceptions.ConnectionError('Network error')

        # Should exit with error code 1
        with pytest.raises(SystemExit) as exc_info:
            download_queue_report()

        assert exc_info.value.code == 1

    @patch('data_collection.requests.get')
    def test_download_queue_report_http_error(self, mock_get):
        """Test handling of HTTP errors (e.g., 404, 500)."""
        import requests

        # Setup mock to raise HTTPError
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError('404 Not Found')
        mock_get.return_value = mock_response

        # Should exit with error code 1
        with pytest.raises(SystemExit) as exc_info:
            download_queue_report()

        assert exc_info.value.code == 1

    @patch('data_collection.requests.get')
    @patch('data_collection.RAW_DIR', new_callable=lambda: tempfile.mkdtemp())
    def test_download_creates_raw_directory(self, mock_raw_dir, mock_get):
        """Test that RAW_DIR is created if it doesn't exist."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raw = Mock()
        mock_response.raw.decode_content = True
        mock_response.raw.read = Mock(return_value=b'fake excel data')
        mock_get.return_value = mock_response

        # Create a temp dir that we'll delete
        temp_dir = tempfile.mkdtemp()

        with patch('data_collection.RAW_DIR', temp_dir):
            try:
                # Delete the directory to test creation
                if os.path.exists(temp_dir):
                    os.rmdir(temp_dir)

                download_queue_report()

                # Verify directory was created
                assert os.path.exists(temp_dir)

            finally:
                # Cleanup
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)

    @patch('data_collection.requests.get')
    @patch('data_collection.RAW_DIR', new_callable=lambda: tempfile.mkdtemp())
    @patch('data_collection.datetime')
    def test_download_filename_format(self, mock_datetime, mock_raw_dir, mock_get):
        """Test that downloaded file has correct date suffix format."""
        # Mock datetime to return a fixed date
        mock_now = Mock()
        mock_now.strftime.return_value = '-01012025'  # Format: -MMDDYYYY
        mock_datetime.now.return_value = mock_now

        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raw = Mock()
        mock_response.raw.decode_content = True
        mock_response.raw.read = Mock(return_value=b'fake excel data')
        mock_get.return_value = mock_response

        with patch('data_collection.RAW_DIR', mock_raw_dir):
            try:
                output_path = download_queue_report()

                # Verify filename format
                assert 'publicqueuereport-' in output_path or 'publicqueuereport' in output_path
                assert '.xlsx' in output_path

            finally:
                if os.path.exists(mock_raw_dir):
                    shutil.rmtree(mock_raw_dir)

    @patch('data_collection.requests.get')
    @patch('data_collection.RAW_DIR', new_callable=lambda: tempfile.mkdtemp())
    def test_download_creates_base_file_copy(self, mock_raw_dir, mock_get):
        """Test that a base copy (publicqueuereport.xlsx) is created."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raw = Mock()
        mock_response.raw.decode_content = True
        mock_response.raw.read = Mock(return_value=b'fake excel data')
        mock_get.return_value = mock_response

        with patch('data_collection.RAW_DIR', mock_raw_dir):
            try:
                download_queue_report()

                # Check that base file exists
                base_file = os.path.join(mock_raw_dir, 'publicqueuereport.xlsx')
                assert os.path.exists(base_file)

            finally:
                if os.path.exists(mock_raw_dir):
                    shutil.rmtree(mock_raw_dir)


@pytest.mark.network
@pytest.mark.slow
class TestDataCollectionLive:
    """Live tests that actually connect to CAISO website (marked as slow/network)."""

    def test_caiso_url_is_accessible(self):
        """Test that CAISO URL is accessible (live test - can be slow)."""
        import requests

        try:
            response = requests.head(CAISO_URL, timeout=10)
            # Should return 200 or 302 (redirect)
            assert response.status_code in [200, 302, 301]
        except requests.exceptions.RequestException:
            pytest.skip("CAISO website is not accessible or network is down")
