"""
Tests for cleanup_raw.py module
"""
import pytest
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from cleanup_raw import main as cleanup_main


@pytest.mark.unit
class TestCleanupRaw:
    """Test suite for cleanup_raw functionality."""

    def test_cleanup_skips_main_file(self, temp_dir):
        """Test that publicqueuereport.xlsx is never deleted."""
        # Create main file
        main_file = os.path.join(temp_dir, 'publicqueuereport.xlsx')
        Path(main_file).touch()

        # Create an old dated file
        old_date = (datetime.now() - timedelta(weeks=60)).strftime('%m%d%Y')
        old_file = os.path.join(temp_dir, f'publicqueuereport-{old_date}.xlsx')
        Path(old_file).touch()

        # Run cleanup with mocked RAW_DIR
        with patch('cleanup_raw.RAW_DIR', temp_dir):
            cleanup_main()

        # Main file should still exist
        assert os.path.exists(main_file)
        # Old file should be deleted
        assert not os.path.exists(old_file)

    def test_cleanup_removes_old_files(self, temp_dir):
        """Test that files older than 1 year are removed."""
        # Create file older than 1 year
        old_date = (datetime.now() - timedelta(weeks=60)).strftime('%m%d%Y')
        old_file = os.path.join(temp_dir, f'publicqueuereport-{old_date}.xlsx')
        Path(old_file).touch()

        with patch('cleanup_raw.RAW_DIR', temp_dir):
            cleanup_main()

        assert not os.path.exists(old_file)

    def test_cleanup_preserves_recent_files(self, temp_dir):
        """Test that files newer than 1 year are kept."""
        # Create file less than 1 year old
        recent_date = (datetime.now() - timedelta(weeks=10)).strftime('%m%d%Y')
        recent_file = os.path.join(temp_dir, f'publicqueuereport-{recent_date}.xlsx')
        Path(recent_file).touch()

        with patch('cleanup_raw.RAW_DIR', temp_dir):
            cleanup_main()

        assert os.path.exists(recent_file)

    def test_cleanup_handles_invalid_filename_format(self, temp_dir):
        """Test that files with invalid date formats are skipped."""
        # Create file with invalid format
        invalid_file = os.path.join(temp_dir, 'invalidfilename.xlsx')
        Path(invalid_file).touch()

        with patch('cleanup_raw.RAW_DIR', temp_dir):
            cleanup_main()

        # File should still exist (not deleted, just skipped)
        assert os.path.exists(invalid_file)

    def test_cleanup_handles_empty_directory(self, temp_dir):
        """Test that cleanup handles empty directories gracefully."""
        with patch('cleanup_raw.RAW_DIR', temp_dir):
            # Should not raise any exceptions
            cleanup_main()

    def test_cleanup_date_parsing(self, temp_dir):
        """Test that date parsing works correctly for various date formats."""
        # Create files with different date formats
        # Note: cleanup uses timedelta(weeks=52) = 364 days as cutoff
        test_dates = [
            (datetime.now() - timedelta(weeks=60), True),   # Should delete (420 days old)
            (datetime.now() - timedelta(weeks=53), True),   # Should delete (371 days old)
            (datetime.now() - timedelta(weeks=50), False),  # Should keep (350 days old)
            (datetime.now() - timedelta(days=1), False),    # Should keep
        ]

        created_files = []
        for date, should_delete in test_dates:
            date_str = date.strftime('%m%d%Y')
            file_path = os.path.join(temp_dir, f'publicqueuereport-{date_str}.xlsx')
            Path(file_path).touch()
            created_files.append((file_path, should_delete))

        with patch('cleanup_raw.RAW_DIR', temp_dir):
            cleanup_main()

        for file_path, should_delete in created_files:
            if should_delete:
                assert not os.path.exists(file_path), f"{file_path} should have been deleted"
            else:
                assert os.path.exists(file_path), f"{file_path} should have been kept"

    def test_cleanup_handles_permission_errors(self, temp_dir, monkeypatch):
        """Test that cleanup handles permission errors gracefully."""
        # Create a file
        old_date = (datetime.now() - timedelta(weeks=60)).strftime('%m%d%Y')
        test_file = os.path.join(temp_dir, f'publicqueuereport-{old_date}.xlsx')
        Path(test_file).touch()

        # Mock os.remove to raise PermissionError
        original_remove = os.remove
        def mock_remove(path):
            if 'publicqueuereport-' in path:
                raise PermissionError("Permission denied")
            return original_remove(path)

        monkeypatch.setattr(os, 'remove', mock_remove)

        with patch('cleanup_raw.RAW_DIR', temp_dir):
            # Should not crash, just skip the file
            cleanup_main()

        # File should still exist due to permission error
        assert os.path.exists(test_file)

    def test_cleanup_counts_removed_files(self, temp_dir, capsys):
        """Test that cleanup reports the correct number of removed files."""
        # Create multiple old files
        for i in range(3):
            old_date = (datetime.now() - timedelta(weeks=60 + i)).strftime('%m%d%Y')
            old_file = os.path.join(temp_dir, f'publicqueuereport-{old_date}.xlsx')
            Path(old_file).touch()

        with patch('cleanup_raw.RAW_DIR', temp_dir):
            cleanup_main()

        captured = capsys.readouterr()
        assert 'Removed 3 old files' in captured.out


# Import patch from unittest.mock
from unittest.mock import patch
