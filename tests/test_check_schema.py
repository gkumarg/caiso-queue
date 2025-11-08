"""
Tests for check_schema.py module

Note: check_schema.py is a diagnostic script that prints database schema information.
These tests verify the components that would be used in such a diagnostic script.
"""
import pytest
import os
import sys
import sqlite3
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


@pytest.mark.unit
class TestCheckSchemaComponents:
    """Test individual components that would be in a refactored check_schema."""

    def test_get_table_names(self, temp_db):
        """Test getting table names from database."""
        conn = sqlite3.connect(temp_db)

        # Create test tables
        pd.DataFrame({'col': [1]}).to_sql('table1', conn, if_exists='replace', index=False)
        pd.DataFrame({'col': [2]}).to_sql('table2', conn, if_exists='replace', index=False)

        # Get table names
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        table_names = tables['name'].tolist()

        conn.close()

        assert 'table1' in table_names
        assert 'table2' in table_names
        assert len(table_names) >= 2

    def test_get_table_schema(self, temp_db):
        """Test getting schema information from a table."""
        conn = sqlite3.connect(temp_db)

        # Create a table with known columns
        test_data = pd.DataFrame({
            'queue_position': ['Z001'],
            'project_name': ['Test'],
            'mw_1': [100.0],
        })
        test_data.to_sql('test_table', conn, if_exists='replace', index=False)

        # Get schema
        schema = pd.read_sql("PRAGMA table_info(test_table)", conn)
        column_names = schema['name'].tolist()

        conn.close()

        assert 'queue_position' in column_names
        assert 'project_name' in column_names
        assert 'mw_1' in column_names

    def test_check_column_exists(self, temp_db):
        """Test checking if a specific column exists in a table."""
        conn = sqlite3.connect(temp_db)

        # Create table
        pd.DataFrame({
            'col1': [1],
            'col2': [2],
        }).to_sql('test_table', conn, if_exists='replace', index=False)

        # Check schema
        schema = pd.read_sql("PRAGMA table_info(test_table)", conn)
        columns = schema['name'].tolist()

        conn.close()

        # Test column existence
        assert 'col1' in columns
        assert 'col2' in columns
        assert 'col3' not in columns
