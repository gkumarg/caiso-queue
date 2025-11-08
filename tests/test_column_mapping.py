"""
Tests for column_mapping.py module
"""
import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from column_mapping import get_column_mapping, map_dataframe_columns, COLUMN_MAPPING


@pytest.mark.unit
class TestColumnMapping:
    """Test suite for column mapping functionality."""

    def test_get_column_mapping_returns_dict(self):
        """Test that get_column_mapping returns a dictionary."""
        mapping = get_column_mapping()
        assert isinstance(mapping, dict)
        assert len(mapping) > 0

    def test_column_mapping_contains_expected_keys(self):
        """Test that the mapping contains key columns."""
        mapping = get_column_mapping()

        # Check for essential mappings
        assert 'Unnamed: 0_level_0 Project Name' in mapping
        assert 'Unnamed: 1_level_0 Queue Position' in mapping
        assert 'Location County' in mapping
        assert 'Location State' in mapping

    def test_column_mapping_contains_expected_values(self):
        """Test that mappings produce correct simplified names."""
        mapping = get_column_mapping()

        assert mapping['Unnamed: 0_level_0 Project Name'] == 'project_name'
        assert mapping['Unnamed: 1_level_0 Queue Position'] == 'queue_position'
        assert mapping['Location County'] == 'county'
        assert mapping['Location State'] == 'state'
        assert mapping['MWs MW-1'] == 'mw_1'

    def test_map_dataframe_columns_basic(self):
        """Test basic column mapping on a DataFrame."""
        # Create sample DataFrame with original column names
        df = pd.DataFrame({
            'Unnamed: 0_level_0 Project Name': ['Project A', 'Project B'],
            'Unnamed: 1_level_0 Queue Position': ['Z001', 'Z002'],
            'Location County': ['KERN', 'RIVERSIDE'],
            'MWs MW-1': [100.0, 200.0],
        })

        # Map columns
        df_mapped = map_dataframe_columns(df)

        # Check that new column names are correct
        assert 'project_name' in df_mapped.columns
        assert 'queue_position' in df_mapped.columns
        assert 'county' in df_mapped.columns
        assert 'mw_1' in df_mapped.columns

        # Check that old column names are gone
        assert 'Unnamed: 0_level_0 Project Name' not in df_mapped.columns
        assert 'Location County' not in df_mapped.columns

    def test_map_dataframe_columns_with_confidential_name(self):
        """Test mapping for withdrawn projects with confidential column name."""
        # Create DataFrame with confidential project name column
        df = pd.DataFrame({
            'Unnamed: 0_level_0 Project Name - Confidential': ['Withdrawn Project'],
            'Unnamed: 1_level_0 Queue Position': ['W001'],
            'Location County': ['FRESNO'],
        })

        # Map columns
        df_mapped = map_dataframe_columns(df)

        # Check that confidential column is mapped to project_name
        assert 'project_name' in df_mapped.columns
        assert df_mapped['project_name'].iloc[0] == 'Withdrawn Project'

    def test_map_dataframe_columns_preserves_unmapped_columns(self):
        """Test that columns not in the mapping are preserved."""
        df = pd.DataFrame({
            'Unnamed: 0_level_0 Project Name': ['Project A'],
            'custom_column_not_in_mapping': ['custom_value'],
        })

        df_mapped = map_dataframe_columns(df)

        # Check that unmapped column is preserved
        assert 'custom_column_not_in_mapping' in df_mapped.columns
        assert df_mapped['custom_column_not_in_mapping'].iloc[0] == 'custom_value'

    def test_map_dataframe_columns_preserves_data(self):
        """Test that data values are preserved during mapping."""
        original_data = {
            'Unnamed: 0_level_0 Project Name': ['Project A', 'Project B', 'Project C'],
            'MWs MW-1': [100.0, 200.0, 300.0],
        }
        df = pd.DataFrame(original_data)

        df_mapped = map_dataframe_columns(df)

        # Check that data is preserved
        assert list(df_mapped['project_name']) == ['Project A', 'Project B', 'Project C']
        assert list(df_mapped['mw_1']) == [100.0, 200.0, 300.0]

    def test_map_dataframe_columns_empty_dataframe(self):
        """Test mapping with an empty DataFrame."""
        df = pd.DataFrame()
        df_mapped = map_dataframe_columns(df)

        assert len(df_mapped) == 0
        assert len(df_mapped.columns) == 0

    def test_fuel_type_columns_mapping(self):
        """Test that all fuel type columns are mapped correctly."""
        mapping = get_column_mapping()

        assert mapping['Generating Facility Fuel-1'] == 'fuel_type_1'
        assert mapping['Generating Facility Fuel-2'] == 'fuel_type_2'
        assert mapping['Generating Facility Fuel-3'] == 'fuel_type_3'

    def test_mw_columns_mapping(self):
        """Test that all MW columns are mapped correctly."""
        mapping = get_column_mapping()

        assert mapping['MWs MW-1'] == 'mw_1'
        assert mapping['MWs MW-2'] == 'mw_2'
        assert mapping['MWs MW-3'] == 'mw_3'
        assert mapping['MWs Net MWs to Grid'] == 'net_mw'

    def test_location_columns_mapping(self):
        """Test that location-related columns are mapped correctly."""
        mapping = get_column_mapping()

        assert mapping['Location County'] == 'county'
        assert mapping['Location State'] == 'state'

    def test_special_characters_in_column_names(self):
        """Test that column names with special characters (newlines, etc.) are handled."""
        mapping = get_column_mapping()

        # These columns contain newlines in the original Excel file
        assert 'Unnamed: 2_level_0 Interconnection Request\nReceive Date' in mapping
        assert mapping['Unnamed: 2_level_0 Interconnection Request\nReceive Date'] == 'request_receive_date'
