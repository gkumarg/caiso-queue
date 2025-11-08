# Test Suite for CAISO Queue ETL & Analysis

This directory contains the test suite for the CAISO Queue project.

## Test Structure

```
tests/
├── __init__.py              # Test package initialization
├── conftest.py              # Shared pytest fixtures and configuration
├── fixtures/                # Test data and fixtures
├── test_column_mapping.py   # Tests for column mapping module
├── test_parse_queue.py      # Tests for data parsing module
├── test_data_collection.py  # Tests for data collection module
└── test_analyze_queue.py    # Tests for analysis module
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run specific test file
```bash
pytest tests/test_column_mapping.py
```

### Run specific test class
```bash
pytest tests/test_column_mapping.py::TestColumnMapping
```

### Run specific test method
```bash
pytest tests/test_column_mapping.py::TestColumnMapping::test_get_column_mapping_returns_dict
```

### Run with verbose output
```bash
pytest -v
```

### Run with coverage report
```bash
pytest --cov=scripts --cov=dashboard --cov-report=html --cov-report=term
```

### Run only unit tests (fast)
```bash
pytest -m unit
```

### Run only integration tests
```bash
pytest -m integration
```

### Skip slow/network tests
```bash
pytest -m "not slow and not network"
```

## Test Markers

Tests are organized using pytest markers:

- `@pytest.mark.unit` - Fast unit tests that don't require external resources
- `@pytest.mark.integration` - Integration tests that may require database or files
- `@pytest.mark.slow` - Tests that take significant time to run
- `@pytest.mark.network` - Tests that require network access

## Writing New Tests

### Test File Naming
- Test files should be named `test_*.py` or `*_test.py`
- Test classes should start with `Test`
- Test functions should start with `test_`

### Using Fixtures

Fixtures are defined in `conftest.py` and are automatically available to all tests:

```python
def test_example(sample_queue_data):
    """Example test using a fixture."""
    assert len(sample_queue_data) > 0
```

Available fixtures:
- `sample_queue_data` - Sample DataFrame with queue data
- `sample_withdrawn_data` - Sample withdrawn projects data
- `temp_db` - Temporary SQLite database
- `temp_dir` - Temporary directory for file operations
- `mock_db_with_data` - Pre-populated test database
- `mock_env_vars` - Mock environment variables

### Example Test

```python
import pytest

@pytest.mark.unit
class TestExample:
    """Example test class."""

    def test_basic_functionality(self):
        """Test basic functionality."""
        result = 1 + 1
        assert result == 2

    def test_with_fixture(self, sample_queue_data):
        """Test using a fixture."""
        assert 'queue_position' in sample_queue_data.columns
```

## Test Coverage

To generate a coverage report:

```bash
pytest --cov=scripts --cov=dashboard --cov-report=html
```

Then open `htmlcov/index.html` in your browser.

## Continuous Integration

Tests are automatically run in CI/CD via GitHub Actions. See `.github/workflows/` for configuration.

## Troubleshooting

### Import Errors
If you get import errors, make sure you're running pytest from the project root:
```bash
cd /path/to/caiso-queue
pytest
```

### Database Locked Errors
If you get database locked errors, make sure no other processes are accessing the test database.

### Missing Dependencies
Install test dependencies:
```bash
pip install -r requirements.txt
```

## Best Practices

1. **Keep tests isolated** - Each test should be independent
2. **Use fixtures** - Reuse common setup code via fixtures
3. **Test edge cases** - Include tests for error conditions
4. **Clear test names** - Test names should describe what they test
5. **Fast tests** - Keep unit tests fast; mark slow tests appropriately
6. **Mock external dependencies** - Mock network calls, file I/O, etc.

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Pytest Fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [Pytest Markers](https://docs.pytest.org/en/stable/mark.html)
