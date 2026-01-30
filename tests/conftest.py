from __future__ import annotations

import tempfile
from pathlib import Path
from typing import List, NamedTuple, Union
from unittest import mock

import pytest
from snowflake.connector.cursor import SnowflakeCursor


class MockResultMetadata(NamedTuple):
    """Mock result metadata for cursor columns."""

    name: str
    type_code: int = 1


class MockCursor(SnowflakeCursor):
    """Mock cursor for testing Snowflake queries."""

    def __init__(self, rows: List[Union[tuple, dict]], columns: List[str]):
        super().__init__(mock.Mock())
        self._rows = rows
        self._columns = [MockResultMetadata(c) for c in columns]
        self.query = "SELECT A MOCK QUERY"

    def fetchone(self):
        if self._rows:
            return self._rows.pop(0)
        return None

    def fetchall(self):
        return self._rows

    @property
    def rowcount(self):
        return len(self._rows)

    @property
    def description(self):
        yield from self._columns

    @classmethod
    def from_input(cls, rows, columns):
        return cls(rows, columns)


@pytest.fixture
def mock_cursor():
    """Fixture to create mock cursors for testing."""
    return MockCursor.from_input


@pytest.fixture
def temporary_directory():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


@pytest.fixture
def sample_package_files(temporary_directory):
    """Create sample files for package testing."""
    # Create main file
    (temporary_directory / "main.py").write_text("print('hello')")

    # Create subdirectory with files
    subdir = temporary_directory / "lib"
    subdir.mkdir()
    (subdir / "utils.py").write_text("def helper(): pass")
    (subdir / "config.yml").write_text("key: value")

    return temporary_directory
