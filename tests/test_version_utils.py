"""Tests for version_utils module."""

import pytest

from snowflake_cli_package.version_utils import (
    get_alphanum_key,
    increment_version,
    max_alphanumeric,
    sorted_alphanumeric,
)


class TestGetAlphanumKey:
    """Tests for get_alphanum_key function."""

    def test_empty_string(self):
        assert get_alphanum_key("") == []

    def test_none_value(self):
        assert get_alphanum_key(None) == []

    def test_simple_number(self):
        result = get_alphanum_key("123")
        assert result == ["", 123, ""]

    def test_semantic_version(self):
        result = get_alphanum_key("1.2.3")
        # Split on digits: ['', '1', '.', '2', '.', '3', '']
        assert result == ["", 1, ".", 2, ".", 3, ""]

    def test_version_with_prefix(self):
        result = get_alphanum_key("v1.0.0")
        assert result == ["v", 1, ".", 0, ".", 0, ""]

    def test_date_version(self):
        result = get_alphanum_key("20260129120500")
        assert result == ["", 20260129120500, ""]


class TestSortedAlphanumeric:
    """Tests for sorted_alphanumeric function."""

    def test_empty_list(self):
        assert sorted_alphanumeric([]) == []

    def test_semantic_versions(self):
        versions = ["1.0.10", "1.0.2", "1.0.9", "1.0.1"]
        expected = ["1.0.1", "1.0.2", "1.0.9", "1.0.10"]
        assert sorted_alphanumeric(versions) == expected

    def test_mixed_versions(self):
        versions = ["2.0.0", "1.10.0", "1.9.0", "1.2.0"]
        expected = ["1.2.0", "1.9.0", "1.10.0", "2.0.0"]
        assert sorted_alphanumeric(versions) == expected

    def test_date_versions(self):
        versions = ["20260201", "20260115", "20260130"]
        expected = ["20260115", "20260130", "20260201"]
        assert sorted_alphanumeric(versions) == expected

    def test_prefixed_versions(self):
        versions = ["v1.10.0", "v1.2.0", "v1.9.0"]
        expected = ["v1.2.0", "v1.9.0", "v1.10.0"]
        assert sorted_alphanumeric(versions) == expected

    def test_complex_versions(self):
        """Test that 1.0.10 comes after 1.0.9 (not before as in lexicographic)."""
        versions = ["1.0.9", "1.0.10"]
        result = sorted_alphanumeric(versions)
        assert result == ["1.0.9", "1.0.10"]
        # Verify this is different from lexicographic sort
        assert sorted(versions) == ["1.0.10", "1.0.9"]  # Wrong order!


class TestMaxAlphanumeric:
    """Tests for max_alphanumeric function."""

    def test_empty_list(self):
        assert max_alphanumeric([]) is None

    def test_list_with_none_values(self):
        assert max_alphanumeric([None, None]) is None

    def test_list_with_empty_strings(self):
        assert max_alphanumeric(["", ""]) is None

    def test_mixed_none_and_values(self):
        assert max_alphanumeric([None, "1.0.0", None, "2.0.0"]) == "2.0.0"

    def test_semantic_versions(self):
        versions = ["1.0.0", "1.0.10", "1.0.2", "1.0.9"]
        assert max_alphanumeric(versions) == "1.0.10"

    def test_date_versions(self):
        versions = ["20260115", "20260201", "20260130"]
        assert max_alphanumeric(versions) == "20260201"

    def test_single_version(self):
        assert max_alphanumeric(["1.0.0"]) == "1.0.0"


class TestIncrementVersion:
    """Tests for increment_version function."""

    def test_simple_patch_increment(self):
        assert increment_version("1.0.0") == "1.0.1"

    def test_patch_increment_with_higher_number(self):
        assert increment_version("1.0.9") == "1.0.10"

    def test_increment_two_part_version(self):
        assert increment_version("1.0") == "1.1"

    def test_increment_single_number(self):
        assert increment_version("5") == "6"

    def test_increment_date_version(self):
        # For date versions, it increments the last numeric part
        assert increment_version("20260129") == "20260130"

    def test_increment_with_prefix(self):
        # Versions with prefixes should increment the last numeric part
        assert increment_version("v1.0.0") == "v1.0.1"

    def test_increment_complex_version(self):
        assert increment_version("1.2.3") == "1.2.4"
        assert increment_version("10.20.30") == "10.20.31"
