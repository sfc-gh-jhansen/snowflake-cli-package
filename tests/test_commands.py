"""Tests for CLI commands."""

from unittest import mock

import pytest

from snowflake_cli_package.commands import (
    list_packages_command,
    list_versions_command,
    max_version_command,
)


PACKAGE_MANAGER = "snowflake_cli_package.commands.PackageManager"


class TestListVersionsCommand:
    """Tests for list-versions command."""

    @mock.patch(PACKAGE_MANAGER)
    def test_list_versions_returns_versions(self, mock_manager_class):
        """Test that list-versions returns versions as collection."""
        mock_manager = mock_manager_class.return_value
        mock_manager.list_versions.return_value = ["1.0.0", "1.0.1", "2.0.0"]

        result = list_versions_command(
            package_name="my-package",
            stage="@db.schema.stage",
        )

        # CollectionResult.result is a generator, convert to list
        result_list = list(result.result)
        assert len(result_list) == 3
        assert result_list[0] == {"version": "1.0.0"}
        assert result_list[1] == {"version": "1.0.1"}
        assert result_list[2] == {"version": "2.0.0"}

    @mock.patch(PACKAGE_MANAGER)
    def test_list_versions_empty_returns_message(self, mock_manager_class):
        """Test that empty versions returns informative message."""
        mock_manager = mock_manager_class.return_value
        mock_manager.list_versions.return_value = []

        result = list_versions_command(
            package_name="my-package",
            stage="@db.schema.stage",
        )

        result_list = list(result.result)
        assert len(result_list) == 1
        assert "No versions found" in result_list[0]["message"]


class TestListPackagesCommand:
    """Tests for list-packages command."""

    @mock.patch(PACKAGE_MANAGER)
    def test_list_packages_returns_packages(self, mock_manager_class):
        """Test that list-packages returns packages as collection."""
        mock_manager = mock_manager_class.return_value
        mock_manager.list_packages.return_value = ["alpha-pkg", "beta-pkg"]

        result = list_packages_command(stage="@db.schema.stage")

        result_list = list(result.result)
        assert len(result_list) == 2
        assert result_list[0] == {"package": "alpha-pkg"}
        assert result_list[1] == {"package": "beta-pkg"}

    @mock.patch(PACKAGE_MANAGER)
    def test_list_packages_empty_returns_message(self, mock_manager_class):
        """Test that empty packages returns informative message."""
        mock_manager = mock_manager_class.return_value
        mock_manager.list_packages.return_value = []

        result = list_packages_command(stage="@db.schema.stage")

        result_list = list(result.result)
        assert len(result_list) == 1
        assert "No packages found" in result_list[0]["message"]


class TestMaxVersionCommand:
    """Tests for max-version command."""

    @mock.patch(PACKAGE_MANAGER)
    def test_max_version_returns_version(self, mock_manager_class):
        """Test that max-version returns the maximum version."""
        mock_manager = mock_manager_class.return_value
        mock_manager.get_max_version.return_value = "2.5.10"

        result = max_version_command(
            package_name="my-package",
            stage="@db.schema.stage",
        )

        assert result.message == "2.5.10"

    @mock.patch(PACKAGE_MANAGER)
    def test_max_version_no_versions_returns_message(self, mock_manager_class):
        """Test that no versions returns informative message."""
        mock_manager = mock_manager_class.return_value
        mock_manager.get_max_version.return_value = None

        result = max_version_command(
            package_name="my-package",
            stage="@db.schema.stage",
        )

        assert "No versions found" in result.message
