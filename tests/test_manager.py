"""Tests for PackageManager class."""

from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli.api.exceptions import CliError

from snowflake_cli_package.manager import PACKAGES_BASE_PATH, PackageManager


STAGE_MANAGER = "snowflake_cli_package.manager.StageManager"
PACKAGE_MANAGER = "snowflake_cli_package.manager.PackageManager"


class TestListVersions:
    """Tests for list_versions method."""

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_list_versions_returns_sorted_versions(self, mock_list_files, mock_cursor):
        """Test that versions are returned sorted alphanumerically."""
        mock_list_files.return_value = mock_cursor(
            rows=[
                {"name": "test_stage/packages/my-package/1.0.10/file.txt"},
                {"name": "test_stage/packages/my-package/1.0.2/file.txt"},
                {"name": "test_stage/packages/my-package/1.0.9/file.txt"},
            ],
            columns=["name"],
        )

        manager = PackageManager()
        versions = manager.list_versions("@db.schema.test_stage", "my-package")

        assert versions == ["1.0.2", "1.0.9", "1.0.10"]

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_list_versions_empty_stage(self, mock_list_files, mock_cursor):
        """Test that empty list is returned when no files exist."""
        mock_list_files.return_value = mock_cursor(rows=[], columns=["name"])

        manager = PackageManager()
        versions = manager.list_versions("@db.schema.test_stage", "my-package")

        assert versions == []

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_list_versions_handles_exception(self, mock_list_files):
        """Test that exceptions are handled gracefully."""
        mock_list_files.side_effect = Exception("Connection error")

        manager = PackageManager()
        versions = manager.list_versions("@db.schema.test_stage", "my-package")

        assert versions == []

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_list_versions_extracts_unique_versions(self, mock_list_files, mock_cursor):
        """Test that duplicate versions are deduplicated."""
        mock_list_files.return_value = mock_cursor(
            rows=[
                {"name": "test_stage/packages/my-package/1.0.0/file1.txt"},
                {"name": "test_stage/packages/my-package/1.0.0/file2.txt"},
                {"name": "test_stage/packages/my-package/1.0.0/subdir/file3.txt"},
                {"name": "test_stage/packages/my-package/2.0.0/file.txt"},
            ],
            columns=["name"],
        )

        manager = PackageManager()
        versions = manager.list_versions("@db.schema.test_stage", "my-package")

        assert versions == ["1.0.0", "2.0.0"]


class TestGetMaxVersion:
    """Tests for get_max_version method."""

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_get_max_version(self, mock_list_files, mock_cursor):
        """Test that max version is returned correctly."""
        mock_list_files.return_value = mock_cursor(
            rows=[
                {"name": "test_stage/packages/my-package/1.0.10/file.txt"},
                {"name": "test_stage/packages/my-package/1.0.2/file.txt"},
                {"name": "test_stage/packages/my-package/1.0.9/file.txt"},
            ],
            columns=["name"],
        )

        manager = PackageManager()
        max_version = manager.get_max_version("@db.schema.test_stage", "my-package")

        assert max_version == "1.0.10"

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_get_max_version_no_versions(self, mock_list_files, mock_cursor):
        """Test that None is returned when no versions exist."""
        mock_list_files.return_value = mock_cursor(rows=[], columns=["name"])

        manager = PackageManager()
        max_version = manager.get_max_version("@db.schema.test_stage", "my-package")

        assert max_version is None


class TestVersionExists:
    """Tests for version_exists method."""

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_version_exists_true(self, mock_list_files, mock_cursor):
        """Test that True is returned when version exists."""
        mock_list_files.return_value = mock_cursor(
            rows=[{"name": "test_stage/packages/my-package/1.0.0/file.txt"}],
            columns=["name"],
        )

        manager = PackageManager()
        exists = manager.version_exists("@db.schema.test_stage", "my-package", "1.0.0")

        assert exists is True

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_version_exists_false(self, mock_list_files, mock_cursor):
        """Test that False is returned when version does not exist."""
        mock_list_files.return_value = mock_cursor(
            rows=[{"name": "test_stage/packages/my-package/1.0.0/file.txt"}],
            columns=["name"],
        )

        manager = PackageManager()
        exists = manager.version_exists("@db.schema.test_stage", "my-package", "2.0.0")

        assert exists is False


class TestListPackages:
    """Tests for list_packages method."""

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_list_packages_returns_sorted(self, mock_list_files, mock_cursor):
        """Test that packages are returned sorted alphabetically."""
        mock_list_files.return_value = mock_cursor(
            rows=[
                {"name": "test_stage/packages/zebra-pkg/1.0.0/file.txt"},
                {"name": "test_stage/packages/alpha-pkg/1.0.0/file.txt"},
                {"name": "test_stage/packages/beta-pkg/1.0.0/file.txt"},
            ],
            columns=["name"],
        )

        manager = PackageManager()
        packages = manager.list_packages("@db.schema.test_stage")

        assert packages == ["alpha-pkg", "beta-pkg", "zebra-pkg"]

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_list_packages_empty(self, mock_list_files, mock_cursor):
        """Test that empty list is returned when no packages exist."""
        mock_list_files.return_value = mock_cursor(rows=[], columns=["name"])

        manager = PackageManager()
        packages = manager.list_packages("@db.schema.test_stage")

        assert packages == []

    @mock.patch(f"{STAGE_MANAGER}.list_files")
    def test_list_packages_deduplicates(self, mock_list_files, mock_cursor):
        """Test that duplicate package names are deduplicated."""
        mock_list_files.return_value = mock_cursor(
            rows=[
                {"name": "test_stage/packages/my-package/1.0.0/file.txt"},
                {"name": "test_stage/packages/my-package/2.0.0/file.txt"},
                {"name": "test_stage/packages/other-pkg/1.0.0/file.txt"},
            ],
            columns=["name"],
        )

        manager = PackageManager()
        packages = manager.list_packages("@db.schema.test_stage")

        assert packages == ["my-package", "other-pkg"]


class TestPush:
    """Tests for push method."""

    @mock.patch(f"{PACKAGE_MANAGER}.version_exists")
    def test_push_fails_if_version_exists(self, mock_version_exists, temporary_directory):
        """Test that push fails if version already exists."""
        mock_version_exists.return_value = True

        manager = PackageManager()
        with pytest.raises(CliError) as exc_info:
            list(
                manager.push(
                    stage="@db.schema.test_stage",
                    package_name="my-package",
                    version="1.0.0",
                    local_path=temporary_directory,
                )
            )

        assert "already exists" in str(exc_info.value)
        assert "immutable" in str(exc_info.value)

    def test_push_fails_if_path_not_exists(self):
        """Test that push fails if local path does not exist."""
        manager = PackageManager()
        with pytest.raises(CliError) as exc_info:
            list(
                manager.push(
                    stage="@db.schema.test_stage",
                    package_name="my-package",
                    version="1.0.0",
                    local_path=Path("/nonexistent/path"),
                )
            )

        assert "does not exist" in str(exc_info.value)

    def test_push_fails_if_path_is_file(self, temporary_directory):
        """Test that push fails if local path is a file, not directory."""
        file_path = temporary_directory / "file.txt"
        file_path.write_text("content")

        manager = PackageManager()
        with pytest.raises(CliError) as exc_info:
            list(
                manager.push(
                    stage="@db.schema.test_stage",
                    package_name="my-package",
                    version="1.0.0",
                    local_path=file_path,
                )
            )

        assert "must be a directory" in str(exc_info.value)


class TestPull:
    """Tests for pull method."""

    @mock.patch(f"{PACKAGE_MANAGER}.version_exists")
    def test_pull_fails_if_version_not_exists(
        self, mock_version_exists, temporary_directory
    ):
        """Test that pull fails if version does not exist."""
        mock_version_exists.return_value = False

        manager = PackageManager()
        with pytest.raises(CliError) as exc_info:
            manager.pull(
                stage="@db.schema.test_stage",
                package_name="my-package",
                version="1.0.0",
                local_path=temporary_directory,
            )

        assert "does not exist" in str(exc_info.value)

    def test_pull_fails_if_path_not_exists(self):
        """Test that pull fails if local path does not exist."""
        manager = PackageManager()
        with pytest.raises(CliError) as exc_info:
            manager.pull(
                stage="@db.schema.test_stage",
                package_name="my-package",
                version="1.0.0",
                local_path=Path("/nonexistent/path"),
            )

        assert "does not exist" in str(exc_info.value)

    def test_pull_fails_if_path_is_file(self, temporary_directory):
        """Test that pull fails if local path is a file, not directory."""
        file_path = temporary_directory / "file.txt"
        file_path.write_text("content")

        manager = PackageManager()
        with pytest.raises(CliError) as exc_info:
            manager.pull(
                stage="@db.schema.test_stage",
                package_name="my-package",
                version="1.0.0",
                local_path=file_path,
            )

        assert "must be a directory" in str(exc_info.value)

    @mock.patch(f"{PACKAGE_MANAGER}._get_directory_recursive")
    @mock.patch(f"{PACKAGE_MANAGER}.version_exists")
    def test_pull_resolves_latest_version(
        self, mock_version_exists, mock_get_recursive, temporary_directory, mock_cursor
    ):
        """Test that 'latest' version is resolved correctly."""
        mock_version_exists.return_value = True
        mock_get_recursive.return_value = []

        with mock.patch.object(
            PackageManager, "get_max_version", return_value="2.0.0"
        ):
            manager = PackageManager()
            manager.pull(
                stage="@db.schema.test_stage",
                package_name="my-package",
                version="latest",
                local_path=temporary_directory,
            )

        # Verify that get_max_version was called to resolve "latest"
        mock_get_recursive.assert_called_once()


class TestGetVersionPathStr:
    """Tests for _get_version_path_str method."""

    def test_builds_correct_path(self):
        """Test that version path is built correctly."""
        manager = PackageManager()
        path = manager._get_version_path_str(
            stage="@db.schema.test_stage",
            package_name="my-package",
            version="1.2.3",
        )

        assert path == "@db.schema.test_stage/packages/my-package/1.2.3"

    def test_handles_stage_without_at_prefix(self):
        """Test that stage without @ prefix works."""
        manager = PackageManager()
        path = manager._get_version_path_str(
            stage="db.schema.test_stage",
            package_name="my-package",
            version="1.0.0",
        )

        # StagePath adds @ prefix if missing
        assert "packages/my-package/1.0.0" in path
