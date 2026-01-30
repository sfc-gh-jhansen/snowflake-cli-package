"""Package manager for Snowflake stage-based package storage."""

from __future__ import annotations

import logging
import shutil
from collections import deque
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Deque, Generator, List

from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath

from snowflake_cli_package.version_utils import max_alphanumeric

log = logging.getLogger(__name__)

# Base path within the stage for all packages
PACKAGES_BASE_PATH = "packages"


class PackageManager(SqlExecutionMixin):
    """
    Manages generic packages stored on a Snowflake stage.

    Package structure on stage:
        @stage/packages/{package_name}/{version}/{files...}
    """

    def __init__(self):
        super().__init__()
        self._stage_manager = StageManager()

    def _get_package_base_path(self, stage: str, package_name: str) -> StagePath:
        """Build the base path for a package on the stage."""
        stage_path = StagePath.from_stage_str(stage)
        return stage_path / PACKAGES_BASE_PATH / package_name

    def _get_version_path_str(
        self, stage: str, package_name: str, version: str
    ) -> str:
        """
        Build the full path string for a specific package version.

        We build this as a string to avoid StagePath's file detection issue
        with version numbers that contain dots (e.g., "1.2.0").
        """
        # Get the base path (without version)
        base_path = self._get_package_base_path(stage, package_name)
        # Append version as string to avoid StagePath's joinpath/is_file check
        return f"{base_path.absolute_path()}/{version}"

    def list_versions(self, stage: str, package_name: str) -> list[str]:
        """
        List all available versions for a package.

        Args:
            stage: The stage path (e.g., "@my_db.my_schema.packages")
            package_name: Name of the package

        Returns:
            List of version strings, sorted alphanumerically
        """
        package_path = self._get_package_base_path(stage, package_name)

        try:
            files = self._stage_manager.list_files(package_path).fetchall()
        except Exception as e:
            log.debug("Error listing files for package %s: %s", package_name, e)
            return []

        # Extract unique versions from file paths
        # File paths look like: packages/package_name/version/file.txt
        versions = set()
        package_prefix = f"{PACKAGES_BASE_PATH}/{package_name}/"

        for file_info in files:
            file_name = file_info.get("name", "")
            if package_prefix in file_name:
                # Extract the version part from the path
                relative_path = file_name.split(package_prefix, 1)[-1]
                parts = PurePosixPath(relative_path).parts
                if parts:
                    versions.add(parts[0])

        from snowflake_cli_package.version_utils import sorted_alphanumeric

        return sorted_alphanumeric(list(versions))

    def get_max_version(self, stage: str, package_name: str) -> str | None:
        """
        Get the maximum (latest) version for a package using alphanumeric comparison.

        Args:
            stage: The stage path
            package_name: Name of the package

        Returns:
            The maximum version string, or None if no versions exist
        """
        versions = self.list_versions(stage, package_name)
        return max_alphanumeric(versions)

    def version_exists(self, stage: str, package_name: str, version: str) -> bool:
        """
        Check if a specific version of a package exists.

        Args:
            stage: The stage path
            package_name: Name of the package
            version: Version to check

        Returns:
            True if the version exists, False otherwise
        """
        versions = self.list_versions(stage, package_name)
        return version in versions

    def list_packages(self, stage: str) -> list[str]:
        """
        List all available packages on the stage.

        Args:
            stage: The stage path (e.g., "@my_db.my_schema.packages")

        Returns:
            List of package names, sorted alphabetically
        """
        stage_path = StagePath.from_stage_str(stage)
        packages_path = stage_path / PACKAGES_BASE_PATH

        try:
            files = self._stage_manager.list_files(packages_path).fetchall()
        except Exception as e:
            log.debug("Error listing packages: %s", e)
            return []

        # Extract unique package names from file paths
        # File paths look like: stage_name/packages/package_name/version/file.txt
        packages = set()
        packages_prefix = f"{PACKAGES_BASE_PATH}/"

        for file_info in files:
            file_name = file_info.get("name", "")
            if packages_prefix in file_name:
                # Extract the package name part from the path
                relative_path = file_name.split(packages_prefix, 1)[-1]
                parts = PurePosixPath(relative_path).parts
                if parts:
                    packages.add(parts[0])

        return sorted(packages)

    @staticmethod
    def _find_deepest_directories(root_directory: Path) -> list[Path]:
        """
        Find all leaf directories (directories with no subdirectories).

        Uses BFS to build a tree and returns leaves sorted by depth (deepest first).
        """
        deepest_dirs: list[Path] = []
        queue: Deque[Path] = deque()
        queue.append(root_directory)

        while queue:
            current_dir = queue.popleft()
            children_directories = sorted(
                [d for d in current_dir.iterdir() if d.is_dir()]
            )
            if not children_directories and current_dir not in deepest_dirs:
                deepest_dirs.append(current_dir)
            else:
                queue.extend([c for c in children_directories if c not in deepest_dirs])

        # Sort by depth (deepest first)
        return sorted(deepest_dirs, key=lambda d: len(d.parts), reverse=True)

    @staticmethod
    def _copy_to_tmp_dir(source_path: Path, dest_dir: Path) -> None:
        """Copy files from source to destination, preserving directory structure."""
        for item in source_path.rglob("*"):
            dest_path = dest_dir / item.relative_to(source_path)
            if item.is_dir():
                dest_path.mkdir(parents=True, exist_ok=True)
            else:
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, dest_path)

    def _put_directory_recursive(
        self,
        local_path: Path,
        stage_path: str,
        parallel: int = 4,
    ) -> Generator[dict, None, None]:
        """
        Upload a directory recursively to a stage path.

        Uses the same efficient approach as Snowflake CLI's put_recursive:
        1. Copy files to a temp directory
        2. Process from deepest directories first
        3. Upload each directory with a single PUT (all files at once)
        4. Delete the directory after uploading so parent only has files

        This avoids StagePath's file detection issue with version numbers
        while maintaining efficiency (one PUT per directory, not per file).

        Args:
            local_path: Local directory to upload
            stage_path: Target stage path (as string, not StagePath)
            parallel: Number of parallel upload threads

        Yields:
            Dict with upload results for each file
        """
        # Ensure stage path ends with /
        if not stage_path.endswith("/"):
            stage_path += "/"

        with TemporaryDirectory() as tmp:
            temp_dir = Path(tmp)

            # Copy files to temp directory
            self._copy_to_tmp_dir(local_path, temp_dir)

            # Find deepest directories first
            deepest_dirs_list = self._find_deepest_directories(temp_dir)

            while deepest_dirs_list:
                directory = deepest_dirs_list.pop(0)

                # Skip root if there are still directories to process
                if directory == temp_dir and deepest_dirs_list:
                    continue

                # Upload directory contents (at this point it only has files)
                if list(directory.iterdir()):
                    # Build target path as string to avoid StagePath issues
                    rel_dir = directory.relative_to(temp_dir)
                    if str(rel_dir) == ".":
                        target_path = stage_path
                    else:
                        target_path = stage_path + str(PurePosixPath(rel_dir)) + "/"

                    log.debug("Uploading directory %s to %s", directory, target_path)

                    # Upload all files in directory with single PUT
                    cursor = self._stage_manager.put(
                        local_path=directory,
                        stage_path=target_path,
                        parallel=parallel,
                        overwrite=False,
                        auto_compress=False,
                        use_dict_cursor=True,
                    )

                    # Yield results
                    for result in cursor.fetchall():
                        result["source"] = str(rel_dir / result["source"])
                        result["target"] = target_path + result["target"]
                        yield result

                # Stop if we've processed root
                if directory == temp_dir:
                    break

                # Add parent to list if not already there
                if directory.parent not in deepest_dirs_list and not any(
                    existing.is_relative_to(directory.parent)
                    for existing in deepest_dirs_list
                ):
                    deepest_dirs_list.append(directory.parent)

                # Remove directory so parent only contains files
                shutil.rmtree(directory)

    def push(
        self,
        stage: str,
        package_name: str,
        version: str,
        local_path: Path,
        parallel: int = 4,
    ) -> Generator[dict, None, None]:
        """
        Push files from a local directory to a new package version.

        Args:
            stage: The stage path (e.g., "@my_db.my_schema.packages")
            package_name: Name of the package
            version: Version string for this release
            local_path: Local directory containing files to upload
            parallel: Number of parallel upload threads

        Yields:
            Dict with upload results for each file

        Raises:
            CliError: If the version already exists or local path is invalid
        """
        # Validate local path
        if not local_path.exists():
            raise CliError(f"Local path does not exist: {local_path}")
        if not local_path.is_dir():
            raise CliError(f"Local path must be a directory: {local_path}")

        # Check if version already exists
        if self.version_exists(stage, package_name, version):
            raise CliError(
                f"Version '{version}' already exists for package '{package_name}'. "
                "Package versions are immutable."
            )

        # Build the target path as a string (avoiding StagePath for version)
        stage_path_str = self._get_version_path_str(stage, package_name, version)

        log.info(
            "Pushing package '%s' version '%s' to %s",
            package_name,
            version,
            stage_path_str,
        )

        # Upload files recursively using our custom implementation
        yield from self._put_directory_recursive(
            local_path=local_path,
            stage_path=stage_path_str,
            parallel=parallel,
        )

    def _get_directory_recursive(
        self,
        stage_path: str,
        local_path: Path,
        parallel: int = 4,
    ) -> List[dict]:
        """
        Download files recursively from a stage path to a local directory.

        Downloads files to a temp directory first (since Snowflake GET preserves
        the full stage path structure), then moves them to the correct location.

        Args:
            stage_path: Source stage path (as string)
            local_path: Local directory to download to
            parallel: Number of parallel download threads

        Returns:
            List of dicts with download results
        """
        # Ensure local path exists
        if not local_path.exists():
            local_path.mkdir(parents=True, exist_ok=True)

        # Ensure stage path ends with / for listing
        stage_path_for_list = stage_path.rstrip("/") + "/"

        # List all files in the stage path
        try:
            files = self._stage_manager.list_files(stage_path_for_list).fetchall()
        except Exception as e:
            log.debug("Error listing files at %s: %s", stage_path_for_list, e)
            return []

        if not files:
            return []

        results = []

        # Extract info from stage path
        # stage_path is like @db.schema.stage/packages/pkg/1.0.0
        # file names from list are like stage_name/packages/pkg/1.0.0/subdir/file.txt
        # (Snowflake LIST includes the stage name as prefix in returned paths)
        stage_path_clean = stage_path.lstrip("@").rstrip("/")

        # Split to get the fully qualified stage name and the path within the stage
        stage_parts = stage_path_clean.split("/", 1)
        stage_fqn = stage_parts[0] if stage_parts else ""  # e.g., "db.schema.stage"
        path_in_stage = stage_parts[1] if len(stage_parts) > 1 else ""  # e.g., "packages/pkg/1.0.0"

        # The simple stage name (last part after dots) is what appears in LIST output
        stage_simple_name = stage_fqn.split(".")[-1]  # e.g., "stage" from "db.schema.stage"

        # base_prefix is what we expect files to start with in LIST output
        # It's: stage_simple_name/path_in_stage
        if path_in_stage:
            base_prefix = f"{stage_simple_name}/{path_in_stage}"
        else:
            base_prefix = stage_simple_name

        with TemporaryDirectory() as tmp:
            temp_dir = Path(tmp)

            for file_info in files:
                file_name = file_info.get("name", "")
                if not file_name:
                    continue

                # Calculate relative path from our base
                # file_name is like "test_plugin/packages/my-package/1.2.0/subfolder/file.txt"
                # base_prefix is like "test_plugin/packages/my-package/1.2.0"
                if base_prefix and file_name.startswith(base_prefix + "/"):
                    relative_path = file_name[len(base_prefix) + 1:]
                elif base_prefix and file_name.startswith(base_prefix):
                    relative_path = file_name[len(base_prefix):]
                    if relative_path.startswith("/"):
                        relative_path = relative_path[1:]
                else:
                    relative_path = file_name

                if not relative_path:
                    continue

                # Build full stage file path
                # file_name from LIST is like "stage_name/packages/pkg/1.0.0/file.txt"
                # We need to strip the stage_simple_name prefix and use the FQN
                if file_name.startswith(stage_simple_name + "/"):
                    path_after_stage = file_name[len(stage_simple_name) + 1:]
                else:
                    path_after_stage = file_name
                stage_file_path = f"@{stage_fqn}/{path_after_stage}"

                log.debug("Downloading %s", stage_file_path)

                try:
                    # Download to temp directory
                    self._stage_manager.get(
                        stage_path=stage_file_path,
                        dest_path=temp_dir,
                        parallel=parallel,
                    )

                    # Debug: List all files in temp directory
                    all_temp_files = list(temp_dir.rglob("*"))
                    log.debug(
                        "After downloading %s, temp dir contains: %s",
                        stage_file_path,
                        [str(f.relative_to(temp_dir)) for f in all_temp_files if f.is_file()],
                    )

                    # Determine final destination
                    relative_dir = str(PurePosixPath(relative_path).parent)
                    if relative_dir == ".":
                        final_dir = local_path
                    else:
                        final_dir = local_path / relative_dir
                        final_dir.mkdir(parents=True, exist_ok=True)

                    final_path = final_dir / PurePosixPath(relative_path).name
                    just_filename = PurePosixPath(file_name).name

                    # Try to find the downloaded file - check multiple possible locations
                    downloaded_file = None
                    possible_locations = [
                        temp_dir / file_name,  # Full path preserved
                        temp_dir / just_filename,  # Just filename
                        temp_dir / relative_path,  # Relative path from version
                    ]

                    for loc in possible_locations:
                        if loc.exists() and loc.is_file():
                            downloaded_file = loc
                            log.debug("Found downloaded file at: %s", loc)
                            break

                    # If not found in expected locations, search for it
                    if downloaded_file is None:
                        for f in all_temp_files:
                            if f.is_file() and f.name == just_filename:
                                downloaded_file = f
                                log.debug("Found downloaded file via search: %s", f)
                                break

                    if downloaded_file and downloaded_file.exists():
                        shutil.move(str(downloaded_file), str(final_path))
                        results.append({
                            "file": relative_path,
                            "status": "downloaded",
                            "target": str(final_path),
                        })
                    else:
                        log.warning(
                            "Downloaded file not found. Searched: %s. Temp contents: %s",
                            [str(p) for p in possible_locations],
                            [str(f) for f in all_temp_files],
                        )
                        results.append({
                            "file": relative_path,
                            "status": "failed",
                            "error": "Downloaded file not found",
                        })

                except Exception as e:
                    log.error("Failed to download %s: %s", stage_file_path, e)
                    results.append({
                        "file": relative_path,
                        "status": "failed",
                        "error": str(e),
                    })

        return results

    def pull(
        self,
        stage: str,
        package_name: str,
        version: str,
        local_path: Path,
        parallel: int = 4,
    ) -> List[dict]:
        """
        Pull files from a package version to a local directory.

        Args:
            stage: The stage path (e.g., "@my_db.my_schema.packages")
            package_name: Name of the package
            version: Version string to pull (or "latest" for max version)
            local_path: Local directory to download to
            parallel: Number of parallel download threads

        Returns:
            List of dicts with download results

        Raises:
            CliError: If the version doesn't exist or local path is invalid
        """
        # Handle "latest" version
        if version.lower() == "latest":
            max_ver = self.get_max_version(stage, package_name)
            if max_ver is None:
                raise CliError(
                    f"No versions found for package '{package_name}'. "
                    "Cannot resolve 'latest'."
                )
            version = str(max_ver)
            log.info("Resolved 'latest' to version '%s'", version)

        # Check if version exists
        if not self.version_exists(stage, package_name, version):
            raise CliError(
                f"Version '{version}' does not exist for package '{package_name}'."
            )

        # Build the source path as a string (avoiding StagePath for version)
        stage_path_str = self._get_version_path_str(stage, package_name, version)

        log.info(
            "Pulling package '%s' version '%s' from %s to %s",
            package_name,
            version,
            stage_path_str,
            local_path,
        )

        # Download files recursively
        return self._get_directory_recursive(
            stage_path=stage_path_str,
            local_path=local_path,
            parallel=parallel,
        )
