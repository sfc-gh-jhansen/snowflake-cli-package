"""CLI commands for the package plugin."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CollectionResult, MessageResult

from snowflake_cli_package.manager import PackageManager
from snowflake_cli_package.version_utils import increment_version

app = SnowTyperFactory(
    name="package",
    help="Manages generic package workflows with Snowflake.",
)


@app.command(
    name="greet",
    requires_connection=False,
    requires_global_options=False,
)
def greet_command(
    name: str = typer.Option("Jane", "--name", "-n", help="Name to greet"),
) -> MessageResult:
    """
    Says hello to someone.
    """
    return MessageResult(f"Hello, {name}!")


@app.command(
    name="push",
    requires_connection=True,
)
def push_command(
    local_path: Path = typer.Argument(
        ...,
        help="Local directory containing files to upload.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
    package_name: str = typer.Argument(
        ...,
        help="Name of the package.",
    ),
    version: Optional[str] = typer.Argument(
        None,
        help="Version string for this release. If not provided, auto-increments from the latest version.",
    ),
    stage: str = typer.Option(
        ...,
        "--stage",
        "-s",
        help="Stage path for package storage (e.g., @my_db.my_schema.packages).",
    ),
    parallel: int = typer.Option(
        4,
        "--parallel",
        "-p",
        help="Number of parallel upload threads.",
    ),
    **options,
) -> CollectionResult:
    """
    Push files from a local directory to a new package version.

    Uploads all files recursively from LOCAL_PATH to the stage at:
    @stage/packages/PACKAGE_NAME/VERSION/

    If VERSION is not provided, the command will auto-increment from the
    latest existing version (or start at 1.0.0 if no versions exist).

    Package versions are immutable - pushing to an existing version will fail.
    """
    manager = PackageManager()

    # Determine version if not provided
    if version is None:
        max_version = manager.get_max_version(stage, package_name)
        if max_version is None:
            version = "1.0.0"
        else:
            version = increment_version(str(max_version))

    # Push the package
    results = list(
        manager.push(
            stage=stage,
            package_name=package_name,
            version=version,
            local_path=local_path,
            parallel=parallel,
        )
    )

    if not results:
        return CollectionResult([{"message": f"No files found in {local_path}"}])

    return CollectionResult(results)


@app.command(
    name="pull",
    requires_connection=True,
)
def pull_command(
    local_path: Path = typer.Argument(
        ...,
        help="Local directory to download files to.",
        resolve_path=True,
    ),
    package_name: str = typer.Argument(
        ...,
        help="Name of the package.",
    ),
    version: str = typer.Argument(
        ...,
        help="Version string to pull. Use 'latest' to pull the most recent version.",
    ),
    stage: str = typer.Option(
        ...,
        "--stage",
        "-s",
        help="Stage path for package storage (e.g., @my_db.my_schema.packages).",
    ),
    parallel: int = typer.Option(
        4,
        "--parallel",
        "-p",
        help="Number of parallel download threads.",
    ),
    **options,
) -> MessageResult:
    """
    Pull files from a package version to a local directory.

    Downloads all files from @stage/packages/PACKAGE_NAME/VERSION/
    to LOCAL_PATH, preserving the directory structure.

    Use 'latest' as VERSION to automatically pull the most recent version.
    """
    manager = PackageManager()

    # Pull the package
    results = manager.pull(
        stage=stage,
        package_name=package_name,
        version=version,
        local_path=local_path,
        parallel=parallel,
    )

    file_count = len(results)
    if file_count == 0:
        return MessageResult(f"No files found in package '{package_name}' version '{version}'")

    return MessageResult(
        f"Successfully pulled {file_count} file(s) from '{package_name}' "
        f"version '{version}' to {local_path}"
    )


@app.command(
    name="list-versions",
    requires_connection=True,
)
def list_versions_command(
    package_name: str = typer.Argument(
        ...,
        help="Name of the package.",
    ),
    stage: str = typer.Option(
        ...,
        "--stage",
        "-s",
        help="Stage path for package storage (e.g., @my_db.my_schema.packages).",
    ),
    **options,
) -> CollectionResult:
    """
    List all available versions for a package.

    Versions are sorted alphanumerically (e.g., 1.0.9 < 1.0.10).
    """
    manager = PackageManager()
    versions = manager.list_versions(stage, package_name)

    if not versions:
        return CollectionResult([{"message": f"No versions found for package '{package_name}'"}])

    return CollectionResult([{"version": v} for v in versions])


@app.command(
    name="list-packages",
    requires_connection=True,
)
def list_packages_command(
    stage: str = typer.Option(
        ...,
        "--stage",
        "-s",
        help="Stage path for package storage (e.g., @my_db.my_schema.packages).",
    ),
    **options,
) -> CollectionResult:
    """
    List all available packages on the stage.
    """
    manager = PackageManager()
    packages = manager.list_packages(stage)

    if not packages:
        return CollectionResult([{"message": "No packages found"}])

    return CollectionResult([{"package": p} for p in packages])


@app.command(
    name="max-version",
    requires_connection=True,
)
def max_version_command(
    package_name: str = typer.Argument(
        ...,
        help="Name of the package.",
    ),
    stage: str = typer.Option(
        ...,
        "--stage",
        "-s",
        help="Stage path for package storage (e.g., @my_db.my_schema.packages).",
    ),
    **options,
) -> MessageResult:
    """
    Get the maximum (latest) version for a package.

    Uses alphanumeric comparison (e.g., 1.0.9 < 1.0.10).
    """
    manager = PackageManager()
    max_ver = manager.get_max_version(stage, package_name)

    if max_ver is None:
        return MessageResult(f"No versions found for package '{package_name}'")

    return MessageResult(str(max_ver))
