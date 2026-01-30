"""Utility functions for alphanumeric version sorting and manipulation."""

from __future__ import annotations

import re
from typing import List


def alphanum_convert(text: str) -> int | str:
    """Convert a string to int if it's numeric, otherwise lowercase it."""
    if text.isdigit():
        return int(text)
    return text.lower()


def get_alphanum_key(key: str | int | None) -> list:
    """
    Return a list containing the parts of the key (split by number parts).

    Each number is converted to an integer and string parts are left as strings.
    This enables correct sorting in Python when the lists are compared.

    Example:
        get_alphanum_key('1.2.2') results in ['', 1, '.', 2, '.', 2, '']
        get_alphanum_key('1.0.10') results in ['', 1, '.', 0, '.', 10, '']

    This ensures that '1.0.10' > '1.0.2' (correct) rather than '1.0.10' < '1.0.2' (string comparison).
    """
    if key == "" or key is None:
        return []
    alphanum_key = [alphanum_convert(c) for c in re.split("([0-9]+)", str(key))]
    return alphanum_key


def sorted_alphanumeric(data: List[str]) -> List[str]:
    """Sort a list of strings using alphanumeric comparison."""
    return sorted(data, key=get_alphanum_key)


def max_alphanumeric(versions: List[str | int | None]) -> str | int | None:
    """
    Find the maximum version from a list using alphanumeric comparison.

    Args:
        versions: List of version strings/numbers (may contain None values)

    Returns:
        The maximum version, or None if the list is empty or contains only None values
    """
    # Filter out None and empty values
    valid_versions = [v for v in versions if v is not None and v != ""]
    if not valid_versions:
        return None
    return max(valid_versions, key=get_alphanum_key)


def increment_version(version: str) -> str:
    """
    Increment the last numeric component of a version string.

    Examples:
        "1.0.0" -> "1.0.1"
        "1.2.9" -> "1.2.10"
        "20260129" -> "20260130"
        "v1.0" -> "v1.1"

    Args:
        version: The version string to increment

    Returns:
        The incremented version string
    """
    # Split the version into numeric and non-numeric parts
    parts = re.split(r"(\d+)", version)

    # Find the last numeric part and increment it
    for i in range(len(parts) - 1, -1, -1):
        if parts[i].isdigit():
            parts[i] = str(int(parts[i]) + 1)
            break

    return "".join(parts)
