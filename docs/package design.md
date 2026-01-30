This repo contains a functioning basic Snowflake CLI plugin. Right now it only has one simple command called greet.

Let's start with the design phase, ask me whatever questions are needed for a great design. And I don't want to build all this in one pass, but rather want to build and test it one capability at a time (listed below) starting with the ability to push a package version. Below are the design details.


Design inputs:
* I want this plugin to manage generic packages for Snowflake
* I've done research on generic package/artifact repositories/registries, and my favorite is `GitLab Generic Packages` (https://docs.gitlab.com/user/packages/generic_packages/) which has a very simple and flexible design
* The core principle here is that we'll use a Snowflake stage as the storage for the packages
* I don't want to use any additional or external storage or state mechanism beyond the Snowflake stage (so no database to store additional state)
* Please review the Snowflake CLI code base (/Users/jhansen/Repos/snowflake-cli) to understand the coding conventions and the plugin conventions used there. Code in this project should match those conventions where possible.
* I'd like to adopt the following path convention within the Snowflake stage
   * /packages/:package_name/:package_version/:file_name
   * I've removed the GitLab concept of project ID as that's not relevant here
   * Where the package vesion (:package_vesrion) should be a folder than can contain one or more files (:file_name)
* The Snowflake CLI already has re-usable classes for interacting with Snowflake stages which I want to re-use here
* For the package_version I'd like it to be flexible and up to the user to decide, but will likely follow one of these conventions:
   * semantic version numbers (MAJOR.MINOR.PATCH): "1.0.0", "1.2.0", "1.12.2", etc.
   * date and time stamps: "20260129120500"
   * or any other number strings
* Must handle string version number sorting correctly
   * Must use alphanumeric sorting, and not lexicographic sorting, i.e. "1.0.9" comes before "1.0.10"
   * See below for the code I've used in the past, but please let me know if there's a better way to do it
* Build classes and code in a resuable way, like other Snowflake CLI plugins, which I belive is stored in the `manager.py` file
* I want to support the following capabilities
   * Push files from a local folder (recursively) to a new version of the package
   * Pull files from a version of the package to a local folder
   * List all the available vesrions for a package
   * List all available packages
   * Get the max available version for a package
* I want to use the language of "push" and "pull" for the corresponding package operations
* Some things I'm not sure how best to handle:
   * I'm not sure how best to handle creating new versions of a package, in particular how to determine the next version number during a push operation. Does the user just call push with a folder path and the tool generates and uses the next version number? Or does the user supply the version number to the push command (maybe with a helper function to get the max version first as a starting point)? I'm leaning towards the second, because it's more flexible and only the user will know when to increment the major and minor versions.


Code snippets to sort version number strings alphanumericly:

```python
import re

def alphanum_convert(text: str):
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


def sorted_alphanumeric(data):
    """Sort a list of strings using alphanumeric comparison."""
    return sorted(data, key=get_alphanum_key)


def max_alphanumeric(versions: list[str | int | None]) -> str | int | None:
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
```
