#!/usr/bin/env python3
"""Check for markers in comments and issue warning or exception."""

import argparse
import logging
import os
import shlex
import subprocess
from typing import List, Tuple, Union

logger = logging.getLogger(__name__)
console = logging.StreamHandler()
logger.addHandler(console)
logger.setLevel(logging.INFO)


def find_markers(
    path: str, markers: List[str], ignore_files: List[str]
) -> Union[List[Tuple[str, str, str]], List[str]]:
    """Find all markers still left in the code contained in the given path.

    Ignores this script file. Other files to be ignored can be specified.
    This typically applies to the Gitlab job where this script is used
    (so that markers need to be passed as arguments there).

    Args:
        path: Folder where we analyse all code-files
        markers: Markers that we look for
        ignore_files: Single files to be ignored. If the file is located
            in the root dir, of the repository, prefix with os.curdir (".")

    Returns:
        Found markers or empty list
    """
    cmd = (
        "grep"
        r" --include \*.go"
        r" --include \*.py"
        r" --include \*.sc"
        r" --include \*.sh"
        r" --include \*.yml"
        r" --include \*.yaml"
        rf" -nrE {'|'.join(markers)} {path}"
    )

    cmd_splt = shlex.split(cmd)
    try:
        output = subprocess.check_output(cmd_splt).decode("utf-8")
    except subprocess.CalledProcessError:
        # No matches were found
        logging.info("No markers were found in the code :)")
        return []

    output_lines = [
        line
        for line in output.split("\n")
        if line and not line.startswith((*ignore_files,)) and os.path.basename(__file__) not in line
    ]

    return (
        [
            (file, line_no, marker_code.strip())
            for file, line_no, marker_code in zip(*(line.split(":", 2) for line in output_lines), strict=True)
        ]
        if output_lines
        else []
    )


def generate_output_for_results(forbidden_markers: List[str], marker_results: List[Tuple[str]]):
    """Writes log entries for the marker_results.

    Args:
        forbidden_markers: Markers that lead to an error instead of a warning
        marker_results: List containing information about occurences of markers

    """
    forbidden_marker_occured = False
    for file, line_no, marker_code in marker_results:
        if any((x in marker_code for x in forbidden_markers)):
            logger.error(f"Forbidden marker in line {line_no} of file {file}: " f"{forbidden_markers}")
            forbidden_marker_occured = True
        else:
            logger.warning(f"There is a remaining marker in line {line_no} of " f"file {file}: {marker_code}")
    if forbidden_marker_occured:
        raise RuntimeError("A forbidden marker occured, check log for more details.")


def get_arguments():
    """Parse arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Get arguments for checking makers in the source codes")
    parser.add_argument("--paths", help="Path of the code folders", nargs="+", required=True)
    parser.add_argument(
        "--markers",
        nargs="+",
        help="Markers that shall be identified in the code",
        required=True,
    )
    parser.add_argument(
        "--forbidden_markers",
        nargs="+",
        help="Markers that will return an error if they appear in the code",
        default=["FIXME"],
    )
    parser.add_argument(
        "--ignore_files",
        nargs="+",
        help="Single files that are to be excluded from the report,"
        "can be full or relative path."
        "Beware, relative paths can lead to multiple matches.",
        default=[],
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_arguments()
    # determine which markers are still contained in the code
    marker_results = []
    for path in args.paths:
        marker_results += find_markers(path, args.markers, args.ignore_files)

    generate_output_for_results(args.forbidden_markers, marker_results)
