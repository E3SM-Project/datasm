import sys
import os
import argparse
import json
from pathlib import Path
from subprocess import Popen
from tempfile import TemporaryDirectory
from warehouse.util import sproket_with_id
from warehouse.util import con_message


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish a dataset to ESGF")
    parser.add_argument(
        '--dataset-id',
        type=str,
        required=True,
        help="The dataset ID to use for the publication check")
    return parser.parse_args()


def publish_dataset(dataset_id: str):   # should be renamed
    """
    Tests whether a dataset is already available on ESGF.

    Returns 0 if already published, 1 if not.
    """

    _, files = sproket_with_id(dataset_id)
    if files is None or not files:
        con_message('warning',
            f"Dataset {dataset_id} has not been published to ESGF")
        return 1
    return 0


def main():
    parsed_args = parse_args()
    return publish_dataset(parsed_args)


if __name__ == "__main__":
    sys.exit(main())
