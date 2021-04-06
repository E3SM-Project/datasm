import sys
import os
import argparse
import json
from pathlib import Path
from subprocess import Popen
from tempfile import TemporaryDirectory
from warehouse.util import sproket_with_id


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish a dataset to ESGF")
    parser.add_argument(
        '--dataset-id',
        type=str,
        required=True,
        help="The dataset ID to use for the publication check")
    return parser.parse_args()


def publish_dataset(dataset_id: str):
    """
    Checks that a dataset isn't already available on ESGF, and if its not
    users the esgpublish utility to publish it

    Returns 0 if successful, 1 otherwise
    """

    _, files = sproket_with_id(dataset_id)
    if files is None or not files:
        print(
            f"Dataset {dataset_id} has not been published to ESGF")
        return 1
    return 0


def main():
    parsed_args = parse_args()
    return publish_dataset(parsed_args)


if __name__ == "__main__":
    sys.exit(main())
