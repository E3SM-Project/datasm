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
        '--src-path',
        type=str,
        required=True,
        help="Source mapfile of the dataset to be published")
    parser.add_argument(
        '--project',
        type=str,
        required=True,
        help="The project to be published to, either E3SM or CMIP6")
    parser.add_argument(
        '--optional-facets',
        nargs='*',
        help="Optional facets to be added to the dataset (E3SM project only)")
    default_log_path = Path(os.environ['PWD'], 'publication_logs')
    parser.add_argument(
        '--log-path',
        default=default_log_path,
        help=f"Path where to store publisher logs, default = {default_log_path}")
    return parser.parse_args()


def validate_args(args):
    """
    Ensure the src path exists.
    Ensure the project is either E3SM or CMIP6
    """
    src_path = Path(args.src)
    project = args.project
    if not src_path.exists():
        print("Source mapfile does not exist")
        return False

    if not project or project not in ['E3SM', 'CMIP6']:
        print('Invalid project, requires either E3SM or CMIP6')
        return False

    return True


def publish_dataset(args: Namespace):
    """
    Checks that a dataset isn't already available on ESGF, and if its not
    users the esgpublish utility to publish it

    Returns 0 if successful, 1 otherwise
    """
    src_path = Path(args.src)
    project = args.project
    optional_facets = args.optional_facets
    log_path = Path(args.log_path)
    if not log_path.exists():
        log_path.mkdir(parents=True, exist_ok=True)

    # get the dataset_id from the mapfile
    with open(src_path, 'r') as instream:
        line = instream.readline()
    dataset_id = line.split('|')[0].replace('#', 'v')

    # check that this dataset doesnt already exist
    _, files = sproket_with_id(dataset_id)
    if files is not None and files.any():
        print(
            f"Dataset {dataset_id} has already been published to ESGF and is marked as the latest version")
        return 1

    with TemporaryDirectory() as tmpdir:
        if project == 'CMIP6':
            cmd = f"esgpublish --project cmip6 --map {src_path}"
        else:
            cmd = f"esgpublish --project e3sm --map {src_path}"
            if optional_facets is not None and optional_facets.any():
                project_metadata_path = os.path.join(
                    tmpdir, f'{dataset_id}.json')
                with open(project_metadata_path, 'w') as outstream:
                    json.dump(optional_facets, outstream)
                cmd += f" --json {project_metadata_path}"

        print(f"Running: {' '.join(cmd)}")
        log = Path(log_path, f"{dataset_id}.log")
        print(f"Writing publication log to {log}")

        with open(log, 'w') as logstream:
            proc = Popen(
                cmd.split(),
                stdout=logstream,
                stderr=logstream,
                universal_newlines=True)
            proc.wait()
        return proc.returncode


def main():
    parsed_args = parse_args()

    if not validate_args(parsed_args):
        sys.exit(1)

    return publish_dataset(parsed_args)


if __name__ == "__main__":
    sys.exit(main())
