import sys
import os
import argparse
import json
from pathlib import Path
from subprocess import Popen
from tempfile import TemporaryDirectory
from warehouse.util import con_message
from warehouse.util import search_esgf


def parse_args():
    parser = argparse.ArgumentParser(description="Publish a dataset to ESGF")
    parser.add_argument(
        "--src-path",
        type=str,
        required=True,
        help="Source mapfile of the dataset to be published",
    )
    parser.add_argument(
        "--optional-facets",
        nargs="*",
        help="Optional facets to be added to the dataset (E3SM project only)",
    )
    default_log_path = Path(os.environ["PWD"], "publication_logs")
    parser.add_argument(
        "--log-path",
        default=default_log_path,
        help=f"Path where to store publisher logs, default = {default_log_path}",
    )
    return parser.parse_args()


def validate_args(args):
    """
    Ensure the src path exists.
    Ensure the project is either E3SM or CMIP6
    """
    src_path = Path(args.src_path)
    if not src_path.exists():
        con_message("error", "Source mapfile does not exist")
        return False

    return True


def publish_dataset(args):
    """
    Checks that a dataset isn't already available on ESGF, and if its not
    users the esgpublish utility to publish it

    Returns 0 if successful, 1 otherwise
    """
    src_path = Path(args.src_path)
    optional_facets = None
    if args.optional_facets:
        optional_facets = {}
        for item in args.optional_facets:
            key, value = item.split("=")
            optional_facets[key] = value
    log_path = Path(args.log_path)
    if not log_path.exists():
        log_path.mkdir(parents=True, exist_ok=True)

    # get the dataset_id from the mapfile
    with open(src_path, "r") as instream:
        line = instream.readline()
    dataset_id = line.split("|")[0].replace("#", ".v").strip()

    # check that this dataset doesnt already exist
    if "CMIP6" in dataset_id:
        project = "cmip6"
    else:
        project = "e3sm"
    facets = {"instance_id": dataset_id, "type": "Dataset"}
    docs = search_esgf(project, facets)

    if docs and int(docs[0]["number_of_files"]) != 0:
        msg = f"Dataset {dataset_id} has already been published to ESGF and is marked as the latest version"
        con_message("error", msg)
        return 1

    with open(src_path, "r") as instream:
        items = instream.readline().split("|")
        if "E3SM" in items[0].split(".")[0]:
            project = "e3sm"
        else:
            project = "cmip6"

    with TemporaryDirectory() as tmpdir:
        cmd = f"esgpublish --project {project} --map {src_path}"
        if project == "e3sm":
            if optional_facets is not None and optional_facets:
                project_metadata_path = os.path.join(tmpdir, f"{dataset_id}.json")
                with open(project_metadata_path, "w") as outstream:
                    json.dump(optional_facets, outstream)
                cmd += f" --json {project_metadata_path}"

        con_message("info", f"Running: {cmd}")
        log = Path(log_path, f"{dataset_id}.log")
        con_message("info", f"Writing publication log to {log}")

        with open(log, "w") as logstream:
            # FOR TESTING ONLY
            # cmd = cmd + " --help"
            proc = Popen(
                cmd.split(), stdout=logstream, stderr=logstream, universal_newlines=True
            )
            proc.wait()

        con_message("info", f"Return code {str(proc.returncode)} on cmd: {cmd}")

        return proc.returncode


def main():
    parsed_args = parse_args()

    if not validate_args(parsed_args):
        sys.exit(1)

    return publish_dataset(parsed_args)


if __name__ == "__main__":
    sys.exit(main())
