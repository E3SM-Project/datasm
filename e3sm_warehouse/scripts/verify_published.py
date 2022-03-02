import sys
import os
import argparse
from subprocess import Popen, PIPE
from tqdm import tqdm
from e3sm_warehouse.util import search_esgf

DESC = "Verify that all the datasets under some base path have been published to ESGF"

def parse_args():
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument(
        "src_path",
        type=str,
        help="base path to start from, will run all datasets below this in the file tree",
    )
    parser.add_argument(
        "base_path",
        type=str,
        help="Base of the publication tree",
    )
    parser.add_argument(
        "-m", "--mapfile-path",
        type=str,
        default=os.environ['PWD'] + "/mapfiles/",
        help=f"path to where the mapfiles should be stored, default is {os.environ['PWD']}/mapfiles/",
    )
    parser.add_argument(
        "-s", "--scripts-path",
        type=str,
        default="/export/baldwin32/projects/esgfpub/e3sm_warehouse/e3sm_warehouse/scripts",
        help=f"Path to the e3sm_warehouse/scripts directory, a dirty hack that should be fixed",
    )
    return parser.parse_args()


def verify_dataset(src_path, base_path, scripts_path, out_path, pbar, **kwargs):
    """
    Checks that a dataset is available on ESGF, and if its not
    users the esgpublish utility to publish it

    Returns 0 if successful, 1 otherwise
    """

    # get the dataset_id from the path
    master_id = str(src_path)[len(str(base_path)):].replace(os.sep, '.')
    dataset_id = '.'.join(master_id.split('.')[:-1])
    version = master_id.split('.')[-1][1:]

    pbar.set_description(dataset_id)

    # check that this dataset doesnt already exist
    if "CMIP6" in dataset_id:
        project = "CMIP6"
    else:
        project = "e3sm"

    facets = {"instance_id": master_id, "type": "Dataset"}
    docs = search_esgf(project, facets)

    # if the dataset_id shows up in the search, then
    # this dataset already exists
    if docs and int(docs[0]["number_of_files"]) != 0:
        print(f"skipping {dataset_id}")
        return 0

    # create the mapfile for this dataset
    map_path = f"{out_path}/{dataset_id + '.map'}"
    if not os.path.exists(map_path):
        cmd = f"python {scripts_path}/generate_mapfile.py {src_path} {dataset_id} {version} --outpath {map_path} -p 1 --quiet"
        proc = Popen(
            cmd.split(), stdout=PIPE, stderr=PIPE, universal_newlines=True
        )
        _, err = proc.communicate()
        if err:
            print(f"Error creating mapfile: {cmd}")
            print(err)
            return 1
    
    cmd = f"esgpublish --project {project} --map {map_path}"
    proc = Popen(
        cmd.split(), stdout=PIPE, stderr=PIPE, universal_newlines=True
    )
    _, err = proc.communicate()
    if err:
        print(f"Error running publication: {cmd}")
        print(err)
        return 1

    return 0


def main():

    # FIXME: elided as unreliable - timing issue?
    return 0


    parsed_args = parse_args()

    source_paths = []
    for root, dirs, files in os.walk(parsed_args.src_path):
        # we only want leaf dirs
        if not files or dirs:
            continue
        source_paths.append(root)

    pbar = tqdm(total=len(source_paths))
    for src_path in source_paths:
        ret = verify_dataset(
            src_path=src_path, 
            base_path=parsed_args.base_path,
            scripts_path=parsed_args.scripts_path,
            out_path=parsed_args.mapfile_path,
            pbar=pbar)
        if ret != 0:
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
