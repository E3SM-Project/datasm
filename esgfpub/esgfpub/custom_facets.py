import os
import stat
import requests
from tqdm import tqdm
from subprocess import Popen, PIPE
from pathlib import Path
from esgfpub.util import print_message, colors


def yield_leaf_dirs(path):
    for dirpath, dirs, files in tqdm(os.walk(path), desc=f'{colors.OKGREEN}[+]{colors.ENDC} Walking directory tree'):
        if dirs:
            continue
        if not files:
            continue
        yield dirpath


def collect_dataset_ids(data_path):
    dataset_ids = list()
    if not os.path.exists(data_path):
        raise ValueError("Directory does not exist: {}".format(data_path))
    dirs = [x for x in yield_leaf_dirs(data_path)]
    for d in dirs:
        tail, _ = os.path.split(d)
        cmip = False
        if "CMIP6" in tail:
            cmip = True
            idx = tail.index('CMIP6')
        elif "E3SM" in tail:
            idx = tail.index('E3SM')
        else:
            raise ValueError(
                "This appears to be neither a CMIP6 or E3SM data directory: {}".format(tail))

        dataset_id = tail[idx:]
        dataset_id = dataset_id.replace(os.sep, '.')
        dataset_ids.append(dataset_id)

    return dataset_ids


def run_cmd(command):
    popen = Popen(command, stdout=PIPE)
    return iter(popen.stdout.readline, b"")


def update_custom(facets, datadir, dataset_ids=None, debug=False):

    print_message("Generating custom facet mapfile", 'ok')
    if not dataset_ids:
        dataset_ids = []
        for path in datadir:
            dataset_ids.extend(collect_dataset_ids(path))

    print_message("Sending custom facets to the ESGF node", 'ok')

    cert_path = Path(os.environ['HOME'] + '/.globus/certificate-file')
    if not cert_path.exists():
        raise ValueError(f"The globus certificate doesnt exist where its expected, {str(cert_path.resolve())}")
    cert_path = str(cert_path.resolve())
    
    for dataset in tqdm(dataset_ids):
        url = "https://esgf-node.llnl.gov/esg-search/ws/updateById"
        for facet in facets:
            idx = facet.index('=')
            key = facet[:idx]
            val = facet[idx + 1:]
            obj = {
                "id": dataset + '|esgf-data2.llnl.gov',
                "action": "set",
                "field": key,
                "value": val,
                "core": "datasets"
            }
            res = requests.get(
                url, data=obj, verify=False, cert=cert_path)
            if res.status_code != 200:
                print(f"Error sending request {obj}, got response {res}")
            

    return 0
