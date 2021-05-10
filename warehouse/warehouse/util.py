import sys
import json
import traceback
import requests

from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
from pathlib import Path


def load_file_lines(file_path):
    if not file_path:
        return list()
    file_path = Path(file_path)
    if not file_path.exists() or not file_path.is_file():
        raise ValueError(
            f"file at path {file_path.resolve()} either doesnt exist or is not a regular file")
    with open(file_path, "r") as instream:
        retlist = [[i for i in x.split('\n') if i].pop()
                   for x in instream.readlines() if x[:-1]]
    return retlist


def get_last_status_line(file_path):
    with open(file_path, 'r') as instream:
        last_line = None
        for line in instream.readlines():
            if "STAT" in line:
                last_line = line
        return last_line
# -----------------------------------------------


def print_list(prefix, items):
    for x in items:
        print(f'{prefix}{x}')
# -----------------------------------------------


def print_file_list(outfile, items):
    with open(outfile, 'w') as outstream:
        for x in items:
            outstream.write(f"{x}\n")
# -----------------------------------------------


def print_debug(e):
    """
    Print an exceptions relevent information
    """
    print('1', e.__doc__)
    print('2', sys.exc_info())
    print('3', sys.exc_info()[0])
    print('4', sys.exc_info()[1])
    _, _, tb = sys.exc_info()
    print('5', traceback.print_tb(tb))
# -----------------------------------------------


# def sproket_with_id(dataset_id, sproket_path='sproket', **kwargs):

#     # create the path to the config, write it out
#     tempfile = NamedTemporaryFile(suffix='.json')
#     with open(tempfile.name, mode='w') as tmp:
#         config_string = json.dumps({
#             'search_api': "https://esgf-node.llnl.gov/esg-search/search/",
#             'data_node_priority': ["esgf-data2.llnl.gov", "aims3.llnl.gov", "esgf-data1.llnl.gov"],
#             'fields': {
#                 'dataset_id': dataset_id,
#                 'latest': 'true'
#             }
#         })

#         tmp.write(config_string)
#         tmp.seek(0)

#         cmd = [sproket_path, '-config', tempfile.name, '-y', '-urls.only']
#         proc = Popen(cmd, shell=False, stdout=PIPE, stderr=PIPE)
#         out, err = proc.communicate()
#     if err:
#         print(err.decode('utf-8'))
#         return dataset_id, None

#     files = sorted([i.decode('utf-8') for i in out.split()])
#     return dataset_id, files
# -----------------------------------------------


def search_esgf(project, facets, node="esgf-node.llnl.gov", filter_values=['cf_standard_name', 'variable', 'variable_long_name', 'variable_units'], latest='true'):
    """
    Make a search request to an ESGF node and return information about the datasets that match the search parameters

    Parameters:
        project (str): The ESGF project to search inside
        facets (dict): A dict with keys of facets, and values of facet values to search
        node (str): The esgf index node to querry
        filter_values (list): A list of string values to be filtered out of the return document
        latest (str): boolean (true/false not True/False) to search for only the latest version of a dataset
    """

    url = f"https://{node}/esg-search/search/?project={project}&format=application%2Fsolr%2Bjson&latest={latest}&{'&'.join([f'{k}={v}' for k,v in facets.items()])}"
    req = requests.get(url)
    if req.status_code != 200:
        raise ValueError(f"ESGF search request failed: {url}")

    docs = [{
            k: v for k, v in doc.items()
            if k not in filter_values
            } for doc in req.json()['response']['docs']]
    return docs
# -----------------------------------------------
