import os, sys
import argparse
import re
from argparse import RawTextHelpFormatter
import requests
import time
from datetime import datetime
import pytz
import yaml



def ts():
    return pytz.utc.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")

helptext = '''
    Usage:  esgf_search --project project [--unrestricted]

'''

def logmsg(the_msg):
    the_ts = ts()
    print(f"{the_ts}: {the_msg}")

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('--project', action='store', dest="project", required=True)
    optional.add_argument('--unrestricted', action='store_true', dest="unrestricted", required=False)

    args = parser.parse_args()
    return args


# HTTP Request Functions ====================================

def raw_search_esgf(
    facets,
    offset="0",
    limit="50",
    node="esgf-node.llnl.gov",
    qtype="Dataset",
    fields="*",
    latest="true",
):
    """
    Make a search request to an ESGF node and return information about the datasets that match the search parameters

    Parameters:
        facets (dict): A dict with keys of facets, and values of facet values to search
        offset (str) : Offset into available results to return data
        limit (str)  : Number of results to return (default = 50, max = 10000)
        node (str)   : The esgf index node to query
        qtype (str)  : The query type, one of "Dataset" (default), "File" or "Aggregate"
        fields (str) : A comma-separated string of metadata field names, default '*' MUST be overridden.
        latest (str) : Boolean (true/false not True/False) to search for only the latest version of a dataset
    """

    if fields == "*":
        print("ERROR: Must specify string of one or more CSV fieldnames with fields=string")
        return None

    if len(facets):
        url = f"https://{node}/esg-search/search/?offset={offset}&limit={limit}&type={qtype}&format=application%2Fsolr%2Bjson&latest={latest}&fields={fields}&{'&'.join([f'{k}={v}' for k,v in facets.items()])}"
    else:
        url = f"https://{node}/esg-search/search/?offset={offset}&limit={limit}&type={qtype}&format=application%2Fsolr%2Bjson&latest={latest}&fields={fields}"

    # print(f"Executing URL: {url}")
    req = requests.get(url)
    # print(f"type req = {type(req)}")

    if req.status_code != 200:
        # print(f"ERROR: ESGF search request returned non-200 status code: {req.status_code}")
        return list(), 0

    numFound = req.json()["response"]["numFound"]

    docs = [
        {k: v for k, v in doc.items()}
        for doc in req.json()["response"]["docs"]
    ]

    return docs, numFound

# ----------------------------------------------

def safe_search_esgf(
    facets,
    node="esgf-node.llnl.gov",
    qtype="Dataset",
    fields="*",
    latest="true",
):
    """
    Make a search request to an ESGF node and return information about the datasets that match the search parameters

    Parameters:
        project (str): The ESGF project to search inside
        facets (dict): A dict with keys of facets, and values of facet values to search
        node (str)   : The esgf index node to query
        qtype (str)  : The query type, one of "Dataset" (default), "File" or "Aggregate"
        fields (str) : a comma-separated string of metadata field names, default '*' MUST be overridden.
        latest (str) : boolean (true/false not True/False) to search for only the latest version of a dataset
    """

    # logmsg("    SSE: entered safe_search_esgf")

    full_docs = list()
    full_found = 0

    qlimit = 10000
    curr_offset = 0

    while True:
        # logmsg(f"    SSE: calling raw_search_esgf with curr_offset {curr_offset}")
        docs, numFound = raw_search_esgf(facets, offset=curr_offset, limit=f"{qlimit}", qtype=f"{qtype}", fields=f"{fields}", latest=f"{latest}")
        # logmsg(f"    SSE: raw_search returned {len(docs)} records")
        full_docs = full_docs + docs
        full_found = full_found + numFound
        if len(docs) < qlimit:
            # logmsg("    SSE: returning to caller")
            return full_docs, full_found
        curr_offset += qlimit
        # time.sleep(1)

    return full_docs, full_found

# ----------------------------------------------







# ==== Conduct ESGF Search Node Queries =======================

def collect_esgf_search_datasets(facets):

    logmsg(f"CESD: entered collect_esgf_search_datasets, facets = {facets}")
    logmsg(f"CESD: calling safe_search_esgf conditioned on facets")

    docs, numFound = safe_search_esgf(facets, qtype="Dataset", fields="id,title,instance_id,version,data_node,number_of_files")  # test if datanode made a difference

    if docs == None:
        print("ERROR: could not execute Dataset query")
        sys.exit(1)

    logmsg(f"CESD: docs returned ({len(docs)})")

    # print(f"Dataset Count: {len(docs)}")

    esgf_collected = dict()
    bad = 0
    for item in docs:
        ident = item["id"]
        esgf_collected[ident] = dict()
        esgf_collected[ident]["title"] = item["title"]                  # dsid, master_id
        esgf_collected[ident]["inst_id"] = item["instance_id"]          # dsid.vers
        esgf_collected[ident]["version"] = "v" + item["version"]
        esgf_collected[ident]["data_node"] = item["data_node"]
        esgf_collected[ident]["file_count"] = item["number_of_files"]

    total_file_count = 0

    for ident in esgf_collected:

        dataset_id_key = ident
        title = esgf_collected[ident]["title"]
        f_count = esgf_collected[ident]["file_count"]
        total_file_count += f_count

        # should pull "project" from dataset_id_key, if needed at all
        project = "e3sm"

        facets = {"project": f"{project}", "dataset_id": f"{dataset_id_key}"}

        docs, numFound = safe_search_esgf(facets, qtype="File", fields="title,url")     # title is filename here

        if not docs or len(docs) == 0:
            # print(f"PROBLEM: {ident}: No records returned.")  # check if datanode is the criteria
            continue
        if len(docs) != f_count:
            # print(f"PROBLEM: {ident}: MISMATCHED FILECOUNTS: Dataset = {f_count}, File = {len(docs)}")
            esgf_collected[ident]["file_count"] = str(len(docs))

        url_list = list()
        [ url_list.append(item["url"]) for item in docs ]
        esgf_collected[ident]["url"] = url_list[0][0]   # [0] for first file, [0] for the HTTP URL.

        '''
        file_list = list()
        [ file_list.append(item["title"]) for item in docs ]
        # print(f"  First:  ({ident}): {file_list[0]}")
        # print(f"  Final:  ({ident}): {file_list[-1]}")
        esgf_collected[ident]["first_file"] = file_list[0]
        esgf_collected[ident]["final_file"] = file_list[-1]
        '''

    # print(f"Total files reported: {total_file_count}")

    return esgf_collected





def main():

    args = assess_args()
    unrestricted = args.unrestricted
    project = args.project
    if project not in [ 'E3SM', 'CMIP6' ]:
        logmsg("Error: Project must be either 'E3SM' or 'CMIP6'.")
        sys.exit(0)
    if project == 'E3SM':
        project = project.lower()

    facets = { "project": project }
    esgf_report = collect_esgf_search_datasets(facets)

    for dsid_key in esgf_report:
        dsid = esgf_report[dsid_key]["title"]
        vers = esgf_report[dsid_key]["version"]
        filecount = esgf_report[dsid_key]["file_count"]
        url = esgf_report[dsid_key]["url"]

        # print(f"DSID={dsid}, VERS={vers}, FILECOUNT={filecount}")
        print(f"DSID={dsid}: URL={url}")





    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())


