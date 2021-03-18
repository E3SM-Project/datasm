from esgcet.pub_client import publisherClient
import sys, json, requests
import argparse
import configparser as cfg
from datetime import datetime
from pathlib import Path


DEFAULT_INDEX_NODE = "esgf-node.llnl.gov"
DEFAULT_DATA__NODE = "esgf-data2.llnl.gov"

def gen_xml(dataset_id, datatype, facets):
    now = datetime.utcnow()
    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    idfield = "id"
    if datatype == "files":
        idfield = "dataset_id"
    txt = f"""<updates core="{datatype}" action="set">
        <update>
          <query>{idfield}={dataset_id}</query>
          <field name="_timestamp">
             <value>{ts}</value>
          </field>
          """

    # import ipdb; ipdb.set_trace()
    for key, value in facets.items():
        txt += f"""<field name="{key}">
                       <value>{value}</value>
                   </field>
        """

    txt += f"""
        </update>
    </updates>"""

    return txt


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f', '--facets',
        required=True,
        nargs="*",
        help="Space sepparated key/value pairs for facets to get updated, in the form key=value")
    parser.add_argument(
        '-c', '--cert',
        required=True,
        help="Path to ESGF cert")
    parser.add_argument(
        '-d', '--dataset-id',
        required=True,
        help="Dataset-id to be updated, this should be the whole dataset_id with the version number at the end")
    parser.add_argument(
        '--index-node',
        default=DEFAULT_INDEX_NODE,
        help=f"Path to ESGF cert, default={DEFAULT_INDEX_NODE}")
    parser.add_argument(
        '--data-node',
        default=DEFAULT_DATA__NODE,
        help=f"Path to ESGF cert, default={DEFAULT_DATA__NODE}")
    parser.add_argument(
        '--verbose',
        action="store_true",
        help="Print more verbose status messages")

    args = parser.parse_args()

    cert_path = args.cert
    dataset_id = args.dataset_id
    # trim off the version number to get the master id
    master_id = '.'.join(dataset_id.split('.')[:-1])
    index_node = args.index_node
    data_node = args.data_node
    verbose = args.verbose
    
    facets = {}
    for item in args.facets:
        key, value = item.split('=')
        facets[key] = value

    url = f'https://{index_node}/esg-search/search/?offset=0&limit=1&type=Dataset&format=application%2Fsolr%2Bjson&latest=true&query=*&master_id={master_id}'
    if verbose:
        print(url)
    res = requests.get(url)

    if verbose:
        print(res.text)
    if not res.status_code == 200:
        print('Error', file=sys.stderr)
        return 1

    res = json.loads(res.text)
    
    docs = res['response']["docs"]
    if docs:
        dataset_id = docs[0]['id']
        client = publisherClient(cert_path, index_node)
        update_record = gen_xml(dataset_id, "datasets", facets)
        if verbose:
            print(update_record)
        client.update(update_record)
        update_record = gen_xml(dataset_id, "files", facets)
        if verbose:
            print(update_record)
        client.update(update_record)
    else:
        print(f"Unable to find recods for dataset {dataseta_id}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())