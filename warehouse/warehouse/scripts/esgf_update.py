from esgcet.pub_client import publisherClient
import sys
import json
import requests
import argparse
import configparser as cfg
from datetime import datetime
from pathlib import Path
from tqdm import tqdm


DEFAULT_INDEX_NODE = "esgf-node.llnl.gov"
DEFAULT_DATA__NODE = "esgf-data2.llnl.gov"


def gen_xml(dataset_id, datatype, facets):
    now = datetime.utcnow()
    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    id_field = "id" if datatype != "files" else "dataset_id"
    txt = f"""
    <updates core="{datatype}" action="set">
        <update>
            <query>{id_field}={dataset_id}</query>
            <field name="_timestamp">
                <value>{ts}</value>
            </field>
          """

    for key, value in facets.items():
        txt += f"""
            <field name="{key}">
                <value>{value}</value>
            </field>"""

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
        '-s', '--search',
        required=True,
        help="Search criteria, for example master_id=<dataset-id>, or experiment=1950-Control&model_version=1_0")
    parser.add_argument(
        '--index-node',
        default=DEFAULT_INDEX_NODE,
        help=f"Path to ESGF cert, default={DEFAULT_INDEX_NODE}")
    parser.add_argument(
        '--data-node',
        default=DEFAULT_DATA__NODE,
        help=f"Path to ESGF cert, default={DEFAULT_DATA__NODE}")
    parser.add_argument(
        '-y', '--yes',
        action="store_true",
        help="skip the manual verification")
    parser.add_argument(
        '--verbose',
        action="store_true",
        help="Print more verbose status messages")

    args = parser.parse_args()

    cert_path = args.cert
    search = args.search
    index_node = args.index_node
    data_node = args.data_node
    verbose = args.verbose

    facets = {}
    for item in args.facets:
        key, value = item.split('=')
        facets[key] = value

    url = f'https://{index_node}/esg-search/search/?offset=0&limit=10000&type=Dataset&format=application%2Fsolr%2Bjson&latest=true&{search}'
    if verbose:
        print(url)
    res = requests.get(url)

    if not res.status_code == 200:
        print('Error', file=sys.stderr)
        return 1

    res = json.loads(res.text)

    docs = res['response']["docs"]
    if len(docs) == 0:
        print(f"Unable to find records matching search {search}")
        return 1

    print("Found the following datasets:")
    for doc in docs:
        print(f"\t{doc['id']}")

    if not args.yes:
        response = input(
            f"Found {len(docs)} datasets, would you like to update them all? y/[n]")
        if response.lower() != 'y':
            print("User failed to answer 'y', exiting")
            return 1

    import warnings
    warnings.filterwarnings("ignore")

    client = publisherClient(cert_path, index_node)
    for doc in tqdm(docs):
        dataset_id = doc['id']
        update_record = gen_xml(dataset_id, "datasets", facets)
        if verbose:
            print(update_record)
        client.update(update_record)
        update_record = gen_xml(dataset_id, "files", facets)
        if verbose:
            print(update_record)
        client.update(update_record)

    return 0


if __name__ == "__main__":
    sys.exit(main())
