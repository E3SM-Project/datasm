import sys
import argparse
from shutil import move
from pathlib import Path
from tempfile import NamedTemporaryFile
from warehouse.util import con_message


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish a dataset to ESGF")
    parser.add_argument(
        'mapfile_path',
        type=str,
        help="The mapfile that needs to have its values swapped out")
    parser.add_argument(
        'warehouse_base',
        type=str,
        help="The base warehouse path that needs to be removed")
    parser.add_argument(
        'pub_base',
        type=str,
        help="The base publication path that needs to be substituted")
    parser.add_argument(
        'warehouse_version',
        type=str,
        help="The name of the version directory used in the warehouse")
    parser.add_argument(
        'pub_version',
        type=str,
        help="The name of the publication version directory thats going to get published")
    return parser.parse_args()


def main():
    parsed_args = parse_args()

    con_message('info', 'Begin fix_mapfile_paths')
    mapfile_path = Path(parsed_args.mapfile_path)
    ware_base = parsed_args.warehouse_base
    pub_base = parsed_args.pub_base
    ware_version = "/" + parsed_args.warehouse_version + "/"
    pub_version = "/" + parsed_args.pub_version + "/"

    tempfile = NamedTemporaryFile(mode='w', delete=False, dir=str(mapfile_path.parent))
    with open(mapfile_path, 'r') as instream:
        for line in instream.readlines():
            items = line.split('|')
            items[1] = items[1].replace(ware_base, pub_base, 1)
            items[1] = items[1].replace(ware_version, pub_version, 1)
            line = '|'.join(items)
            tempfile.write(line)

    mapfile_temp = str(mapfile_path.resolve())
    mapfile_path.unlink()
    move(tempfile.name, mapfile_temp)
    con_message('info', f'Completed fix_mapfile_paths, mapfile={mapfile_temp}')

    return 0


if __name__ == "__main__":
    sys.exit(main())
