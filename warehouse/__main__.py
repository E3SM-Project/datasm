import sys
from datetime import datetime
from warehouse.util import print_file_list
from warehouse import (
    parse_args,
    get_ensemble_dirs,
    load_ds_status_list,
    produce_status_listing_vcounts )


def report(args):
    ts=datetime.now().strftime('%Y%m%d_%H%M%S')
    ensem_out = f'warehouse_ensem-{ts}'
    paths_out = f'warehouse_paths-{ts}'
    stats_out = f'warehouse_status-{ts}'

    ensembles = get_ensemble_dirs(
        warehouse_root=args.root,
        print_paths=args.paths,
        paths_out=paths_out)

    wh_datasets = load_ds_status_list(ensembles)
    status_list = produce_status_listing_vcounts(wh_datasets)
    print_file_list(stats_out, status_list)
    return 0


def main():
    args = parse_args()
    if not args:
        return -1
    subcommand = args.subparser_name
    if subcommand == 'report':
        return report(args)

if __name__ == "__main__":
  sys.exit(main())