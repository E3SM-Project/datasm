from datetime import datetime
from warehouse.util import print_file_list
from warehouse import (
    parse_args,
    get_ensemble_dirs,
    load_ds_status_list,
    produce_status_listing_vcounts 
)

allowed_modes = ['all', 'empty', 'nonempty']

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

def add_report_args(parser):
    
    report_parser = parser.add_parser(
        name='report',
        help="Print out a report of the dataset status for all datasets under the given root")
    report_parser.add_argument(
        '-t', '--target',
        help=f"What status files should be targetted, allowed values are: {', '.join(allowed_modes)}. default is all",
        default='all')
    report_parser.add_argument(
        '-r', '--root',
        help="Path to warehouse root directory, default is '/p/user_pub/e3sm/warehouse/E3SM'",
        default='/p/user_pub/e3sm/warehouse/E3SM')
    report_parser.add_argument(
        '-e', '--ensemble',
        action="store_true",
        help="Print which ensemble member the status belongs to, default is True",
        default=False)
    report_parser.add_argument(
        '-p', '--paths',
        action="store_true",
        help="Write out a file containing all the dataset paths",
        default=False)
    return 'report', report_parser

def check_report_args(args):
    if args.target not in allowed_modes:
        print(
            f"ERROR: {args.target} is not of the allowed values: {', '.join(allowed_modes)} may be specified. Default is all.")
        return 'report'
    return True