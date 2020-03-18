"""
A tool for automating much of the ESGF publication process
"""
import warnings
warnings.simplefilter('ignore')
from esgfpub.util import parse_args
from sys import exit as sysexit


def main():

    ARGS = parse_args()
    subcommand = ARGS.subparser_name
    if subcommand == 'check':
        from esgfpub.checker import data_check
        return data_check(
            spec_path=ARGS.case_spec,
            data_path=ARGS.data_path,
            cases=ARGS.cases,
            ens=ARGS.ens,
            tables=ARGS.tables,
            variables=ARGS.variables,
            published=ARGS.published,
            verify=ARGS.verify,
            plot_path=ARGS.plot_path,
            model_versions=ARGS.model_versions,
            sproket=ARGS.sproket,
            only_plots=ARGS.only_plots,
            num_workers=ARGS.num_workers,
            serial=ARGS.serial,
            debug=ARGS.debug,
            exclude=ARGS.exclude,
            dataset_ids=ARGS.dataset_ids,
            projects=ARGS.project,
            cluster_address=ARGS.local_cluster,
            to_json=ARGS.to_json)
    elif subcommand == 'stage':
        from esgfpub.stager import stage
        return stage(ARGS)
    elif subcommand == 'publish':
        from esgfpub.publisher import publish
        return publish(
            mapsin=ARGS.maps_in,
            mapsout=ARGS.maps_done,
            mapserr=ARGS.maps_err,
            ini=ARGS.ini,
            loop=ARGS.loop,
            username=ARGS.username,
            debug=ARGS.debug)
    else:
        raise ValueError("Unrecognised subcommand")


if __name__ == "__main__":
    sysexit(main())
