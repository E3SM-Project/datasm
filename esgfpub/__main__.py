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
            file_system=ARGS.file_system,
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
            to_json=ARGS.to_json,
            digest=ARGS.digest,
            data_types=ARGS.data_types)
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
            cred_file=ARGS.credentials,
            sproket=ARGS.sproket,
            debug=ARGS.debug)
    elif subcommand == 'custom':
        from esgfpub.custom_facets import update_custom
        update_custom(
            facets=ARGS.facets,
            outpath=ARGS.output,
            generate_only=ARGS.generate_only,
            mapdir=ARGS.mapdir,
            datadir=ARGS.datadir,
            debug=ARGS.debug)
    else:
        raise ValueError("Unrecognised subcommand")


if __name__ == "__main__":
    sysexit(main())
