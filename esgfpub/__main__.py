"""
A tool for automating much of the ESGF publication process
"""
from sys import exit as sysexit
from esgfpub.checker import data_check
from esgfpub.publisher import publish
from esgfpub.stager import stage
from esgfpub.util import parse_args


def main():

    ARGS = parse_args()
    subcommand = ARGS.subparser_name
    if subcommand == 'check':
        return data_check(
            spec_path=ARGS.case_spec,
            data_path=ARGS.data_path,
            cases=ARGS.cases,
            ens=ARGS.ens,
            tables=ARGS.tables,
            variables=ARGS.variables,
            published=ARGS.published,
            verify=ARGS.verify,
            model_versions=ARGS.model_versions,
            sproket=ARGS.sproket,
            max_connections=ARGS.max_connections,
            serial=ARGS.serial,
            debug=ARGS.debug,
            dataset_ids=ARGS.dataset_ids,
            projects=ARGS.project,
            to_json=ARGS.to_json)
    elif subcommand == 'stage':
        return stage(ARGS)
    elif subcommand == 'publish':
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
