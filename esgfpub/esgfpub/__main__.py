"""
A tool for automating much of the ESGF publication process
"""
import warnings
warnings.simplefilter('ignore')
from shutil import rmtree
from esgfpub.util import parse_args
import os
from sys import exit as sysexit


def main():

    ARGS = parse_args()
    subcommand = ARGS.subparser_name
    if subcommand == 'check':
        
        from esgfpub.checker import data_check
        return data_check(**vars(ARGS))
    elif subcommand == 'stage':
        
        from esgfpub.stager import stage
        return stage(ARGS)
    elif subcommand == 'publish':
        
        from esgfpub.publisher import publish
        return publish(
            mapsin=ARGS.maps_in,
            mapsout=ARGS.maps_done,
            mapserr=ARGS.maps_err,
            loop=ARGS.loop,
            sproket=ARGS.sproket,
            logpath=ARGS.logs,
            debug=ARGS.debug)
    elif subcommand == 'custom':
        
        from esgfpub.custom_facets import update_custom
        update_custom(
            facets=ARGS.facets,
            datadir=ARGS.datadir,
            dataset_ids=ARGS.dataset_ids,
            debug=ARGS.debug)
    else:
        raise ValueError("Unrecognized subcommand")


if __name__ == "__main__":
    sysexit(main())
