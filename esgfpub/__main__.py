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
            ini=ARGS.ini,
            loop=ARGS.loop,
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

    if os.path.exists('dask-worker-space'):
        rmtree('dask-worker-space')


if __name__ == "__main__":
    sysexit(main())
