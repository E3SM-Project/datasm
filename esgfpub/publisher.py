import os
import stat
import json
import yaml
from os import remove
from time import sleep
from subprocess import Popen, PIPE
from esgfpub.util import print_message, check_ds_exists
from esgfpub import resources
from datetime import datetime
from tempfile import TemporaryDirectory


def get_facet_info(datasetID):
    ds_split =datasetID.split('.') 
    project = ds_split[0]
    if project != 'E3SM':
        print(f"Only able to load facet info from E3SM project datasets")
        return 0

    resource_path, _ = os.path.split(resources.__file__)
    spec_path = os.path.join(resource_path, 'dataset_spec.yaml')
    with open(spec_path, 'r') as ip:
        spec = yaml.safe_load(ip)
    
    model_version = ds_split[1]
    casename = ds_split[2]
    res = ds_split[3]

    try:
        casespec = spec['project'][project][model_version][casename]
    except IndexError as e:
        print(f"Does this experiment {casename} have the correct entry in the dataset spec?")
        raise e

    campaign = casespec.get('campaign')
    if not campaign:
        return None, None, None
    science_driver = casespec.get('science_driver')
    if not science_driver:
        return None, None, None
    period = f"{casespec['start']}-{casespec['end']}"
    return campaign, science_driver, period

def print_while_running(process):
    while not process.poll(): 
        yield process.stdout.readline()
    raise StopIteration


def publish_maps(mapfiles, mapsin, mapsout, mapserr, logpath, sproket='spoket', debug=False):
    os.makedirs(logpath, exist_ok=True)
    with TemporaryDirectory() as tmpdir:
        
        for m in mapfiles:
            if m[-4:] != '.map':
                msg = "Unrecognized file type, this doesnt appear to be an ESGF mapfile. Moving to the err directory {}".format(m)
                print_message(msg)
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapserr, m))
                continue

            print_message(f"Starting publication for {m}", 'ok')

            datasetID = m[:-4]
            project = datasetID.split('.')[0]
            if check_ds_exists(datasetID, debug=debug, sproket=sproket):
                msg = f"Dataset {datasetID} already exists"
                print_message(msg, 'err')
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapserr, m))
                continue
            if project == 'CMIP6':
                project = 'cmip6'
                project_metadata = None
            elif project == 'E3SM':
                campaign, driver, period = get_facet_info(datasetID)
                if campaign and driver and period:
                    project_metadata_path = os.path.join(tmpdir, f'{datasetID}.json')
                    project_metadata = {
                        'campaign': campaign,
                        'science_driver': driver,
                        'period': period
                    }
                    with open(project_metadata_path, 'w') as op:
                        json.dump(project_metadata, op)
            else:
                raise ValueError(
                    "Unrecognized project name for mapfile: {}".format(m))
            
            map_path = os.path.join(mapsin, m)
            cmd = f"esgpublish --project {project} --map {map_path}".split()
            if project_metadata:
                cmd.extend(['--json', project_metadata_path])
            
            print_message(f"Running: {' '.join(cmd)}", 'ok')
            log = os.path.join(logpath, f"{datasetID}.log")
            print_message(f"Writing publication log to {log}", 'ok')
            
            with open(log, 'w') as outstream:
                proc = Popen(cmd, stdout=outstream, stderr=outstream, universal_newlines=True)
                proc.wait()

            if proc.returncode != 0:
                if proc.stderr:
                    print(proc.stderr.readlines(), flush=True)
                print_message(
                    f"Error in publication, moving {m} to {mapserr}\n", "error")
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapserr, m))
            else:
                print_message(
                    f"Publication success, moving {m} to {mapsout}\n", "info")
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapsout, m))


def publish(mapsin, mapsout, mapserr, loop, logpath, sproket='sproket', debug=False):

    if loop:
        print_message("Starting publisher loop", 'ok')
    else:
        print_message("Starting one-off publisher", 'ok')
    while True:
        mapfiles = [x for x in os.listdir(mapsin) if x.endswith('.map')]
        if mapfiles:
            publish_maps(mapfiles, mapsin, mapsout,
                        mapserr, logpath, debug=debug, sproket=sproket)
        if not loop:
            break
        sleep(30)

    return 0
