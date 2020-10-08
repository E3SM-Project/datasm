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

    casespec = [x for x in spec['project'][project][model_version] if x['experiment'] == casename].pop()
    campaign = casespec['Campaign']
    science_driver = casespec['Science Driver']
    period = f"{casespec['start']}-{casespec['end']}"
    return campaign, science_driver, period

def print_while_running(process):
    for line in iter(process.stdout.readline, ""):
        if process.poll():
            raise StopIteration
        yield line#.decode('utf-8')


def publish_maps(mapfiles, mapsin, mapsout, mapserr, sproket='spoket', debug=False):
    with TemporaryDirectory() as tmpdir:
        
        for m in mapfiles:
            if m[-4:] != '.map':
                msg = "Unrecognized file type, this doesnt appear to be an ESGF mapfile. Moving to the err directory {}".format(m)
                print_message(msg)
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapserr, m))
                continue

            print(f"Starting publication for {m}")

            datasetID = m[:-4]
            projectID = datasetID.split('.')[0]
            if check_ds_exists(datasetID, debug=debug, sproket=sproket):
                msg = f"Dataset {datasetID} already exists"
                print_message(msg, 'err')
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapserr, m))
                continue
            if projectID == 'CMIP6':
                project = 'cmip6'
                project_metadata = None
            elif projectID == 'E3SM':
                project = 'e3sm'
                campaign, driver, period = get_facet_info(datasetID)
                project_metadata_path = os.path.join(tmpdir, f'{datasetID}.json')
                project_metadata = {
                    'Campaign': campaign,
                    'Science Driver': driver,
                    'Period': period
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
            print(f"Running: {' '.join(cmd)}")
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            for line in print_while_running(proc):
                print(line, end='')
            proc.stdout.close()
            proc.wait()

            if proc.returncode != 0:
                print(proc.stderr.readlines())
                print_message(
                    f"Error in publication, moving {m} to {mapserr}", "error")
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapserr, m))
            else:
                print_message(
                    f"Publication success, moving {m} to {mapsout}", "info")
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapsout, m))
        


def publish(mapsin, mapsout, mapserr, loop, sproket='sproket', debug=False):

    # if not os.path.exists(cred_file):
    #     raise ValueError('The given credential file does not exist')

    # if cred_file:
    #     with open(cred_file, 'r') as ip:
    #         creds = json.load(ip)
    #         try:
    #             username = creds['username']
    #         except:
    #             raise ValueError("Missing username from credetial file")
    #         try:
    #             password = creds['password']
    #         except:
    #             raise ValueError("Missing password from credential file")
    # else:
    #     username = None
    #     password = None

    if loop:
        print_message("Starting publisher loop", 'ok')
    else:
        print_message("Starting one-off publisher", 'ok')
    while True:
        mapfiles = [x for x in os.listdir(mapsin) if x.endswith('.map')]
        if mapfiles:
            publish_maps(mapfiles, mapsin, mapsout,
                        mapserr, debug=debug, sproket=sproket)
        if not loop:
            break
        sleep(30)

    return 0
