import os
import stat
import pexpect
import getpass
import json
from os import remove
from time import sleep
from subprocess import check_call, CalledProcessError
from esgfpub.util import print_message, check_ds_exists
from datetime import datetime


def publish_maps(mapfiles, ini, mapsin, mapsout, mapserr, username=None, password=None, sproket='spoket', debug=False):
    for m in mapfiles:
        if debug:
            print_message(f'Starting mapfile: {m}', 'info')
        if m[-4:] != '.map':
            msg = "Unrecognized file type, this doesnt appear to be an ESGF mapfile. Moving to the err directory {}".format(m)
            print_message(msg)
            os.rename(
                os.path.join(mapsin, m),
                os.path.join(mapserr, m))
            continue
        if check_ds_exists(m[:-4], debug=debug, sproket=sproket):
            msg = f"Dataset {m[:-4]} already exists"
            print_message(msg, 'err')
            os.rename(
                os.path.join(mapsin, m),
                os.path.join(mapserr, m))
            continue
        if m[:5] == 'CMIP6':
            project = 'cmip6'
        elif m[:4] == 'E3SM':
            project = 'e3sm'
        else:
            raise ValueError(
                "Unrecognized project name for mapfile: {}".format(m))

        if debug:
            print_message("Running myproxy-logon with stored credentials", 'info')

        script = f"""#!/bin/sh
source /usr/local/conda/bin/activate esgf-pub
echo {password} | myproxyclient logon -S -s esgf-node.llnl.gov -l {username} -t 72 -o ~/.globus/certificate-file"""

        tempfile = "login.sh"
        if os.path.exists(tempfile):
            os.remove(tempfile)

        with open(tempfile, 'w') as fp:
            fp.write(script)
        st = os.stat(tempfile)
        os.chmod(tempfile, st.st_mode | stat.S_IEXEC)
        try:
            check_call('./' + tempfile)
        except CalledProcessError as error:
            print_message("Error while creating myproxy-logon certificate")
            return error.returncode
        os.remove(tempfile)
        map_path = os.path.join(mapsin, m)
        script = f"""#!/bin/sh
source /usr/local/conda/bin/activate esgf-pub
esgpublish -i {ini} --project {project} --map {map_path} --no-thredds-reinit --commit-every 100
if [ $? -ne  0 ]; then exit $?; fi
esgpublish -i {ini} --project {project} --map {map_path} --service fileservice --noscan --thredds  --no-thredds-reinit
if [ $? -ne  0 ]; then exit $?; fi
esgpublish --project {project} --thredds-reinit
esgpublish -i {ini} --project {project} --map {map_path} --service fileservice --noscan --publish
if [ $? -ne  0 ]; then exit $?; fi
"""

        tempfile = "pub_script.sh"
        if os.path.exists(tempfile):
            os.remove(tempfile)

        with open(tempfile, 'w') as fp:
            fp.write(script)
        st = os.stat(tempfile)
        os.chmod(tempfile, st.st_mode | stat.S_IEXEC)

        if debug:
            print_message(f'Running publication script: {tempfile}', 'info')
            print_message(script, 'info')

        try:
            start = datetime.now()
            check_call('./' + tempfile)
            end = datetime.now()
        except  CalledProcessError as error:
            print_message(
                f"Error in publication, moving {m} to {mapserr}", "error")
            os.rename(
                os.path.join(mapsin, m),
                os.path.join(mapserr, m))
        else:
            print_message(
                f"Publication success, runtime: {end - start}", "info")
            os.rename(
                os.path.join(mapsin, m),
                os.path.join(mapsout, m))



def publish(mapsin, mapsout, mapserr, ini, loop, sproket='sproket', cred_file=None, debug=False):

    if not os.path.exists(cred_file):
        raise ValueError('The given credential file does not exist')

    if cred_file:
        with open(cred_file, 'r') as ip:
            creds = json.load(ip)
            try:
                username = creds['username']
            except:
                raise ValueError("Missing username from credetial file")
            try:
                password = creds['password']
            except:
                raise ValueError("Missing password from credential file")
    else:
        username = None
        password = None

    if loop:
        print_message("Starting publisher loop", 'ok')
    else:
        print_message("Starting one-off publisher", 'ok')
    while True:
        mapfiles = [x for x in os.listdir(mapsin) if x.endswith('.map')]
        if mapfiles:
            publish_maps(mapfiles, ini, mapsin, mapsout,
                        mapserr, username, password, 
                        debug=debug, sproket=sproket)
        if not loop:
            break
        sleep(30)

    return 0
