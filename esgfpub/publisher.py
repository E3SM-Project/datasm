import os
import stat
import pexpect
import getpass
import json
from os import remove
from time import sleep
from subprocess import call
from esgfpub.util import print_message


def publish_maps(mapfiles, ini, mapsin, mapsout, mapserr, username, password, debug=False):
    for m in mapfiles:
        if debug:
            print_message('Starting mapfile: {}'.format(m), 'info')
        if m[-4:] != '.map':
            continue
        if m[:5] == 'CMIP6':
            project = 'cmip6'
        elif m[:4] == 'E3SM':
            project = 'e3sm'
        else:
            raise ValueError(
                "Unrecognized project name for mapfile: {}".format(m))

        if debug:
            print_message('Creating myproxy certificate', 'info')
        cmd = 'myproxy-logon -s esgf-node.llnl.gov -l {} -t 72 -o ~/.globus/certificate-file'.format(
            username)
        if debug:
            print_message("Running myproxy-logon with stored credentials", 'info')
        proc = pexpect.spawn(cmd)
        proc.expect('Enter MyProxy pass phrase:')
        proc.sendline(password)

        script = """#!/bin/sh
source /usr/local/conda/bin/activate esgf-pub
esgpublish -i {ini} --project {project} --map {map} --commit-every 100 --no-thredds-reinit
esgpublish -i {ini} --project {project} --map {map} --service fileservice --noscan --thredds  --no-thredds-reinit
esgpublish --project {project} --thredds-reinit
esgpublish -i {ini} --project {project} --map {map} --service fileservice --noscan --publish
""".format(
            ini=ini, map=os.path.join(mapsin, m), project=project, username=username, password=password)

        tempfile = "pub_script.sh"
        if os.path.exists(tempfile):
            os.remove(tempfile)

        with open(tempfile, 'w') as fp:
            fp.write(script)
        st = os.stat(tempfile)
        os.chmod(tempfile, st.st_mode | stat.S_IEXEC)

        if debug:
            print_message('Running publication script: {}'.format(
                tempfile), 'info')
            print_message(script, 'info')

        try:
            try:
                call('./' + tempfile)
            except Exception as e:
                print_message(
                    "Error in publication, moving {} to {}".format(m, mapserr))
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapserr, m))
                raise(e)
            else:
                if debug:
                    print_message(
                        "Publication success, moving {} to {}".format(m, mapsout), "info")
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapsout, m))
        finally:
            if not fp.closed:
                fp.close()


def publish(mapsin, mapsout, mapserr, ini, loop, cred_file, debug=False):

    if not os.path.exists(cred_file):
        raise ValueError('The given credential file does not exist')

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

    while True:
        mapfiles = [x for x in os.listdir(mapsin) if x.endsWith('.map')]
        publish_maps(mapfiles, ini, mapsin, mapsout,
                     mapserr, username, password, debug)
        if not loop:
            break
        sleep(30)

    return 0
