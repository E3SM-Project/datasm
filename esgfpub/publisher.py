import os
import stat
import pexpect
import getpass
from os import remove
from time import sleep
from subprocess import Popen, PIPE, CalledProcessError
from tempfile import NamedTemporaryFile
from esgfpub.util import print_message


def execute(cmd):
    proc = Popen(cmd, shell=True, stdout=PIPE,
                 stderr=PIPE, universal_newlines=True)
    while proc.poll() is None:
        line = proc.stdout.readline()
        if line != "":
            print_message(line, 'ok')

        line = proc.stderr.readline()
        if line != "" and 'psycopg2' not in line and ' """)' not in line:
            print_message(line)
    if proc.returncode:
        raise CalledProcessError(proc.returncode)


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
            print_message(cmd, 'info')
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

        tempfile = NamedTemporaryFile(delete=False)

        with open(tempfile.name, 'w') as fp:
            fp.write(script)
        st = os.stat(tempfile.name)
        os.chmod(tempfile.name, st.st_mode | stat.S_IEXEC)

        if debug:
            print_message('Running publication script: {}'.format(
                tempfile.name), 'info')
            print_message(script, 'info')

        try:
            cmd = ['bash', tempfile.name]
            try:
                execute(cmd)
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
                        "Publication success, moving {} to ".format(m, mapsout), "info")
                os.rename(
                    os.path.join(mapsin, m),
                    os.path.join(mapsout, m))
        finally:
            if not fp.closed:
                fp.close()


def publish(mapsin, mapsout, mapserr, ini, loop, username, debug=False):

    # password = getpass.getpass("Please enter my-proxy logon password: ")
    password = 'Ab1-B4sCkxaW'
    if loop:
        if debug:
            print_message("Entering publication loop", "info")
        while True:
            mapfiles = os.listdir(mapsin)
            if not mapfiles:
                sleep(30)
            else:
                publish_maps(mapfiles, ini, mapsin, mapsout,
                             mapserr, username, password, debug)
    else:
        mapfiles = os.listdir(mapsin)
        publish_maps(mapfiles, ini, mapsin, mapsout,
                     mapserr, username, password, debug)
    return 0
