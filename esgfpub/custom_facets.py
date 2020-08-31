import os
import stat
from tqdm import tqdm
from subprocess import Popen, PIPE
from esgfpub.util import print_message, colors


def yield_leaf_dirs(path):
    for dirpath, dirs, files in tqdm(os.walk(path), desc=f'{colors.OKGREEN}[+]{colors.ENDC} Walking directory tree'):
        if dirs:
            continue
        if not files:
            continue
        yield dirpath


def collect_dataset_ids(data_path):
    dataset_ids = list()
    if not os.path.exists(data_path):
        raise ValueError("Directory does not exist: {}".format(data_path))
    dirs = [x for x in yield_leaf_dirs(data_path)]
    for d in dirs:
        tail, _ = os.path.split(d)
        cmip = False
        if "CMIP6" in tail:
            cmip = True
            idx = tail.index('CMIP6')
        elif "E3SM" in tail:
            idx = tail.index('E3SM')
        else:
            raise ValueError(
                "This appears to be neither a CMIP6 or E3SM data directory: {}".format(tail))

        dataset_id = tail[idx:]
        dataset_id = dataset_id.replace(os.sep, '.')
        dataset_ids.append(dataset_id)

    if cmip:
        project = 'cmip6'
    else:
        project = 'e3sm'

    return dataset_ids, project


def generate_custom(facets, outpath='./custom_facets.map', mapdir=None, datadir=None, debug=False):
    """
    Create a custom facet mapfile, and returns the project that the datasets belong to
    
    Params:
        facets (list): a list of strings containing '=' seperated key, value pairs
        outpath (str): a path to where the new custom facet mapfile should be saved
        mapdir (str):  (optional) if supplied, will use a directory of esgf mapfiles as a source
        datadir (str): (optional) if supplied, will walk down the directory tree collecting dataset ids
        debug (bool): (optional) will print debug info
    Returns:
        str: the project (CMIP6/E3SM) the datasets belong to
    """
    for facet in facets:
        if facet.index('=') == -1:
            raise ValueError(
                'Facets must be in the form of facet_name=facet_value, {} does not have an "="'.format(facet))
    facet_str = " | ".join(facets)

    output = []
    if mapdir:
        maplist = [os.path.join(mapdir, f) for f in os.listdir(
            mapdir) if os.path.isfile(os.path.join(mapdir, f))]
        if debug:
            print_message("mapfiles:", 'info')
            for item in maplist:
                print_message('\t' + item, 'info')
        for m in maplist:
            with open(m, "r") as amaplines:
                aline = amaplines.readline()
                dataset_id = aline.split(' ')[0]
                hash_index = dataset_id.find('#')
                dataset_id = dataset_id[:hash_index]
                output.append(f"{dataset_id} | {facet_str}\n")
        if 'CMIP6' in output[0].split('|')[0]:
            project = 'cmip6'
        else:
            project = 'e3sm'
    else:
        if not datadir:
            raise ValueError(
                "If no mapfile directory is given, a datadir must be used")
        if isinstance(datadir, str):
            datadir = [datadir]
        for p in datadir:
            dataset_ids, project = collect_dataset_ids(p)
            for dataset in dataset_ids:
                output.append(f"{dataset_id} | {facet_str}\n")

    with open(outpath, 'w') as outfile:
        for line in output:
            if debug:
                print_message(line, 'info')
            outfile.write(line)

    return project

def run_cmd(command):    
    popen = Popen(command, stdout=PIPE)
    return iter(popen.stdout.readline, b"")

def update_custom(facets, outpath='./custom_facets.map', generate_only=False, mapdir=None, datadir=None, debug=False):

    print_message("Generating custom facet mapfile", 'ok')
    project = generate_custom(
        facets=facets,
        outpath=outpath,
        mapdir=mapdir,
        datadir=datadir,
        debug=debug)
    print_message("Mapfile generation complete", 'ok')

    if generate_only:
        return 0

    print_message("Sending custom facets to the ESGF node", 'ok')

    # render out a shell script so that the esgf-pub environment can be loaded
    facet_update_string = f"""#!/bin/sh
source /usr/local/conda/bin/activate esgf-pub
esgadd_facetvalues --project {project} --map {outpath} --noscan --thredds --service fileservice"""
    if debug:
        print_message(facet_update_string, 'info')
    update_script = 'update_custom.sh'
    with open(update_script, 'w') as op:
        op.write(facet_update_string)
    st = os.stat(update_script)
    os.chmod(update_script, st.st_mode | stat.S_IEXEC)

    proc = Popen(['./' + update_script], shell=True, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if debug:
        print_message(out)
        print_message(err)
    
    # the esgadd_facetvalues tool sends all of its output to stderr
    for line in err.split('\n'):
        if "Writing THREDDS catalog" in line:
            search_string = "/esg/content/thredds/esgcet/"
            idx = line.index(search_string)
            xml_path = line[idx + len(search_string):]
            cmd = f"""wget --no-check-certificate --ca-certificate ~/.globus/certificate-file --certificate ~/.globus/certificate-file --private-key ~/.globus/certificate-file --verbose --post-data="uri=https://aims3.llnl.gov/thredds/catalog/esgcet/{xml_path}&metadataRepositoryType=THREDDS" https://esgf-node.llnl.gov/esg-search/ws/harvest"""
            print(cmd)
            os.popen(cmd)
    return 0
    
