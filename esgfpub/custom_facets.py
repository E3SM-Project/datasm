import os
import stat
from tqdm import tqdm
from subprocess import call
from esgfpub.util import print_message, colors


def yield_leaf_dirs(folder):
    """Walk through every files in a directory"""
    for dirpath, dirs, files in tqdm(os.walk(folder), desc=colors.OKGREEN + '[+] ' + colors.ENDC + "Walking directory tree"):
        if dirs:
            continue
        if not files:
            continue
        yield dirpath


def collect_dataset_ids(data_path):
    dataset_ids = list()
    dirs = [x for x in yield_leaf_dirs(data_path)]
    for d in dirs:
        tail, head = os.path.split(d)
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
                output.append("{id} | {facets}\n".format(
                    id=dataset_id, facets=facet_str))
        if 'CMIP6' in output[0].split('|')[0]:
            project = 'cmip6'
        else:
            project = 'e3sm'
    else:
        if not datadir:
            raise ValueError(
                "If no mapfile directory is given, a datadir must be used")
        dataset_ids, project = collect_dataset_ids(datadir)
        for dataset in dataset_ids:
            output.append("{id} | {facets}\n".format(
                id=dataset, facets=facet_str))

    with open(outpath, 'w') as outfile:
        for line in output:
            if debug:
                print_message(line, 'info')
            outfile.write(line)

    return project


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
    facet_update_string = """#!/bin/sh
source /usr/local/conda/bin/activate esgf-pub
esgadd_facetvalues --project {project} --map {map} --noscan --thredds --service fileservice""".format(
        project=project, map=outpath)
    if debug:
        print_message(facet_update_string, 'info')
    update_script = './update_custom.sh'
    with open(update_script, 'w') as op:
        op.write(facet_update_string)
    st = os.stat(update_script)
    os.chmod(update_script, st.st_mode | stat.S_IEXEC)
    retcode = call(update_script)
    if retcode == 0:
        print_message("Custom facet update complete", 'ok')
        if debug:
            print_message("Cleaning up custom facet script", 'info')
        os.remove(update_script)
    else:
        print_message("Error during custom facet update")
    return retcode
    
