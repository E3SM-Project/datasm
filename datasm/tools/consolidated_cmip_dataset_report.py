import os, sys
import argparse
import re
from argparse import RawTextHelpFormatter
import requests
import time
from datetime import datetime
import pytz
import yaml

'''
The Big Idea:  Create a dictionary "ds_struct[]" keyed by dataset_ID, whose values will be the desired
output fields of the report:

    Campaign, Model, Experiment, Resolution, Ensemble, DatasetType, Realm, Grid, Freq, DAWPS, D, A, W, P, S,
        StatDate, Status, W_Version, W_Count, P_Version, P_Count, S_Version, S_Count, W_Path, P_Path

For each of these 5 sources of dataset_IDs:

    {dataset_spec, archive_map, warehouse_dirs, publication_dirs, sproket_esgf_search}

construct a list of all obtainable dataset_IDs. For each dataset_ID in the list, seek that entry in the
ds_struct. Update the entry (adding new if not found) with data appropriate to the section being processed.
'''

helptext = '''
    Usage:  consolidated_cmip_dataset_report [--unrestricted]

        The report is produced by plying 5 sources to determine whether datasets exist
        in any of (DatasetSpec,Archive,Warehouse,PubDirs,(ESGF)SearchNode), hereafter (D,A,W,P,S).

        Existence is determined by:

        (D):  Appearance in the E3SM Dataset_Spec (/p/user_pub/e3sm/staging/resource/dataset_spec.yaml)
        (A):  Appearance in the Archive_Map (/p/user_pub/e3sm/archive/.cfg/Archive_Map)
        (W):  The Warehouse directories (/p/user_pub/e3sm/warehouse/E3SM/(facets)/[v0|v1...]/
        (P):  The Publication directories (/p/user_pub/work/E3SM/(facets)/[v0|v1...]/
        (S):  The (ESGF) Search node (determined by live https request queries to esgf-node.llnl.gov)

    if [--unrestricted] is specified, datasets will be included even if they do NOT appear in the Dataset_Spec.
'''

# INPUT FILES (These could be in a nice config somewhere.  staging/.paths
staging_paths = '/p/user_pub/e3sm/staging/.paths'

PB_ROOT = '/p/user_pub/work'
WH_ROOT = '/p/user_pub/e3sm/warehouse'
DS_SPEC = '/p/user_pub/e3sm/staging/resource/dataset_spec.yaml'
DS_STAT = '/p/user_pub/e3sm/staging/status'

ARCH_MAP  = '/p/user_pub/e3sm/archive/.cfg/Archive_Map'
esgf_pr   = ''

# output_mode
gv_csv = True

# esgf_pr   = '/p/user_pub/e3sm/bartoletti1/Pub_Status/sproket/ESGF_publication_report-20200915.144250'

def ts():
    return pytz.utc.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    optional.add_argument('--unrestricted', action='store_true', dest="unrestricted", required=False)

    args = parser.parse_args()
    return args

# Generic Convenience Functions =============================

def loadFileLines(afile):
    retlist = []
    if len(afile):
        with open(afile,"r") as f:
            retlist = f.read().split('\n')
        retlist = [ _ for _ in retlist if _[:-1] ]
    return retlist

# HTTP Request Functions ====================================

def raw_search_esgf(
    facets,
    offset="0",
    limit="50",
    node="esgf-node.llnl.gov",
    qtype="Dataset",
    fields="*",
    latest="true",
):
    """
    Make a search request to an ESGF node and return information about the datasets that match the search parameters

    Parameters:
        facets (dict): A dict with keys of facets, and values of facet values to search
        offset (str) : offset into available results to return data
        limit (str)  : number of results to return (default = 50, max = 10000)
        node (str)   : The esgf index node to query
        qtype (str)  : The query type, one of "Dataset" (default), "File" or "Aggregate"
        fields (str) : a comma-separated string of metadata field names, default '*' MUST be overridden.
        latest (str) : boolean (true/false not True/False) to search for only the latest version of a dataset
    """

    if fields == "*":
        print("ERROR: Must specify string of one or more CSV fieldnames with fields=string")
        return None

    if len(facets):
        url = f"https://{node}/esg-search/search/?offset={offset}&limit={limit}&type={qtype}&format=application%2Fsolr%2Bjson&latest={latest}&fields={fields}&{'&'.join([f'{k}={v}' for k,v in facets.items()])}"
    else:
        url = f"https://{node}/esg-search/search/?offset={offset}&limit={limit}&type={qtype}&format=application%2Fsolr%2Bjson&latest={latest}&fields={fields}"

    # print(f"DEBUG: Executing URL: {url}")
    req = requests.get(url)
    # print(f"type req = {type(req)}")

    if req.status_code != 200:
        # print(f"ERROR: ESGF search request returned non-200 status code: {req.status_code}")
        return list(), 0

    numFound = req.json()["response"]["numFound"]

    docs = [
        {k: v for k, v in doc.items()}
        for doc in req.json()["response"]["docs"]
    ]

    return docs, numFound

# ----------------------------------------------

def safe_search_esgf(
    facets,
    node="esgf-node.llnl.gov",
    qtype="Dataset",
    fields="*",
    latest="true",
):
    """
    Make a search request to an ESGF node and return information about the datasets that match the search parameters

    Parameters:
        project (str): The ESGF project to search inside
        facets (dict): A dict with keys of facets, and values of facet values to search
        node (str)   : The esgf index node to query
        qtype (str)  : The query type, one of "Dataset" (default), "File" or "Aggregate"
        fields (str) : a comma-separated string of metadata field names, default '*' MUST be overridden.
        latest (str) : boolean (true/false not True/False) to search for only the latest version of a dataset
    """

    full_docs = list()
    full_found = 0

    qlimit=10000
    curr_offset = 0

    while True:
        docs, numFound = raw_search_esgf(facets, offset=curr_offset, limit=f"{qlimit}", qtype=f"{qtype}", fields=f"{fields}", latest=f"{latest}")
        # print(f"DEBUG: raw_search returned {len(docs)} records")
        full_docs = full_docs + docs
        full_found = full_found + numFound
        if len(docs) < qlimit:
            return full_docs, full_found
        curr_offset += qlimit
        # time.sleep(1)

    return full_docs, full_found

# ----------------------------------------------


#### BEGIN rationalizing archive and publication experiment-case names, and dataset-type names ####
# dsid = proj.model.experiment.resolution[.tuning].realm.grid.outtype.freq.ens.ver
# SPECIAL CASES:  grid          out_type        freq    ->  realm_grid_freq
# climo:          

def get_dsid_dstype( dsid ):  # only works because "tuning" comes before realm, grid and frequency
    comps=dsid.split('.')
    realm = comps[-5]
    gridv = comps[-4]   # may be overloaded with "namefile" or "restart"
    otype = comps[-3]
    freq = comps[-2]    # will be "fixed" for gridv in "namefile" or "restart"

    if realm == 'atmos':
        realm = 'atm'
    elif realm == 'land':
        realm = 'lnd'
    elif realm == 'ocean':
        realm = 'ocn'

    grid = gridv
    if gridv == 'native':
        grid = 'nat'
    elif otype == 'monClim':    # should be "climo"
        freq = 'mon'
    elif otype == 'seasonClim': # should be "climo"
        freq = 'season'
    elif gridv == 'restart' or gridv == 'namefile' or gridv == 'namelist':
        freq = 'fixed'
    else:
        grid = gridv
    return '_'.join([realm,grid,freq])

#### COMPLETED rationalizing archive and publication experiment-case names, and dataset-type names ####

def clean_timestamp(ts):
    parts = len(ts.split(' '))
    if parts == 1:
        return ts
    if parts > 2:
        print(f"UNKNOWN FORMAT: {ts}")
        return ts

    dtpart, tmpart = ts.split(' ')
    dparts = len(dtpart.split('/'))
    if dparts != 3:
        print(f"UNKNOWN FORMAT: {ts}")
        return ts
    tparts1 = len(tmpart.split(':'))
    tparts2 = len(tmpart.split('.'))
    if not tparts1 == 3 and not (tparts2 == 3 or tparts2 == 4):
        print(f"UNKNOWN FORMAT: {ts}")
        return ts

    d_out = ''.join(dtpart.split('/'))

    if tparts1 == 3:
        t_out = ''.join(tmpart.split(':'))
    else:
        if tparts2 == 3:
            t_out = ''.join(tmpart.split('.'))
        else:
            t_out1 = ''.join(tmpart.split('.')[:3])
            t_out = '_'.join([t_out1, tmpart.split('.')[3]])

    new_ts = '_'.join([d_out, t_out])

    # print(f"{new_ts}")
    return new_ts


def isVLeaf(_):
    if len(_) > 1 and _[0] == 'v' and _[1] in '0123456789':
        return True
    return False

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def maxversion(vlist):
    nlist = list()
    for txt in vlist:
        if not is_number(txt[1:]):
            continue
        nlist.append(float(txt[1:]))
    if not nlist or len(nlist) == 0:
        return "vNONE"
    nlist.sort()
    retv = nlist[-1]
    if retv.is_integer():
        return f"v{int(retv)}"
    return f"v{retv}"


def get_maxv_info(edir):
    ''' given an ensemble directory, return the max "v#" subdirectory and its file count '''
    v_dict = dict()
    for root, dirs, files in os.walk(edir):
        if not dirs:
            v_dict[os.path.split(root)[1]] = len(files)
    maxv = maxversion(v_dict.keys())
    return maxv, v_dict[maxv]

def get_leaf_dirs_by_walk(rootpath,project):
    leaf_dirs = []
    seekpath = os.path.join(rootpath,project)
    for root, dirs, files in os.walk(seekpath):
        if not dirs:     # at leaf-directory
            leaf_dirs.append(root)
    return leaf_dirs

def get_dataset_path_tuples(rootpath):
    # leaf_dirs = get_leaf_dirs_by_walk(rootpath,'E3SM')
    leaf_dirs = get_leaf_dirs_by_walk(rootpath,'CMIP6')
    path_tuples = list()
    for adir in leaf_dirs:
        ensdir, vleaf = os.path.split(adir)
        if not isVLeaf(vleaf):
            continue
        for root, dirs, files in os.walk(adir):
            if files:
                path_tuples.append( tuple([ensdir,vleaf,len(files)]))
            else:
                path_tuples.append( tuple([ensdir,vleaf,0]))
    return path_tuples
                
def bookend_files(adir):
    for root, dirs, files in os.walk(adir):
        if files:
            return sorted(files)[0], sorted(files)[-1]
    return "", ""

def get_statfile_path(dsid):
    s_path = os.path.join(DS_STAT,dsid + '.status')
    if os.path.exists(s_path):
        return s_path
    return ""

# sf_status = get_sf_laststat(epath)

def get_sf_laststat(dsid):
    sf_path = get_statfile_path(dsid)
    if sf_path == '':
        return ':NO_STATUS_FILE_PATH'
    sf_rawlist = loadFileLines(sf_path)
    sf_list = list()
    for aline in sf_rawlist:
        if aline.split(':')[0] != "STAT":
            continue
        sf_list.append(aline)
    if len(sf_list) == 0:
        return ':EMPTY_STATUS_FILE'
    sf_last = sf_list[-1]
    last_stat = ':'.join(sf_last.split(':')[1:])
    return last_stat

''' NEW STUFF ====================================================================================================

ds_struct[] will be keyed by FULL DSID.  The Values with be a dictionary of 

    (E3SM):  Campaign, Model, Experiment, Resolution, Ensemble, Output_Type, DatasetType, Realm, Grid, Freq, DAWPS, D, A, W, P, S, StatDate, Status, W_Version, W_Count, P_Version, P_Count, S_Version, S_Count, W_Path, P_Path, FirstFile, LastFile
    (CMIP6): Project, Activity, Institution, SourceID, Experiment, Variant, Frequency, Variable, Grid, DAWPS, D, A, W, P, S, StatDate, Status, W_Version, W_Count, P_Version, P_Count, S_Version, S_Count, W_Path, P_Path, FirstFile, LastFile

'''
 
# ==== new stuff ==== #

def new_ds_record():
    return { 'Project':'', 'Activity':'', 'Institution':'', 'SourceID':'', 'Experiment':'', 'Variant':'', 'Frequency':'', 'Variable':'', 'Grid':'', 'DAWPS':'', 'D':'_', 'A':'_', 'W':'_', 'P':'_', 'S':'_', 'StatDate':'', 'Status':'', \
        'W_Version':'', 'W_Count':0, 'P_Version':'', 'P_Count':0, 'S_Version':'', 'S_Count':0, 'W_Path':'', 'P_Path':'', 'FirstFile':'', 'LastFile':'' }

def realm_grid_freq_from_dstype(dstype):
    dstlist = dstype.split('_')
    rcode = dstlist[0]
    gcode = dstlist[1]
    if len(dstlist) == 3:
        freq = dstlist[2]
    else:
        freq = '_'.join([dstlist[2], dstlist[3]])

    if rcode == 'atm':
        realm = 'atmos'
    elif rcode == 'lnd':
        realm = 'land'
    elif rcode == 'ocn':
        realm = 'ocean'
    else:
        realm = rcode

    if gcode == 'nat':
        grid = 'native'
    else:
        grid = gcode

    return realm, grid, freq

def campaign_via_model_experiment(model,experiment):
    if model in ['1_0']:
        if experiment in ['1950-Control-HR','1950-Control-LR','1950-Control-LRtunedHR','1950-Control-21yrContHiVol-HR',\
            'F2010-HR','F2010-LR','F2010-LRtunedHR','F2010-nudgeUV-HR','F2010-nudgeUV-LR','F2010-nudgeUV-LRtunedHR',\
            'F2010-nudgeUV-1850aero-HR','F2010-nudgeUV-1850aero-LR','F2010-nudgeUV-1850aero-LRtunedHR',\
            'F2010-plus4k-HR','F2010-plus4k-LR','F2010-plus4k-LRtunedHR']:
            return 'HR-v1'
        return 'DECK-v1'
    elif model in ['1_1','1_1_ECA']:
        return 'BGC-v1'
    elif model in ['1_2','1_2_1','1_3']:
        return 'CRYO'
    elif model in ['2_0']:
        return 'DECK-v2'
    else:
        return "UNKNOWN_CAMPAIGN"

# ==== Conduct ESGF Search Node Queries =======================

def collect_esgf_search_datasets(facets):

    docs, numFound = safe_search_esgf(facets, qtype="Dataset", fields="id,title,instance_id,version,data_node,number_of_files")  # test if datanode made a difference

    if docs == None:
        print("ERROR: could not execute Dataset query")
        sys.exit(1)

    # print(f"Dataset Count: {len(docs)}")

    esgf_collected = dict()
    bad = 0
    for item in docs:
        ident = item["id"]
        esgf_collected[ident] = dict()
        esgf_collected[ident]["title"] = item["title"]                  # dsid, master_id
        esgf_collected[ident]["inst_id"] = item["instance_id"]          # dsid.vers
        esgf_collected[ident]["version"] = "v" + item["version"]
        esgf_collected[ident]["data_node"] = item["data_node"]
        esgf_collected[ident]["file_count"] = item["number_of_files"]

    total_file_count = 0

    for ident in esgf_collected:

        dataset_id_key = ident
        title = esgf_collected[ident]["title"]
        f_count = esgf_collected[ident]["file_count"]
        total_file_count += f_count

        # should pull "project" from dataset_id_key, if needed at all
        project = "CMIP6"

        facets = {"project": f"{project}", "dataset_id": f"{dataset_id_key}"}

        docs, numFound = safe_search_esgf(facets, qtype="File", fields="title")     # title is filename here

        if not docs or len(docs) == 0:
            # print(f"PROBLEM: {ident}: No records returned.")  # check if datanode is the criteria
            continue
        if len(docs) != f_count:
            # print(f"PROBLEM: {ident}: MISMATCHED FILECOUNTS: Dataset = {f_count}, File = {len(docs)}")
            esgf_collected[ident]["file_count"] = str(len(docs))

        file_list = list()
        [ file_list.append(item["title"]) for item in docs ]
        # print(f"  First:  ({ident}): {file_list[0]}")
        # print(f"  Final:  ({ident}): {file_list[-1]}")
        esgf_collected[ident]["first_file"] = file_list[0]
        esgf_collected[ident]["final_file"] = file_list[-1]

    # print(f"Total files reported: {total_file_count}")

    return esgf_collected



# ==== generate dsids from dataset spec

def collect_cmip_datasets(dataset_spec):
    for activity_name, activity_val in dataset_spec['project']['CMIP6'].items():
        for version_name, version_value in activity_val.items():
            for experimentname, experimentvalue in version_value.items():
                for ensemble in experimentvalue['ens']:
                    for table_name, table_value in dataset_spec['tables'].items():
                        for variable in table_value:
                            if variable in experimentvalue['except'] or table_name in experimentvalue['except']:
                                continue
                            dataset_id = f"CMIP6.{activity_name}.E3SM-Project.{version_name}.{experimentname}.{ensemble}.{table_name}.{variable}.gr"
                            yield dataset_id


def dsids_from_dataset_spec(dataset_spec_path):
    with open(dataset_spec_path, 'r') as instream:
        dataset_spec = yaml.load(instream, Loader=yaml.SafeLoader)
        cmip6_ids = [x for x in collect_cmip_datasets(dataset_spec)]
        # e3sm_ids = [x for x in collect_e3sm_datasets(dataset_spec)]
        # dataset_ids = cmip6_ids + e3sm_ids

    return cmip6_ids
    # return dataset_ids



# DSID = PROJ.Model.Exper.Resol.[tuning.]Realm.Grid.OutType.Freq.Ens

def dsid_from_archive_map(amline):
    #   Campaign,Model,Experiment,Resolution,Ensemble,DatasetType,ArchivePath,DatatypeTarExtractionPattern,Notes
    amap_items = amline.split(',')
    realm, grid, freq = realm_grid_freq_from_dstype(amap_items[5])
    tuning = 0
    if amap_items[2][0:5] == 'F2010':
        tuning = 1
    if amap_items[2][0:12] == '1950-Control' and not amap_items[2] == '1950-Control-21yrContHiVol-HR':
        amap_items[2] = '1950-Control'
    dsid_items = list()
    dsid_items.append('E3SM')
    dsid_items.append(amap_items[1]) # model
    dsid_items.append(amap_items[2]) # exper
    dsid_items.append(amap_items[3]) # resol
    if tuning:
        dsid_items.append('tuning')     # should not happen from archive_map
    dsid_items.append(realm)
    dsid_items.append(grid)
    dsid_items.append('model-output')
    dsid_items.append(freq)
    dsid_items.append(amap_items[4]) # ensem
        
    dsid = '.'.join(dsid_items)
    return dsid

    # CMIP6.C4MIP.E3SM-Project.E3SM-1-1-ECA.hist-bgc.r1i1p1f1.3hr.pr_highfreq.gr
    # project.activity.institution.sourceid.experiment.variant.freq.variable.grid

def dict_from_dsid(dsid):
    # print(f"dict_from_dsid: {dsid}")
    dsiddict = dict()
    dsidlist = dsid.split('.')
    dsiddict['Project'] = dsidlist[0]
    dsiddict['Activity'] = dsidlist[1]
    dsiddict['Institution'] = dsidlist[2]
    dsiddict['SourceID'] = dsidlist[3]          # ~model
    dsiddict['Experiment'] = dsidlist[4]
    dsiddict['Variant'] = dsidlist[5]
    dsiddict['Frequency'] = dsidlist[6]
    dsiddict['Variable'] = dsidlist[7]
    dsiddict['Grid'] = dsidlist[8]

    return dsiddict

        
def dsids_from_archive_map(arch_map):
    # split the Archive_Map into a list of records, each record a list of fields
    #   Campaign,Model,Experiment,Resolution,Ensemble,DatasetType,ArchivePath,DatatypeTarExtractionPattern,Notes
    contents = loadFileLines(arch_map)
    am_list = [ aline.split(',') for aline in contents if aline[:-1] ]
    dsid_list = list()
    for am_line in am_list:
        dsid_list.append(dsid_from_archive_map(am_line))

    return dsid_list


def dsid_from_warehouse_path(whpath):
    return '.'.join(whpath.split(os.sep)[5:])

def dsid_from_publication_path(pbpath):
    return '.'.join(pbpath.split(os.sep)[4:])

def init_ds_record_from_dsid(dsrec,dsid):
    dsiddict = dict_from_dsid(dsid)
    for key in dsiddict.keys():
        dsrec[key] = dsiddict[key]
    # dsrec['datasettype'] = get_dsid_dstype(dsid)

def dumplist(alist):
    for item in alist:
        print(f'DUMPING: {item}', flush = True)

def report_ds_struct(ds_struct):
    out_line = 'Project,Activity,Institution,SourceID,Experiment,Variant,Frequency,Variable,Grid,DAWPS,D,A,W,P,S,StatDate,Status,W_Version,W_Count,P_Version,P_Count,S_Version,S_Count,W_Path,P_Path,FirstFile,LastFile'
    print(f'{out_line}')
        
    for dsid in ds_struct:
        ds = ds_struct[dsid]
        out_list = []
        out_list.append(ds['Project'])
        out_list.append(ds['Activity'])
        out_list.append(ds['Institution'])
        out_list.append(ds['SourceID'])
        out_list.append(ds['Experiment'])
        out_list.append(ds['Variant'])
        out_list.append(ds['Frequency'])
        out_list.append(ds['Variable'])
        out_list.append(ds['Grid'])
        out_list.append(ds['DAWPS'])
        out_list.append(ds['D'])
        out_list.append(ds['A'])
        out_list.append(ds['W'])
        out_list.append(ds['P'])
        out_list.append(ds['S'])
        out_list.append(ds['StatDate'])
        out_list.append(ds['Status'])
        out_list.append(ds['W_Version'])
        out_list.append(str(ds['W_Count']))
        out_list.append(ds['P_Version'])
        out_list.append(str(ds['P_Count']))
        out_list.append(ds['S_Version'])
        out_list.append(str(ds['S_Count']))
        out_list.append(ds['W_Path'])
        out_list.append(ds['P_Path'])
        out_list.append(ds['FirstFile'])
        out_list.append(ds['LastFile'])
        out_line = ','.join(out_list)
        print(f'{out_line}', flush=True)


debug = False

''' build
    Project, Activity, Institution, SourceID, Experiment, Variant, Frequency, Variable, Grid, DAWPS, D, A, W, P, S,
        StatDate, Status, W_Version, W_Count, P_Version, P_Count, S_Version, S_Count, W_Path, P_Path, FirstFile, LastFile
'''

def main():

    args = assess_args()
    unrestricted = args.unrestricted
    print(f"DEBUG: unrestricted = {unrestricted}")

    ds_struct = dict()

    ''' STAGE 0:  Create initial set of ds_struct[] entries from the dataset_spec. '''

    dsids_stage_1 = dsids_from_dataset_spec(DS_SPEC)
    for dsid in dsids_stage_1:
        ds_struct[dsid] = new_ds_record()
        ds = ds_struct[dsid]
        # dsid = proj.model.experiment.resolution[.tuning].realm.grid.outtype.freq.ens.ver
        init_ds_record_from_dsid(ds, dsid)
        ds['D'] = 'D'

    ds_count = len(ds_struct)
    print(f"{ts()}:DEBUG: Completed Stage 0: dataset_spec: ds_count = {ds_count}", flush=True)

    ''' OUCH.  No CMIP6 stuff exists in the Archive Map '''
    ''' STAGE 1:  Update ds_struct with datasets found in the archive_map. '''

    '''
    # split the Archive_Map into a list of records, each record a list of fields
    #   Campaign,Model,Experiment,Resolution,Ensemble,DatasetType,ArchivePath,DatatypeTarExtractionPattern,Notes
    am_lines = loadFileLines(ARCH_MAP)
    for amline in am_lines:
        dsid = dsid_from_archive_map(amline)
        if not dsid in ds_struct:
            if unrestricted:
                ds_struct[dsid] = new_ds_record()
                init_ds_record_from_dsid(ds_struct[dsid],dsid)
            else:
                continue
        ds = ds_struct[dsid]
        ds['A'] = 'A'

    ds_count = len(ds_struct)
    print(f"{ts()}:DEBUG: Completed Stage 1: archive map: ds_count = {ds_count}", flush=True)
    '''

    ''' STAGE 2:  Walk the warehouse.  Collect dataset path, max version, and filecount of max version '''

    wh_path_tuples = get_dataset_path_tuples(WH_ROOT)   # list of (ensembledir,vleaf,filecount)
    dataset_paths = list(set([ atup[0] for atup in wh_path_tuples ]))
    for w_path in dataset_paths:
        dsid = dsid_from_warehouse_path(w_path)
        if not dsid in ds_struct:
            if unrestricted:
                ds_struct[dsid] = new_ds_record()
                init_ds_record_from_dsid(ds_struct[dsid],dsid)
            else:
                continue
        ds = ds_struct[dsid]

        ds['W'] = 'W'
        maxv, maxc = get_maxv_info(w_path)
        ds['W_Path'] = w_path
        ds['W_Version'] = maxv
        ds['W_Count'] = maxc

    ds_count = len(ds_struct)
    print(f"{ts()}:DEBUG: Completed Stage 2: warehouse: ds_count = {ds_count}", flush=True)

    ''' STAGE 3:  Walk the publication dirs. Collect dataset path, max version, and filecount of max version '''

    pb_path_tuples = get_dataset_path_tuples(PB_ROOT)   # list of (ensembledir,vleaf,filecount)
    # dumplist(pb_path_tuples); # DEBUG
    dataset_paths = list(set([ atup[0] for atup in pb_path_tuples ]))
    for p_path in dataset_paths:
        dsid = dsid_from_publication_path(p_path)

        if not dsid in ds_struct:
            if unrestricted:
                ds_struct[dsid] = new_ds_record()
                init_ds_record_from_dsid(ds_struct[dsid],dsid)
            else:
                continue
        ds = ds_struct[dsid]

        ds['P'] = 'P'
        maxv, maxc = get_maxv_info(p_path)
        ds['P_Path'] = p_path
        ds['P_Version'] = maxv
        ds['P_Count'] = maxc

    ds_count = len(ds_struct)
    print(f"{ts()}:DEBUG: Completed Stage 3: publication: ds_count = {ds_count}", flush=True)

    ''' STAGE 4: Conduct ESGF Server search for published datasets.  Collect max version, and filecount of max version. '''
    ''' To accommodate "unlimited", we must produce the unique set of "institution_ID", and query for each, and update the '''
    ''' "esgf_report" with the results of each call. '''

    if unrestricted:
        esgf_report = dict()
        institutes = set( [ ds_struct[dsid]["Institution"] for dsid in ds_struct ] )
        for inst in institutes:
            facets = { "project": "CMIP6", "institution_id": inst }
            esgf_report.update( collect_esgf_search_datasets(facets) )
    else:
        facets = { "project": "CMIP6" }
        esgf_report = collect_esgf_search_datasets(facets)

    ''' DEBUG
    for dsid_key in esgf_report:
        print(f"ESGF_SEE:{esgf_report[dsid_key]['title']}")

    sys.exit(0)
    '''
    skipcount = 0

    for dsid_key in esgf_report:
        print(f"DEBUG: stage 4: dsid_key = {dsid_key}", file=sys.stderr, flush=True)
        dsid = esgf_report[dsid_key]["title"]
        vers = esgf_report[dsid_key]["version"]
        filecount = esgf_report[dsid_key]["file_count"]
        if not dsid in ds_struct:
            if unrestricted:
                ds_struct[dsid] = new_ds_record()
                init_ds_record_from_dsid(ds_struct[dsid],dsid)
            else:
                skipcount += 1
                continue
        ds = ds_struct[dsid]

        ds['S'] = 'S'
        ds['S_Version'] = vers
        ds['S_Count'] = filecount

    ds_count = len(ds_struct)
    print(f"{ts()}:DEBUG: Completed Stage 4: esgf search: ds_count = {ds_count}, skipcount = {skipcount}", flush=True)

    ''' STAGE 5: Set Campaign, Seek a status file for each ds_struct entry, enter as "date" and "status", set AWPS code '''

    for dsid in ds_struct:
        ds = ds_struct[dsid]
        
        # ds['campaign'] = campaign_via_model_experiment(ds['model'],ds['experiment'])
        sf_data = get_sf_laststat(dsid)
        sf_ts = sf_data.split(':')[0] 
        ds['StatDate'] = clean_timestamp(sf_ts)        # date from last status value
        stat_parts = sf_data.split(':')[1:]
        if stat_parts[0] != "DATASM":
            ds['Status'] = ':'.join(stat_parts)
        else:
            ds['Status'] = ':'.join(stat_parts[1:])    # stat from last status value
        ds['DAWPS'] = ds['D']+ds['A']+ds['W']+ds['P']+ds['S']

    if unrestricted:
        report_ds_struct(ds_struct)
        sys.exit(0)

    # first file and last file of highest version in esgf_search, else in pub, else in warehouse

    dsids_covered = list()

    for dsid_key in esgf_report:
        dsid = esgf_report[dsid_key]["title"]
        ds = ds_struct[dsid]
        ds['FirstFile'] = esgf_report[dsid_key]["first_file"]
        ds['LastFile']  = esgf_report[dsid_key]["final_file"]
        dsids_covered.append(dsid)

    for pb_tup in pb_path_tuples:
        if pb_tup[2] == 0:
            continue
        dsid = dsid_from_publication_path(pb_tup[0])
        if dsid in dsids_covered:
            continue
        pb_path = os.path.join(pb_tup[0],pb_tup[1])
        ffile, lfile = bookend_files(pb_path)
        if len(ffile) and len(lfile):
            dsid = dsid_from_publication_path(pb_tup[0])
            ds = ds_struct[dsid]
            ds['FirstFile'] = ffile
            ds['LastFile'] = lfile
            dsids_covered.append(dsid)

    for wh_tup in wh_path_tuples:
        if wh_tup[2] == 0:
            continue
        dsid = dsid_from_warehouse_path(wh_tup[0])
        if dsid in dsids_covered:
            continue
        if not dsid in ds_struct:
            continue
        ds = ds_struct[dsid]
        if len(ds['FirstFile']):
            continue
        wh_path = os.path.join(wh_tup[0],wh_tup[1])
        ffile, lfile = bookend_files(wh_path)
        if len(ffile) and len(lfile):
            ds['FirstFile'] = ffile
            ds['LastFile'] = lfile
            dsids_covered.append(dsid)
        
    ds_count = len(ds_struct)
    print(f"{ts()}:DEBUG: Completed Stage 5: status files and first/last files: ds_count = {ds_count}", flush=True)

    ''' Print the Report '''

    report_ds_struct(ds_struct)
            
    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())

