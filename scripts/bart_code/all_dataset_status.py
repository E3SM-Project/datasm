import os, sys, argparse, re

'''
The Big Idea:  Using the Archive_Map and the (sproket-based) ESGF publication report,
produce from each the list of "canonical" experiment/dataset "keys" where

	akey = ( <model>, <experiment>, <ensemble> )
and
	dkey = <realm_grid_freq>

Use these to index a dictionary structure "dataset_status[akey][dkey] = ( A: bool, W: bool, P: bool )
with the Boolean values set according to the presence or absence of the key-pair in either of the
two input files (archive map, esgf publication report)
'''


# INPUT FILES
arch_map='/p/user_pub/e3sm/archive/.cfg/Archive_Map'
esgf_pr='/p/user_pub/e3sm/bartoletti1/Pub_Status/sproket/ESGF_publication_report'

# split the Archive_Map into a list of records, each record a list of fields
#   Campaign,Model,Experiment,Ensemble,DatasetType,ArchivePath,DatatypeTarExtractionPattern,Notes
with open(arch_map) as f:
    contents = f.read().split('\n')
am_list = [ aline.split(',') for aline in contents if aline[:-1] ]

# create a sorted list of unique dataset types
dstype_list = [ arch_rec[4] for arch_rec in am_list ]
dstype_list = list(set(dstype_list))
dstype_list.sort()

# create a dictionary keyed by 'arch_loc_key' = tuple (Model,Experiment,Ensemble) with value the SET of archive path(s)
# (Not really needed here, but nice to have, and we can reuse its keys for the dataset_status table)
arch_loc_dict = {}
for _ in am_list:
    arch_loc_dict[ tuple([_[1],_[2],_[3]]) ] = set()
for _ in am_list:
    arch_loc_dict[ tuple([_[1],_[2],_[3]]) ] |= {_[5]}  # add another archive path to the set - may be more than one.

# create a dictionary keyed by 'arch_loc_key', each value a dictionary keyed by dstype, value a Boolean dictionary (A,W,P)
# For each (model,experiment,ensemble), create a dataset_type entry for every allowable dataset-type
# initialize "Archive,Warehouse,Published" to ALL FALSE.
dataset_status = {}
for akey in arch_loc_dict:
    dataset_status[akey] = { dstype: { 'A': False, 'W': False, 'P': False } for dstype in dstype_list }

# for each record in the ArchiveMap, set the corresponding [archive_key][dataset_key]['A'] = True
for _ in am_list:
    akey = tuple([_[1],_[2],_[3]])
    dkey = _[4]
    dataset_status[akey][dkey]['A'] = True

# extract the datasetID from each published dataset listed in the ESGF_publication_report
# obtain the key 'Model,Experiment,Ensemble'

with open(esgf_pr) as f:
    contents = f.read().split('\n')

esgf_pr_list = [ aline.split(',') for aline in contents if aline[:-1] and not aline[0] == 'NOMATCH' ]
#esgf_pr_list = [ aline.split(',') for aline in contents if aline[:-1] ]
 
#### BEGIN rationalizing archive and publication experiment-case names, and dataset-type names ####
# dsid = proj.model.experiment.resolution[.tuning].realm.grid.outtype.freq.ens.ver

# call to specialize a publication experiment name to an archive experiment name
def specialize_expname(expn,reso,tune):
    if expn == 'F2010plus4k':
        expn = 'F2010-plus4k'
    if expn[0:5] == 'F2010' or expn == '1950-Control':
        if reso[0:4] == '1deg' and tune == 'highres':
            expn = expn + '-LRtunedHR'
        else:
            expn = expn + '-HR'
    return expn

def get_dsid_arch_key( dsid ):
    comps=dsid.split('.')
    expname = specialize_expname(comps[2],comps[3],comps[4])
    return comps[1],expname,comps[-2]

def get_dsid_type_key( dsid ):
    comps=dsid.split('.')
    realm = comps[-6]
    gridv = comps[-5]
    otype = comps[-4]
    freq = comps[-3]

    if realm == 'atmos':
        realm = 'atm'
    elif realm == 'land':
        realm = 'lnd'
    elif realm == 'ocean':
        realm = 'ocn'

    if gridv == 'native':
        grid = 'nat'
    elif otype == 'climo':
        grid = 'climo'
    elif otype == 'monClim':
        grid = 'climo'
        freq = 'mon'
    elif otype == 'seasonClim':
        grid = 'climo'
        freq = 'season'
    elif otype == 'time-series':
        grid = 'reg'
        freq = 'ts-' + freq
    else:
        grid = 'reg'
    return '_'.join([realm,grid,freq])

#### COMPLETED rationalizing archive and publication experiment-case names, and dataset-type names ####

# DEBUG/INFO:  Report when any "published" dataset is not an archived dataset
'''
for id in esgf_pr_list:
    akey = get_dsid_arch_key( id[2] )

    if akey in dataset_status:
        print(f' key { akey } in dataset_status')
    else:
        print(f' key { akey } is NEW')
'''
# sys.exit(0)

# Update 'P' status of this (possibly new) dataset_type in the (possibly new) datasetID_key in the dataset_status table
# using the esgf_pr_list (sproket-based publication report)

for id in esgf_pr_list:
    akey = get_dsid_arch_key( id[2] )
    dkey = get_dsid_type_key( id[2] )

    if not akey in dataset_status:  # first time we've encountered this experiment/case
        dataset_status[akey] = { dstype: { 'A': False, 'W': False, 'P': True } for dstype in dstype_list }
    elif not dkey in dataset_status[akey]:  # first time we've encountered this dataset type for this experiment/case
        dataset_status[akey][dkey] = { 'A': False, 'W': False, 'P': True }
    else:  # just set Publication to True
        dataset_status[akey][dkey]['P'] = True

def dataset_print_csv( akey, dkey ):
    print(f'{akey[0]},{akey[1]},{akey[2]},{dkey}')

'''
# print all combos (model,experiment,ensemble,dataset_type)

for akey in dataset_status:
    # print(f'{ akey }:')
    for dkey in dataset_status[akey]:
        # print(f'    { dkey } : { dataset_status[akey][dkey] }')
        dataset_print_csv(akey,dkey)
'''

for akey in dataset_status:
    for dkey in dataset_status[akey]:
        if dataset_status[akey][dkey]['A'] and dataset_status[akey][dkey]['P']:
            print('A_P,',end ='')
            dataset_print_csv(akey,dkey)

for akey in dataset_status:
    for dkey in dataset_status[akey]:
        if dataset_status[akey][dkey]['A'] and not dataset_status[akey][dkey]['P']:
            print('A_notP,',end ='')
            dataset_print_csv(akey,dkey)

for akey in dataset_status:
    for dkey in dataset_status[akey]:
        if dataset_status[akey][dkey]['P'] and not dataset_status[akey][dkey]['A']:
            print('notA_P,',end ='')
            dataset_print_csv(akey,dkey)

for akey in dataset_status:
    for dkey in dataset_status[akey]:
        if not dataset_status[akey][dkey]['A'] and not dataset_status[akey][dkey]['P']:
            print('notA_notP,',end ='')
            dataset_print_csv(akey,dkey)

sys.exit(0)


