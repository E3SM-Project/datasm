import sys, os
import argparse
from argparse import RawTextHelpFormatter
import glob
import shutil
import subprocess
import time
from datetime import datetime


help_text = '''
Produce a consolidates dataset status report, encompassing the AWPS (Archive,Warehouse,Pub_Dir and Sproket)\n\
locations and filecount report and the warehouse status report.
'''

def assess_args():

    parser = argparse.ArgumentParser(description=help_text)
    parser.add_argument('--awps-report', "-a", dest='awps_report', help="latest awps report in CSV format", required=True)
    parser.add_argument('--wh-report', "-w", dest='wh_report', help="latest warehouse status report in CSV format", required=True)

    args = parser.parse_args()

    # print(f'Parsing Args')

    if not args.awps_report or not args.wh_report:
        print(f'missing arguments:  Try --help')
        sys.exit(0)

    return args

# Generic Convenience Functions =============================

def loadFileLines(afile):
    retlist = []
    if len(afile):
        with open(afile,"r") as f:
            retlist = f.read().split('\n')
        retlist = [ _ for _ in retlist if _[:-1] ]
    return retlist

def printList(prefix,alist):
    for _ in alist:
        print(f'{prefix}{_}')

def printFileList(outfile,alist):
    stdout_orig = sys.stdout
    with open(outfile,'w') as f:
        sys.stdout = f

        for _ in alist:
            print(f'{_}',flush=True)

        sys.stdout = stdout_orig

# E3SM-specific Functions =============================

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
    else:
        return "UNKNOWN_CAMPAIGN"


'''
src_selected = []
for root, dirs, files in os.walk(args.targetdir):      # aggregate full sourcefile paths in src_selected
    if not dirs:     # at leaf-directory matching src_selector
        src_selected.append(root)

for adir in src_selected:
    for root, dirs, files in os.walk(adir):
        if files:
            print(str(len(files)) + ',' + adir)
'''

'''
    AWPS: AWPS,[opt S-count],model,experiment,ensemble,ds_type,[opt wh_path]
    WREP: model,experiment,ensemble,ds_type,statusfile,wh_dir_counts,wh_ens_path
    WANT: Campaign,Model,experiment,ensemble,ds_type,AWPS,A,W,P,S,statusfile,wh_dircounts,pubdir_count,sproket_count,Archive,WH_ens_path,Pub_ver_path
'''

def load_report_structure(a_list,w_list):
    report = dict()
    w_dict = dict()

    for aline in w_list:
        arec = aline.split(',')
        akey = ','.join([arec[0],arec[1],arec[2],arec[3]])
        w_dict[akey] = arec


    print('Campaign,Model,Experiment,Ensemble,DatasetType,Realm,Grid,Freq,AWPS,A,W,P,S,StatDate,Status,')
    for aline in a_list:
        arec = aline.split(',')
        akey = ','.join([arec[2],arec[3],arec[4],arec[5]])
        r_awpsv = arec[0]
        r_srcnt = arec[1]
        r_model = arec[2]
        r_exper = arec[3]
        r_ensem = arec[4]
        r_dstyp = arec[5]
        r_campa = campaign_via_model_experiment(r_model,r_exper)
        dstyp_list = r_dstyp.split('_')
        r_realm = dstyp_list[0]
        r_gridv = dstyp_list[1]
        if len(dstyp_list) == 3:
            r_freqv = dstyp_list[2]
        else:
            r_freqv = '_'.join([dstyp_list[2],dstyp_list[3]])
        if arec[6]:
            r_pbdir = arec[6]
        else:
            r_pbdir=''

        w_rec = []
        if akey in w_dict:
            w_rec = w_dict[akey]
            r_statf = w_rec[4]
            r_st_sd = r_statf.split(':')[0]
            r_st_sv = ':'.join(r_statf.split(':')[1:])
        else:
            r_st_sd = ''
            r_st_sv = ''
         
        # print(f'dstype: {r_dstyp} : {r_realm} _ {r_gridv} _ {r_freqv}')

        print(f'{r_campa},{r_model},{r_exper},{r_ensem},{r_dstyp},{r_realm},{r_gridv},{r_freqv},{r_awpsv},{r_awpsv[0]},{r_awpsv[1]},{r_awpsv[2]},{r_awpsv[3]},{r_st_sd},{r_st_sv},')

# can use os.path.islink to test if "files" are actually "links"

def main():

    args = assess_args()

    raw_a_list = loadFileLines(args.awps_report)
    raw_w_list = loadFileLines(args.wh_report)

    load_report_structure(raw_a_list,raw_w_list)

    sys.exit(0)

    sys.exit(0)

    ensdirs = get_ensemble_dirs()
    wh_datasets = load_DS_StatusList(ensdirs)

    status_list = produce_status_listing_vcounts(wh_datasets)

    printFileList(stats_out,status_list)

    sys.exit(0)


if __name__ == "__main__":
  sys.exit(main())


