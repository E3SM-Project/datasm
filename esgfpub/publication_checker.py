import sys
import os
import yaml
import argparse
import json
import re
from tqdm import tqdm
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from esgfpub.util import print_message
from distributed import Client, as_completed, LocalCluster

def get_cmip_start_end(filename):
    if 'clim' in filename:
        return int(filename[-21:-17]), int(filename[-14: -10])
    else:
        return int(filename[-16:-12]), int(filename[-9: -5])

def get_e3sm_start_end(filename):
    if 'climo' in filename:
        return int(filename[-22:-18]), int(filename[-15: -11])
    else:
        return int(filename[-16:-12]), int(filename[-9: -5])


def check_case(path, variables, spec, case, ens, published):

    missing = list()

    all_tables = [x for x in spec['tables']]
    all_vars = list()
    for table in all_tables:
        all_vars.extend(spec['tables'][table])

    num_vars = 0
    vars_expected = list()

    if 'all' in variables:
        num_vars = len(all_vars)
        vars_expected = all_vars[:]
    else:
        for v in variables:
            if v in all_tables:
                num_vars += len(spec['tables'][v])
                vars_expected.extend(spec['tables'][v])
            else:
                num_vars += 1
                vars_expected.append(v)

    with tqdm(total=num_vars, leave=True) as pbar:

        for root, _, files in os.walk(path):
            if not files:
                continue
            if 'r{}i1p1f1'.format(ens) not in root.split(os.sep):
                continue

            files = sorted(files)
            var = files[0].split('_')[0]

            root_base = os.path.split(root)[0]
            if len(os.listdir(root_base)) > 1:
                print("WARNING: multiple directories found for {case}-{ens}-{var}".format(
                    case=case, ens=ens, var=var))

            table = root.split(os.sep)[-4]

            if var not in vars_expected:
                continue

            pbar.set_description('Checking {}'.format(var))

            try:
                vars_expected.remove(var)
            except ValueError:
                if debug:
                    print("{} not in expected list, skipping".format(var))

            if "_fx_" in files[0]:
                continue
            start, end = get_cmip_start_end(files[0])
            freq = end - start + 1
            spans = list(range(spec['cases'][case]['start'],
                               spec['cases'][case]['end'], freq))

            for span in spans:
                found_span = False
                s_start = span
                s_end = span + freq - 1
                if s_end > spec['cases'][case]['end']:
                    s_end = spec['cases'][case]['end']
                for f in files:
                    f_start, f_end = get_cmip_start_end(f)
                    if f_start == s_start and f_end == s_end:
                        found_span = True
                        break
                if not found_span:
                    missing.append("{var}-{start:04d}-{end:04d}".format(
                        var=var, start=s_start, end=s_end))

            pbar.update(1)

    for v in vars_expected:
        missing.append(
            "{var} missing all files".format(var=v))
    return missing






def check_spans(files, start, end, dataset_id):
    missing = []

    if not start or not end:
        start, end = infer_start_end_cmip(files)
    if not start and not end:
        start, end = infer_start_end_e3sm(files)

    f_start, f_end = get_cmip_start_end(files[0])
    freq = f_end - f_start + 1
    spans = list(range(start, end, freq))

    for idx, span_start in enumerate(spans):
        # If its the last element exit
        if span_start == spans[-1]:
            break

        found_span = False
        span_end = spans[idx + 1] -1

        for f in files:
            f_start, f_end = get_cmip_start_end(f)
            if f_start == span_start and f_end == span_end:
                found_span = True
                break
        if not found_span:
            missing.append("{var}-{start:04d}-{end:04d}".format(
                var=dataset_id, start=span_start, end=span_end))
    return missing

def infer_start_end_cmip(files):
    """
    From a list of files with the given naming convention
    return the start year of the first file and the end year of the
    last file

    A typical CMIP6 file will have a name like:
    pbo_Omon_E3SM-1-1-ECA_hist-bgc_r1i1p1f1_gr_185001-185412.nc' 
    """
    f = sorted(files)
    first, last = files[0], files[-1]
    p = r'\d{6}-\d{6}'
    idx = re.search(pattern=p, string=first)
    if not idx:
        return None, None
    start = int(first[idx.start(): idx.start() + 4])

    idx = re.search(pattern=p, string=last)
    end = int(last[idx.start() + 7: idx.start() + 11])

    return start, end

def infer_start_end_e3sm(files):
    """
    From a list of files with the given naming convention
    return the start year of the first file and the end year of the
    last file
    """
    f = sorted(files)
    p = r'\.\d{4}-\d{2}'
    idx = re.search(pattern=p, string=f[0])
    if not idx:
        return None, None
    start = int(f[0][idx.start() + 1: idx.start() + 5])

    idx = re.search(pattern=p, string=f[-1])
    end = int(f[-1][idx.start() + 1: idx.start() + 5])

    return start, end


def infer_start_end_climo(files):
    f = sorted(files)
    p = r'_\d{6}_\d{6}_'
    idx = re.search(pattern=p, string=f[0])
    start = int(f[0][idx.start() + 1: idx.start() + 5])

    idx = re.search(pattern=p, string=f[-1])
    end = int(f[-1][idx.start() + 8: idx.start() + 12])

    return start, end

def check_monthly(files, start=None, end=None):

    missing = []
    pattern = r'\d{4}-\d{2}.*nc'
    idx = re.search(pattern=pattern, string=files[0])
    if not idx:
        raise ValueError('Unexpected file format: {}'.format(files[0]))
    prefix = files[0][:idx.start()]
    suffix = files[0][idx.start() + 7:]

    if not start or not end:
        start, end = infer_start_end_e3sm(files)

    for year in range(start, end + 1):
        for month in range(1, 13):
            name = '{prefix}{year:04d}-{month:02d}{suffix}'.format(
                prefix=prefix, year=year, month=month, suffix=suffix)
            if name not in files:
                missing.append(name)
    return missing

def check_monthly_climos(files, start, end):
    # FIX ME
    missing = []
    pattern = r'_\d{6}_\d{6}_climo.nc'
    files = sorted(files)
    idx = re.search(pattern=pattern, string=files[0])
    if not idx:
        raise ValueError('Unexpected file format: {}'.format(files[0]))
    prefix = files[0][:idx.start() - 2]

    if not start or not end:
        start, end = infer_start_end_climo(files)
    
    f_start, f_end = get_e3sm_start_end(files[0])
    freq = f_end - f_start + 1
    spans = list(range(start, end, freq))

    for idx, span_start in enumerate(spans):
        # If its the last element exit
        if span_start == spans[-1]:
            break

        found_span = False
        span_end = spans[idx + 1] -1
    
        for month in range(1, 13):
            name = '{prefix}{month:02d}_{start:04d}{month:02d}_{end:04d}{month:02d}_climo.nc'.format(
                prefix=prefix, month=month, start=span_start, end=span_end)
            if name not in files:
                missing.append(name)
    return missing

def check_seasonal_climos(files, start, end):
    missing = []
    pattern = r'_\d{6}_\d{6}_climo.nc'
    files = sorted(files)
    idx = re.search(pattern=pattern, string=files[0])
    if not idx:
        raise ValueError('Unexpected file format: {}'.format(files[0]))
    prefix = files[0][:idx.start() - 4]

    if not start or not end:
        start, end = infer_start_end_climo(files)
    
    f_start, f_end = get_e3sm_start_end(files[0])
    freq = f_end - f_start + 1
    spans = list(range(start, end, freq))

    for idx, span_start in enumerate(spans):
        # If its the last element exit
        if span_start == spans[-1]:
            break

        found_span = False
        span_end = spans[idx + 1] -1

        name = '{prefix}_ANN_{span_start:04d}01_{span_end:04d}12_climo.nc'.format(
            prefix=prefix,span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)
        name = '{prefix}_DJF_{span_start:04d}01_{span_end:04d}12_climo.nc'.format(
            prefix=prefix, span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)
        name = '{prefix}_MAM_{span_start:04d}03_{span_end:04d}05_climo.nc'.format(
            prefix=prefix, span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)
        name = '{prefix}_JJA_{span_start:04d}06_{span_end:04d}08_climo.nc'.format(
            prefix=prefix, span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)
        name = '{prefix}_SON_{span_start:04d}09_{span_end:04d}11_climo.nc'.format(
            prefix=prefix, span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)

    return missing

def check_submonthly(files, start, end):

    missing = []
    pattern = r'\d{4}-\d{2}.*nc'
    first = files[0]
    idx = re.search(pattern=pattern, string=first)
    if not idx:
        raise ValueError('Unexpected file format: {}'.format(first))
    if not start or not end:
        start, end = infer_start_end_e3sm(files)

    prefix = first[:idx.start()]
    suffix = first[idx.start() + 7:]
    for year in range(start, end + 1):
        for month in range(1, 13):
            if month == 2:
                continue
            name = '{prefix}{year:04d}-{month:02d}'.format(
                prefix=prefix, year=year, month=month)
            res = [i for i in files if name in i]
            if not res:
                missing.append(name)
    return missing

def check_fixed(files, dataset_id, spec):
    if files:
        return None
    else:
        return dataset_id

def get_ts_start_end(filename):
    p = r'_\d{6}_\d{6}.nc'
    idx = re.search(p, files[0])
    if not idx:
        raise ValueError('Unexpected file format: {}'.format(files[0]))
    start = int(files[0][idx.start() + 1: idx.start() + 5])
    end = int(files[0][idx.start() + 8: idx.start() + 12])
    return start, end

def check_time_series(files, dataset_id, spec, start=None, end=None):
    
    missing = []
    files = files = [x.split('/')[-1] for x in sorted(files)]
    if not start or not end:
        start, end = get_ts_start_end(files[0])
    
    case_info = dataset_id.split('.')
    model_version = case_info[1]
    casename = case_info[2]
    realm = case_info[4]
    ens = case_info[6][1:]
    case_spec = [x for x in spec['project']['E3SM'][model_version] if x['experiment'] == casename].pop()
    if case_spec.get('except'):
        expected_vars = [x for x in spec['time-series'][realm] if x not in case_spec['except']]
    else:
        expected_vars = spec['time-series'][realm]
    
    for v in expected_vars:
        v_files = [x for x in files if x[:x.index('_')] == v]
        if not v_files:
            missing.append('{dataset}-{var}-{start:04d}-{end:04d}'.format(
                    dataset=dataset_id, var=v, start=start, end=end))
        if len(v_files) > 1:
            v_start, v_end = get_ts_start_end(files)
            if start != v_start:
                missing.append('{dataset}-{var}-{start:04d}-{end:04d}'.format(
                    dataset=dataset_id, var=v, start=start, end=v_start))
            if end != v_end:
                missing.append('{dataset}-{var}-{start:04d}-{end:04d}'.format(
                    dataset=dataset_id, var=v, start=end, end=v_end))
            
            f_start, f_end = get_ts_start_end(files[0])
            freq = f_end - f_start + 1
            spans = list(range(start, end, freq))

            for idx, span_start in enumerate(spans):
                # If its the last element exit
                if span_start == spans[-1]:
                    break

                found_span = False
                span_end = spans[idx + 1] -1

                for f in files:
                    f_start, f_end = get_ts_start_end(f)
                    if f_start == span_start and f_end == span_end:
                        found_span = True
                        break
                if not found_span:
                    missing.append('{dataset}-{var}-{start:04d}-{end:04d}'.format(
                    dataset=dataset_id, var=v, start=span_start, end=span_end))

    return missing


def sproket_with_id(dataset_id, sproket, spec=None, start=None, end=None):
    
    # create the path to the config, write it out
    tempfile = NamedTemporaryFile(suffix='.json')
    with open(tempfile.name, mode='w') as tmp:
        config_string = json.dumps({
            'search_api': "https://esgf-node.llnl.gov/esg-search/search/",
            'data_node_priority': ["aims3.llnl.gov", "esgf-data1.llnl.gov"],
            'fields': {
                'dataset_id': dataset_id + '*'
            }
        })
        
        tmp.write(config_string)
        tmp.seek(0)
        cmd = [sproket, '-config', tempfile.name, '-y', '-urls.only']
        proc = Popen(cmd, shell=False, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
    if err:
        print(err.decode('utf-8'))
        return [], dataset_id
    
    if not out:
        return [dataset_id], dataset_id

    files = sorted([i.decode('utf-8') for i in out.split()])
    if '.fx.' in dataset_id and files:
        return [], dataset_id

    if dataset_id[:5] == 'CMIP6':
        missing = check_spans(files, start, end, dataset_id)
    elif dataset_id[:4] == 'E3SM':
        if 'mon.' in dataset_id:
            missing = check_monthly(files, start, end)
        elif 'monClim.' in dataset_id or 'monClim-' in dataset_id:
            missing = check_monthly_climos(files, start, end)
        elif 'seasonClim.' in dataset_id or 'seasonClim-' in dataset_id:
            missing = check_seasonal_climos(files, start, end)
        elif 'clim.' in dataset_id or 'clim-' in dataset_id:
            missing = []
            missing.extend(
                check_monthly_climos(files, start, end))
            missing.extend(
                check_seasonal_climos(files, start, end))
        elif 'time-series' in dataset_id:
            missing = check_time_series(files, dataset_id, spec, start, end)
        elif 'fixed' in dataset_id:
            missing = check_fixed(files, dataset_id, spec)
        else:
            missing = check_submonthly(files, start, end)

    return missing, dataset_id

# The typical CMIP6 path is:
# /CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/ts/gr/v20190719/ts_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_042601-045012.nc
# This file would have the dataset_id:
# CMIP6.CMIP.E3SM-project.E3SM-1-0.piControl.r1i1p1f1.Amon.ts#20190719

def check_cmip(client, dataset_spec, data_path, experiments, ensembles, tables, variables, published, sproket):
    
    missing = []
    futures = []

    for source in dataset_spec['project']['CMIP6']:
        for case in dataset_spec['project']['CMIP6'][source]:
            
            # Check this case if its explicitly given by the user, or if default is set
            if 'all' not in experiments and case['experiment'] not in experiments:
                continue

            if 'all' in ensembles:
                ensembles = case['ens']
            else:
                if isinstance(ensembles, int):
                    ensembles = 'r{}i1f1p1'.format(ensembles)
                elif isinstance(ensembles, list) and isinstance(ensembles[0], int):
                    ensembles = ['r{}i1f1p1'.format(e) in ensembles]

            for ensemble in ensembles:

                for table in dataset_spec['tables']:
                    # skip this table if its not in the user list
                    # and the default isnt set
                    if table not in tables and 'all' not in tables:
                        continue

                    if table in case.get('except', []):
                        continue
                    
                    for variable in dataset_spec['tables'][table]:
                        # skip this varible if its not in the user list
                        # and the default isnt set
                        if variable not in variables and 'all' not in variables:
                            continue

                        if variable in case.get('except', []):
                            continue
                        
                        dataset_id = "CMIP6.*.E3SM-Project.*{source}.*{experiment}*.{ens}.{table}.{variable}.*".format(
                            source=source,
                            experiment=case['experiment'],
                            ens=ensemble,
                            table=table,
                            variable=variable)
                        if client:
                            futures.append(
                                client.submit(
                                    sproket_with_id,
                                    dataset_id,
                                    sproket,
                                    dataset_spec,
                                    case['start'],
                                    case['end']))
                        else:
                            missing, dataset_id = sproket_with_id(
                                dataset_id,
                                sproket,
                                dataset_spec,
                                case['start'],
                                case['end'])
                            if missing:
                                for m in missing:
                                    print_message('Missing file found: {}'.format(m))
                            else:
                                print_message('All files found for: {}'.format(dataset_id), 'ok')
        
    if client:
        pbar = tqdm(
            total=len(futures),
            desc='Contacting ESGF database')
        for f in as_completed(futures):
            res = f.result()
            m, dataset_id = res
            if m:
                missing.extend(m)
            else:
                pbar.set_description('All files found for: {}'.format(dataset_id))
            pbar.update(1)
        pbar.close()
    if missing:
        for m in missing:
            print_message('Missing: {}'.format(m))
        return True # true there was an error
    else:
        return False

def check_e3sm(client, dataset_spec, data_path, experiments, ensembles, tables, variables, published, sproket, to_json, debug=False):
    missing = []
    futures = []

    for version in dataset_spec['project']['E3SM']:
        if debug:
            print_message('checking version: ' + version, 'info')
        for case in dataset_spec['project']['E3SM'][version]:
            
            # Check this case if its explicitly given by the user, or if default is set
            if 'all' not in experiments and case['experiment'] not in experiments:
                continue
            if debug:
                print_message('  checking case: ' + case['experiment'], 'info')

            if 'all' in ensembles:
                ens = case['ens']
            else:
                if isinstance(ensembles, int):
                    ens = 'ens{}'.format(ensembles)
                elif isinstance(ensembles, list) and isinstance(ensembles[0], int):
                    ens = ['ens{}'.format(e) in ensembles]

            for ensemble in ens:
                if debug:
                    print_message('    checking ensemble: ' + ensemble, 'info')
                for res in case['resolution']:
                    for comp in case['resolution'][res]:
                        for item in case['resolution'][res][comp]:
                            for data_type in item['data_types']:
                                if item.get('except') and data_type in item['except']:
                                    continue
                                dataset_id = "E3SM.{version}.{case}.{res}.{comp}.{grid}*{data_type}.*{ens}.*".format(
                                    version=version,
                                    case=case['experiment'],
                                    res=res,
                                    comp=comp,
                                    grid=item['grid'],
                                    data_type=data_type,
                                    ens=ensemble)
                                if client:
                                    futures.append(
                                        client.submit(
                                            sproket_with_id,
                                            dataset_id,
                                            sproket,
                                            dataset_spec,
                                            case['start'],
                                            case['end']))
                                else:
                                    missing, dataset_id = sproket_with_id(
                                        dataset_id,
                                        sproket,
                                        dataset_spec,
                                        case['start'],
                                        case['end'])
                                    if not missing:
                                        print_message('All files found for: {}'.format(dataset_id), 'ok')
        
    if client:
        pbar = tqdm(
            total=len(futures),
            desc='Contacting ESGF database')
        for f in as_completed(futures):
            res = f.result()
            m, dataset_id = res
            if m:
                missing.extend(m)
            else:
                pbar.set_description('All files found for: {}'.format(dataset_id))
            pbar.update(1)
        pbar.close()

    if missing:
        if to_json:
            print_message('Missing datasets being written to {}'.format(to_json))
            if os.path.exists(to_json):
                os.remove(to_json)
            with open(to_json, 'w') as op:
                json.dump(missing, op, indent=4, sort_keys=True)
        else:
            for m in missing:
                print_message('Missing: {}'.format(m))
        return True # true there was an error
    else:
        print_message('All files from datasets found', 'ok')
        return False

def check_datasets_by_id(client, sproket, dataset_ids):
    if client:
        futures = []
        missing = []
        for d in dataset_ids:
            futures.append(
                client.submit(
                    sproket_with_id,
                    d,
                    sproket))
        pbar = tqdm(
            total=len(futures),
            desc='Contacting ESGF database')
        for f in as_completed(futures):
            m, dataset_id = f.result()
            if m:
                missing.extend(m)
            else:
                pbar.set_description('All files found for: {}'.format(dataset_id))
            pbar.update(1)
        pbar.close()
        if missing:
            for m in missing:
                print_message('Missing file found: {}'.format(m))
    else:
        for d in dataset_ids:
            missing, dataset_id = sproket_with_id(d, sproket)
            if missing:
                for m in missing:
                    print_message('Missing file found: {}'.format(m))
            else:
                print_message('All files found for: {}'.format(dataset_id), 'ok')

def publication_checker(variables, spec_path, data_path, cases, ens, tables, published, projects, dataset_ids=None, sproket=None, max_connections=4, debug=False, serial=False, to_json=False):

    if debug:
        print_message("Running in debug mode", 'info')
    
    if published and not sproket:
        raise ValueError("Publication checking is turned on, but no sproket utility path given")

    if data_path and not os.path.exists(data_path):
        raise ValueError("Given data path does not exist")
    if not os.path.exists(spec_path):
        raise ValueError("Given case spec file does not exist")

    with open(spec_path, 'r') as ip:
        case_spec = yaml.load(ip, Loader=yaml.SafeLoader)

    if serial:
        client = None
    else:
        cluster = LocalCluster(
            n_workers=1,
            processes=True,
            threads_per_worker=max_connections)
        client = Client(cluster)
    
    try:
        if dataset_ids:
            if isinstance(dataset_ids, str):
                dataset_ids = [dataset_ids]
            check_datasets_by_id(client, sproket, dataset_ids)
            return

        if not projects or ('cmip6' in projects or 'CMIP6' in projects):
            print_message("Checking for CMIP6 project data", 'ok')
            missing = check_cmip(
                    client=client,
                    dataset_spec=case_spec,
                    data_path=data_path,
                    ensembles=ens,
                    experiments=cases,
                    tables=tables,
                    variables=variables,
                    published=published,
                    sproket=sproket)
            if not missing:
                print_message('All CMIP6 files found', 'ok')
        else:
            print_message('Skipping CMIP6 datasets', 'ok')
        
        if not projects or ('e3sm' in projects or 'E3SM' in projects):
            print_message("Checking for E3SM project data", 'ok')
            missing = check_e3sm(
                    client=client,
                    dataset_spec=case_spec,
                    data_path=data_path,
                    ensembles=ens,
                    experiments=cases,
                    tables=tables,
                    variables=variables,
                    published=published,
                    sproket=sproket,
                    to_json=to_json,
                    debug=debug)
            
            if not missing:
                print_message('All E3SM project files found', 'ok')
        else:
            print_message('Skipping E3SM project datasets', 'ok')
    finally:    
        if client:
            cluster.close()
            client.close()

    return 0
