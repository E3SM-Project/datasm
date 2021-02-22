import warnings
warnings.simplefilter('ignore')
import logging
logger = logging.getLogger("contextlib")
logger.setLevel(logging.ERROR)


from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from esgfpub.util import print_message
from esgfpub.verify import verify_dataset
from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
from tqdm import tqdm
import numpy as np
import matplotlib.pyplot as plt
import re
import json
import yaml
import os

SEASONS = [{
    'name': 'ANN',
    'start': '01',
    'end': '12'
}, {
    'name': 'DJF',
    'start': '01',
    'end': '12'
}, {
    'name': 'MAM',
    'start': '03',
    'end': '05'
}, {
    'name': 'JJA',
    'start': '06',
    'end': '08'
}, {
    'name': 'SON',
    'start': '09',
    'end': '11'
}]


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


def check_spans(files, start, end, dataset_id):

    missing = []
    files_found = []

    if not start or not end:
        start, end = infer_start_end_cmip(files)
    if not start and not end:
        start, end = infer_start_end_e3sm(files)
    
    file_start, file_end = get_cmip_start_end(files[0])
    if file_start != start:
        missing.append(f"{dataset_id}-{start:04d}-{file_end:04d}")

    prev_end = start
    for file in sorted(files):
        file_start, file_end = get_cmip_start_end(file)
        if file_start == start:
            prev_end = file_end
            files_found.append(file)
            continue
        if file_start == prev_end + 1:
            prev_end = file_end
            files_found.append(file)
        else:
            missing.append(f"{dataset_id}-{prev_end:04d}-{file_start:04d}")

    file_start, file_end = get_cmip_start_end(files[-1])
    if file_end != end:
        missing.append(f"{dataset_id}-{file_start:04d}-{end:04d}")

    extra_files = [x for x in files if x not in files_found]
    return missing, extra_files


def infer_start_end_cmip(files):
    """
    From a list of files with the given naming convention
    return the start year of the first file and the end year of the
    last file

    A typical CMIP6 file will have a name like:
    pbo_Omon_E3SM-1-1-ECA_hist-bgc_r1i1p1f1_gr_185001-185412.nc' 
    """
    files = sorted(files)
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
    files_found = []

    pattern = r'\d{4}-\d{2}.*nc'
    idx = re.search(pattern=pattern, string=files[0])
    if not idx:
        print_message(f'Unexpected file format: {files[0]}', 'error')
        return [], []
    prefix = files[0][:idx.start()]
    suffix = files[0][idx.start() + 7:]

    if not start or not end:
        start, end = infer_start_end_e3sm(files)

    for year in range(start, end + 1):
        for month in range(1, 13):
            name = f'{prefix}{year:04d}-{month:02d}{suffix}'
            if name not in files:
                missing.append(name)
            else:
                files_found.append(name)

    extra_files = [x for x in files if x not in files_found]
    return missing, extra_files


def check_climos(files, start, end):
    missing = []
    files_found = []

    pattern = r'_\d{6}_\d{6}_climo.nc'
    files = sorted(files)
    idx = re.search(pattern=pattern, string=files[0])
    if not idx:
        raise ValueError(f'Unexpected file format: {files[0]}')
    prefix = files[0][:idx.start() - 2]

    if not start or not end:
        start, end = infer_start_end_climo(files)

    start, end = get_e3sm_start_end(files[0])

    for month in range(1, 13):
        name = f'{prefix}{month:02d}_{start:04d}{month:02d}_{end:04d}{month:02d}_climo.nc'
        if name not in files:
            missing.append(name)
        else:
            files_found.append(name)

    for season in SEASONS:
        name = f'{prefix}{season["name"]}_{start:04d}{season["start"]}_{end:04d}{season["end"]}_climo.nc'
        if name not in files:
            missing.append(name)
        else:
            files_found.append(name)
    extra_files = [x for x in files if x not in files_found]
    return missing, extra_files


def check_submonthly(files, start, end, debug=False):

    missing, extra = list(), list()
    pattern = re.compile(r'\d{4}-\d{2}.*nc')
    first = files[0]
    idx = pattern.search(first)
    # idx = re.search(pattern=pattern, string=first)
    if not idx:
        raise ValueError(f'Unexpected file format: {first}')
    if not start or not end:
        start, end = infer_start_end_e3sm(files)

    prefix = first[:idx.start()]
    # TODO: Come up with a way of doing this check more 
    # robustly. Its hard because the high-freq files arent consistant
    # from case to case, using different 'h' codes and different frequencies
    # for the time being, if there's at least one file per year it'll get marked as correct
    for year in range(start, end):
        found = None
        for idx, file in enumerate(files):
            pattern = re.compile(fr'{year:04d}-\d{2}.*nc')
            if pattern.search(file):
                found = idx
                break
        if found:
            files.pop(idx)
        else:
            name = f'{prefix}{year:04d}'
            missing.append(name)

    if not missing and debug:
        msg = f'Found {len(files)} files for submonthly dataset'
        print_message(msg, 'info')
    return missing, extra


def check_fixed(files, dataset_id, spec):
    if files:
        return [], []
    else:
        return dataset_id, []


def get_ts_start_end(filename):
    p = re.compile(r'_\d{6}_\d{6}.*nc')
    idx = p.search(filename)
    if not idx:
        raise ValueError(f'Unexpected file format: {filename}')
    start = int(filename[idx.start() + 1: idx.start() + 5])
    end = int(filename[idx.start() + 8: idx.start() + 12])
    return start, end


def check_time_series(files, dataset_id, spec, start=None, end=None):

    missing = []
    extra = []
    files = [x.split('/')[-1] for x in sorted(files)]
    files_found = []
    if not start or not end:
        start, end = get_ts_start_end(files[0])

    case_info = dataset_id.split('.')
    project = case_info[0]
    model_version = case_info[1]
    casename = case_info[2]
    realm = case_info[4]
    if project == 'CMIP6':
        ens = case_info[5]
    else:
        ens = case_info[6][1:]

    case_spec = [x for x in spec['project']['E3SM'][model_version] if x['experiment'] == casename].pop()

    if case_spec.get('except'):
        expected_vars = [x for x in spec['time-series'][realm]
                         if x not in case_spec['except']]
    else:
        expected_vars = spec['time-series'][realm]

    for v in expected_vars:
        
        v_files = list()
        for x in files:
            idx = -36 if 'cmip6_180x360_aave' in x else -17
            if v in x and x[:idx] == v:
                v_files.append(x)

        if not v_files:
            missing.append(f'{dataset_id}-{v}-{start:04d}-{end:04d}')
            continue
        else:
            v_files = sorted(v_files)
            v_start, v_end = get_ts_start_end(v_files[0])
            if start != v_start:
                missing.append(f'{dataset_id}-{v}-{start:04d}-{v_start:04d}')

            prev_end = start
            for file in v_files:
                file_start, file_end = get_ts_start_end(file)
                if file_start == start:
                    prev_end = file_end
                    files_found.append(file)
                    continue
                if file_start == prev_end + 1:
                    prev_end = file_end
                    files_found.append(file)
                else:
                    missing.append(f"{dataset_id}-{prev_end:04d}-{file_start:04d}")
            # f_start, f_end = get_ts_start_end(v_files[0])
            # freq = f_end - f_start + 1
            # spans = list(range(start, end, freq))

            # for idx, span_start in enumerate(spans):
            #     if span_start != spans[-1]:
            #         span_end = spans[idx + 1] - 1
            #     else:
            #         span_end = end

            #     found_span = False

            #     for f in v_files:
            #         f_start, f_end = get_ts_start_end(f)
            #         if f_start == span_start and f_end == span_end:
            #             found_span = True
            #             files_found.append(f)
            #             break
            #     if not found_span:
            #         missing.append(f'{dataset_id}-{v}-{span_start:04d}-{span_end:04d}')
    e = [x for x in files if x not in files_found]
    extra.extend(e)
    return missing, extra


def get_version(filename):
    tail, head = os.path.split(filename)
    path_split = tail.split('/')
    if 'v' in path_split[-1]:
        return int(path_split[-1][1:])
    else:
        print_message(f'Unable to determine version for {filename}')
        return 0


def filepath_to_datasetid(filename):
    tail, head = os.path.split(filename)
    if 'CMIP6' in tail:
        tail = tail.split('/')
        idx = tail.index('CMIP6')
    else:
        tail = tail.split('/')
        idx = tail.index('E3SM')
    
    return '.'.join(tail[idx:])


def check_files(files, spec, start, end, debug=False):
    """
    Checks a set of files for missing or extras

    returns: missing (list), dataset_id (str), extra (list)
    """
    missing = []
    extra = []
    try:
        dataset_id = filepath_to_datasetid(files[0])
    except:
        import ipdb; ipdb.set_trace()
        print("something wonky happened")
    if debug:
        print_message(f"Found dataset: {dataset_id}", 'info')

    if '.fx.' in dataset_id and files:
        return [], dataset_id, []

    nfiles = []
    for file in files:
        file_path, name = os.path.split(file)
        file_attrs = file_path.split('/')
        version = file_attrs[-1]
        nfiles.append((version, file))
    
    latest_version = sorted(nfiles)[-1][0]
    files = [x for version,x in nfiles if version == latest_version]

    if dataset_id[:5] == 'CMIP6':
        missing, extra = check_spans(files, start, end, dataset_id)
    elif dataset_id[:4] == 'E3SM':
        if 'model-output.mon' in dataset_id:
            missing, extra = check_monthly(files, start, end)
        elif 'climo' in dataset_id:
            m, e = check_climos(files, start, end)
            missing.extend(m)
            extra.extend(e)
        elif 'time-series' in dataset_id:
            missing, extra = check_time_series(
                files, dataset_id, spec, start, end)
        elif 'fixed' in dataset_id:
            missing, extra = check_fixed(files, dataset_id, spec)
        else:
            missing, extra = check_submonthly(files, start, end, debug)

    for idx, item in enumerate(missing):
        missing[idx] = dataset_id + ': ' + item
    for idx, item in enumerate(extra):
        extra[idx] = dataset_id + ': ' + item
    return missing, dataset_id, extra


def sproket_with_id(dataset_id, sproket, spec, start=False, end=False, debug=False, **kwargs):

    if debug:
        print_message(f"Searching for: {dataset_id}", 'info')

    # create the path to the config, write it out
    tempfile = NamedTemporaryFile(suffix='.json')
    with open(tempfile.name, mode='w') as tmp:
        config_string = json.dumps({
            'search_api': "https://esgf-node.llnl.gov/esg-search/search/",
            'data_node_priority': ["esgf-data2.llnl.gov", "aims3.llnl.gov", "esgf-data1.llnl.gov"],
            'fields': {
                'dataset_id': dataset_id + '*',
                'latest': 'true'
            }
        })

        tmp.write(config_string)
        tmp.seek(0)

        cmd = [sproket, '-config', tempfile.name, '-y', '-urls.only']

        proc = Popen(cmd, shell=False, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
    if err:
        print(err.decode('utf-8'))
        return [], dataset_id, []

    nothing_found = False
    if not out:
        nothing_found = True

    files = sorted([i.decode('utf-8') for i in out.split()])
    if not files:
        nothing_found = True
    
    if nothing_found:
        if debug:
            msg = f'No dataset found: {dataset_id}'
            print_message(msg, 'error')
        return ['No dataset: ' + dataset_id], dataset_id, []
    else:
        return check_files(files, spec, start, end, debug)

# The typical CMIP6 path is:
# /CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/ts/gr/v20190719/ts_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_042601-045012.nc
# This file would have the dataset_id:
# CMIP6.CMIP.E3SM-project.E3SM-1-0.piControl.r1i1p1f1.Amon.ts#20190719

def collect_cmip_datasets(case_spec, **kwargs):
    # case_spec = kwargs['case_spec']
    model_versions = kwargs.get('model_versions', 'all')
    experiments = kwargs.get('experiments', 'all')
    ensembles = kwargs.get('ens', 'all')
    tables = kwargs.get('tables', 'all')
    variables = kwargs.get('variables', 'all')
    data_version = kwargs.get('data_version', 'all')
    data_types = kwargs.get('data_types', 'all')
    exclude = kwargs.get('exclude')
    debug = kwargs.get('debug')

    for source in case_spec['project']['CMIP6']:
        if facet_filter(source, model_versions, exclude):
            continue
        if debug:
            print_message('checking model version: ' + source, 'info')

        for case in case_spec['project']['CMIP6'][source]:
            if facet_filter(case['experiment'], experiments, exclude):
                continue
            if debug:
                print_message('  checking case: ' + case['experiment'], 'info')

            if 'all' in ensembles:
                ensembles_to_run = case['ens']
            else:
                if isinstance(ensembles, int):
                    ensembles_to_run = f'r{ensembles}i1f1p1'
                elif isinstance(ensembles, list) and isinstance(ensembles[0], int):
                    ensembles_to_run = [f'r{e}i1f1p1' for e in ensembles]

            for ensemble in ensembles_to_run:
                if facet_filter(ensemble, ensembles_to_run, exclude):
                    continue
                if debug:
                    print_message(f'\t\tchecking ensemble: {ensemble}', 'info')

                for table in case_spec['tables']:
                    if facet_filter(table, tables, [*exclude, *case.get('except', [])] ):
                        continue
                    if debug:
                        print_message(f'\t\t\tchecking table: {table}', 'info')

                    for variable in case_spec['tables'][table]:
                        if facet_filter(variable, variables, [*exclude, *case.get('except', [])] ):
                            continue

                        data_version = kwargs.get('data_version')
                        if data_version == 'latest':
                            data_version = '*'

                        dataset_id = f"CMIP6.*.E3SM-Project.{source}.{case['experiment']}.{ensemble}.{table}.{variable}.{data_version}"
                        yield dataset_id, case

def check_cmip(**kwargs):

    missing, extra, futures, dataset_ids = list(), list(), list(), list()

    sproket = kwargs.get('sproket')
    case_spec = kwargs.get('case_spec')
    debug = kwargs.get('debug')
    client = kwargs.get('client')

    if not client:
        pbar = tqdm(desc="Searching ESGF for datasets")

    for dataset_id, case in collect_cmip_datasets(**kwargs):
        if debug:
            msg = f'Looking up dataset {dataset_id}'
            print_message(msg, 'info')
        if client:
            futures.append(
                client.submit(
                    sproket_with_id,
                    dataset_id,
                    sproket,
                    case_spec,
                    case['start'],
                    case['end'],
                    debug))
        else:
            pbar.set_description(f'Looking up: {dataset_id}')
            m, _, e = sproket_with_id(
                dataset_id,
                sproket,
                case_spec,
                case['start'],
                case['end'],
                debug=debug)
            dataset_ids.append(dataset_id)
            pbar.update(1)
            if debug and not m and not e:
                msg = f'All files found for: {dataset_id}'
                print_message(msg, 'ok')
            if debug and m:
                msg = f'Missing dataset: {dataset_id}'
                print_message(msg, 'error')
            if debug and e:
                msg = f'Extra files found in dataset: {dataset_id}'
                print_message(msg, 'error')
            if m:
                missing.extend(m)
            if e:
                extra.extend(e)

    if client:
        pbar = tqdm(total=len(futures))
        for f in as_completed(futures):
            m, dataset_id, e = f.result()
            dataset_ids.append(dataset_id)
            if not m:
                pbar.set_description(f'All files found for: {dataset_id}')
            if debug and not m and not e:
                msg = f'All files found for: {dataset_id}'
                print_message(msg, 'ok')
            if debug and m:
                msg = f'Missing dataset: {dataset_id}'
                print_message(msg, 'error')
            if debug and e:
                msg = f'Extra files found in dataset: {dataset_id}'
                print_message(msg, 'error')
            if m:
                missing.extend(m)
            if e:
                extra.extend(e)
            pbar.update(1)
    pbar.close()

    dataset_ids = [{'id': ds} for ds in dataset_ids]
    return missing, extra, dataset_ids

def collect_e3sm_datasets(**kwargs):
    case_spec = kwargs['case_spec']
    model_versions = kwargs.get('model_versions', 'all')
    experiments = kwargs.get('experiments', 'all')
    ensembles = kwargs.get('ens', 'all')
    tables = kwargs.get('tables', 'all')
    variables = kwargs.get('variables', 'all')
    data_version = kwargs.get('data_version', 'all')
    data_types = kwargs.get('data_types', 'all')
    exclude = kwargs.get('exclude')
    debug = kwargs.get('debug')

    for version in case_spec['project']['E3SM']:
        if version not in model_versions and 'all' not in model_versions:
            continue
        if facet_filter(version, model_versions, exclude):
            continue
        if debug:
            print_message(f'checking version: {version}', 'info')
        for case in case_spec['project']['E3SM'][version]:

            # Check this case if its explicitly given by the user, or if default is set
            if facet_filter(case['experiment'], experiments, exclude):
                continue
            if debug:
                print_message(f"\tchecking case: {case['experiment']}", 'info')

            if 'all' in ensembles:
                ens = case['ens']
            else:
                if isinstance(ensembles, int):
                    ens = f'ens{ensembles}'
                elif isinstance(ensembles, list) and isinstance(ensembles[0], int):
                    ens = [f'ens{e}' for e in ensembles]
                else:
                    ens = ensembles

            for ensemble in ens:
                if debug:
                    print_message(f'\t\tchecking ensemble: {ensemble}', 'info')
                for res in case['resolution']:
                    for comp in case['resolution'][res]:
                        if facet_filter(comp, tables, exclude):
                            continue
                        for item in case['resolution'][res][comp]:
                            for data_type in item['data_types']:
                                if item.get('except') and data_type in item['except']:
                                    continue
                                if facet_filter(data_type, data_types, exclude):
                                    continue
                                dataset_id = f"E3SM.{version}.{case['experiment']}.{res}.{comp}.{item['grid']}.{data_type}.{ensemble}.*"
                                yield dataset_id, case['start'], case['end']


def check_e3sm(**kwargs):
    dataset_ids, missing, extra, futures = list(), list(), list(), list()
    case_spec = kwargs['case_spec']
    sproket = kwargs.get('sproket', 'sproket')
    client = kwargs.get('client')
    debug = kwargs.get('debug')

    if debug:
        for d in dataset_ids:
            print_message(d, 'info')
    if not client:
        pbar = tqdm(desc="Looking up datasets")

    for dataset_id, start, end in collect_e3sm_datasets(**kwargs):
        if client:
            futures.append(
                client.submit(
                    sproket_with_id,
                    dataset_id,
                    sproket,
                    case_spec,
                    start,
                    end))
        else:
            pbar.set_description(f'Checking: {dataset_id}')
            m, dataset_id, e = sproket_with_id(
                dataset_id,
                sproket,
                case_spec,
                start=start,
                end=end,
                debug=debug)
            missing.extend(m)
            extra.extend(e)
            dataset_ids.append(dataset_id)
            if not m and debug:
                print_message(f'All files found for: {dataset_id}', 'info')
            pbar.update(1)

    if client:
        pbar = tqdm(
            total=len(futures),
            desc='Contacting ESGF database')
        for f in as_completed(futures):
            res = f.result()
            m, dataset_id, e = res
            missing.extend(m)
            extra.extend(e)
            dataset_ids.append(dataset_id)
            if not m:
                pbar.set_description(f'All files found for: {dataset_id}')
            pbar.update(1)
        pbar.close()

    return missing, extra, dataset_ids


def check_datasets_by_id(client, sproket, dataset_ids, spec, *args, **kwargs):
    missing = list()
    extra = list()

    if client:

        pbar = tqdm(
            total=len(dataset_ids),
            desc='Contacting ESGF database')
        futures = [client.submit(
            sproket_with_id,
            d,
            sproket,
            spec,
            **kwargs) for d in dataset_ids]
        for f in as_completed(futures):
            m, dataset_id, e = f.result()
            if kwargs.get('debug'):
                msg = f'Checking: {dataset_id}'
                print_message(msg, 'info')
            if m:
                missing.extend(m)
            else:
                pbar.set_description(f'All files found for: {dataset_id}')
            pbar.update(1)
        pbar.close()

    else:
        for d in dataset_ids:
            if kwargs.get('debug'):
                msg = f'Checking: {d}'
                print_message(msg, 'info')

            m, dataset_id, e = sproket_with_id(d, sproket, spec, **kwargs)
            missing.append(m)

    return missing, extra


def publication_check(**kwargs):

    dataset_ids = kwargs.get('dataset_ids')
    client = kwargs.get('client')
    sproket = kwargs.get('sproket', 'sproket')
    case_spec = kwargs['case_spec']

    if dataset_ids:
        if isinstance(dataset_ids, str):
            dataset_ids = [dataset_ids]
        missing, extra = check_datasets_by_id(
            client,
            sproket,
            dataset_ids, 
            spec=case_spec, 
            debug=kwargs.get('debug'))
        return missing, extra

    projects = kwargs.get('projects')
    missing, extra, dataset_ids = list(), list(), list()
    if not projects or ('cmip6' in projects or 'CMIP6' in projects):
        print_message("Checking for CMIP6 project data", 'ok')
        missing, extra, cmip_ids = check_cmip(**kwargs)
        dataset_ids.extend(cmip_ids)
        if not missing:
            print_message('All CMIP6 files found', 'ok')
    else:
        print_message('Skipping CMIP6 datasets', 'ok')

    if not projects or ('e3sm' in projects or 'E3SM' in projects):
        print_message("Checking for E3SM project data", 'ok')
        m, e, e3sm_ids = check_e3sm(**kwargs)
        missing.extend(m)
        extra.extend(e)
        dataset_ids.extend(e3sm_ids)

        if not missing:
            print_message('All E3SM project files found', 'ok')
    else:
        print_message('Skipping E3SM project datasets', 'ok')
    dataset_ids = [{'id': ds} for ds in dataset_ids]
    return missing, extra, dataset_ids


def facet_filter(facet, facets, exclude=None):
    """
    Return True if the given facet should be filtered out
    """
    if (facet not in facets and 'all' not in facets) or (exclude and facet in exclude):
        return True
    return False


def collect_paths(data_path=None, case_spec=None, projects=None, model_versions='all', experiments='all', tables='all', variables='all', ens='all', exclude=None, debug=False, **kwargs):

    dataset_paths, dataset_ids, extra = list(), list(), list()

    for project in os.listdir(data_path):
        if facet_filter(project, projects, exclude):
            continue
        if debug:
            print_message(f'checking project: {project}', 'info')
        project_path = os.path.join(data_path, project)

        if project == 'CMIP6':
            """
            The CMIP6 paths go like
            /base/CMIP6/cmip_project/model_name/case_id/realization_id/table/variable/gr/dataset_version
            """
            for cmip_project in os.listdir(project_path):
                cmip_project_path = os.path.join(
                    project_path, cmip_project, 'E3SM-Project')
                if debug:
                    print_message(f' checking cmip-project: {cmip_project}', 'info')
                if not os.path.exists(cmip_project_path):
                    continue
                for model_version in os.listdir(cmip_project_path):
                    if facet_filter(model_version, model_versions, exclude):
                        continue
                    model_version_path = os.path.join(
                        cmip_project_path, model_version)
                    if debug:
                        print_message(f'  checking model_version: {model_version}', 'info')
                    for case in os.listdir(model_version_path):
                        if facet_filter(case, experiments, exclude):
                            continue
                        if debug:
                            print_message(f'   checking experiment: {case}', 'info')
                        case_path = os.path.join(model_version_path, case)

                        for e in os.listdir(case_path):
                            if facet_filter(e, ens, exclude):
                                continue
                            if debug:
                                print_message(f'    checking ensemble: {e}', 'info')
                            ensemble_path = os.path.join(case_path, e)

                            for table in os.listdir(ensemble_path):
                                if facet_filter(table, tables, exclude):
                                    continue
                                table_path = os.path.join(ensemble_path, table)
                                if debug:
                                    print_message(f'     checking table: {table}', 'info')
                                for v in os.listdir(table_path):
                                    if facet_filter(v, variables, exclude):
                                        continue
                                    variable_path = os.path.join(table_path, v)

                                    # pick just the last version
                                    try:
                                        versions = os.listdir(
                                            os.path.join(variable_path, 'gr'))
                                    except:
                                        import ipdb; ipdb.set_trace()

                                    data_version = kwargs.get('data_version', 'latest')
                                    if data_version == 'latest':
                                        try:
                                            version = sorted(versions)[-1]
                                        except IndexError as e:
                                            raise ValueError(f'Unable to find latest version for {v}') from e
                                    else:
                                        version = data_version
                                    if debug:
                                        print_message(f'      checking variable-{version}: {v}', 'info')

                                    dataset_id = '.'.join(
                                        [project, cmip_project, 'E3SM-Project', model_version, case, e, table, v, 'gr#'+version])
                                    dataset_path = os.path.join(
                                        variable_path, 'gr', version)
                                    if not os.path.exists(dataset_path):
                                        print(f"Cant find requested version, skipping {dataset_id}")
                                        continue
                                    dataset_paths.append(dataset_path)
                                    dataset_ids.append(dataset_id)
        elif project == 'E3SM':
            project_info = case_spec['project'].get(project)
            for model_version in os.listdir(project_path):
                if facet_filter(model_version, model_versions, exclude):
                    continue
                if model_version not in project_info.keys():
                    msg = f'Project not found in data spec: {project}:{model_version}'
                    extra.append(msg)
                    continue
                model_info = project_info[model_version]
                for casename in os.listdir(os.path.join(project_path, model_version)):
                    if facet_filter(casename, cases, exclude):
                        continue
                    case_info = next(
                        (i for i in model_info if i['experiment'] == casename), None)
                    if not case_info:
                        msg = f"Couldnt find case in dataset specifications: {casename}"
                        extra.append(msg)
                        continue

                    case_path = os.path.join(
                        project_path, model_version, casename)
                    for res in os.listdir(case_path):
                        tuning = None
                        if res not in case_info.get('resolution').keys():
                            msg = f"Resolution {res} is present on filesystem but not in case specification: {casename}"
                            extra.append(msg)
                            continue
                        res_path = os.path.join(case_path, res)
                        for comp in os.listdir(res_path):
                            if comp in ['highres', 'lowres']:
                                tuning = comp
                                for comp in os.listdir(os.path.join(res_path, tuning)):
                                    comp_path = os.path.join(
                                        res_path, tuning, comp)
                                    comp_name = next(
                                        (i for i in case_info['resolution'][res].keys() if comp in i), None)
                                    for grid in os.listdir(comp_path):
                                        try:
                                            comp_info = next(
                                                (i for i in case_info['resolution'][res][comp_name] if i['grid'] == grid), None)
                                        except:
                                            import ipdb
                                            ipdb.set_trace()
                                        if not comp_info:
                                            msg = f"Grid {grid} is present on filesystem but not in case specification: {casename}-{res}-{comp}"
                                            extra.append(msg)
                                            continue
                                        for data_type in os.listdir(os.path.join(comp_path, grid)):
                                            for freq in os.listdir(os.path.join(comp_path, grid, data_type)):
                                                if facet_filter(freq, tables, exclude):
                                                    continue
                                                freq_type = f"{data_type}.{freq}"
                                                if freq_type not in comp_info['data_types']:
                                                    msg = f"Frequency {freq} is present on filesystem but not in case specification: {casename}-{res}-{comp}-{grid}"
                                                    extra.append(msg)
                                                    continue
                                                for ensemble in os.listdir(os.path.join(comp_path, grid, data_type, freq)):
                                                    if facet_filter(ensemble, ens, exclude):
                                                        continue
                                                    if ensemble not in case_info['ens']:
                                                        msg = f"Ensemble {ensemble} is present on filesystem but not in case specification: {casename}-{res}-{comp}-{grid}-{freq}"
                                                        extra.append(msg)
                                                        continue

                                                    version = sorted(os.listdir(os.path.join(
                                                        comp_path, grid, data_type, freq, ensemble)))[-1]
                                                    dataset_path = os.path.join(
                                                        comp_path, grid, data_type, freq, ensemble, version)
                                                    dataset_id = '.'.join(
                                                        ['E3SM', model_version, casename, res, comp, grid, data_type, freq, ensemble, version])
                                                    if debug:
                                                        print_message(f'checking dataset: {dataset_id}', 'info')
                                                    dataset_paths.append(
                                                        dataset_path)
                                                    dataset_ids.append(
                                                        dataset_id)
                                                    continue
                            else:
                                if facet_filter(comp, tables, exclude):
                                    continue
                                if comp not in case_info['resolution'][res].keys():
                                    msg = f"Component {comp} is present on filesystem but not in case specification: {casename}-{res}"
                                    extra.append(msg)
                                    continue

                                comp_path = os.path.join(res_path, comp)
                                for grid in os.listdir(comp_path):
                                    comp_info = next(
                                        (i for i in case_info['resolution'][res][comp] if i['grid'] == grid), None)
                                    if not comp_info:
                                        msg = f"Grid {grid} is present on filesystem but not in case specification: {casename}-{res}-{comp}"
                                        extra.append(msg)
                                        continue
                                    for data_type in os.listdir(os.path.join(comp_path, grid)):
                                        for freq in os.listdir(os.path.join(comp_path, grid, data_type)):
                                            if facet_filter(freq, tables, exclude):
                                                continue
                                            freq_type = f"{data_type}.{freq}"
                                            if freq_type not in comp_info['data_types']:
                                                import ipdb
                                                ipdb.set_trace()
                                                msg = f"Frequency freq is present on filesystem but not in case specification: {casename}-{res}-{comp}-{grid}"
                                                extra.append(msg)
                                                continue
                                            for ensemble in os.listdir(os.path.join(comp_path, grid, data_type, freq)):
                                                if facet_filter(ensemble, ens, exclude):
                                                    continue
                                                if ensemble not in case_info['ens']:
                                                    msg = f"Ensemble {ensemble} is present on filesystem but not in case specification: {casename}-{res}-{comp}-{grid}-{freq}"
                                                    extra.append(msg)
                                                    continue

                                                version = sorted(os.listdir(os.path.join(
                                                    comp_path, grid, data_type, freq, ensemble)))[-1]

                                                dataset_path = os.path.join(
                                                    comp_path, grid, data_type, freq, ensemble, version)
                                                dataset_id = '.'.join(
                                                    ['E3SM', model_version, casename, res, comp, grid, data_type, freq, ensemble, version])
                                                dataset_paths.append(
                                                    dataset_path)
                                                dataset_ids.append(dataset_id)
                                                if debug:
                                                    print_message(f'checking dataset: {dataset_id}', 'info')
    return dataset_paths, dataset_ids, extra


def filesystem_check(client, case_spec=None, debug=False, **kwargs):
    """
    Walk down directories on the filesystem checking that every dataset that should be there is, and that there
    are no extra files. If verify is turned on, run a square variance check and plot suspicious time steps.
    """

    missing, futures = list(), list()
    print_message("Starting file-system check", 'ok')
    dataset_paths, dataset_ids, extra = collect_paths(**kwargs)
    expected_datasets = [x for x in collect_cmip_datasets(case_spec, **kwargs)]

    if not client:
        pbar = tqdm(total=len(dataset_paths))

    for idx, dataset_path in enumerate(dataset_paths):
        dataset_id = dataset_ids[idx]
        files = [os.path.join(dataset_path, x) for x in sorted(os.listdir(dataset_path))]
        if not files:
            missing.append(dataset_id)
            continue
        id_split = dataset_id.split('.')
        project = id_split[0]
        if project == 'E3SM':
            model_version = id_split[1]
            case = id_split[2]
        else:
            model_version = id_split[3]
            case = id_split[4]
        start = None
        end = None

        for experiment in case_spec['project'][project][model_version]:
            if experiment['experiment'] == case:
                start = experiment['start']
                end = experiment['end']
                break
        if not start or not end:
            raise ValueError(
                f"No start and/or end found in the case_spec for {dataset_id}")
        if client:
            futures.append(
                client.submit(
                    check_files,
                    files, 
                    case_spec, 
                    start, 
                    end))
        else:
            m, d, e = check_files(files, case_spec, start, end)
            pbar.update(1)
            for idx, item in enumerate(e):
                e[idx] = f'{d}: {item}'
            for idx, item in enumerate(m):
                m[idx] = f'{d}: {item}'
            missing.extend(m)
            extra.extend(e)

    if client:
        pbar = tqdm(total=len(futures))
        for f in as_completed(futures):
            res = f.result()
            m, d, ex = res[0], res[1], res[2]
            for idx, item in enumerate(ex):
                ex[idx] = f'{d}: {item}'
            for idx, item in enumerate(m):
                m[idx] = f'{d}: {item}'
            missing.extend(m)
            extra.extend(ex)
            pbar.update()
    pbar.close()

    print_message("File-system check complete", 'ok')
    return missing, extra


def verification(plot_path=None, debug=None, **kwargs):
    issues, futures = list(), list()
    print_message("Starting dataset verification", 'ok')
    dataset_paths, dataset_ids, _ = collect_paths(**kwargs)

    pbar = tqdm(total=len(dataset_paths))

    for idx, dataset_path in enumerate(dataset_paths):
        dataset_id = dataset_ids[idx]

        # sample id: CMIP6.CMIP.E3SM-Project.E3SM-1-1.historical.r1i1p1f1.Amon.clivi.gr#v20191211
        id_split = dataset_id.split('.')
        variable = id_split[7]

        pbar.set_description(f"Validating {variable}")
        issues.extend(
            verify_dataset(
                dataset_path,
                dataset_id,
                variable,
                plot_path,
                debug))
        pbar.update(1)
    
    pbar.close()

    print_message("Dataset verification complete", 'ok')
    return issues


def data_check(**kwargs):

    debug = kwargs.get('debug')
    if debug:
        print_message("Running in debug mode", 'info')

    published = kwargs.get('published')
    sproket = kwargs.get('sproket')
    if published and not sproket:
        raise ValueError(
            "Publication checking is turned on, but no sproket utility path given")

    data_path = kwargs.get('data_path')
    if data_path and not os.path.exists(data_path):
        raise ValueError("Given data path does not exist")

    spec_path = kwargs.get('spec_path')
    if not os.path.exists(spec_path):
        raise ValueError("Given case spec file does not exist")

    verify = kwargs.get('verify')
    if verify and not data_path:
        raise ValueError(
            "--data-path must be set if dataset verification is turned on")

    projects = kwargs.get('projects', 'all')
    if 'all' not in projects:
        if isinstance(projects, list):
            projects = [x.upper() for x in projects]
        else:
            projects = [projects.upper()]
    kwargs['projects'] = projects

    with open(spec_path, 'r') as ip:
        case_spec = yaml.load(ip, Loader=yaml.SafeLoader)

    serial = kwargs.get('serial')
    cluster_address = kwargs.get('cluster_address')
    num_workers = kwargs.get('num_workers', 4)
    if serial:
        pool = None
    else:
        if debug:
            print_message(f'Creating processpool with {num_workers} workers', 'info')

        pool = ProcessPoolExecutor(max_workers=num_workers)

    dataset_ids = kwargs.get('dataset_ids')
    if dataset_ids:
        published = True
    missing, extra, issues = list(), list(), list()
    try:
        if published:
            m, e, dataset_ids = publication_check(
                case_spec=case_spec,
                client=pool,
                **kwargs)
            missing.extend(m)
            extra.extend(e)

        file_system = kwargs.get('file_system')
        if file_system and data_path:
            m, e = filesystem_check(
                case_spec=case_spec,
                client=pool,
                **kwargs)
            missing.extend(m)
            extra.extend(e)

        if verify and data_path:
            from esgfpub.verify import verify_dataset
            issues = verification(**kwargs)
    finally:
        if pool:
            pool.shutdown()
    
    digest = kwargs.get('digest')
    if digest:
        filtered_extra, filtered_missing = list(), list()

        for m in missing:
            try:
                idx = m.index(':')
            except:
                print(m)
                continue
            if m[:idx] == 'No dataset':
                filtered_missing.append(m)
            else:
                if m[:idx] not in filtered_missing:
                    filtered_missing.append(m[:idx])
        missing = filtered_missing

        for e in extra:
            idx = e.index(':')
            if e[:idx] not in filtered_extra:
                filtered_extra.append(e[:idx])
        extra = filtered_extra

    to_json = kwargs.get('to_json')
    if not to_json:
        tmp = NamedTemporaryFile(suffix=".json")
        to_json = tmp.name
    print_message(f'Missing datasets being written to {to_json}')
    data = {
        'missing': missing,
        'extra': extra }
    if verify:
        data['issues'] = issues
    if os.path.exists(to_json):
        os.remove(to_json)
    with open(to_json, 'w') as op:
        json.dump(data, op, indent=4, sort_keys=True)
        
    report_plot = kwargs.get('report_plot')
    if report_plot:
        dataset_report(
            json_path=to_json, 
            plot_path=report_plot,
            dataset_ids=dataset_ids)

    else:
        if missing:
            print_message("Missing files:")
            for m in missing:
                print_message(f"\t{m}")
            print('-----------------------------------')
        else:
            print_message("No missing files", 'ok')

        if extra:
            print_message("Extra files:")
            for e in extra:
                print_message(f"\t{e}")
            print('-----------------------------------')
        else:
            print_message("No extra files", 'ok')

        if issues and len(issues) > 0:
            print_message("File issues:")
            for i in issues:
                print_message(f"\t{i}")
            print('-----------------------------------')
        else:
            print_message("No file issues", 'ok')

    return 0


def dataset_report(json_path: str, plot_path: str, dataset_ids: list):
    """
    Create a nice plot reporting the status of the published datasets
    Params:
        json_path: the path to the json dataset info
        plot_path: the path to where the plot should be saved
        dataset_ids: list of strings of dataset IDs
    Returns:
        None
    """

    dataset_info = {}
    for dinfo in dataset_ids:
        dataset_id = dinfo['id']
        split = dataset_id.split('.')
        now = datetime.now()
        if split[0] == 'E3SM':
            title = f'E3SM project publication status {now.month}-{now.day}-{now.year}'
            casename = f'{split[1]}.{split[2]}.{split[-2]}'
        else:
            title = f'E3SM data in CMIP6 project publication status {now.month}-{now.day}-{now.year}'
            casename = f'{split[3]}.{split[4]}.{split[5]}'
        
        if casename not in dataset_info.keys():
            dataset_info[casename] = {}
        
        dataset_info[casename][dataset_id] = 'good'

    with open(json_path, 'r') as ip:
        raw_info = json.load(ip)

    no_dataset_str = 'No dataset: '
    for missing in raw_info['missing']:
        if no_dataset_str in missing:
            idx = missing.index(no_dataset_str)
            split = missing[idx + len(no_dataset_str):].split('.')
            dataset_id = '.'.join(split)
        else:
            split = missing.split('.')
            dataset_id = missing.split(':')[0]

        if split[0] == 'E3SM':
            casename = f'{split[1]}.{split[2]}.{split[8]}'
        else:
            casename = f'{split[3]}.{split[4]}.{split[5]}'
        
        if not dataset_info.get(casename):
            print(f"problem looking up {casename}")
            continue

        if not dataset_info[casename].get(dataset_id):
            dataset_info[casename][dataset_id] = 'error'
        else:
            if dataset_info[casename][dataset_id] != 'error':
                dataset_info[casename][dataset_id] = 'error'

    correct = []
    error = []
    casenames = sorted(list(dataset_info.keys()))
    for casename in casenames:
        num_good, num_bad = 0, 0
        for _, val in dataset_info[casename].items():
            if val == 'good':
                num_good += 1
            else:
                num_bad += 1
        correct.append(num_good)
        error.append(num_bad)

    width = 0.35
    ind = np.arange(len(dataset_info.keys()))

    fig, (barax, pieax) = plt.subplots(2, figsize=(20,10))

    p1 = barax.bar(ind, correct, width)
    p2 = barax.bar(ind, error, width, bottom=correct)
    # set the bar chart xticks
    barax.set_xticks(ind)
    barax.set_xticklabels(tuple(x for x in dataset_info.keys()))
    # set the barchar axis labels
    barax.set_ylabel('datasets')
    barax.set_xlabel('')
    # rotate the xaxis ticks
    plt.setp(barax.get_xticklabels(), rotation=30, horizontalalignment='right')
    # add the legend
    barax.legend((p1[0], p2[0]), ('published', 'missing'))
    
    pieax.pie([sum(correct), sum(error)], 
                explode=(0.1, 0),
                shadow=True,
                labels=(f'{sum(correct)} published', f'{sum(error)} missing'),
                autopct='%1.1f%%')
    pieax.axis('equal')
    

    fig.suptitle(title)
    plt.subplots_adjust(hspace=0.5)
    plt.savefig(plot_path)

