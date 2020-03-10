import os
import yaml
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


def check_spans(files, start, end, dataset_id):
    missing = []
    files_found = []

    if not start or not end:
        start, end = infer_start_end_cmip(files)
    if not start and not end:
        start, end = infer_start_end_e3sm(files)

    f_start, f_end = get_cmip_start_end(files[0])
    freq = f_end - f_start + 1
    spans = list(range(start, end, freq))

    for idx, span_start in enumerate(spans):
        found_span = False
        if span_start != spans[-1]:
            span_end = spans[idx + 1] - 1
        else:
            span_end = end

        for f in files:
            f_start, f_end = get_cmip_start_end(f)
            if f_start == span_start and f_end == span_end:
                found_span = True
                files_found.append(f)
                break
        if not found_span:
            missing.append("{var}-{start:04d}-{end:04d}".format(
                var=dataset_id, start=span_start, end=span_end))

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
    files_found = []

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
            else:
                files_found.append(name)

    extra_files = [x for x in files if x not in files_found]
    return missing, extra_files


def check_monthly_climos(files, start, end):
    # FIX ME
    missing = []
    files_found = []

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
        found_span = False
        if span_start != spans[-1]:
            span_end = spans[idx + 1] - 1
        else:
            span_end = end

        found_span = False
        span_end = spans[idx + 1] - 1

        for month in range(1, 13):
            name = '{prefix}{month:02d}_{start:04d}{month:02d}_{end:04d}{month:02d}_climo.nc'.format(
                prefix=prefix, month=month, start=span_start, end=span_end)
            if name not in files:
                missing.append(name)
            else:
                files_found.append(name)
    extra_files = [x for x in files if x not in files_found]
    return missing, extra_files


def check_seasonal_climos(files, start, end):
    missing = []
    files_found = []
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

        if span_start != spans[-1]:
            span_end = spans[idx + 1] - 1
        else:
            span_end = end

        found_span = False
        name = '{prefix}_ANN_{span_start:04d}01_{span_end:04d}12_climo.nc'.format(
            prefix=prefix, span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)
        else:
            files_found.append(name)

        name = '{prefix}_DJF_{span_start:04d}01_{span_end:04d}12_climo.nc'.format(
            prefix=prefix, span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)
        else:
            files_found.append(name)

        name = '{prefix}_MAM_{span_start:04d}03_{span_end:04d}05_climo.nc'.format(
            prefix=prefix, span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)
        else:
            files_found.append(name)

        name = '{prefix}_JJA_{span_start:04d}06_{span_end:04d}08_climo.nc'.format(
            prefix=prefix, span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)
        else:
            files_found.append(name)

        name = '{prefix}_SON_{span_start:04d}09_{span_end:04d}11_climo.nc'.format(
            prefix=prefix, span_start=span_start, span_end=span_end)
        if name not in files:
            missing.append(name)
        else:
            files_found.append(name)

    extra_files = [x for x in files if x not in files_found]
    return missing, extra_files


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

    extra_files = []
    return missing, prefix, extra_files


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
    files = [x.split('/')[-1] for x in sorted(files)]
    files_found = []
    if not start or not end:
        start, end = get_ts_start_end(files[0])

    case_info = dataset_id.split('.')
    model_version = case_info[1]
    casename = case_info[2]
    realm = case_info[4]
    ens = case_info[6][1:]
    case_spec = [x for x in spec['project']['E3SM']
                 [model_version] if x['experiment'] == casename].pop()
    if case_spec.get('except'):
        expected_vars = [x for x in spec['time-series']
                         [realm] if x not in case_spec['except']]
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
                if span_start != spans[-1]:
                    span_end = spans[idx + 1] - 1
                else:
                    span_end = end

                found_span = False
                span_end = spans[idx + 1] - 1

                for f in files:
                    f_start, f_end = get_ts_start_end(f)
                    if f_start == span_start and f_end == span_end:
                        found_span = True
                        files_found.append(f)
                        break
                if not found_span:
                    missing.append('{dataset}-{var}-{start:04d}-{end:04d}'.format(
                        dataset=dataset_id, var=v, start=span_start, end=span_end))
    extra_files = [x for x in files if x not in files_found]
    return missing, extra


def check_files(files, dataset_id, start, end):

    missing = []
    extra = []
    if '.fx.' in dataset_id and files:
        return [], []

    if dataset_id[:5] == 'CMIP6':
        missing, extra = check_spans(files, start, end, dataset_id)
    elif dataset_id[:4] == 'E3SM':
        if 'mon.' in dataset_id:
            missing, extra = check_monthly(files, start, end)
        elif 'monClim.' in dataset_id or 'monClim-' in dataset_id:
            missing, extra = check_monthly_climos(files, start, end)
        elif 'seasonClim.' in dataset_id or 'seasonClim-' in dataset_id:
            missing, extra = check_seasonal_climos(files, start, end)
        elif 'clim.' in dataset_id or 'clim-' in dataset_id:
            missing = []
            m, e = check_monthly_climos(files, start, end)
            missing.extend(m)
            extra.extend(e)

            m, e = check_seasonal_climos(files, start, end)
            missing.extend(m)
            extra.extend(e)
        elif 'time-series' in dataset_id:
            missing, extra = check_time_series(
                files, dataset_id, spec, start, end)
        elif 'fixed' in dataset_id:
            missing, extra = check_fixed(files, dataset_id, spec)
        else:
            missing, extra = check_submonthly(files, start, end)
    return missing, extra


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
        return [], dataset_id, []

    if not out:
        return [dataset_id], dataset_id, []

    files = sorted([i.decode('utf-8') for i in out.split()])
    missing, extra = check_files(files, dataset_id, start, end)
    return missing, dataset_id, extra

# The typical CMIP6 path is:
# /CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/ts/gr/v20190719/ts_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_042601-045012.nc
# This file would have the dataset_id:
# CMIP6.CMIP.E3SM-project.E3SM-1-0.piControl.r1i1p1f1.Amon.ts#20190719


def check_cmip(client, dataset_spec, data_path, experiments, ensembles, tables, variables, published, sproket, debug=False):

    missing = []
    extra = []
    futures = []

    for source in dataset_spec['project']['CMIP6']:
        if debug:
            print_message('checking model version: ' + source, 'info')
        for case in dataset_spec['project']['CMIP6'][source]:

            # Check this case if its explicitly given by the user, or if default is set
            if 'all' not in experiments and case['experiment'] not in experiments:
                continue

            if debug:
                print_message('  checking case: ' + case['experiment'], 'info')

            if 'all' in ensembles:
                ensembles = case['ens']
            else:
                if isinstance(ensembles, int):
                    ensembles = 'r{}i1f1p1'.format(ensembles)
                elif isinstance(ensembles, list) and isinstance(ensembles[0], int):
                    ensembles = ['r{}i1f1p1'.format(e) in ensembles]

            for ensemble in ensembles:
                if debug:
                    print_message('    checking ensemble: ' + ensemble, 'info')
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
                            missing, dataset_id, extra = sproket_with_id(
                                dataset_id,
                                sproket,
                                dataset_spec,
                                case['start'],
                                case['end'])
                            if missing:
                                for m in missing:
                                    print_message(
                                        'Missing file found: {}'.format(m))
                            else:
                                print_message(
                                    'All files found for: {}'.format(dataset_id), 'ok')
                            if extra:
                                for e in extra:
                                    print_message(
                                        'EXTRA file found: {}'.format(e))

    if client:
        pbar = tqdm(
            total=len(futures),
            desc='Contacting ESGF database')
        for f in as_completed(futures):
            res = f.result()
            try:
                m, dataset_id, e = res
            except:
                import ipdb
                ipdb.set_trace()

            if m:
                missing.extend(m)
            else:
                pbar.set_description(
                    'All files found for: {}'.format(dataset_id))

            if e:
                extra.extend(e)

            pbar.update(1)
        pbar.close()

    if missing:
        for m in missing:
            print_message('Missing: {}'.format(m))
    if extra:
        for e in extra:
            print_message('Extra: {}'.format(m))

    return missing, extra


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
                                        print_message(
                                            'All files found for: {}'.format(dataset_id), 'ok')

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
                pbar.set_description(
                    'All files found for: {}'.format(dataset_id))
            pbar.update(1)
        pbar.close()

    if missing:
        if not to_json:
            for m in missing:
                print_message('Missing: {}'.format(m))
        return True  # true there was an error
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
                pbar.set_description(
                    'All files found for: {}'.format(dataset_id))
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
                print_message(
                    'All files found for: {}'.format(dataset_id), 'ok')


def publication_check(client, dataset_spec, data_path, ensembles, experiments, tables, variables, sproket, debug):
    missing, extra = list(), list()
    if dataset_ids:
        if isinstance(dataset_ids, str):
            dataset_ids = [dataset_ids]
        check_datasets_by_id(client, sproket, dataset_ids)
        return

    if not projects or ('cmip6' in projects or 'CMIP6' in projects):
        print_message("Checking for CMIP6 project data", 'ok')
        missing, extra = check_cmip(
            client=client,
            dataset_spec=case_spec,
            data_path=data_path,
            ensembles=ens,
            experiments=cases,
            tables=tables,
            variables=variables,
            sproket=sproket,
            debug=debug)
        if not missing:
            print_message('All CMIP6 files found', 'ok')
    else:
        print_message('Skipping CMIP6 datasets', 'ok')

    if not projects or ('e3sm' in projects or 'E3SM' in projects):
        print_message("Checking for E3SM project data", 'ok')
        m, e = check_e3sm(
            client=client,
            dataset_spec=case_spec,
            data_path=data_path,
            ensembles=ens,
            experiments=cases,
            tables=tables,
            variables=variables,
            sproket=sproket,
            to_json=to_json,
            debug=debug)
        missing.extend(m)
        extra.extend(e)

        if not missing:
            print_message('All E3SM project files found', 'ok')
    else:
        print_message('Skipping E3SM project datasets', 'ok')

    if to_json:
        print_message(
            'Missing datasets being written to {}'.format(to_json))
        data = {
            'missing': missing,
            'extra': extra }
        if os.path.exists(to_json):
            os.remove(to_json)
        with open(to_json, 'w') as op:
            json.dump(data, op, indent=4, sort_keys=True)
    return

def facet_filter(facet, facets):
    """
    Return True if the given facet should be filtered out
    """
    if facet not in facets and 'all' not in facets:
        return True
    return False

def filesystem_check(client, data_path, plot_path, case_spec, projects, model_versions, cases, tables, variables, ens, verify=False, serial=False, to_json=False, debug=False):
    """
    Walk down directories on the filesystem checking that every dataset that should be there is, and that there
    are no extra files. If verify is turned on, run a square variance check and plot suspicious time steps.
    """
    missing, extra, futures = list(), list(), list()
    print_message("Starting file-system check", 'ok')
    for project in os.listdir(data_path):
        if project not in projects and 'all' not in projects:
            continue

        project_path = os.path.join(data_path, project)
        if project == 'CMIP6':
            for cmip_project in os.listdir(project_path):
                cmip_project_path = os.path.join(project_path, cmip_project, 'E3SM-Project')

                for model_version in os.listdir(cmip_project_path):
                    model_version_path = os.path.join(cmip_project_path, model_version)

                    for case in os.listdir(model_version_path):
                        if case not in cases and 'all' not in cases:
                            continue
                        case_path = os.path.join(model_version_path, case)

                        for e in os.listdir(case_path):
                            if e not in ens and 'all' not in ens:
                                continue
                            ensemble_path = os.path.join(case_path, e)

                            for table in os.listdir(ensemble_path):
                                if table not in tables and 'all' not in tables:
                                    continue
                                table_path = os.path.join(ensemble_path, table)

                                for v in os.listdir(table_path):
                                    if v not in variables and 'all' not in variables:
                                        continue
                                    variable_path = os.path.join(table_path, v)
                                    version = sorted(os.listdir(os.path.join(variable_path, 'gr')))[-1]
                                    dataset_id = '.'.join([project, cmip_project, 'E3SM-Project', model_version, case, e, table, v, 'gr#'+version])
                                    
                                    files = sorted(os.listdir(os.path.join(variable_path, 'gr', version)))
                                    for experiment in case_spec['project'][project][model_version]:
                                        if experiment['experiment'] == case:
                                            start = experiment['start']
                                            end = experiment['end']
                                            break
                                    futures.append(
                                        client.submit(
                                            check_files, 
                                                files, dataset_id, start, end))
        elif project == 'E3SM':
            
            project_info = case_spec['project'].get(project)
            for model_version in os.listdir(project_path):
                if facet_filter(model_version, model_versions):
                    continue
                if model_version not in project_info.keys():
                    print_message('Project not found in data spec: {}:{}'.format(project, model_version))
                    continue
                model_info = project_info[model_version]
                for casename in os.listdir(os.path.join(project_path, model_version)):
                    if facet_filter(casename, cases):
                        continue
                    case_info = next((i for i in model_info if i['experiment'] == casename), None)
                    if not case_info:
                        print_message("Couldnt find case in dataset specifications: {}".format(casename))
                        continue

                    case_path = os.path.join(project_path, model_version, casename)
                    for res in os.listdir(case_path):
                        if res not in case_info.get('resolution').keys():
                            print_message("Couldnt find resolution {} in specification for case {}".format(res, casename))
                            continue
                        res_path = os.path.join(case_path, res)
                        for comp in os.listdir(res_path):
                            if facet_filter(comp, tables):
                                continue
                            if comp not in case_info['resolution'][res].keys():
                                print_message("Couldnt find component {} in specification for case {}-{}".format(comp, casename, res))
                                continue
                            comp_path = os.path.join(res_path, comp)
                            for grid in os.listdir(comp_path):
                                comp_info = next((i for i in case_info['resolution'][res][comp] if i['grid'] == grid), None)
                                if not comp_info:
                                    print_message("Couldnt find component {} in specification for case {}-{}-{}".format(grid, casename, res, comp))
                                    continue
                                for data_type in os.listdir(os.path.join(comp_path, grid)):
                                    for freq in os.listdir(os.path.join(comp_path, grid, data_type)):
                                        if facet_filter(freq, tables):
                                            continue
                                        if freq not in comp_info['data_types']:
                                            print_message("Couldnt find component {} in specification for case {}-{}-{}-{}".format(freq, casename, res, comp, grid))
                                            continue
                                        for ensemble in os.listdir(os.path.join(comp_path, grid, data_type, freq)):
                                            if facet_filter(ensemble, ens):
                                                continue
                                            if ensemble not in case_info['ens']:
                                                print_message("Couldnt find component {} in specification for case {}-{}-{}-{}-{}".format(ensemble, casename, res, comp, grid, freq))
                                                continue

                                            version = sorted(os.listdir(os.path.join(comp_path, grid, data_type, freq, ensemble)))[-1]
                                            files = sorted(os.listdir(os.path.join(comp_path, grid, data_type, freq, ensemble, version)))
                                            dataset_id = '.'.join(['E3SM', model_version, casename, res, comp, grid, data_type, freq, ensemble, version])
                                            if debug:
                                                print_message("Checking: {}".format(dataset_id), "info")
                                            futures.append(
                                                client.submit(
                                                    check_files, files, dataset_id, case_info['start'], case_info['end']))
                                            
    pbar = tqdm(total=len(futures))
    for f in as_completed(futures):
        m, ex = f.result()
        missing.extend(m)
        extra.extend(ex)
        pbar.update()
    pbar.close()
    if missing:
        for m in missing:
            print_message("Missing file: {}".format(m))
    else:
        print_message("Found all files", 'ok')
    if extra:
        for e in extra:
            print_message("Extra file: {}".format(e))
    if to_json:
        # APPEND TO THE JSON OUTPUT
        pass


    
    return




def data_check(
        variables,
        spec_path,
        cases,
        ens,
        tables,
        published,
        projects,
        data_path=False,
        model_versions=None,
        verify=False,
        dataset_ids=None,
        sproket=None,
        max_connections=4,
        debug=False,
        serial=False,
        to_json=False):

    if debug:
        print_message("Running in debug mode", 'info')

    if published and not sproket:
        raise ValueError(
            "Publication checking is turned on, but no sproket utility path given")

    if data_path and not os.path.exists(data_path):
        raise ValueError("Given data path does not exist")
    if not os.path.exists(spec_path):
        raise ValueError("Given case spec file does not exist")

    with open(spec_path, 'r') as ip:
        case_spec = yaml.load(ip, Loader=yaml.SafeLoader)


    if serial:
        client = None
    else:
        if debug:
            print_message('Setting up dask workers', 'info')
        cluster = LocalCluster(
            n_workers=1,
            processes=True,
            threads_per_worker=max_connections)
        client = Client(cluster)
        if debug:
            print_message('... worker setup complete', 'info')

    if published:
        publication_check(
            client, 
            case_spec, 
            data_path, 
            ens, 
            cases, 
            tables, 
            variables, 
            sproket, 
            debug)
    
    if data_path:
        filesystem_check(
            client=client,
            data_path=data_path, 
            plot_path='', 
            case_spec=case_spec, 
            projects=projects,
            model_versions=model_versions,
            cases=cases,
            tables=tables,
            variables=variables, 
            ens=ens,
            debug=debug)
    
    if client:
        client.close()
        cluster.close()

    return 0
