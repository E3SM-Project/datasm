import os
import sys
import yaml
import argparse
from pprint import pprint
import copy

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--yaml-template', required=True)
    parser.add_argument('-o', '--outpath', default='./configs')
    args = parser.parse_args()

    with open(args.yaml_template, 'r') as ip:
        template = yaml.load(ip, Loader=yaml.SafeLoader)
    
    models = ['1_1', '1_1_ECA']
    cases = ['piControl', 'BCRC', 'BCRD', 'BDRC', 'BDRD']

    for m in models:
        for case in cases:
            values = copy.deepcopy(template)
            values['model_version'] = m
            if case != 'piControl':
                values['experiment'] = 'hist-' + case
            else:
                values['experiment'] = case
                
            for data_type, _ in values['data_paths'].items():
                if m == '1_1':
                    values['data_paths'][data_type] = values['data_paths'][data_type].replace('CASENAME', '{}-ctc'.format(case))
                elif m == '1_1_ECA':
                    values['data_paths'][data_type] = values['data_paths'][data_type].replace('CASENAME', '{}-eca'.format(case))

            pprint(values)
            config_name = os.path.join(args.outpath, '{}-{}.yaml'.format(m, case))
            if not os.path.exists(args.outpath) or not os.path.isdir(args.outpath):
                os.makedirs(args.outpath)
            
            with open(config_name, 'w') as op:
                yaml.dump(values, op)

if __name__ == '__main__':
    sys.exit(main())