import yaml
from pathlib import Path
from warehouse.workflows.jobs import WorkflowJob

NAME = 'PublishEsgf'


class PublishEsgf(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME

        mapfile_path = sorted([x for x in self.dataset.publication_path.glob('*.map')]).pop()

        optional_facets = {}
        if 'CMIP6' not in self.dataset.dataset_id:
            dataset_attrs = self.dataset.dataset_id.split('.')
            model_version = dataset_attrs[1]
            experiment_name = dataset_attrs[2]
            with open(self._spec_path, 'r') as instream:
                spec = yaml.load(instream, Loader=yaml.SafeLoader)
                experiment_info = spec['project']['E3SM'][model_version][experiment_name]
                if (campaign := experiment_info.get('campaign')):
                    optional_facets['campaign'] = campaign
                if (science_driver := experiment_info.get('science_driver')):
                    optional_facets['science_driver'] = science_driver
                if (period := experiment_info.get('period')):
                    optional_facets['period'] = period
                else:
                    optional_facets['period'] = f"{experiment_info['start']} - {experiment_info['end']}"

        self._cmd = f"""
            cd {self.scripts_path}
            python publish_to_esgf.py --src-path {mapfile_path} --log-path {self._slurm_out.resolve()} """
        
        if optional_facets:
            self._cmd += '--optional-facets "' + '" "'.join([f"{key}={value}" for key, value in optional_facets.items()]) + '"'
        
