from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateMapfile'

class GenerateMapfile(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self.cmd = '''
            cd {self.scripts_path}
            ./generate_esgmapfile.sh -p {self._job_workers} {self.dataset.working_dir}
            '''
