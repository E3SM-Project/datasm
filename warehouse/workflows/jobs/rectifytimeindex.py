from warehouse.workflows.jobs import WorkflowJob

NAME = 'RectifyTimeIndex'

class RectifyTimeIndex(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME
        self._cmd = f"""
cd {self.scripts_path}
python rectify_time_index.py -j {self._job_workers} {self.dataset.working_dir} --output {self.find_outpath} --no-gaps
"""
