from warehouse.workflows.jobs import WorkflowJob

NAME = 'ValidateEsgf'

class ValidateEsgf(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._cmd = f"""
            cd {self.scripts_path}
            sleep 15
            ts=`date -u +%Y%m%d_%H%M%S_%6N`
            echo "$ts:slurm:calls validate_esgf.py"
            python validate_esgf.py --dataset-id {self.dataset.dataset_id}
            # retcode=$?
            # ts=`date -u +%Y%m%d_%H%M%S_%6N`
            # echo "$ts:slurm:validate_esgf.py returned code $retcode"
        """
