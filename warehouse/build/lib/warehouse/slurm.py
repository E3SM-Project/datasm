
import logging
import os
from pathlib import Path
from subprocess import Popen, PIPE
from time import sleep
import json
import os
import stat
from warehouse.util import print_debug


class Slurm(object):
    """
    A python interface for slurm using subprocesses
    """

    def __init__(self):
        """
        Check if the system has Slurm installed
        """
        if not any(os.access(os.path.join(path, 'sinfo'), os.X_OK) for path in os.environ["PATH"].split(os.pathsep)):
            raise Exception(
                'Unable to find slurm, is it installed on this system?')
    # -----------------------------------------------

    def render_script(self, cmd, script_path, slurm_opts=[], **kwargs):
        """
        Write out an executable bash script
        Parameters:
            cmd (string) : a bash command to run
            script_path (string) : the path to where to store the sbatch script
            slurm_opts List[(str, str)] : a list of slurm argument key value pairs
        """

        with open(script_path, 'w') as outstream:
            outstream.write("#!/bin/bash\n\n")
            for key, val in slurm_opts:
                outstream.write(f"#SBATCH {key} {val}\n")
            outstream.write(cmd + '\n')
        st = os.stat(script_path)
        os.chmod(script_path, st.st_mode | stat.S_IEXEC)

    def sbatch(self, cmd, sbatch_args=None):
        """
        Submit to the batch queue in non-interactive mode

        Parameters:
            cmd (str): The path to the run script that should be submitted
            sbatch_args (str): The additional arguments to pass to sbatch
        Returns:
            job id of the new job (int)
        """
        try:
            out, err = self._submit('sbatch', cmd, sbatch_args)
        except Exception as e:
            print('Batch job submission failed')
            print_debug(e)
            return 0

        if err:
            raise Exception('SLURM ERROR: ' + err)

        out = out.split()
        if 'error' in out:
            return 0
        try:
            job_id = int(out[-1])
        except IndexError as e:
            print("error submitting job to slurm " + str(out) + " " + str(err))
            return 0

        return job_id
    # -----------------------------------------------

    def _submit(self, subtype, cmd, sargs=None):

        cmd = [subtype, cmd, sargs] if sargs is not None else [subtype, cmd]
        tries = 0
        while tries != 10:
            proc = Popen(cmd, shell=False, stderr=PIPE, stdout=PIPE)
            out, err = proc.communicate()
            out = out.decode('utf-8')
            if err:
                err = err.decode('utf-8')
                print_debug(err, status='err')
                logging.error(err)
                tries += 1
                sleep(tries * 2)

                qinfo = self.queue()
                for job in qinfo:
                    if job.get('COMMAND') == cmd[1]:
                        return 'Submitted batch job {}'.format(job['JOBID']), None
                print('Unable to submit job, trying again')
            else:
                break
        if tries >= 10:
            raise Exception('SLURM ERROR: Transport endpoint is not connected')
        return out, None
    # -----------------------------------------------

    def showjob(self, jobid):
        """
        A wrapper around scontrol show job

        Parameters:
            jobid (str): the job id to get information about
        Returns:
            A jobinfo object containing information about the job with the given jobid
        """
        if not isinstance(jobid, str):
            jobid = str(jobid)
        success = False
        while not success:
            try:
                proc = Popen(['scontrol', 'show', 'job', jobid],
                             shell=False, stderr=PIPE, stdout=PIPE)
                out, err = proc.communicate()
                out = out.decode('utf-8')
                if err:
                    err = err.decode('utf-8')
                    print_debug(err, status='err')
                    if err == 'slurm_load_jobs error: Invalid job id specified':
                        raise ValueError(
                            f"Unable to find slurm job with id {jobid}")
                    sleep(1)
                else:
                    success = True
            except:
                success = False
                sleep(1)

        job_info = JobInfo()
        for item in out.split('\n'):
            for j in item.split(' '):
                index = j.find('=')
                if index <= 0:
                    continue
                attribute = self.slurm_to_jobinfo(j[:index])
                if attribute is None:
                    continue
                job_info.set_attr(
                    attr=attribute,
                    val=j[index + 1:])
        return job_info
    # -----------------------------------------------

    def slurm_to_jobinfo(self, attr):
        if attr == 'Partition':
            return 'PARTITION'
        elif attr == 'Command':
            return 'COMMAND'
        elif attr == 'UserId':
            return 'USER'
        elif attr == 'JobName':
            return 'NAME'
        elif attr == 'JobState':
            return 'STATE'
        elif attr == 'JobId':
            return 'JOBID'
        elif attr == 'RunTime':
            return 'RUNTIME'
        else:
            return None
    # -----------------------------------------------

    def get_node_number(self):
        """
        Use sinfo to return the number of nodes in the cluster
        """
        cmd = 'sinfo show nodes | grep up | wc -l'
        p = Popen([cmd], stderr=PIPE, stdout=PIPE, shell=True)
        out, err = p.communicate()
        out = out.decode('utf-8')
        err = err.decode('utf-8')
        if err:
            print(err)
        try:
            num_nodes = int(out)
        except:
            num_nodes = 1
        return num_nodes
    # -----------------------------------------------

    def queue(self):
        """
        Get job queue status

        Returns: list of jobs in the queue
        """
        tries = 0
        while tries != 10:
            try:
                cmd = ['squeue', '-u', os.environ['USER'], '-o', '%i|%j|%o|%t']
                proc = Popen(cmd, shell=False, stderr=PIPE, stdout=PIPE)
                out, err = proc.communicate()
                out = out.decode('utf-8')
                err = err.decode('utf-8')
                if err or not out:
                    tries += 1
                    sleep(tries)
                    print(err)
                else:
                    break
            except:
                sleep(1)
        if tries == 10:
            raise Exception('SLURM ERROR: Unable to communicate with squeue')

        queueinfo = []
        for item in out.split(b'\n')[1:]:
            if not item:
                break
            line = [x for x in item.split(b'|') if x]
            queueinfo.append({
                'JOBID': line[0],
                'NAME': line[1],
                'COMMAND': line[2],
                'STATE': line[3],
            })
        return queueinfo
    # -----------------------------------------------

    def cancel(self, job_id):
        tries = 0
        while tries != 10:
            try:
                cmd = ['scancel', str(job_id)]
                proc = Popen(cmd, shell=False, stderr=PIPE, stdout=PIPE)
                out, err = proc.communicate()
                out = out.decode('utf-8')
                err = err.decode('utf-8')
                if err:
                    print(err)
                    tries += 1
                    sleep(tries)
                else:
                    return True
            except Exception as e:
                print_debug(e)
                sleep(1)
        return False
    # -----------------------------------------------


class JobInfo(object):
    """
    A simple container class for slurm job information
    """

    def __init__(self, jobid=None,
                        jobname=None,
                        partition=None,
                        state=None,
                        time=None,
                        user=None,
                        command=None):
        self.jobid = jobid
        self.jobname = jobname
        self.partition = partition
        self.time = time
        self.user = user
        self.command = command
        self.state = state
            
    # -----------------------------------------------

    def __str__(self):
        return json.dumps({
            'JOBID': self.jobid,
            'JOBNAME': self.jobname,
            'PARTITION': self.partition,
            'STATE': self.state,
            'TIME': self.time,
            'USER': self.user,
            'COMMAND': self.command
        })
    # -----------------------------------------------

    def set_attr(self, attr, val):
        """
        set the appropriate attribute with the value supplied
        """
        if attr == 'PARTITION':
            self.partition = val
        elif attr == 'COMMAND':
            self.command = val
        elif attr == 'NAME':
            self.jobname = val
        elif attr == 'JOBID':
            self.jobid = val
        elif attr == 'STATE':
            self.state = val
        elif attr == 'RUNTIME':
            self.time = val
        elif attr == 'USER':
            self.user = val
        else:
            msg = '{} is not an allowed attribute'.format(attr)
            raise Exception(msg)
    # -----------------------------------------------

    @property
    def state(self):
        return self.state
    # -----------------------------------------------

    @state.setter
    def state(self, state):
        if state in ['Q', 'W', 'PD', 'PENDING']:
            self.state = 'PENDING'
        elif state in ['R', 'RUNNING']:
            self.state = 'RUNNING'
        elif state in ['E', 'CD', 'CG', 'COMPLETED', 'COMPLETING']:
            self.state = 'COMPLETED'
        elif state in ['FAILED', 'F']:
            self.state = 'FAILED'
        else:
            self.state = state
    # -----------------------------------------------
