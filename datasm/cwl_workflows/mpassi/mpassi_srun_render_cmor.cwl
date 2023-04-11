#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, mpassi_sbatch_cmor.py]
requirements:
  - class: InitialWorkDirRequirement
    listing:
      - entryname: mpassi_sbatch_cmor.py
        entry: |
          from sys import exit, argv
          from subprocess import call
          from jinja2 import Template
          import argparse

          template_string = """#!/bin/bash

          RETURN=1
          until [ $RETURN -eq 0 ]; do
              e3sm_to_cmip \
                  -s \
                  --realm mpassi \
                  -v {{ variables }} \
                  --tables-path {{ tables }} \
                  --user-metadata {{ metadata }} \
                  --map {{ map }} \
                  --logdir {{ outdir }} \
                  --output {{ outdir }} \
                  --input {{ input }} \
                  --timeout {{ e_timeout }}
              RETURN=$?
              if [ $RETURN != 0 ]; then
                  echo "Restarting"
              fi
          done"""

          def render_sbatch(values):
              template = Template(template_string)
              script_path = 'cmip6_convert_{}.sh'.format(values.variables)
              try:
                  script_contents = template.render(
                      variables=values.variables,
                      account=values.account,
                      partition=values.partition,
                      tables=values.tables,
                      metadata=values.metadata,
                      map=values.map,
                      outdir=values.outdir,
                      input=values.input,
                      e_timeout=values.e_timeout,
                      workflow_output=values.workflow_output)
                  with open(script_path, 'w') as outfile:
                      outfile.write(script_contents)
                  
                  call(['chmod', '+x', script_path])
                  return call(['srun', '-A', values.account, '--partition', values.partition, '-t', values.s_timeout, script_path])
              except Exception as e:
                  raise e

          if __name__ == "__main__":
              parser = argparse.ArgumentParser()
              parser.add_argument('--variables', required=True)
              parser.add_argument('--account', required=True)
              parser.add_argument('--tables', required=True)
              parser.add_argument('--metadata', required=True)
              parser.add_argument('--map', required=True)
              parser.add_argument('--outdir', required=True)
              parser.add_argument('--input', required=True)
              parser.add_argument('--e_timeout', required=True)
              parser.add_argument('--s_timeout', required=True)
              parser.add_argument('--partition', required=True)
              parser.add_argument('--workflow_output', required=True)
              exit(
                  render_sbatch(
                      parser.parse_args()))

inputs:
  input_directory:
    type: Directory
    inputBinding:
      prefix: --input
  partition:
    type: string
    inputBinding:
      prefix: --partition
  account:
    type: string
    inputBinding:
      prefix: --account
  tables_path:
    type: string
    inputBinding:
      prefix: --tables
  metadata:
    type: string
    inputBinding:
      prefix: --metadata
  var_list:
    type: string
    inputBinding:
      prefix: --variables
  mapfile:
    type: string
    inputBinding:
      prefix: --map
  slurm_timeout:
    type: string
    inputBinding:
      prefix: --s_timeout
  e2c_timeout:
    type: int
    inputBinding:
      prefix: --e_timeout
  workflow_output:
    type: string
    inputBinding:
      prefix: --workflow_output

outputs:
  cmorized:
    type: Directory
    outputBinding:
      glob: CMIP6

arguments:
  - --outdir
  - $(runtime.outdir)
