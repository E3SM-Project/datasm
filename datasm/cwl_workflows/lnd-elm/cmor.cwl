#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
requirements:
  InlineJavascriptRequirement: {}
  InitialWorkDirRequirement:
    listing: 
      - $(inputs.raw_file_list)
inputs:
  tables_path:
    type: string
    inputBinding:
      prefix: --tables-path
  metadata_path:
    type: string
    inputBinding:
      prefix: --user-metadata
  num_workers:
    type: int
    inputBinding:
      prefix: --num-proc
  var_list:
    type: string[]
    inputBinding:
      prefix: -v
  raw_file_list:
    type: File[]
  account:
    type: string
  partition:
    type: string
  slurm_timeout:
    type: string
  e2c_timeout:
    type: int

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - -t
  - $(inputs.slurm_timeout)
  - e3sm_to_cmip
  - --timeout
  - $(inputs.e2c_timeout)
  - -s
  - --input-path
  - .
  - --output-path
  - $(runtime.outdir)
  - --realm
  - lnd

outputs: 
  cmip6_dir: 
    type: Directory
    outputBinding:
      glob: CMIP6
