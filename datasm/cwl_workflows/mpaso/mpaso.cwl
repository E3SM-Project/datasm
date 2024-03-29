#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement

inputs:
  data_path: string
  metadata_file: string
  workflow_output: string

  mapfile: string
  frequency: int

  namelist_path: string
  region_file: string
  restart_path: string

  tables_path: string
  cmor_var_list: string[]

  slurm_timeout: string
  e2c_timeout: int
  partition: string
  account: string

steps:

  step_get_start_end:
    run: get_start_end.cwl
    in:
      data_path: data_path
    out:
      - start_year
      - end_year

  step_segments:
    run: mpaso_split.cwl
    in:
      start: step_get_start_end/start_year
      end: step_get_start_end/end_year
      frequency: frequency
      input: data_path
      namelist: namelist_path
      restart: restart_path
      region_file: region_file
    out:
      - segments
  
  step_render_cmor_template:
    run:
      mpaso_sbatch_render.cwl
    in:
      input_path: step_segments/segments
      tables_path: tables_path
      metadata: metadata_file
      var_list: cmor_var_list
      mapfile: mapfile
      slurm_timeout: slurm_timeout
      e2c_timeout: e2c_timeout
      partition: partition
      account: account
      workflow_output: workflow_output
    scatter:
      - input_path
      - var_list
    scatterMethod: 
      flat_crossproduct
    out:
      - cmorized

outputs: 
  cmorized:
    type: 
      Directory[]
    outputSource: step_render_cmor_template/cmorized
