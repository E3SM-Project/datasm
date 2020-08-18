#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow
requirements:
  - class: ScatterFeatureRequirement

inputs:
  cmip_root: string
  variable: string

steps:
  discover:
    run: discover_datasets.cwl
    in:
      cmip_root: cmip_root
      variable: variable
    out: [datasets]

  file_to_strings:
    run: file_to_string_list.cwl
    in:
      infile: discover/datasets
    out: [list_of_strings]

  means:
    run: compute_mean.cwl
    in:
      dataset_path: file_to_strings/list_of_strings
    scatter: 
      dataset_path
    out:
      [dataset_mean]
  
  plot:
    run: plot_means.cwl
    in:
      dataset_paths: means/dataset_mean
    out:
      [mean_plot]
  

outputs:
  plots:
    type: File
    outputSource: plot/mean_plot
  means:
    type: File[]
    outputSource: means/dataset_mean