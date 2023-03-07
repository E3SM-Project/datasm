#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, find_casename.py]
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: find_casename.py
        entry: |
          import os
          import sys
          import argparse
          import json
          def main():
              p = argparse.ArgumentParser()
              p.add_argument('--atm-data-path')
              p.add_argument('--find-patt', help="file match pattern")
              args = p.parse_args()
              filename = os.listdir(args.atm_data_path)[0]
              i = filename.index(args.find_patt)
              if i == -1:
                  print(f"DEBUG: did not find patt {args.find_patt} in filename {filename}")
                  return -1
              with open("cwl.output.json", "w") as output:
                  json.dump({"casename": filename[:i]}, output)
          if __name__ == "__main__":
              sys.exit(main())

inputs:
  atm_data_path:
    type: string
    inputBinding:
      prefix: --atm-data-path
  find_patt:
    type: string
    inputBinding:
      prefix: --find-patt

outputs:
  casename:
    type: string
