#!/usr/bin/env cwl-runner
### FTS Parser

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, cms.registry]

doc: |
  This tool examines a directory with raw CMS data, looking
  for File Transfer Summary (FTS) files. It examnines each
  FTS file under directory (recursively) and creates a unified
  data model for input raw CMS data. If any FTS files for
  different years are incompatible with one another, a
  warning is reported. However, so far we found that all years
  are compatible.


inputs:
  input:
    type: Directory
    inputBinding:
      prefix: --input
    doc: |
      A path to directory, containing unpacked CMS
      files. The tool will recursively look in subdirectories
      for FTS files
  output:
    type: string
    default: "cms.yaml"
    doc: A path to a file name with resulting data model
    inputBinding:
      prefix: --output

outputs:
  log:
    type: File?
    outputBinding:
      glob: "registry*.log"
  model:
    type: File?
    outputBinding:
      glob: "*.yaml"
  errors:
    type: stderr

stderr: parse_fts.err
