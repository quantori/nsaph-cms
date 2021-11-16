#!/usr/bin/env cwl-runner
### Resource Loader

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, nsaph.util.pg_json_dump]

doc: |
  This tool ensures that resources required by
  the processing steps are loaded into the database


inputs:
  database:
    type: File
    doc: Path to database connection file, usually database.ini
    inputBinding:
      prefix: --db
  connection_name:
    type: string
    doc: The name of the section in the database.ini file
    inputBinding:
      prefix: --connection
  table:
    type: string
    doc: the name of the table containing required resource
    inputBinding:
      prefix: --table

arguments:
    - valueFrom: "ensure"
      prefix: --action


outputs:
  log:
    type: File
    outputBinding:
      glob: "*.log"

  errors:
    type: stderr

stderr: resource.err
