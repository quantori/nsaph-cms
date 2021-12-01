#!/usr/bin/env cwl-runner
### Index Builder

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, cms.aggregates]

doc: |
  This tool verifies correct counts for a random selection of
  mediciad MAX data


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
  depends_on:
    type: File?
    doc: a special field used to enforce dependencies and execution order


outputs:
  log:
    type: File
    outputBinding:
      glob: "*.log"
  errors:
    type: stderr

stderr: verification.err
