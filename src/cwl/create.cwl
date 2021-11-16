#!/usr/bin/env cwl-runner
### Medicaid Table/View Initializer

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, nsaph.loader.data_loader]
requirements:
  InlineJavascriptRequirement: {}

doc: |
  This tool executes DDL to drop and recreate
  creates a table, a view or a materialized view in the database.
  It is assumed it is run for Medicaid domain


inputs:
  #$import: db.yaml
  table:
    type: string
    doc: the name of the table to be created
    inputBinding:
      prefix: --table
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

arguments:
    - valueFrom: "--reset"
    - valueFrom: "medicaid"
      prefix: --domain


outputs:
  log:
    type: File
    outputBinding:
      glob: "*.log"
  errors:
    type: stderr

stderr: $("create_" + inputs.table + ".err")

