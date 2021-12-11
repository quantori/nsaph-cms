#!/usr/bin/env cwl-runner
### Materialized View Creator

cwlVersion: v1.2
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  InlineJavascriptRequirement: {}

doc: |
  This tool is a shortcut to ingest CMS raw data

inputs:
  registry:
    type: File?
    doc: |
      A path to the data model file
  input:
    type: Directory
    doc: |
      A path to directory, containing unpacked CMS
      files. The tool will recursively look for data files
      according to provided pattern
  database:
    type: File
    doc: Path to database connection file, usually database.ini
  connection_name:
    type: string
    doc: The name of the section in the database.ini file
  table:
    type: string
    doc: the name of the table to be created
  domain:
    type: string
    doc: the name of the domain
    default: cms
  incremental:
    type:  boolean
    default: true
  
  depends_on:
    type: File?
    doc: a special field used to enforce dependencies and execution order

steps:
  reset:
    run: reset.cwl
    doc: Initializes Raw CMS tables
    in:
      registry: registry
      domain: domain
      table:  table
      database: database
      connection_name: connection_name
    out: [log, errors]

  create:
    run: load_raw.cwl
    doc: Run data loader to load files to the database
    in:
      depends_on: reset/log
      registry: registry
      domain: domain
      table: table
      database: database
      input: input
      connection_name: connection_name
      incremental: incremental
      pattern:
        valueFrom: |
          ${
            var table = inputs.table
            if (inputs.table == 'admissions')
              table = 'ip'
            return "**/maxdata_*_" + table + "_*.csv*"
          }
    out: [ log, errors ]

  index:
    run: index.cwl
    doc: Build indices
    in:
      depends_on: create/log
      registry: registry
      domain: domain
      table: table
      incremental: incremental
      database: database
      connection_name: connection_name

    out: [ log, errors ]

  vacuum:
    run: vacuum.cwl
    doc: Vacuum the view
    in:
      depends_on: index/log
      registry: registry
      domain: domain
      table: table
      database: database
      connection_name: connection_name
    out: [ log, errors ]

outputs:
  reset_log:
    type: File
    outputSource: reset/log
  create_log:
    type: File
    outputSource: create/log
  index_log:
    type: File
    outputSource: index/log
  vacuum_log:
    type: File
    outputSource: vacuum/log

  reset_err:
    type: File
    outputSource: reset/errors
  create_err:
    type: File
    outputSource: create/errors
  index_err:
    type: File
    outputSource: index/errors
  vacuum_err:
    type: File
    outputSource: vacuum/errors
