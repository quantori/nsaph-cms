#!/usr/bin/env cwl-runner
### Materialized View Creator

cwlVersion: v1.2
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}

doc: |
  This tool is a shortcut to create a materialized view and build
  all indices associated with the view

inputs:
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
    default: medicaid
  depends_on:
    type: File?
    doc: a special field used to enforce dependencies and execution order

steps:
  create:
    run: create.cwl
    doc: Execute DDL
    in:
      table: table
      database: database
      connection_name: connection_name
    out: [ log, errors ]

  index:
    run: index.cwl
    doc: Build indices
    in:
      depends_on: create/log
      domain: domain
      table: table
      database: database
      connection_name: connection_name
    out: [ log, errors ]

  vacuum:
    run: vacuum.cwl
    doc: Vacuum the view
    in:
      depends_on: index/log
      domain: domain
      table: table
      database: database
      connection_name: connection_name
    out: [ log, errors ]

outputs:
  create_log:
    type: File
    outputSource: create/log
  index_log:
    type: File
    outputSource: index/log
  vacuum_log:
    type: File
    outputSource: vacuum/log

  create_err:
    type: File
    outputSource: create/errors
  index_err:
    type: File
    outputSource: index/errors
  vacuum_err:
    type: File
    outputSource: vacuum/errors
