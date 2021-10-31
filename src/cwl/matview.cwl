#!/usr/bin/env cwl-runner

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
    in:
      table: table
      database: database
      connection_name: connection_name
    out: [ log ]

  index:
    run: index.cwl
    in:
      depends_on: create/log
      domain: domain
      table: table
      database: database
      connection_name: connection_name
    out: [ log ]

outputs:
  create_log:
    type: File
    outputSource: create/log
  index_log:
    type: File
    outputSource: index/log