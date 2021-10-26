#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, nsaph.loader.data_loader]

doc: |
  This tool loads patient summary data into a database.
  It should be run after the data is inspected and
  data model is created from FTS files


inputs:
  registry:
    type: File
    inputBinding:
      prefix: --registry
    doc: |
      A path to the data model file
  domain:
    type: string
    default: cms
    doc: the name of the domain
    inputBinding:
      prefix: --domain
  table:
    type: string
    default: ps
    doc: the name of the table being populated
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
  incremental:
    type: boolean
    inputBinding:
      prefix: --incremental
    doc: |
      if defined, then the data ingestion is incremental.
      Transactions are committed after every file is processed
      and files that have already been prcoessed are skipped
  input:
    type: Directory
    inputBinding:
      prefix: --data
    doc: |
      A path to directory, containing unpacked CMS
      files. The tool will recursively look for data files
      according to provided pattern
  pattern:
    type: string
    default: "**/maxdata_*_ps_*.csv*"
    inputBinding:
      prefix: --pattern
  threads:
    type: int
    default: 4
    doc: number of threads, concurrently writing into the database
  page_size:
    type: int
    default: 1000
    doc: explicit page size for the database
  log_frequency:
    type: long
    default: 100000
    doc: informational logging occurs every specified number of records
  limit:
    type: long?
    doc: |
      if specified, the process will stop after ingesting
      the specified number of records



outputs:
  log:
    type: File
    outputBinding:
      glob: "*.log"

