#!/usr/bin/env cwl-runner
### Full Medicaid Processing Pipeline

cwlVersion: v1.2
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  InlineJavascriptRequirement: {}

doc: |
  This workflow ingests Medicaid data, provided by
  Centers for Medicare & Medicaid Services (CMS)
  to researches. The expected input format is
  Medicaid Analytic eXtract (MAX) data.

  The workflow parses File transfer summary (FTS) files,
  loads the raw data into a PostgreSQL DBMS and then processes
  the data to prepare it for using by NSAPH researches.
  See [documentation](../Medicaid.md) for detailed
  information.

inputs:
  database:
    type: File
    doc: Path to database connection file, usually database.ini
  connection_name:
    type: string
    doc: The name of the section in the database.ini file
  input:
    type: Directory
    doc: |
      A path to directory, containing unpacked CMS
      files. The tool will recursively look for data files
      according to provided pattern

steps:
  states:
    run: ensure_resource.cwl
    doc: |
      Ensures the presence of `us_states` table in the database.
      The table contains mapping between state names, ids
      (two letter abbreviations), FIPS codes and
      [ISO-3166-2 codes](https://en.wikipedia.org/wiki/ISO_3166-2)
    in:
      database: database
      connection_name: connection_name
      table:
        valueFrom: "us_states"
    out: [log]
  iso:
    run: ensure_resource.cwl
    doc: |
      Ensures the presence of `us_iso` table in the database.
      The table provides a mapping between states, counties and zip
      codes. It contains FIPS and
      [ISO-3166-2 codes](https://en.wikipedia.org/wiki/ISO_3166-2)
    in:
      database: database
      connection_name: connection_name
      table:
        valueFrom: "us_iso"
    out: [log]
  fts:
    run: parse_fts.cwl
    in:
      input: input
      output:
        valueFrom: cms.yaml
    out: [log, model, errors]

  reset_cms:
    run: reset.cwl
    doc: Initializes Raw CMS tables
    in:
      registry: fts/model
      domain:
        valueFrom: "cms"
      table:
        valueFrom: "ps"
      database: database
      connection_name: connection_name
    out: [log, errors]

  load_ps:
    run: load_ps.cwl
    doc: Loads Patient Summaries
    in:
      depends_on: reset_cms/log
      registry: fts/model
      domain:
        valueFrom: "cms"
      table:
        valueFrom: "ps"
      input: input
      database: database
      connection_name: connection_name
      incremental:
        valueFrom: $(true)
      pattern:
        valueFrom: "**/maxdata_*_ps_*.csv*"
    out: [log, errors]

  index_ps:
    run: index.cwl
    in:
      depends_on: load_ps/log
      registry: fts/model
      domain:
        valueFrom: "cms"
      table:
        valueFrom: "ps"
      database: database
      connection_name: connection_name
    out: [log, errors]

  vacuum_ps:
    run: vacuum.cwl
    in:
      depends_on: index_ps/log
      registry: fts/model
      domain:
        valueFrom: "cms"
      table:
        valueFrom: "ps"
      database: database
      connection_name: connection_name
    out: [log, errors]

  create_beneficiaries:
    run: matview.cwl
    doc: Creates `Beneficiaries` Table
    in:
      depends_on: vacuum_ps/log
      table:
        valueFrom: "beneficiaries"
      database: database
      connection_name: connection_name
    out:
      - create_log
      - index_log
      - vacuum_log
      - create_err
      - index_err
      - vacuum_err

  create_monthly_view:
    run: matview.cwl
    doc: Creates internally used `Monthly View`
    in:
      depends_on: create_beneficiaries/vacuum_log
      table:
        valueFrom: "monthly"
      database: database
      connection_name: connection_name
    out:
      - create_log
      - index_log
      - vacuum_log
      - create_err
      - index_err
      - vacuum_err

  create_enrollments:
    run: matview.cwl
    doc: Creates `Enrollment` Table
    in:
      depends_on: create_monthly_view/vacuum_log
      table:
        valueFrom: "enrollments"
      database: database
      connection_name: connection_name
    out:
      - create_log
      - index_log
      - vacuum_log
      - create_err
      - index_err
      - vacuum_err

  create_eligibility:
    run: matview.cwl
    doc: Creates `Eligibility` Table
    in:
      depends_on: create_enrollments/vacuum_log
      table:
        valueFrom: "eligibility"
      database: database
      connection_name: connection_name
    out:
      - create_log
      - index_log
      - vacuum_log
      - create_err
      - index_err
      - vacuum_err


outputs:
  resource1_log:
    type: File
    outputSource: states/log
  resource2_log:
    type: File
    outputSource: iso/log
  parse_log:
    type: File
    outputSource: fts/log
  reset_log:
    type: File
    outputSource: reset_cms/log
  ps_create_log:
    type: File
    outputSource: load_ps/log
  ps_index_log:
    type: File
    outputSource: index_ps/log
  ps_vacuum_log:
    type: File
    outputSource: vacuum_ps/log
  ben_create_log:
    type: File
    outputSource: create_beneficiaries/create_log
  ben_index_log:
    type: File
    outputSource: create_beneficiaries/index_log
  ben_vacuum_log:
    type: File
    outputSource: create_beneficiaries/vacuum_log
  mnth_create_log:
    type: File
    outputSource: create_monthly_view/create_log
  mnth_index_log:
    type: File
    outputSource: create_monthly_view/index_log
  mnth_vacuum_log:
    type: File
    outputSource: create_monthly_view/vacuum_log
  enrlm_create_log:
    type: File
    outputSource: create_enrollments/create_log
  enrlm_index_log:
    type: File
    outputSource: create_enrollments/index_log
  enrlm_vacuum_log:
    type: File
    outputSource: create_enrollments/vacuum_log
  elgb_create_log:
    type: File
    outputSource: create_eligibility/create_log
  elgb_index_log:
    type: File
    outputSource: create_eligibility/index_log
  elgb_vacuum_log:
    type: File
    outputSource: create_eligibility/vacuum_log

  parse_err:
    type: File
    outputSource: fts/errors
  reset_err:
    type: File
    outputSource: reset_cms/errors
  ps_create_err:
    type: File
    outputSource: load_ps/errors
  ps_index_err:
    type: File
    outputSource: index_ps/errors
  ps_vacuum_err:
    type: File
    outputSource: vacuum_ps/errors
  ben_create_err:
    type: File
    outputSource: create_beneficiaries/create_err
  ben_index_err:
    type: File
    outputSource: create_beneficiaries/index_err
  ben_vacuum_err:
    type: File
    outputSource: create_beneficiaries/vacuum_err
  mnth_create_err:
    type: File
    outputSource: create_monthly_view/create_err
  mnth_index_err:
    type: File
    outputSource: create_monthly_view/index_err
  mnth_vacuum_err:
    type: File
    outputSource: create_monthly_view/vacuum_err
  enrlm_create_err:
    type: File
    outputSource: create_enrollments/create_err
  enrlm_index_err:
    type: File
    outputSource: create_enrollments/index_err
  enrlm_vacuum_err:
    type: File
    outputSource: create_enrollments/vacuum_err
  elgb_create_err:
    type: File
    outputSource: create_eligibility/create_err
  elgb_index_err:
    type: File
    outputSource: create_eligibility/index_err
  elgb_vacuum_err:
    type: File
    outputSource: create_eligibility/vacuum_err
