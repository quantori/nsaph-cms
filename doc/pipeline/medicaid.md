# Full Medicaid Processing Pipeline
**Workflow**

**Source**: [medicaid.cwl](../../src/cwl/medicaid.cwl)

<!-- toc -->

- [Description](#description)
- [Inputs](#inputs)
- [Outputs](#outputs)
- [Steps](#steps)

<!-- tocstop -->

## Description
This workflow ingests Medicaid data, provided by
Centers for Medicare & Medicaid Services (CMS)
to researches. The expected input format is
Medicaid Analytic eXtract (MAX) data.

The workflow parses File transfer summary (FTS) files,
loads the raw data into a PostgreSQL DBMS and then processes
the data to prepare it for using by NSAPH researches.
See [documentation](../Medicaid.md) for detailed
information.


## Inputs

| Name | Type | Default | Description |
|------|------|---------|-------------|
|database|File| |Path to database connection file, usually database.ini|
|connection_name|string| |The name of the section in the database.ini file|
|input|Directory| |A path to directory, containing unpacked CMS files. The tool will recursively look for data files according to provided pattern |

## Outputs

| Name | Type | Description |
|------|------|-------------|
|resource1_log|File| |
|parse_log|File| |
|reset_log|File| |
|ps_create_log|File| |
|ps_index_log|File| |
|ben_create_log|File| |
|ben_index_log|File| |
|mnth_create_log|File| |
|mnth_index_log|File| |
|enrlm_create_log|File| |
|enrlm_index_log|File| |
|elgb_create_log|File| |
|elgb_index_log|File| |

## Steps

| Name | Runs | Description |
|------|------|-------------|
|states|[ensure_resource.cwl](ensure_resource.md)|Ensures the presence of `us_states` table in the database. The table contains mapping between state names, ids (two letter abbreviations), FIPS codes and [ISO-3166-2 codes](https://en.wikipedia.org/wiki/ISO_3166-2) |
|iso|[ensure_resource.cwl](ensure_resource.md)|Ensures the presence of `us_iso` table in the database. The table provides a mapping between states, counties and zip codes. It contains FIPS and [ISO-3166-2 codes](https://en.wikipedia.org/wiki/ISO_3166-2) |
|fts|[parse_fts.cwl](parse_fts.md)| |
|reset_cms|[reset.cwl](reset.md)|Initializes Raw CMS tables|
|load_ps|[load_ps.cwl](load_ps.md)|Loads Patient Summaries|
|index_ps|[index.cwl](index.md)| |
|create_beneficiaries|[matview.cwl](matview.md)|Creates `Beneficiaries` Table|
|create_monthly_view|[matview.cwl](matview.md)|Creates internally used `Monthly View`|
|create_enrollments|[matview.cwl](matview.md)|Creates `Enrollment` Table|
|create_eligibility|[matview.cwl](matview.md)|Creates `Eligibility` Table|
