---
orphan: true
---
# Medicare in-database processing pipeline
**Workflow**

**Source**: [medicare.cwl](../members/medicare_beneficiaries_cwl.md)

```{contents}
---
local:
---
```

## Description
This workflow processes raw Medicare data. The assumed initial state
is that raw data is already in the database. We assume that the data
for each year is in a separate set of tables consisting of at least
two tables: patient summary and inpatient admissions. The first step
combines these disparate tables into a single view, creating uniform
columns.


## Inputs

| Name            | Type   | Default | Description                                            |
|-----------------|--------|---------|--------------------------------------------------------|
| database        | File   |         | Path to database connection file, usually database.ini |
| connection_name | string |         | The name of the section in the database.ini file       |

## Outputs

| Name                   | Type | Description |
|------------------------|------|-------------|
| ps_create_log          | File |             |
| ps_create_err          | File |             |
| ps2_create_log         | File |             |
| ps2_create_err         | File |             |
| bene_view_log          | File |             |
| bene_view_err          | File |             |
| bene_table_create_log  | File |             |
| bene_table_index_log   | File |             |
| bene_table_vacuum_log  | File |             |
| bene_table_create_err  | File |             |
| bene_table_index_err   | File |             |
| bene_table_vacuum_err  | File |             |
| enrlm_view_log         | File |             |
| enrlm_view_err         | File |             |
| enrlm_table_create_log | File |             |
| enrlm_table_index_log  | File |             |
| enrlm_table_vacuum_log | File |             |
| enrlm_table_create_err | File |             |
| enrlm_table_index_err  | File |             |
| enrlm_table_vacuum_err | File |             |

## Steps

| Name               | Runs                              | Description                                                                                      |
|--------------------|-----------------------------------|--------------------------------------------------------------------------------------------------|
| create_ps          | [medicare_ps.cwl](medicare_ps.md) | Combines patient summaries from disparate summary tables (one table per year) into a single view |
| create__ps_view    | [create.cwl](create.md)           | Execute DDL                                                                                      |
| create_bene_view   | [create.cwl](create.md)           | Execute DDL                                                                                      |
| create_bene_table  | [matview.cwl](matview.md)         | Creates `Beneficiaries` Table                                                                    |
| create_enrlm_view  | [create.cwl](create.md)           | Execute DDL                                                                                      |
| create_enrlm_table | [matview.cwl](matview.md)         | Creates `Enrollments` Table                                                                      |
