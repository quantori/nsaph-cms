# Materialized View Creator
**Workflow**

**Source**: [matview.cwl](../../src/cwl/matview.cwl)

<!-- toc -->

- [Description](#description)
- [Inputs](#inputs)
- [Outputs](#outputs)
- [Steps](#steps)

<!-- tocstop -->

## Description
This tool is a shortcut to create a materialized view and build
all indices associated with the view


## Inputs

| Name | Type | Default | Description |
|------|------|---------|-------------|
|database|File| |Path to database connection file, usually database.ini|
|connection_name|string| |The name of the section in the database.ini file|
|table|string| |the name of the table to be created|
|domain|string|`medicaid`|the name of the domain|
|depends_on|File| |a special field used to enforce dependencies and execution order|

## Outputs

| Name | Type | Description |
|------|------|-------------|
|create_log|File| |
|index_log|File| |

## Steps

| Name | Runs | Description |
|------|------|-------------|
|create|[create.cwl](create.md)|Execute DDL|
|index|[index.cwl](index.md)|Build indices|
