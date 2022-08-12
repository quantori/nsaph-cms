# Generic Table (View/Materialized View) Initializer
**Tool** 	[nsaph.loader.data_loader](../../../../platform/doc/members/data_loader.html)

[//]: # (TODO: use intersphinx)


**Source**: [reset.cwl](../members/reset_cwl.md)

<!-- toc -->

- [Description](#description)
- [Inputs](#inputs)
- [Outputs](#outputs)

<!-- tocstop -->

## Description
This tool drops the tables and deletes all previous data from the database


## Inputs

| Name | Type | Default | Description |
|------|------|---------|-------------|
|registry|File| |A path to the data model file |
|domain|string|`cms`|the name of the domain|
|table|string|`ps`|the name of the table being deleted|
|database|File| |Path to database connection file, usually database.ini|
|connection_name|string| |The name of the section in the database.ini file|

## Outputs

| Name | Type | Description |
|------|------|-------------|
|log|File| |
|errors|stderr| |
