# Resource Loader
**Tool** 	[nsaph.util.pg_json_dump](../../../../platform/doc/members/pg_json_dump.html)

**Source**: [ensure_resource.cwl](../../src/cwl/ensure_resource.cwl)

<!-- toc -->

- [Description](#description)
- [Inputs](#inputs)
- [Outputs](#outputs)

<!-- tocstop -->

## Description
This tool ensures that resources required by
the processing steps are loaded into the database


## Inputs

| Name | Type | Default | Description |
|------|------|---------|-------------|
|database|File| |Path to database connection file, usually database.ini|
|connection_name|string| |The name of the section in the database.ini file|
|table|string| |the name of the table containing required resource|

## Outputs

| Name | Type | Description |
|------|------|-------------|
|log|File| |
