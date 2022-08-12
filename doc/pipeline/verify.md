# Index Builder
**Tool** 	[cms.aggregates](../members/aggregates.md)

**Source**: [verify.cwl](../members/verify_cwl.md)

<!-- toc -->

- [Description](#description)
- [Inputs](#inputs)
- [Outputs](#outputs)

<!-- tocstop -->

## Description
This tool verifies correct counts for a random selection of
mediciad MAX data


## Inputs

| Name | Type | Default | Description |
|------|------|---------|-------------|
|database|File| |Path to database connection file, usually database.ini|
|connection_name|string| |The name of the section in the database.ini file|
|depends_on|File| |a special field used to enforce dependencies and execution order|

## Outputs

| Name | Type | Description |
|------|------|-------------|
|log|File| |
|errors|stderr| |
