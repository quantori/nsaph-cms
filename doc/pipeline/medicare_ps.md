---
orphan: true
---
# Medicaid Table/View Initializer
**Tool** 	[cms.tools.mcr_create_ps](../../src/python/cms/tools/mcr_create_ps.py)

[//]: # (TODO: unknown file 'mcr_create_ps.py')


**Source**: [medicare_ps.cwl](../members/medicare_ps_cwl.md)

```{contents}
---
local:
---
```

## Description
This tool combines disparate tables with raw Medicare data into a single
view, transforming common columns to uniform type and format


## Inputs

| Name            | Type   | Default | Description                                                      |
|-----------------|--------|---------|------------------------------------------------------------------|
| database        | File   |         | Path to database connection file, usually database.ini           |
| connection_name | string |         | The name of the section in the database.ini file                 |
| depends_on      | File   |         | a special field used to enforce dependencies and execution order |

## Outputs

| Name   | Type   | Description |
|--------|--------|-------------|
| log    | File   |             |
| errors | stderr |             |
