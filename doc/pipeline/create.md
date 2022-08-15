---
orphan: true
---
# Medicaid Table/View Initializer
**Tool** 	[nsaph.loader.data_loader](../../../platform/doc/members/data_loader)

**Source**: [create.cwl](../members/create_cwl.md)

```{contents}
---
local:
---
```

## Description
This tool executes DDL to drop and recreate
creates a table, a view or a materialized view in the database.
It is assumed it is run for Medicaid domain


## Inputs

| Name            | Type    | Default    | Description                                                      |
|-----------------|---------|------------|------------------------------------------------------------------|
| table           | string  |            | the name of the table to be created                              |
| database        | File    |            | Path to database connection file, usually database.ini           |
| connection_name | string  |            | The name of the section in the database.ini file                 |
| sloppy          | boolean | `False`    |                                                                  |
| domain          | string  | `medicaid` |                                                                  |
| depends_on      | File    |            | a special field used to enforce dependencies and execution order |

## Outputs

| Name   | Type   | Description |
|--------|--------|-------------|
| log    | File   |             |
| errors | stderr |             |
