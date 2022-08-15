---
orphan: true
---
# Workflow to load health data from files
**Workflow**

**Source**: [ingest.cwl](../members/ingest_cwl.md)

```{contents}
---
local:
---
```

## Description
This tool is a shortcut to ingest CMS raw data


## Inputs

| Name            | Type      | Default | Description                                                                                                                     |
|-----------------|-----------|---------|---------------------------------------------------------------------------------------------------------------------------------|
| registry        | File      |         | A path to the data model file                                                                                                   |
| input           | Directory |         | A path to directory, containing unpacked CMS files. The tool will recursively look for data files according to provided pattern |
| database        | File      |         | Path to database connection file, usually database.ini                                                                          |
| connection_name | string    |         | The name of the section in the database.ini file                                                                                |
| table           | string    |         | the name of the table to be created                                                                                             |
| domain          | string    | `cms`   | the name of the domain                                                                                                          |
| incremental     | boolean   | `True`  |                                                                                                                                 |
| depends_on      | File      |         | a special field used to enforce dependencies and execution order                                                                |

## Outputs

| Name       | Type | Description |
|------------|------|-------------|
| reset_log  | File |             |
| create_log | File |             |
| index_log  | File |             |
| vacuum_log | File |             |
| reset_err  | File |             |
| create_err | File |             |
| index_err  | File |             |
| vacuum_err | File |             |

## Steps

| Name   | Runs                        | Description                                   |
|--------|-----------------------------|-----------------------------------------------|
| reset  | [reset.cwl](reset.md)       | Initializes Raw CMS tables                    |
| create | [load_raw.cwl](load_raw.md) | Run data loader to load files to the database |
| index  | [index.cwl](index.md)       | Build indices                                 |
| vacuum | [vacuum.cwl](vacuum.md)     | Vacuum the view                               |
