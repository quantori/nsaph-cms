# FTS Parser
**Tool** 	[cms.registry](../members/registry_py.md)

**Source**: [parse_fts.cwl](../members/parse_fts_cwl.md)

<!-- toc -->

- [Description](#description)
- [Inputs](#inputs)
- [Outputs](#outputs)

<!-- tocstop -->

## Description
This tool examines a directory with raw CMS data, looking
for File Transfer Summary (FTS) files. It examnines each
FTS file under directory (recursively) and creates a unified
data model for input raw CMS data. If any FTS files for
different years are incompatible with one another, a
warning is reported. However, so far we found that all years
are compatible.


## Inputs

| Name | Type | Default | Description |
|------|------|---------|-------------|
|input|Directory| |A path to directory, containing unpacked CMS files. The tool will recursively look in subdirectories for FTS files |
|output|string|`cms.yaml`|A path to a file name with resulting data model|

## Outputs

| Name | Type | Description |
|------|------|-------------|
|log|File| |
|model|File| |
|errors|stderr| |
