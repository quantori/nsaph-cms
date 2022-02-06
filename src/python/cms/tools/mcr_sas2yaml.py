#  Copyright (c) 2022. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  Author: Michael A Bouzinier
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import glob
import logging
import re

import sys
from typing import List

import os
import yaml
from nsaph.data_model.utils import split

from nsaph.loader.introspector import Introspector
from nsaph.pg_keywords import PG_INT_TYPE


class SASIntrospector:
    @classmethod
    def process(cls, registry_path: str, pattern: str, root_dir:str = '.'):
        introspector = SASIntrospector(registry_path, root_dir)
        introspector.traverse(pattern)
        introspector.save()
        yaml.dump(introspector.registry, sys.stdout)
        return

    def __init__(self, registry_path: str, root_dir:str = '.'):
        self.registry_path = registry_path
        self.domain = "cms"
        self.root_dir = root_dir
        self.registry = None
        if not os.path.isfile(self.registry_path):
            self.init_registry()
        else:
            self.read_registry()
        return

    def init_registry(self):
        self.registry = {
            self.domain: {
                "reference": "",
                "schema": self.domain,
                "index": "explicit",
                "quoting": 3,
                "header": False,
                "tables": {
                }
            }
        }
        return

    def read_registry(self):
        with open(self.registry_path) as f:
            self.registry = yaml.safe_load(f)
        return

    def save(self):
        with open(self.registry_path, "wt") as f:
            yaml.dump(self.registry, f)
        return

    def traverse(self, pattern: str):
        files: List[str] = glob.glob(os.path.join(self.root_dir, pattern),
                                     recursive=True)
        for f in files:
            if f.endswith(".sas7bdat"):
                self.handle_sas_file(f)
            else:
                raise ValueError("Not implemented: " + f)
        return

    def handle_sas_file(self, f: str):
        basedir, fname = os.path.split(f)
        ydir, basedir = os.path.split(basedir)
        ydir = os.path.basename(ydir)
        if ydir not in fname:
            if "all_file" in fname and ydir.isdigit():
                logging.warning("No year: " + f)
            else:
                raise ValueError("Ambiguous year for " + f)
        year = int(ydir)
        if basedir == "denominator":
            table = "mcr_bene_{:d}".format(year)
            index_all = True
        elif basedir == "inpatient":
            table = "mcr_ip_{:d}".format(year)
            index_all = False
        else:
            raise ValueError("Unrecognized directory name for " + f)
        self.add_sas_table(table, f, index_all, year)
        return

    @classmethod
    def matches(cls, s: str, candidates: List[str]):
        if s in candidates:
            return True
        patterns = [c.replace('*', '.*') for c in candidates if '*' in c]
        for p in patterns:
            if re.fullmatch(p, s):
                return True
        return False

    def add_sas_table(self, table: str, file_path: str, index_all: bool,
                      year: int):
        introspector = Introspector(file_path)
        introspector.introspect()
        columns = introspector.get_columns()
        specials = {
            "bene_id":  (None, ["bene_id", "intbid", "qid", "bid_5333*"]),
            "state":  (None, ["state", "ssa_state", "state_code",
                              "bene_rsdnc_ssa_state_cd", "state_cd",
                              "medpar_bene_rsdnc_ssa_state_cd"]),
            "zip": (None, ["zip", "zipcode", "bene_zip_cd", "bene_zip",
                           "bene_mlg_cntct_zip_cd",
                           "medpar_bene_mlg_cntct_zip_cd"]),
            "year": (None, ["year", "enrolyr", "bene_enrollmt_ref_yr",
                            "rfrnc_yr"])
        }
        for column in columns:
            cname, c = split(column)
            if cname == "year":
                yc = cname
            is_key = False
            for key in specials:
                key_column, candidates = specials[key]
                if self.matches(cname, candidates):
                    if key_column is not None:
                        raise ValueError("Multiple {} columns in {}"
                                         .format(key, file_path))
                    specials[key] = (column, candidates)
                    is_key = True
            if index_all and not is_key:
                c["index"] = "true"

        for key in specials:
            column, _ = specials[key]
            if column is None:
                if key == "year":
                    columns.append(
                        {
                            key: {
                                "type": PG_INT_TYPE,
                                "index": "true",
                                "source": {
                                    "type": "generated",
                                    "code": "GENERATED ALWAYS AS ({:d}) STORED"
                                        .format(year)
                                }
                            }
                        }
                    )
                    logging.warning("Generating year column for " + file_path)
                    continue
                raise ValueError("No {} column in {}".format(key, file_path))
            cname, c = split(column)
            if key == cname:
                c["index"] = "true"
                continue
            columns.append(
                {
                    key: {
                        "type": c["type"],
                        "index": "true",
                        "source": {
                            "type": "generated",
                            "code": "GENERATED ALWAYS AS ({}) STORED"
                                .format(cname)
                        }
                    }
                }
            )
        
        columns.append({
            "FILE": {
                "description": "original file name",
                "index": "required_before_loading_data: true",
                "source": {
                    "type": "file"
                },
                "type": "VARCHAR(128)"
            }
        })
        columns.append({
            "RECORD": {
                "description": "Record (line) number in the file",
                "index": True,
                "type": "SERIAL"
            }
        })

        self.registry[self.domain]["tables"][table] = {
            "columns": columns,
            "primary_key": [
                "FILE",
                "RECORD"
            ]
        }


if __name__ == '__main__':
    SASIntrospector.process(*sys.argv[1:])