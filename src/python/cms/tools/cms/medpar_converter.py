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
import concurrent
import glob
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import List

from cms.tools.cms.medpar import Medpar


class MedParFileSet:
    def __init__(self, fts:str, dat: List[str]):
        self.fts = fts
        self.dat = dat
        self.year = None
        self.dir = os.path.dirname(fts)
        self.name, _ = os.path.splitext(os.path.basename(fts))
        d = self.dir
        while len(d) > 3:
            d, e = os.path.split(d)
            if e.isdigit():
                yyyy = int(e)
                if 1999 < yyyy < 2030:
                    if e in self.name:
                        self.year = yyyy
        if self.year is None:
            raise ValueError("Could not find year for " + fts)

    def __str__(self) -> str:
        return "{:d}: {}.(fts|{:d}-dat)".format(
            self.year, self.name, len(self.dat)
        )


class MedparConverter:
    @classmethod
    def find(cls, path: str) -> List[MedParFileSet]:
        datasets = []
        fts_files = sorted(glob.glob(os.path.join(path, "**", "*.fts"), recursive=True))
        for fts in fts_files:
            base, ext = os.path.splitext(fts)
            dat = sorted(glob.glob(base + "*.dat"))
            if not dat:
                raise ValueError(
                    "Mismatch: {} does not have corresponding dat file(s)".
                        format(fts)
                )
            datasets.append(MedParFileSet(fts, dat))
        return datasets

    def __init__(self, path: str):
        self.path = path
        self.datasets = self.find(path)

    def list(self):
        for dataset in self.datasets:
            print(dataset)

    def convert_dataset(self, dataset: MedParFileSet):
        m = Medpar(
            dir_path=dataset.dir,
            name=dataset.name,
            year=str(dataset.year),
            dest = os.path.join(self.path, str(dataset.year))
        )
        try:
            m.info()
            m.export()
            return "{}: SUCCESS".format(dataset.fts)
        except Exception as x:
            print(x)
            return "{}: FAILED".format(dataset.fts)

    def convert(self):
        with ThreadPoolExecutor() as executor:
            futures = []
            for dataset in self.datasets:
                futures.append(
                    executor.submit(self.convert_dataset, dataset=dataset)
                )
        for future in concurrent.futures.as_completed(futures):
            print(future.result())
            

if __name__ == '__main__':
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = os.curdir
    converter = MedparConverter(path)
    converter.list()
    converter.convert()
