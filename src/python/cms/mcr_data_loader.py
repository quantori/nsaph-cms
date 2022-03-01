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
import os
from typing import List, Tuple, Any, Callable

from cms.fts2yaml import mcr_type, MedicareFTS
from nsaph.loader.data_loader import DataLoader
from nsaph_utils.utils.fwf import FWFReader


class MedicareDataLoader(DataLoader):
    @classmethod
    def open(cls, name) -> FWFReader:
        f, ext = os.path.splitext(name)
        if ext.lower() == ".fts":
            fts_path = name
            dat_path = f + ".dat"
        elif ext.lower() == ".dat":
            dat_path = name
            fts_path = f + ".fts"
        else:
            dat_path = name + ".dat"
            fts_path = name + ".fts"
        basedir, fname = os.path.split(f)
        t = mcr_type(fname)
        fts = MedicareFTS(t).init(fts_path)
        return FWFReader(fts.to_fwf_meta(dat_path))

    def __init__(self, context):
        super().__init__(context)

    def get_files(self) -> List[Tuple[Any, Callable]]:
        objects = []
        for path in self.context.data:
            objects.append(self.open(path))
        return objects


if __name__ == '__main__':
    loader = MedicareDataLoader(None)
    loader.run()
