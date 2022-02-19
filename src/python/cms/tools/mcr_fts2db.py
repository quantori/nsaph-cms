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
import copy
import glob
import os
from typing import List

from cms.registry import Registry

from cms.create_schema_config import CMSSchema

from cms.fts2yaml import mcr_type
from nsaph.loader.data_loader import DataLoader

from nsaph.loader import LoaderConfig


class MedicareLoader:
    @classmethod
    def process(cls):
        loader = MedicareLoader()
        loader.traverse(loader.pattern)

    def __init__(self):
        self.pattern = "[1-2]*/*.fts"
        self.context = LoaderConfig(__doc__)
        self.context.domain = "cms"
        self.context.set_empty_args()
        self.root_dir=self.context.data
        if not self.context.incremental and not self.context.sloppy:
            self.context.reset = True
        return

    def traverse(self, pattern: str):
        if isinstance(self.root_dir, list):
            dirs = self.root_dir
        else:
            dirs = [self.root_dir]
        files: List[str] = []
        for d in dirs:
            files.extend(glob.glob(os.path.join(d, pattern), recursive=True))
        for f in files:
            self.handle(f)
        return

    def handle(self, fts_path: str):
        basedir, fname = os.path.split(fts_path)
        _, ydir = os.path.split(basedir)
        year = int(ydir)
        f, ext = os.path.splitext(fts_path)
        data_path = f + ".csv.gz"
        ttype = mcr_type(fname)
        ctxt = CMSSchema(None,
                         path=self.context.registry,
                         inpt=fts_path,
                         tp= "medicare",
                         reset=False)
        reg = Registry(ctxt)
        reg.update()
        context = copy.deepcopy(self.context)
        context.table = "{}_{:d}".format(ttype, year)
        context.pattern = [os.path.join("**", os.path.basename(data_path))]
        loader = DataLoader(context)
        loader.csv_delimiter = '\t'
        loader.run()


if __name__ == '__main__':
    MedicareLoader.process()
