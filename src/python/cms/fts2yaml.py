#  Copyright (c) 2021. Harvard University
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
import os
from typing import List

import yaml

from nsaph import ORIGINAL_FILE_COLUMN
from nsaph_utils.utils.io_utils import fopen
from nsaph.pg_keywords import *


MEDICARE_FILE_TYPES = ["mbsf_ab", "mbsf_d", "mbsf_abcd", "medpar"]


def width(s:str):
    if '.' in s:
        x = s.split('.')
        return (int(x[0]), int(x[1]))
    return (int(s), None)


class ColumnAttribute:
    def __init__(self, start:int, end:int, conv):
        self.start = start
        self.end = end
        self.conv = conv

    def arg(self, line:str):
        try:
            return self.conv(line[self.start:self.end].strip())
        except:
            pass

    def __str__(self):
        return "[{:d}:{:d}] {}".format(self.start, self.end, str(self.conv))


class ColumnReader:
    def __init__(self, constructor, pattern):
        self.constructor = constructor
        fields = pattern.split(' ')
        assert len(fields) == constructor.nattrs
        self.attributes = []
        c = 0
        for i in range(0, len(fields)):
            l = len(fields[i])
            f = constructor.conv(i)
            self.attributes.append(ColumnAttribute(c, c+l, f))
            c += l + 1
        self.attributes[-1].end = None

    def read(self, line):
        attrs = [a.arg(line) for a in self.attributes]
        return self.constructor(*attrs)


class FTSColumn:
    @classmethod
    def conv(cls, i):
        if i in [0, 4]:
            f = int
        else:
            f = str
        return f

    def __init__(self, order, column, c_type, c_format, c_width, label):
        self.order = order
        self.column = column
        self.type = c_type
        self.format = c_format
        self.width = c_width
        self.label = label
        self._attrs = [
            attr for attr in self.__dict__ if attr[0] != '_'
        ]

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, FTSColumn):
            return False
        if o is self:
            return True
        for attr in self._attrs:
            if getattr(self, attr) != getattr(o, attr):
                return False
        return True

    def analyze_format(self):
        if self.format is not None:
            if self.format[0].isdigit():
                fmt = self.format
                can_be_numeric = True
            else:
                fmt = self.format[1:]
                can_be_numeric = False
            x = fmt.split('.')
            if x[0].isdigit():
                w = int(x[0])
            else:
                w = None
            if len(x) > 1 and x[1]:
                scale = int(x(1))
            else:
                scale = None
        else:
            scale = None
            can_be_numeric = True
            w = self.width
        return can_be_numeric, scale, w

    def to_sql_type(self):
        t = self.type.upper()
        if t in [PG_SERIAL_TYPE]:
            return t

        can_be_numeric, scale, wdt = self.analyze_format()
        if t == "CHAR":
            return "{}({:d})".format(PG_STR_TYPE, wdt)
        if t == "NUM":
            if not can_be_numeric:
                return "{}({:d})".format(PG_STR_TYPE, wdt)
            if scale is not None:
                return "{}({:d},{:d})".format(PG_NUMERIC_TYPE, wdt, scale)
            return "{}".format(PG_INT_TYPE)
        if t == "DATE":
            return PG_DATE_TYPE
        raise Exception("Unexpected column type: {}".format(t))

    def __str__(self) -> str:
        return "{:d}: {} [{}]".format(self.order, self.column, self.type)


class MedicaidFTSColumn(FTSColumn):
    nattrs = 6


class MedicareFTSColumn(FTSColumn):
    nattrs = 7

    @classmethod
    def conv(cls, i):
        if i in [0, 4, 5]:
            f = int
        else:
            f = str
        return f

    def __init__(self, order: int, long_name:str, short_name:str, type:str, start:int, width, desc:str):
        super().__init__(
            order,
            column=short_name,
            c_type=type,
            c_width=width,
            c_format=None,
            label=desc
        )
        self.long_name = long_name
        self.start = start - 1
        self.end = self.start + self.width


class CMSFTS:
    common_indices = [
                "BENE_ID",
                "EL_DOB",
                "EL_SEX_CD",
                "EL_DOD",
                "EL_RACE_ETHNCY_CD",
                ORIGINAL_FILE_COLUMN
            ]

    def __init__(self, type_of_data: str):
        """

        :param type_of_data: Can be either `ps` for personal summary or
            `ip` for inpatient admissions data
        """

        self.name = type_of_data.lower()
        self.indices = self.common_indices
        self.columns = None
        self.pk = None
        self.constructor = None
        return

    def init(self, path: str):
        pass

    def read_file(self, f):
        with fopen(f, "rt") as fts:
            lines = [line for line in fts]
        i = 0
        column_reader = None
        for i in range(0, len(lines)):
            line = lines[i]
            if line.startswith('---') and '------------------' in line:
                column_reader = ColumnReader(self.constructor, line)
                break
            continue

        if 1 > i or i > len(lines) - 2:
            raise Exception("Column definitions are not found in {}".format(f))

        columns = []
        while i < len(lines):
            i += 1
            line = lines[i]
            if not line.strip():
                break
            if line.startswith("Note:"):
                break
            if line.startswith("-") and "End" in line:
                break
            column = column_reader.read(line)
            columns.append(column)

        self.on_after_read_file(columns)
        if not self.columns:
            self.columns = columns
            return

        if len(columns) != len(self.columns):
            raise Exception("Reconciliation required: {}, number of columns".format(f))

        for i in range(len(columns)):
            if columns[i] != self.columns[i]:
                raise Exception("Reconciliation required: {}, column: {}".format(f, columns[i]))

    def on_after_read_file(self, columns: List[FTSColumn]):
        self.add_file_column(columns)

    @staticmethod
    def add_record_column(columns: List[FTSColumn]):
        column = FTSColumn(
            order=len(columns) + 1,
            column="RECORD",
            c_type=PG_SERIAL_TYPE,
            c_format=None,
            c_width=None,
            label="Record number in the file"
        )
        columns.append(column)

    @staticmethod
    def add_file_column(columns: List[FTSColumn]):
        column = FTSColumn(
            order=len(columns) + 1,
            column=ORIGINAL_FILE_COLUMN,
            c_type="CHAR",
            c_format="128",
            c_width=128,
            label="RESDAC original file name"
        )
        columns.append(column)

    def column_to_dict(self, c: MedicaidFTSColumn) -> dict:
        d = {
            "type": c.to_sql_type(),
            "description": c.label
        }
        if c.column in self.indices:
            d["index"] = True
        if c.column == ORIGINAL_FILE_COLUMN:
            d["source"] = {
                "type": "file"
            }
            d["index"] = {"required_before_loading_data": True}
        return d

    def to_dict(self):
        table = dict()
        table[self.name] = dict()
        table[self.name]["columns"] = [
            {
                c.column: self.column_to_dict(c)
            } for c in self.columns
        ]
        table[self.name]["primary_key"] = self.pk
        return table

    def print_yaml(self, root_dir: str = None):
        self.init(root_dir)
        table = self.to_dict()
        print(yaml.dump(table))


class MedicaidFTS(CMSFTS):
    def __init__(self, type_of_data: str):
        super().__init__(type_of_data)
        self.constructor = MedicaidFTSColumn
        assert self.name in ["ps", "ip"]
        self.pattern = "**/maxdata_{}_*.fts".format(type_of_data)
        if self.name == "ps":
            year_column = "MAX_YR_DT"
            self.pk = ["MSIS_ID", "STATE_CD", year_column]
            self.indices += self.pk.copy()
            self.indices.append("EL_AGE_GRP_CD")
        else:
            year_column = "YR_NUM"
            self.pk = ["FILE", "RECORD"]
            self.indices += ["MSIS_ID", "STATE_CD", year_column, "RECORD"]

    def init(self, path: str = None):
        if path is not None:
            pattern = os.path.join(path, self.pattern)
        else:
            pattern = self.pattern
        files = glob.glob(pattern)
        for file in files:
            self.read_file(file)
        return self

    def on_after_read_file(self, columns: List[FTSColumn]):
        super().on_after_read_file(columns)
        if self.name == "ip":
            self.add_record_column(columns)


class MedicareFTS(CMSFTS):
    def __init__(self, type_of_data: str):
        super().__init__(type_of_data)
        self.constructor = MedicareFTSColumn
        assert self.name in MEDICARE_FILE_TYPES
        self.pattern = "**/{}_*.fts".format(type_of_data)
        self.pk = ["FILE", "RECORD"]
        if self.name.startswith("mbsf"):
            year_column = "RFRNC_YR"
        elif self.name == "medpar":
            year_column = "MEDPAR_YR_NUM"
        else:
            raise ValueError(self.name)
        self.indices += ["BENE_ID", year_column]
        if  self.name.startswith("mbsf_ab"):
            self.indices.append("STATE_CD")

    def init(self, path: str):
        self.read_file(path)
        return self

    def on_after_read_file(self, columns: List[FTSColumn]):
        super().on_after_read_file(columns)
        self.add_record_column(columns)


if __name__ == '__main__':
    source = MedicaidFTS("ps")
    source.print_yaml()

