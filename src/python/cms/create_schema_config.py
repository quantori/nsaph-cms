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

from nsaph_utils.utils.context import Context, Argument, Cardinality


class CMSSchema(Context):
    """
    Configuration object to configure parsing
    File Transfer Summary files and creating
    YAML data model
    """

    _output = Argument("output",
            help = "Output path for schema",
            type = str,
            default = None,
            cardinality = Cardinality.single
        )

    _input = Argument("input",
            help = "Path to directory containing FTS files."
                   + "Files are looked for by using "
                   + " '**/maxdata_(ps|ip)_*.fts' pattern",
            type = str,
            default = None,
            cardinality = Cardinality.single
        )

    def __init__(self, doc):
        self.output = None
        ''' Output path for schema '''
        self.input = None
        ''' Path to directory containing FTS files '''
        super().__init__(CMSSchema, doc, include_default = False)
