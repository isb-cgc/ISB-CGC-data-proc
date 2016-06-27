'''
a wrapper to google cloud sql.

the MySQLdb module is thread safe but the connections to the database are not.  so the
recommendation is that each thread have an independent connection.

Copyright 2015, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
from collections import OrderedDict

import isbcgc_cloudsql_model

class ISBCGC_database_helper(isbcgc_cloudsql_model.ISBCGC_database_helper):
    """
    this class manages the cloud sql metadata datadictionary upload
    """
    metadata_datadictionary = {
        'table_name': 'metadata_datadictionary',
        'primary_key_name': 'metadata_datadictionary_id',
        'columns': [
            ['MetadataFieldName', 'VARCHAR(70)', 'NULL'],
            ['ISBCGCTerm', 'VARCHAR(90)', 'NULL'],
            ['MetadataTable', 'VARCHAR(22)', 'NULL'],
            ['Source', 'VARCHAR(200)', 'NULL'],
            ['SourceComment', 'VARCHAR(200)', 'NULL'],
            ['ValueType', 'VARCHAR(30)', 'NULL'],
            ['Definition', 'VARCHAR(1000)', 'NULL'],
            ['ExampleValue', 'VARCHAR(500)', 'NULL'],
            ['ControlledVocabulary', 'VARCHAR(63)', 'NULL'],
            ['CDEPublicID', 'VARCHAR(10)', 'NULL']
        ],
#         'natural_key_cols': [
#             'MetadataFieldName',
#             'MetadataTable'
#         ],
        'indices_defs': [
            ['MetadataFieldName'],
            ['ISBCGCTerm'],
            ['MetadataTable'],
            ['Source'],
            ['SourceComment'],
            ['ValueType'],
            ['ExampleValue'],
            ['ControlledVocabulary'],
            ['CDEPublicID']
        ]
    }

    isbcgc_cloudsql_model.ISBCGC_database_helper.metadata_tables = OrderedDict(
        [
            ('metadata_datadictionary', metadata_datadictionary)
        ]
    )

    self = None

    def __init__(self, config, log):
        isbcgc_cloudsql_model.ISBCGC_database_helper.__init__(self, config, log)

    @classmethod
    def initialize(cls, config, log):
        if cls.self:
            log.warning('class has already been initialized')
        else:
            cls.self = ISBCGC_database_helper(config, log)
