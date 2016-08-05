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
    this class manages the cloud sql metadata annotation upload
    """
    metadata_annotation = {
        'table_name': 'metadata_annotation',
        'primary_key_name': 'metadata_annotation_id',

        'columns': [
            ['annotationId', 'INTEGER', 'NOT NULL'],
            ['annotationCategoryId', 'INTEGER', 'NOT NULL'],
            ['annotationCategoryName', 'VARCHAR(100)', 'NOT NULL'],
            ['annotationClassificationId', 'INTEGER', 'NOT NULL'],
            ['annotationClassification', 'VARCHAR(30)', 'NOT NULL'],
            ['annotationNoteText', 'VARCHAR(700)', 'NULL'],
            ['Study', 'VARCHAR(4)', 'NOT NULL'],
            ['itemTypeId', 'INTEGER', 'NOT NULL'],
            ['itemTypeName', 'VARCHAR(20)', 'NOT NULL'],
            ['itemBarcode', 'VARCHAR(28)', 'NOT NULL'],
            ['AliquotBarcode', 'VARCHAR(28)', 'NULL'],
            ['ParticipantBarcode', 'VARCHAR(12)', 'NOT NULL'],
            ['SampleBarcode', 'VARCHAR(16)', 'NULL'],
            ['dateAdded', 'DATETIME', 'NULL'],
            ['dateCreated', 'DATETIME', 'NOT NULL'],
            ['dateEdited', 'DATETIME', 'NULL'],
        ],

        'indices_defs': [
            ['annotationId'],
            ['annotationCategoryId'],
            ['annotationCategoryName'],
            ['annotationClassificationId'],
            ['annotationClassification'],
            ['Study'],
            ['itemTypeId'],
            ['itemBarcode'],
            ['AliquotBarcode'],
            ['ParticipantBarcode'],
            ['SampleBarcode'],
        ]
    }

    metadata_tables = OrderedDict(
        [
            ('metadata_annotation', metadata_annotation)
        ]
    )

    @classmethod
    def initialize(cls, config, log):
        cls.setup_tables(config, log)
