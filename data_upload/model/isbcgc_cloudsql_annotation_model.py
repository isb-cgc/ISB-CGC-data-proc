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
import isbcgc_cloudsql_model
from collections import OrderedDict

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

    metadata_annotation2samples = {
        'table_name': 'metadata_annotation2samples',

        'columns': [
            ['metadata_annotation_id', 'INTEGER', 'NOT NULL'],
            ['metadata_samples_id', 'INTEGER', 'NOT NULL'],
        ],

        'indices_defs': [
            ['metadata_annotation_id', 'metadata_samples_id'],
        ],
        
        'foreign_keys': [
            [
                'metadata_annotation_id',
                'metadata_annotation',
                'metadata_annotation_id'
            ],
            [
                'metadata_samples_id',
                'metadata_samples',
                'metadata_samples_id'
            ]
        ]
    }

    metadata_annotation2clinical = {
        'table_name': 'metadata_annotation2clinical',

        'columns': [
            ['metadata_annotation_id', 'INTEGER', 'NOT NULL'],
            ['metadata_clinical_id', 'INTEGER', 'NOT NULL'],
        ],

        'indices_defs': [
            ['metadata_annotation_id', 'metadata_clinical_id'],
        ],
        
        'foreign_keys': [
            [
                'metadata_annotation_id',
                'metadata_annotation',
                'metadata_annotation_id'
            ],
            [
                'metadata_clinical_id',
                'metadata_clinical',
                'metadata_clinical_id'
            ]
        ]
    }

    metadata_annotation2biospecimen = {
        'table_name': 'metadata_annotation2biospecimen',

        'columns': [
            ['metadata_annotation_id', 'INTEGER', 'NOT NULL'],
            ['metadata_biospecimen_id', 'INTEGER', 'NOT NULL'],
        ],

        'indices_defs': [
            ['metadata_annotation_id', 'metadata_biospecimen_id'],
        ],
        
        'foreign_keys': [
            [
                'metadata_annotation_id',
                'metadata_annotation',
                'metadata_annotation_id'
            ],
            [
                'metadata_biospecimen_id',
                'metadata_biospecimen',
                'metadata_biospecimen_id'
            ]
        ]
    }

    metadata_annotation2data = {
        'table_name': 'metadata_annotation2data',

        'columns': [
            ['metadata_annotation_id', 'INTEGER', 'NOT NULL'],
            ['metadata_data_id', 'INTEGER', 'NOT NULL'],
        ],

        'indices_defs': [
            ['metadata_annotation_id', 'metadata_data_id'],
        ],
        
        'foreign_keys': [
            [
                'metadata_annotation_id',
                'metadata_annotation',
                'metadata_annotation_id'
            ],
            [
                'metadata_data_id',
                'metadata_data',
                'metadata_data_id'
            ]
        ]
    }

    isbcgc_cloudsql_model.ISBCGC_database_helper.metadata_tables = OrderedDict([('metadata_annotation', metadata_annotation),
            ('metadata_annotation2clinical', metadata_annotation2clinical),
            ('metadata_annotation2biospecimen', metadata_annotation2biospecimen),
            ('metadata_annotation2samples', metadata_annotation2samples),
            ('metadata_annotation2data', metadata_annotation2data)]
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
