'''
a wrapper to google cloud sql.

the MySQLdb module is thread safe but the connections to the database are not.  so the
recommendation is that each thread have an independent connection.  currently, each
database access will use its own connection and at the end of the method, close it.
if this becomes expensive, timewise, a mapping of thread to connection can be utilized.

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

from isbcgc_cloudsql_tcga_model import ISBCGC_database_helper

class ISBCGC_database_helper(ISBCGC_database_helper):
    """
    this class manages the cloud sql metadata upload
    """
    TCGA_metadata_annotation = {
        'table_name': 'TCGA_metadata_annotation',
        'primary_key_name': 'metadata_annotation_id',
        'columns': [
            ['annotation_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['category', 'VARCHAR(100)', 'NOT NULL'],
            ['classification', 'VARCHAR(30)', 'NOT NULL'],
            ['status', 'VARCHAR(20)', 'NOT NULL'],
            ['notes', 'VARCHAR(700)', 'NULL'],
            ['entity_type', 'VARCHAR(10)', 'NOT NULL'],
            ['entity_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['entity_barcode', 'VARCHAR(28)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NULL'],
            ['program_name', 'VARCHAR(40)', 'NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(30)', 'NOT NULL'],
            ['sample_barcode', 'VARCHAR(35)', 'NULL'],
            ['aliquot_barcode', 'VARCHAR(40)', 'NULL'],
            ['date_created', 'DATETIME', 'NULL'],
            ['date_edited', 'DATETIME', 'NOT NULL'],
            ['annotation_submitter_id', 'VARCHAR(10)', 'NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
        ],

        'indices_defs': [
            ['category'],
            ['classification'],
            ['entity_type'],
            ['entity_barcode'],
            ['project_short_name'],
            ['program_name'],
            ['case_barcode'],
            ['date_created'],
            ['date_edited'],
            ['annotation_submitter_id'],
            ['endpoint_type']
        ]
    }
    
    """
    these class manages the cloud sql metadata annotation associations upload
    """
    TCGA_metadata_annotation2samples = {
        'table_name': 'TCGA_metadata_annotation2samples',

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
                'TCGA_metadata_annotation',
                'metadata_annotation_id'
            ],
            [
                'metadata_samples_id',
                'TCGA_metadata_samples',
                'metadata_samples_id'
            ]
        ]
    }

    TCGA_metadata_annotation2clinical = {
        'table_name': 'TCGA_metadata_annotation2clinical',

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
                'TCGA_metadata_annotation',
                'metadata_annotation_id'
            ],
            [
                'metadata_clinical_id',
                'TCGA_metadata_clinical',
                'metadata_clinical_id'
            ]
        ]
    }

    TCGA_metadata_annotation2biospecimen = {
        'table_name': 'TCGA_metadata_annotation2biospecimen',

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
                'TCGA_metadata_annotation',
                'metadata_annotation_id'
            ],
            [
                'metadata_biospecimen_id',
                'TCGA_metadata_biospecimen',
                'metadata_biospecimen_id'
            ]
        ]
    }

    TCGA_metadata_annotation2data = {
        'table_name': 'TCGA_metadata_annotation2data',

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
                'TCGA_metadata_annotation',
                'metadata_annotation_id'
            ],
            [
                'metadata_data_id',
                'TCGA_metadata_data',
                'metadata_data_id'
            ]
        ]
    }

    metadata_tables = OrderedDict(
        [
            ('TCGA_metadata_annotation', TCGA_metadata_annotation),
            ('TCGA_metadata_annotation2clinical', TCGA_metadata_annotation2clinical),
            ('TCGA_metadata_annotation2biospecimen', TCGA_metadata_annotation2biospecimen),
            ('TCGA_metadata_annotation2samples', TCGA_metadata_annotation2samples),
            ('TCGA_metadata_annotation2data', TCGA_metadata_annotation2data)
        ]
    )

