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

import isbcgc_cloudsql_model

class ISBCGC_database_helper(isbcgc_cloudsql_model.ISBCGC_database_helper):
    """
    this class manages the cloud sql metadata upload
    """
    metadata_gdc_annotation = {
        'table_name': 'metadata_gdc_annotation',
        'primary_key_name': 'metadata_gdc_annotation_id',
        'columns': [
            ['status', 'VARCHAR(20)', 'NOT NULL'],
            ['annotationId', 'VARCHAR(36)', 'NOT NULL'],
            ['annotationCategoryName', 'VARCHAR(100)', 'NOT NULL'],
            ['annotationClassification', 'VARCHAR(30)', 'NOT NULL'],
            ['annotationNoteText', 'VARCHAR(700)', 'NULL'],
            ['itemTypeId', 'VARCHAR(36)', 'NOT NULL'],
            ['itemTypeName', 'VARCHAR(20)', 'NOT NULL'],
            ['itemBarcode', 'VARCHAR(28)', 'NOT NULL'],
            ['AliquotBarcode', 'VARCHAR(28)', 'NULL'],
            ['ParticipantBarcode', 'VARCHAR(12)', 'NOT NULL'],
            ['SampleBarcode', 'VARCHAR(16)', 'NULL'],
            ['dateCreated', 'DATETIME', 'NOT NULL'],
            ['dateEdited', 'DATETIME', 'NULL'],
        ],

        'indices_defs': [
            ['annotationId'],
            ['annotationCategoryName'],
            ['annotationClassification'],
            ['itemTypeId'],
            ['itemBarcode'],
            ['AliquotBarcode'],
            ['ParticipantBarcode'],
            ['SampleBarcode'],
        ]
    }
    
    metadata_gdc_clinical = {
        'table_name': 'metadata_gdc_clinical',
        'primary_key_name': 'metadata_gdc_clinical_id',
        'columns': [
            ['ParticipantBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['ParticipantUUID', 'VARCHAR(36)', 'NOT NULL'],
            ['Project', 'VARCHAR(40)', 'NULL'],
            ['Program', 'VARCHAR(40)', 'NULL'],
        ],
#         'natural_key_cols': [
#             'ParticipantBarcode'
#         ],
        'indices_defs': [
            ['ParticipantBarcode'],
            ['Project'],
            ['Program'],
        ]
    }
    
    metadata_gdc_biospecimen = {
        'table_name': 'metadata_gdc_biospecimen',
        'primary_key_name': 'metadata_gdc_biospecimen_id',
        'columns': [
            ['ParticipantBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['Project', 'VARCHAR(40)', 'NULL'],
            ['Program', 'VARCHAR(40)', 'NULL'],
            ['SampleBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['SampleUUID', 'VARCHAR(36)', 'NULL'],
            ['PathologyReportUUID', 'VARCHAR(36)', 'NULL'],
        ],
#         'natural_key_cols': [
#             'SampleBarcode'
#         ],
        'indices_defs': [
            ['ParticipantBarcode'],
            ['SampleBarcode'],
            ['Project'],
            ['Program'],
        ],
#         'foreign_key': [
#             'ParticipantBarcode',
#             'metadata_gdc_clinical',
#             'ParticipantBarcode'
#         ]
    }
    
    metadata_gdc_data = {
        'table_name': 'metadata_gdc_data',
        'primary_key_name': 'metadata_gdc_data_id',
        'columns': [
            ['ParticipantBarcode', 'VARCHAR(35)', 'NULL'],
            ['SampleBarcode', 'VARCHAR(45)', 'NULL'],
            ['AliquotBarcode', 'VARCHAR(45)', 'NULL'],
            ['AliquotUUID', 'VARCHAR(36)', 'NULL'],
            ['Project', 'VARCHAR(40)', 'NULL'],
            ['Program', 'VARCHAR(40)', 'NULL'],
            ['DataType', 'VARCHAR(35)', 'NOT NULL'],
            ['FileName', 'VARCHAR(200)', 'NOT NULL'],
            ['md5sum', 'VARCHAR(33)', 'NULL'],
            ['DataFormat', 'VARCHAR(10)', 'NOT NULL'],
            ['access', 'VARCHAR(10)', 'NOT NULL'],
            ['state', 'VARCHAR(20)', 'NULL'],
            ['DataCategory', 'VARCHAR(30)', 'NOT NULL'],
            ['file_size', 'INT', 'NOT NULL'],
            ['file_state', 'VARCHAR(30)', 'NULL'],
            ['ExperimentalStrategy', 'VARCHAR(20)', 'NULL'],
            ['MetadataFilename', 'VARCHAR(200)', 'NULL'],
        ],
#         'natural_key_cols': [
#             'AliquotBarcode',
#             'DatafileName'
#         ],
        'indices_defs': [
            ['ParticipantBarcode'],
            ['SampleBarcode'],
            ['AliquotBarcode'],
            ['AliquotUUID'],
            ['Project'],
            ['Program'],
            ['DataType'],
            ['FileName'],
            ['DataFormat'],
            ['access'],
            ['state'],
            ['DataCategory'],
            ['file_state'],
            ['ExperimentalStrategy']
        ],
#         'foreign_key': [
#             'SampleBarcode',
#             'metadata_gdc_biospecimen',
#             'SampleBarcode'
#         ]
    }

    metadata_gdc_samples = {
        'table_name': 'metadata_gdc_samples',
        'primary_key_name': 'metadata_gdc_samples_id',  # todo: define this?

        'columns': [
            ['ParticipantBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['Project', 'VARCHAR(40)', 'NOT NULL'],
            ['Program', 'VARCHAR(40)', 'NOT NULL'],
            ['SampleBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['SampleUUID', 'VARCHAR(36)', 'NOT NULL']
        ],
#         'natural_key_cols': [
#             'SampleBarcode'
#         ],
        'indices_defs': [
            ['ParticipantBarcode'],
            ['SampleBarcode'],
            ['Project'],
            ['Program'],
        ],
        'natural_key_cols': ['SampleBarcode'],
#         'foreign_key': [
#             'SampleBarcode',
#         ]
    }
    
    isbcgc_cloudsql_model.ISBCGC_database_helper.metadata_tables = OrderedDict(
        [
            ('metadata_gdc_annotation', metadata_gdc_annotation),
            ('metadata_gdc_clinical', metadata_gdc_clinical),
            ('metadata_gdc_biospecimen', metadata_gdc_biospecimen),
            ('metadata_gdc_data', metadata_gdc_data),
            ('metadata_gdc_samples', metadata_gdc_samples)
        ]
    )


    @classmethod
    def initialize(cls, config, log):
        cls.setup_tables(config, log)

