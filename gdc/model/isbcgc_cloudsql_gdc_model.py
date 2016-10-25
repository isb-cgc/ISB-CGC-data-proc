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
            ['Project', 'VARCHAR(40)', 'NULL'],
            ['Program', 'VARCHAR(40)', 'NULL'],
            ['AliquotBarcode', 'VARCHAR(28)', 'NULL'],
            ['ParticipantBarcode', 'VARCHAR(12)', 'NOT NULL'],
            ['SampleBarcode', 'VARCHAR(16)', 'NULL'],
            ['dateCreated', 'DATETIME', 'NOT NULL'],
            ['dateEdited', 'DATETIME', 'NULL'],
            ['Endpoint', 'VARCHAR(8)', 'NULL'],
        ],

        'indices_defs': [
            ['annotationId'],
            ['annotationCategoryName'],
            ['annotationClassification'],
            ['itemTypeId'],
            ['itemBarcode'],
            ['Project'],
            ['Program'],
            ['AliquotBarcode'],
            ['ParticipantBarcode'],
            ['SampleBarcode'],
            ['Endpoint'],
        ]
    }
    
    metadata_gdc_project = {
        'table_name': 'metadata_gdc_project',
        'primary_key_name': 'metadata_gdc_project_id',
        'columns': [
            ['Project', 'VARCHAR(40)', 'NOT NULL'],
            ['ProjectName', 'VARCHAR(80)', 'NOT NULL'],
            ['released', 'VARCHAR(5)', 'NOT NULL'],
            ['state', 'VARCHAR(10)', 'NOT NULL'],
            ['PrimarySite', 'VARCHAR(20)', 'NULL'],
            ['dbGaP_AccessionNumber', 'VARCHAR(12)', 'NULL'],
            ['DiseaseType', 'VARCHAR(120)', 'NULL'],
            ['CountCases', 'INTEGER', 'NULL'],
            ['CountFiles', 'INTEGER', 'NULL'],
            ['SumFileSize', 'BIGINT', 'NULL'],
            ['Program', 'VARCHAR(40)', 'NULL'],
            ['Program_dbGaP_AccessionNumber', 'VARCHAR(12)', 'NULL'],
            ['ProgramID', 'VARCHAR(36)', 'NULL'],
            ['Endpoint', 'VARCHAR(8)', 'NULL'],
        ],
#         'natural_key_cols': [
#             'ParticipantBarcode'
#         ],
        'indices_defs': [
            ['DiseaseType'],
            ['Project'],
            ['Program'],
            ['Endpoint'],
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
            ['Endpoint', 'VARCHAR(8)', 'NULL'],
        ],
#         'natural_key_cols': [
#             'ParticipantBarcode'
#         ],
        'indices_defs': [
            ['ParticipantBarcode'],
            ['Project'],
            ['Program'],
            ['Endpoint'],
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
            ['Endpoint', 'VARCHAR(8)', 'NULL'],
        ],
#         'natural_key_cols': [
#             'SampleBarcode'
#         ],
        'indices_defs': [
            ['ParticipantBarcode'],
            ['SampleBarcode'],
            ['Project'],
            ['Program'],
            ['Endpoint'],
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
            ['SubmitterID', 'VARCHAR(50)', 'NULL'],
            ['CaseID', 'VARCHAR(36)', 'NULL'],
            ['CasesSubmitterID', 'VARCHAR(35)', 'NULL'],
            ['SamplesSubmitterID', 'VARCHAR(45)', 'NULL'],
            ['AliquotsSubmitterID', 'VARCHAR(45)', 'NULL'],
            ['AliquotsAliquotID', 'VARCHAR(36)', 'NULL'],
            ['ProjectID', 'VARCHAR(40)', 'NULL'],
            ['ProgramName', 'VARCHAR(40)', 'NULL'],
            ['DataType', 'VARCHAR(35)', 'NOT NULL'],
            ['DataCategory', 'VARCHAR(30)', 'NOT NULL'],
            ['ExperimentalStrategy', 'VARCHAR(50)', 'NULL'],
            ['Type', 'VARCHAR(40)', 'NULL'],
            ['FileID', 'VARCHAR(36)', 'NOT NULL'],
            ['FileName', 'VARCHAR(200)', 'NOT NULL'],
            ['FileSize', 'BIGINT', 'NOT NULL'],
            ['FileState', 'VARCHAR(30)', 'NULL'],
            ['DataFormat', 'VARCHAR(10)', 'NOT NULL'],
            ['MD5Sum', 'VARCHAR(33)', 'NULL'],
            ['Access', 'VARCHAR(10)', 'NOT NULL'],
            ['Acl', 'VARCHAR(25)', 'NULL'],
            ['State', 'VARCHAR(20)', 'NULL'],
            ['Platform', 'VARCHAR(50)', 'NULL'],
            ['MetadataFilesFileName', 'VARCHAR(200)', 'NULL'],
            ['AnnotationsStatus', 'VARCHAR(20)', 'NULL'],
            ['AnnotationsId', 'VARCHAR(36)', 'NULL'],
            ['AnnotationsCategory', 'VARCHAR(100)', 'NULL'],
            ['AnnotationsClassification', 'VARCHAR(30)', 'NULL'],
            ['AnnotationsNote', 'VARCHAR(700)', 'NULL'],
            ['AnnotationsEntityID', 'VARCHAR(36)', 'NULL'],
            ['AnnotationsEntityType', 'VARCHAR(20)', 'NULL'],
            ['AnnotationsEntitySubmitterID', 'VARCHAR(28)', 'NULL'],
            ['CreatedDatetime', 'DATE', 'NULL'],
            ['UpdatedDatetime', 'DATE', 'NULL'],
            ['Endpoint', 'VARCHAR(8)', 'NULL'],
            ['Analysis_InputFiles_FileID', 'VARCHAR(36)', 'NULL'],
            ['Analysis_WorkflowLink', 'VARCHAR(60)', 'NULL'],
            ['Analysis_WorkflowType', 'VARCHAR(60)', 'NULL'],
            ['AssociatedEntities_CaseID', 'VARCHAR(36)', 'NULL'],
            ['AssociatedEntities_EntityID', 'VARCHAR(36)', 'NULL'],
            ['AssociatedEntities_EntitySubmitterID', 'VARCHAR(45)', 'NULL'],
            ['AssociatedEntities_EntityType', 'VARCHAR(15)', 'NULL'],
            ['DownstreamAnalyses_OutputFiles_FileID', 'VARCHAR(36)', 'NULL'],
            ['DownstreamAnalyses_WorkflowLink', 'VARCHAR(200)', 'NULL'],
            ['DownstreamAnalyses_WorkflowType', 'VARCHAR(60)', 'NULL'],
        ],
#         'natural_key_cols': [
#             'AliquotBarcode',
#             'DatafileName'
#         ],
        'indices_defs': [
            ['SubmitterID'],
            ['CaseID'],
            ['CasesSubmitterID'],
            ['SamplesSubmitterID'],
            ['AliquotsSubmitterID'],
            ['AliquotsAliquotID'],
            ['ProjectID'],
            ['ProgramName'],
            ['DataType'],
            ['DataCategory'],
            ['ExperimentalStrategy'],
            ['Type'],
            ['FileID'],
            ['FileName'],
            ['FileState'],
            ['DataFormat'],
            ['Access'],
            ['Acl'],
            ['State'],
            ['AnnotationsStatus'],
            ['AnnotationsId'],
            ['AnnotationsCategory'],
            ['AnnotationsClassification'],
            ['AnnotationsEntityID'],
            ['AnnotationsEntityType'],
            ['AnnotationsEntitySubmitterID'],
            ['CreatedDatetime'],
            ['UpdatedDatetime'],
            ['Endpoint'],
            ['Analysis_InputFiles_FileID'],
            ['Analysis_WorkflowLink'],
            ['Analysis_WorkflowType'],
            ['AssociatedEntities_CaseID'],
            ['AssociatedEntities_EntityID'],
            ['AssociatedEntities_EntitySubmitterID'],
            ['AssociatedEntities_EntityType'],
            ['DownstreamAnalyses_OutputFiles_FileID'],
            ['DownstreamAnalyses_WorkflowLink'],
            ['DownstreamAnalyses_WorkflowType'],
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
            ['SampleUUID', 'VARCHAR(36)', 'NOT NULL'],
            ['Endpoint', 'VARCHAR(8)', 'NULL'],
        ],
        'indices_defs': [
            ['ParticipantBarcode'],
            ['SampleBarcode'],
            ['Project'],
            ['Program'],
            ['Endpoint'],
        ],
        'natural_key_cols': ['SampleBarcode'],
#         'foreign_key': [
#             'SampleBarcode',
#         ]
    }
    
    isbcgc_cloudsql_model.ISBCGC_database_helper.metadata_tables = OrderedDict(
        [
            ('metadata_gdc_annotation', metadata_gdc_annotation),
            ('metadata_gdc_project', metadata_gdc_project),
            ('metadata_gdc_clinical', metadata_gdc_clinical),
            ('metadata_gdc_biospecimen', metadata_gdc_biospecimen),
            ('metadata_gdc_data', metadata_gdc_data),
            ('metadata_gdc_samples', metadata_gdc_samples)
        ]
    )

    @classmethod
    def initialize(cls, config, log):
        cls.setup_tables(config, log)

