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
    this class manages the cloud sql metadata upload for CCLE program
    """
    CCLE_metadata_project = {
        'table_name': 'CCLE_metadata_project',
        'primary_key_name': 'metadata_project_id',
        'columns': [
            ['project_short_name', 'VARCHAR(40)', 'NOT NULL'],
            ['name', 'VARCHAR(80)', 'NOT NULL'],
            ['program_name', 'VARCHAR(40)', 'NULL'],
            ['primary_site', 'VARCHAR(20)', 'NULL'],
            ['dbgap_accession_number', 'VARCHAR(12)', 'NULL'],
            ['disease_type', 'VARCHAR(120)', 'NULL'],
            ['summary_case_count', 'INTEGER', 'NULL'],
            ['summary_file_count', 'INTEGER', 'NULL'],
            ['summary_file_size', 'BIGINT', 'NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
        ],
#         'natural_key_cols': [
#             'case_barcode'
#         ],
        'indices_defs': [
            ['primary_site'],
            ['disease_type'],
            ['project_short_name'],
            ['program_name'],
            ['endpoint_type'],
        ]
    }
    
    CCLE_metadata_clinical = {
        'table_name': 'CCLE_metadata_clinical',
        'primary_key_name': 'metadata_clinical_id',
        'columns': [
            ['endpoint_type', 'VARCHAR(10)', 'NOT NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['program_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['summary_file_count', 'INT', 'NOT NULL'],
            ['gender', 'VARCHAR(1)', 'NULL'],
            ['histology', 'VARCHAR(60)', 'NULL'],
            ['hist_subtype', 'VARCHAR(60)', 'NULL'],
            ['site_primary', 'VARCHAR(35)', 'NULL'],
            ['source', 'VARCHAR(12)', 'NULL']
        ],
#         'natural_key_cols': [
#             'case_barcode'
#         ],
        'indices_defs': [
            ['endpoint_type'],
            ['case_gdc_id'],
            ['case_barcode'],
            ['program_name'],
            ['project_short_name'],
            ['project_disease_type'],
            ['summary_file_count'],
            ['gender'],
            ['histology'],
            ['hist_subtype'],
            ['site_primary'],
            ['source']
        ]
    }
    
    CCLE_metadata_biospecimen = {
        'table_name': 'CCLE_metadata_biospecimen',
        'primary_key_name': 'metadata_biospecimen_id',
        'columns': [
            ['endpoint_type', 'VARCHAR(10)', 'NOT NULL'],
            ['sample_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['sample_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['program_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['sample_type', 'VARCHAR(2)', 'NOT NULL']
        ],
#         'natural_key_cols': [
#             'sample_barcode'
#         ],
        'indices_defs': [
            ['endpoint_type'],
            ['sample_gdc_id'],
            ['sample_barcode'],
            ['case_barcode'],
            ['program_name'],
            ['project_short_name'],
            ['project_disease_type'],
            ['sample_type']
        ],
#         'foreign_key': [
#             'case_barcode',
#             'metadata_clinical',
#             'case_barcode'
#         ]
    }
    
    CCLE_metadata_data = {
        'table_name': 'CCLE_metadata_data',
        'primary_key_name': 'metadata_data_id',
        'columns': [
            ['file_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['case_barcode', 'VARCHAR(35)', 'NULL'],
            ['sample_gdc_id', 'VARCHAR(45)', 'NULL'],
            ['sample_barcode', 'VARCHAR(45)', 'NULL'],
            ['sample_type_code', 'VARCHAR(2)', 'NULL'],
            ['aliquot_barcode', 'VARCHAR(45)', 'NULL'],
            ['aliquot_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['program_name', 'VARCHAR(40)', 'NULL'],
            ['data_type', 'VARCHAR(35)', 'NOT NULL'],
            ['data_category', 'VARCHAR(30)', 'NOT NULL'],
            ['experimental_strategy', 'VARCHAR(50)', 'NULL'],
            ['type', 'VARCHAR(40)', 'NULL'],
            ['file_submitter_id', 'VARCHAR(36)', 'NOT NULL'],
            ['file_name', 'VARCHAR(200)', 'NOT NULL'],
            ['file_size', 'BIGINT', 'NOT NULL'],
            ['file_state', 'VARCHAR(30)', 'NULL'],
            ['data_format', 'VARCHAR(10)', 'NOT NULL'],
            ['md5sum', 'VARCHAR(33)', 'NULL'],
            ['access', 'VARCHAR(10)', 'NOT NULL'],
            ['acl', 'VARCHAR(25)', 'NULL'],
            ['state', 'VARCHAR(20)', 'NULL'],
            ['platform', 'VARCHAR(50)', 'NULL'],
            ['file_namekey', 'VARCHAR(200)', 'NULL'],
            ['file_uploaded', 'VARCHAR(5)', 'NOT NULL'],
            ['created_datetime', 'DATETIME', 'NULL'],
            ['updated_datetime', 'DATETIME', 'NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
            ['analysis_analysis_id', 'VARCHAR(36)', 'NULL'],
            ['analysis_workflow_link', 'VARCHAR(60)', 'NULL'],
            ['analysis_workflow_type', 'VARCHAR(60)', 'NULL'],
            ['center_code', 'VARCHAR(8)', 'NULL'],
            ['center_name', 'VARCHAR(50)', 'NULL'],
            ['center_type', 'VARCHAR(8)', 'NULL'],
            ['species', 'VARCHAR(30)', 'NULL'],
            ['archive_file_name', 'VARCHAR(60)', 'NULL'],
            ['archive_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['archive_submitter_id', 'VARCHAR(50)', 'NULL'],
            ['archive_revision', 'INT', 'NULL'],
            ['archive_state', 'VARCHAR(8)', 'NULL'],
# this should probably be a foreign key back to the index file record
            ['index_file_id', 'VARCHAR(36)', 'NULL'],
            ['index_file_name', 'VARCHAR(50)', 'NULL'],
            ['index_file_size', 'BIGINT', 'NULL'],
        ],
#         'natural_key_cols': [
#             'aliquot_barcode',
#             'DatafileName'
#         ],
        'indices_defs': [
            ['file_gdc_id'],
            ['case_gdc_id'],
            ['case_barcode'],
            ['sample_gdc_id'],
            ['sample_barcode'],
            ['sample_type_code'],
            ['aliquot_barcode'],
            ['aliquot_gdc_id'],
            ['project_short_name'],
            ['project_disease_type'],
            ['program_name'],
            ['data_type'],
            ['data_category'],
            ['experimental_strategy'],
            ['type'],
            ['data_format'],
            ['state'],
            ['platform'],
            ['file_uploaded'],
            ['endpoint_type'],
            ['analysis_workflow_link'],
            ['analysis_workflow_type']
        ],
#         'foreign_key': [
#             'sample_barcode',
#             'metadata_biospecimen',
#             'sample_barcode'
#         ]
    }

    CCLE_metadata_samples = {
        'table_name': 'CCLE_metadata_samples',
        'primary_key_name': 'metadata_samples_id',
        'columns': [
            ['case_barcode', 'VARCHAR(45)', 'NOT NULL'],
            ['program_name', 'VARCHAR(40)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(40)', 'NOT NULL'],
            ['sample_barcode', 'VARCHAR(45)', 'NOT NULL'],
            ['sample_type', 'VARCHAR(2)', 'NOT NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NOT NULL'],
            ['gender', 'VARCHAR(1)', 'NULL'],
            ['histology', 'VARCHAR(60)', 'NULL'],
            ['hist_subtype', 'VARCHAR(60)', 'NULL'],
            ['site_primary', 'VARCHAR(35)', 'NULL']
        ],
        'indices_defs': [
            ['case_barcode'],
            ['sample_barcode'],
            ['program_name'],
            ['project_short_name'],
            ['project_disease_type'],
            ['sample_barcode'],
            ['sample_type'],
            ['endpoint_type'],
            ['gender'],
            ['histology'],
            ['hist_subtype'],
            ['site_primary']
        ],
        'natural_key_cols': ['sample_barcode'],
#         'foreign_key': [
#             'sample_barcode',
#         ]
    }
    
    metadata_tables = OrderedDict(
        [
            ('CCLE_metadata_project', CCLE_metadata_project),
            ('CCLE_metadata_clinical', CCLE_metadata_clinical),
            ('CCLE_metadata_biospecimen', CCLE_metadata_biospecimen),
            ('CCLE_metadata_samples', CCLE_metadata_samples),
            ('CCLE_metadata_data', CCLE_metadata_data)
        ]
    )

