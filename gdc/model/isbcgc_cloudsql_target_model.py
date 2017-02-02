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
    this class manages the cloud sql metadata upload for the TARGET program
    """
    TARGET_metadata_project = {
        'table_name': 'TARGET_metadata_project',
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
    
    TARGET_metadata_clinical = {
        'table_name': 'TARGET_metadata_clinical',
        'primary_key_name': 'metadata_clinical_id',
        'columns': [
            ['endpoint_type', 'VARCHAR(10)', 'NOT NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['program_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['summary_file_count', 'INT', 'NOT NULL'],
            ['gender', 'VARCHAR(6)', 'NULL'],
            ['vital_status', 'VARCHAR(5)', 'NULL'],
            ['race', 'VARCHAR(41)', 'NULL'],
            ['ethnicity', 'VARCHAR(22)', 'NULL'],
            ['first_event', 'VARCHAR(25)', 'NULL'],
            ['days_to_birth', 'INT', 'NULL'],
            ['days_to_death', 'INT', 'NULL'],
            ['age_at_diagnosis', 'INT', 'NULL'],
            ['year_of_diagnosis', 'INT', 'NULL'],
            ['year_of_last_follow_up', 'INT', 'NULL'],
            ['overall_survival', 'INT', 'NULL'],
            ['event_free_survival', 'INT', 'NULL'],
            ['wbc_at_diagnosis', 'FLOAT', 'NULL'],
            ['protocol', 'VARCHAR(70)', 'NULL']
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
            ['vital_status'],
            ['race'],
            ['ethnicity'],
            ['first_event'],
            ['days_to_birth'],
            ['days_to_death'],
            ['age_at_diagnosis'],
            ['year_of_diagnosis'],
            ['year_of_last_follow_up'],
            ['overall_survival'],
            ['event_free_survival'],
            ['wbc_at_diagnosis'],
            ['protocol']
        ]
    }
    
    TARGET_metadata_biospecimen = {
        'table_name': 'TARGET_metadata_biospecimen',
        'primary_key_name': 'metadata_biospecimen_id',
        'columns': [
            ['endpoint_type', 'VARCHAR(10)', 'NOT NULL'],
            ['sample_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['sample_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['program_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['sample_type', 'VARCHAR(2)', 'NOT NULL'],
            ['tumor_code', 'VARCHAR(2)', 'NOT NULL']
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
            ['sample_type'],
            ['tumor_code']
        ],
#         'foreign_key': [
#             'case_barcode',
#             'metadata_clinical',
#             'case_barcode'
#         ]
    }
    
    TARGET_metadata_samples = {
        'table_name': 'TARGET_metadata_samples',
        'primary_key_name': 'metadata_samples_id',  # todo: define this?

        'columns': [
            ['case_barcode', 'VARCHAR(45)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['program_name', 'VARCHAR(40)', 'NOT NULL'],
            ['sample_barcode', 'VARCHAR(45)', 'NOT NULL'],
            ['sample_type', 'VARCHAR(2)', 'NOT NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
            ['age_at_diagnosis', 'INT', 'NULL'],
            ['gender', 'VARCHAR(6)', 'NULL'],
            ['vital_status', 'VARCHAR(5)', 'NULL'],
            ['race', 'VARCHAR(41)', 'NULL'],
            ['ethnicity', 'VARCHAR(22)', 'NULL'],
            ['first_event', 'VARCHAR(25)', 'NULL'],
            ['days_to_birth', 'INT', 'NULL'],
            ['days_to_death', 'INT', 'NULL'],
            ['year_of_diagnosis', 'INT', 'NULL'],
            ['event_free_survival', 'INT', 'NULL'],
            ['overall_survival', 'INT', 'NULL'],
            ['wbc_at_diagnosis', 'FLOAT', 'NULL'],
            ['protocol', 'VARCHAR(70)', 'NULL']
        ],
        'indices_defs': [
            ['case_barcode'],
            ['sample_barcode'],
            ['project_short_name'],
            ['project_disease_type'],
            ['program_name'],
            ['endpoint_type'],
            ['gender'],
            ['vital_status'],
            ['race'],
            ['ethnicity'],
            ['first_event'],
            ['days_to_birth'],
            ['days_to_death'],
            ['age_at_diagnosis'],
            ['year_of_diagnosis'],
            ['overall_survival'],
            ['event_free_survival'],
            ['wbc_at_diagnosis'],
            ['protocol']
        ],
        'natural_key_cols': ['sample_barcode'],
#         'foreign_key': [
#             'sample_barcode',
#         ]
    }
    
    TARGET_metadata_attrs = {
        'table_name': 'TARGET_metadata_attrs',
        'primary_key_name': 'metadata_attrs_id',  # todo: define this?

        'columns': [
            ['attribute', 'VARCHAR(70)', 'NOT NULL'],
            ['code', 'VARCHAR(1)', 'NOT NULL'],
            ['spec', 'VARCHAR(4)', 'NOT NULL']
        ],
        'indices_defs': [
            ['attribute'],
            ['code'],
            ['spec']
        ]
    }

    TARGET_metadata_data_HG19 = {
        'table_name': 'TARGET_metadata_data_HG19',
        'primary_key_name': 'metadata_data_id',
        'columns': [
            ['file_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(35)', 'NOT NULL'],
            ['sample_gdc_id', 'VARCHAR(45)', 'NULL'],
            ['sample_barcode', 'VARCHAR(45)', 'NULL'],
            ['sample_type', 'VARCHAR(2)', 'NULL'],
            ['aliquot_barcode', 'VARCHAR(45)', 'NULL'],
            ['aliquot_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['program_name', 'VARCHAR(40)', 'NOT NULL'],
            ['data_type', 'VARCHAR(35)', 'NOT NULL'],
            ['data_category', 'VARCHAR(30)', 'NOT NULL'],
            ['experimental_strategy', 'VARCHAR(50)', 'NULL'],
            ['type', 'VARCHAR(40)', 'NULL'],
            ['file_name', 'VARCHAR(120)', 'NOT NULL'],
            ['file_size', 'BIGINT', 'NOT NULL'],
            ['file_state', 'VARCHAR(30)', 'NULL'],
            ['data_format', 'VARCHAR(10)', 'NOT NULL'],
            ['md5sum', 'VARCHAR(33)', 'NULL'],
            ['access', 'VARCHAR(10)', 'NOT NULL'],
            ['acl', 'VARCHAR(25)', 'NULL'],
            ['platform', 'VARCHAR(50)', 'NULL'],
            ['file_name_key', 'VARCHAR(250)', 'NULL'],
            ['file_uploaded', 'VARCHAR(5)', 'NOT NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
            ['analysis_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['analysis_workflow_link', 'VARCHAR(60)', 'NULL'],
            ['analysis_workflow_type', 'VARCHAR(60)', 'NULL'],
            ['center_code', 'VARCHAR(8)', 'NULL'],
            ['center_name', 'VARCHAR(50)', 'NULL'],
            ['center_type', 'VARCHAR(8)', 'NULL'],
            ['species', 'VARCHAR(30)', 'NULL'],
# this should probably be a foreign key back to the index file record
            ['index_file_id', 'VARCHAR(36)', 'NULL'],
            ['index_file_name', 'VARCHAR(80)', 'NULL'],
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
            ['sample_type'],
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

    TARGET_metadata_data_HG38 = {
        'table_name': 'TARGET_metadata_data_HG38',
        'primary_key_name': 'metadata_data_id',
        'columns': [
            ['file_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(35)', 'NOT NULL'],
            ['sample_gdc_id', 'VARCHAR(45)', 'NULL'],
            ['sample_barcode', 'VARCHAR(45)', 'NULL'],
            ['sample_type', 'VARCHAR(2)', 'NULL'],
            ['aliquot_barcode', 'VARCHAR(45)', 'NULL'],
            ['aliquot_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['program_name', 'VARCHAR(40)', 'NOT NULL'],
            ['data_type', 'VARCHAR(35)', 'NOT NULL'],
            ['data_category', 'VARCHAR(30)', 'NOT NULL'],
            ['experimental_strategy', 'VARCHAR(50)', 'NULL'],
            ['type', 'VARCHAR(40)', 'NULL'],
            ['file_name', 'VARCHAR(120)', 'NOT NULL'],
            ['file_size', 'BIGINT', 'NOT NULL'],
            ['file_state', 'VARCHAR(30)', 'NULL'],
            ['data_format', 'VARCHAR(10)', 'NOT NULL'],
            ['md5sum', 'VARCHAR(33)', 'NULL'],
            ['access', 'VARCHAR(10)', 'NOT NULL'],
            ['acl', 'VARCHAR(25)', 'NULL'],
            ['platform', 'VARCHAR(50)', 'NULL'],
            ['file_name_key', 'VARCHAR(250)', 'NULL'],
            ['file_uploaded', 'VARCHAR(5)', 'NOT NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
            ['analysis_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['analysis_workflow_link', 'VARCHAR(60)', 'NULL'],
            ['analysis_workflow_type', 'VARCHAR(60)', 'NULL'],
            ['center_code', 'VARCHAR(8)', 'NULL'],
            ['center_name', 'VARCHAR(50)', 'NULL'],
            ['center_type', 'VARCHAR(8)', 'NULL'],
            ['species', 'VARCHAR(30)', 'NULL'],
# this should probably be a foreign key back to the index file record
            ['index_file_id', 'VARCHAR(36)', 'NULL'],
            ['index_file_name', 'VARCHAR(80)', 'NULL'],
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
            ['sample_type'],
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

    metadata_tables = OrderedDict(
        [
            ('TARGET_metadata_project', TARGET_metadata_project),
            ('TARGET_metadata_clinical', TARGET_metadata_clinical),
            ('TARGET_metadata_biospecimen', TARGET_metadata_biospecimen),
            ('TARGET_metadata_samples', TARGET_metadata_samples),
            ('TARGET_metadata_data_HG19', TARGET_metadata_data_HG19),
            ('TARGET_metadata_data_HG38', TARGET_metadata_data_HG38),
            ('TARGET_metadata_attrs', TARGET_metadata_attrs)
        ]
    )

