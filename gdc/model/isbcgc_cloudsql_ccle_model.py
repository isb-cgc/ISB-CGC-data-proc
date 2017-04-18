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
            ['disease_code', 'VARCHAR(30)', 'NOT NULL'],
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
            ['disease_code'],
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
            ['disease_code', 'VARCHAR(30)', 'NOT NULL'],
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
            ['disease_code'],
            ['sample_type']
        ],
#         'foreign_key': [
#             'case_barcode',
#             'metadata_clinical',
#             'case_barcode'
#         ]
    }
    
    CCLE_metadata_samples = {
        'table_name': 'CCLE_metadata_samples',
        'primary_key_name': 'metadata_samples_id',
        'columns': [
            ['case_barcode', 'VARCHAR(45)', 'NOT NULL'],
            ['program_name', 'VARCHAR(40)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NOT NULL'],
            ['disease_code', 'VARCHAR(40)', 'NOT NULL'],
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
            ['disease_code'],
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
    
    CCLE_metadata_attrs = {
        'table_name': 'CCLE_metadata_attrs',
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

    CCLE_metadata_data_HG19 = {
        'table_name': 'CCLE_metadata_data_HG19',
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
            ['disease_code', 'VARCHAR(30)', 'NOT NULL'],
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
            ['index_file_name', 'VARCHAR(120)', 'NULL'],
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
            ['disease_code'],
            ['program_name'],
            ['data_type'],
            ['data_category'],
            ['experimental_strategy'],
            ['type'],
            ['file_name_key'],
            ['file_uploaded'],
            ['data_format'],
            ['platform'],
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

    CCLE_metadata_data_type_availability = {
        'table_name': 'CCLE_metadata_data_type_availability',
        'primary_key_name': 'metadata_data_type_availability_id',
        'columns': [
            ['genomic_build', 'VARCHAR(4)', 'NOT NULL'],
            ['isb_label', 'VARCHAR(40)', 'NOT NULL']
        ],
        'natural_key_cols': [
            'genomic_build',
            'isb_label'
        ],
        'indices_defs': [
            ['genomic_build'],
            ['isb_label']
        ]
    }
            
    CCLE_metadata_sample_data_availability = {
        'table_name': 'CCLE_metadata_sample_data_availability',

        'columns': [
            ['metadata_data_type_availability_id', 'INTEGER', 'NOT NULL'],
            ['sample_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['count', 'INTEGER', 'NOT NULL']
        ],

        'indices_defs': [
            ['metadata_data_type_availability_id', 'sample_barcode'],
            ['sample_barcode'],
            ['count']
        ],
        
        'foreign_keys': [
            [
                'metadata_data_type_availability_id',
                'CCLE_metadata_data_type_availability',
                'metadata_data_type_availability_id'
            ],
            [
                'sample_barcode',
                'CCLE_metadata_biospecimen',
                'sample_barcode'
            ],
            [
                'sample_barcode',
                'CCLE_metadata_samples',
                'sample_barcode'
            ],
            [
                'sample_barcode',
                'CCLE_metadata_data_HG19',
                'sample_barcode'
            ]
        ]
    }

    metadata_tables = OrderedDict(
        [
            ('CCLE_metadata_project', CCLE_metadata_project),
            ('CCLE_metadata_clinical', CCLE_metadata_clinical),
            ('CCLE_metadata_biospecimen', CCLE_metadata_biospecimen),
            ('CCLE_metadata_samples', CCLE_metadata_samples),
            ('CCLE_metadata_data_HG19', CCLE_metadata_data_HG19),
            ('CCLE_metadata_attrs', CCLE_metadata_attrs),
            ('CCLE_metadata_data_type_availability', CCLE_metadata_data_type_availability),
            ('CCLE_metadata_sample_data_availability', CCLE_metadata_sample_data_availability)
        ]
    )

