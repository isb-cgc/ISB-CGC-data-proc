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
            ['updated_datetime', 'DATETIME', 'NULL'],
            ['created_datetime', 'DATETIME', 'NOT NULL'],
            ['submitter_id', 'VARCHAR(10)', 'NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
        ],

        'indices_defs': [
            ['annotation_gdc_id'],
            ['category'],
            ['classification'],
            ['status'],
            ['state'],
            ['entity_type'],
            ['entity_gdc_id'],
            ['entity_barcode'],
            ['project_short_name'],
            ['program_name'],
            ['case_gdc_id'],
            ['case_barcode'],
            ['updated_datetime'],
            ['created_datetime'],
            ['submitter_id'],
            ['endpoint_type']
        ]
    }
    
    metadata_gdc_program = {
        'table_name': 'metadata_gdc_program',
        'primary_key_name': 'metadata_gdc_program_id',
        'columns': [
            ['program_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['program_name', 'VARCHAR(80)', 'NOT NULL'],
            ['dbgap_accession_number', 'VARCHAR(9)', 'NOT NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NOT NULL']
        ],
#         'natural_key_cols': [
#             'ParticipantBarcode'
#         ],
        'indices_defs': [
            ['program_gdc_id'],
            ['program_name']
        ]
    }
    
    metadata_gdc_project = {
        'table_name': 'metadata_gdc_project',
        'primary_key_name': 'metadata_gdc_project_id',
        'columns': [
            ['project_short_name', 'VARCHAR(40)', 'NOT NULL'],
            ['name', 'VARCHAR(80)', 'NOT NULL'],
            ['released', 'VARCHAR(5)', 'NOT NULL'],
            ['state', 'VARCHAR(10)', 'NOT NULL'],
            ['primary_site', 'VARCHAR(20)', 'NULL'],
            ['dbgap_accession_number', 'VARCHAR(12)', 'NULL'],
            ['disease_type', 'VARCHAR(120)', 'NULL'],
            ['summary_case_count', 'INTEGER', 'NULL'],
            ['summary_file_count', 'INTEGER', 'NULL'],
            ['summary_file_size', 'BIGINT', 'NULL'],
            ['program_name', 'VARCHAR(40)', 'NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
        ],
#         'natural_key_cols': [
#             'ParticipantBarcode'
#         ],
        'indices_defs': [
            ['primary_site'],
            ['disease_type'],
            ['project_short_name'],
            ['program_name'],
            ['endpoint_type'],
        ]
    }
    
    metadata_gdc_clinical = {
        'table_name': 'metadata_gdc_clinical',
        'primary_key_name': 'metadata_gdc_clinical_id',
        'columns': [
            ['endpoint_type', 'VARCHAR(8)', 'NOT NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(12)', 'NOT NULL'],
            ['program_name', 'VARCHAR(40)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NOT NULL'],
            ['summary_file_count', 'INT', 'NOT NULL'],
            ['created_datetime', 'DATETIME', 'NOT NULL'],
            ['updated_datetime', 'DATETIME', 'NOT NULL'],
            ['state', 'VARCHAR(10)', 'NOT NULL'],
            ['age_at_initial_pathologic_diagnosis', 'INT', 'NULL'],
            ['age_began_smoking_in_years', 'INT', 'NULL'],
            ['anatomic_neoplasm_subdivision', 'VARCHAR(63)', 'NULL'],
            ['batch_number', 'INT', 'NULL'],
            ['bcr', 'VARCHAR(63)', 'NULL'],
            ['BMI', 'FLOAT(3,1)', 'NULL'],
            ['clinical_M', 'VARCHAR(12)', 'NULL'],
            ['clinical_N', 'VARCHAR(12)', 'NULL'],
            ['clinical_stage', 'VARCHAR(12)', 'NULL'],
            ['clinical_T', 'VARCHAR(12)', 'NULL'],
            ['colorectal_cancer', 'VARCHAR(10)', 'NULL'],
            ['country', 'VARCHAR(63)', 'NULL'],
            ['days_to_birth', 'INT', 'NULL'],
            ['days_to_death', 'INT', 'NULL'],
            ['days_to_initial_pathologic_diagnosis', 'INT', 'NULL'],
            ['days_to_last_followup', 'INT', 'NULL'],
            ['days_to_last_known_alive', 'INT', 'NULL'],
            ['days_to_submitted_specimen_dx', 'INT', 'NULL'],
            ['ethnicity', 'VARCHAR(30)', 'NULL'],
            ['gender', 'VARCHAR(15)', 'NULL'],
            ['gleason_score_combined', 'INT', 'NULL'],
            ['h_pylori_infection', 'VARCHAR(10)', 'NULL'],
            ['height', 'INT', 'NULL'],
            ['histological_type', 'VARCHAR(120)', 'NULL'],
            ['history_of_colon_polyps', 'VARCHAR(8)', 'NULL'],
            ['history_of_neoadjuvant_treatment', 'VARCHAR(63)', 'NULL'],
            ['hpv_calls', 'VARCHAR(20)', 'NULL'],
            ['hpv_status', 'VARCHAR(20)', 'NULL'],
            ['icd_10', 'VARCHAR(8)', 'NULL'],
            ['icd_o_3_histology', 'VARCHAR(10)', 'NULL'],
            ['icd_o_3_site', 'VARCHAR(8)', 'NULL'],
            ['lymphatic_invasion', 'VARCHAR(8)', 'NULL'],
            ['lymphnodes_examined', 'VARCHAR(8)', 'NULL'],
            ['lymphovascular_invasion_present', 'VARCHAR(8)', 'NULL'],
            ['menopause_status', 'VARCHAR(120)', 'NULL'],
            ['mononucleotide_and_dinucleotide_marker_panel_analysis_status', 'VARCHAR(20)', 'NULL'],
            ['neoplasm_histologic_grade', 'VARCHAR(15)', 'NULL'],
            ['new_tumor_event_after_initial_treatment', 'VARCHAR(8)', 'NULL'],
            ['number_of_lymphnodes_examined', 'INT', 'NULL'],
            ['number_of_lymphnodes_positive_by_he', 'INT', 'NULL'],
            ['number_pack_years_smoked', 'INT', 'NULL'],
            ['other_dx', 'VARCHAR(70)', 'NULL'],
            ['other_malignancy_anatomic_site', 'VARCHAR(65)', 'NULL'],
            ['other_malignancy_histological_type', 'VARCHAR(150)', 'NULL'],
            ['other_malignancy_malignancy_type', 'VARCHAR(90)', 'NULL'],
            ['pathologic_M', 'VARCHAR(12)', 'NULL'],
            ['pathologic_N', 'VARCHAR(12)', 'NULL'],
            ['pathologic_stage', 'VARCHAR(15)', 'NULL'],
            ['pathologic_T', 'VARCHAR(12)', 'NULL'],
            ['person_neoplasm_cancer_status', 'VARCHAR(15)', 'NULL'],
            ['pregnancies', 'VARCHAR(10)', 'NULL'],
            ['primary_neoplasm_melanoma_dx', 'VARCHAR(10)', 'NULL'],
            ['primary_therapy_outcome_success', 'VARCHAR(70)', 'NULL'],
            ['psa_value', 'FLOAT', 'NULL'],
            ['race', 'VARCHAR(50)', 'NULL'],
            ['residual_tumor', 'VARCHAR(5)', 'NULL'],
            ['stopped_smoking_year', 'INT', 'NULL'],
            ['tobacco_smoking_history', 'VARCHAR(50)', 'NULL'],
            ['TSSCode', 'VARCHAR(2)', 'NULL'],
            ['tumor_tissue_site', 'VARCHAR(100)', 'NULL'],
            ['tumor_type', 'VARCHAR(30)', 'NULL'],
            ['venous_invasion', 'VARCHAR(8)', 'NULL'],
            ['vital_status', 'VARCHAR(8)', 'NULL'],
            ['weight', 'INT', 'NULL'],
            ['year_of_initial_pathologic_diagnosis', 'INT', 'NULL'],
            ['year_of_tobacco_smoking_onset', 'INT', 'NULL']
        ],
#         'natural_key_cols': [
#             'ParticipantBarcode'
#         ],
        'indices_defs': [
            ['endpoint_type'],
            ['case_gdc_id'],
            ['case_barcode'],
            ['program_name'],
            ['project_short_name'],
            ['created_datetime'],
            ['updated_datetime'],
            ['state'],
            ['age_at_initial_pathologic_diagnosis'],
            ['age_began_smoking_in_years'],
            ['anatomic_neoplasm_subdivision'],
            ['BMI'],
            ['clinical_M'],
            ['clinical_N'],
            ['clinical_stage'],
            ['clinical_T'],
            ['colorectal_cancer'],
            ['country'],
            ['days_to_birth'],
            ['days_to_death'],
            ['days_to_initial_pathologic_diagnosis'],
            ['days_to_last_followup'],
            ['days_to_last_known_alive'],
            ['days_to_submitted_specimen_dx'],
            ['ethnicity'],
            ['gender'],
            ['height'],
            ['histological_type'],
            ['history_of_colon_polyps'],
            ['history_of_neoadjuvant_treatment'],
            ['icd_10'],
            ['icd_o_3_histology'],
            ['icd_o_3_site'],
            ['lymphatic_invasion'],
            ['lymphnodes_examined'],
            ['lymphovascular_invasion_present'],
            ['mononucleotide_and_dinucleotide_marker_panel_analysis_status'],
            ['new_tumor_event_after_initial_treatment'],
            ['number_of_lymphnodes_examined'],
            ['number_pack_years_smoked'],
            ['other_dx'],
            ['other_malignancy_anatomic_site'],
            ['other_malignancy_histological_type'],
            ['other_malignancy_malignancy_type'],
            ['pathologic_M'],
            ['pathologic_N'],
            ['pathologic_stage'],
            ['pathologic_T'],
            ['primary_neoplasm_melanoma_dx'],
            ['primary_therapy_outcome_success'],
            ['race'],
            ['stopped_smoking_year'],
            ['tobacco_smoking_history'],
            ['tumor_tissue_site'],
            ['tumor_type'],
            ['venous_invasion'],
            ['vital_status'],
            ['weight'],
            ['year_of_initial_pathologic_diagnosis'],
            ['year_of_tobacco_smoking_onset']
        ]
    }
    
    metadata_gdc_biospecimen = {
        'table_name': 'metadata_gdc_biospecimen',
        'primary_key_name': 'metadata_gdc_biospecimen_id',
        'columns': [
            ['endpoint_type', 'VARCHAR(8)', 'NOT NULL'],
            ['sample_gdc_id', 'VARCHAR(32)', 'NOT NULL'],
            ['sample_barcode', 'VARCHAR(16)', 'NOT NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(12)', 'NOT NULL'],
            ['created_datetime', 'DATETIME', 'NOT NULL'],
            ['updated_datetime', 'DATETIME', 'NOT NULL'],
            ['program_name', 'VARCHAR(40)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NOT NULL'],
            ['pathology_report_uuid', 'VARCHAR(36)', 'NOT NULL'],
            ['sample_type', 'VARCHAR(70)', 'NOT NULL'],
            ['sample_type_code', 'VARCHAR(2)', 'NOT NULL'],
            ['state', 'VARCHAR(10)', 'NOT NULL'],
            ['tumor_code', 'VARCHAR(60)', 'NOT NULL'],
            ['tumor_code_code', 'VARCHAR(10)', 'NOT NULL'],
            ['avg_percent_lymphocyte_infiltration', 'FLOAT', 'NULL'],
            ['avg_percent_monocyte_infiltration', 'FLOAT', 'NULL'],
            ['avg_percent_necrosis', 'FLOAT', 'NULL'],
            ['avg_percent_neutrophil_infiltration', 'FLOAT', 'NULL'],
            ['avg_percent_normal_cells', 'FLOAT', 'NULL'],
            ['avg_percent_stromal_cells', 'FLOAT', 'NULL'],
            ['avg_percent_tumor_cells', 'FLOAT', 'NULL'],
            ['avg_percent_tumor_nuclei', 'FLOAT', 'NULL'],
            ['batch_number', 'INT', 'NULL'],
            ['bcr', 'VARCHAR(63)', 'NULL'],
            ['days_to_collection', 'INT', 'NULL'],
            ['days_to_sample_procurement', 'INT', 'NULL'],
            ['is_ffpe', 'VARCHAR(5)', 'NULL'],
            ['max_percent_lymphocyte_infiltration', 'FLOAT', 'NULL'],
            ['max_percent_monocyte_infiltration', 'FLOAT', 'NULL'],
            ['max_percent_necrosis', 'FLOAT', 'NULL'],
            ['max_percent_neutrophil_infiltration', 'FLOAT', 'NULL'],
            ['max_percent_normal_cells', 'FLOAT', 'NULL'],
            ['max_percent_stromal_cells', 'FLOAT', 'NULL'],
            ['max_percent_tumor_cells', 'FLOAT', 'NULL'],
            ['max_percent_tumor_nuclei', 'FLOAT', 'NULL'],
            ['min_percent_lymphocyte_infiltration', 'FLOAT', 'NULL'],
            ['min_percent_monocyte_infiltration', 'FLOAT', 'NULL'],
            ['min_percent_necrosis', 'FLOAT', 'NULL'],
            ['min_percent_neutrophil_infiltration', 'FLOAT', 'NULL'],
            ['min_percent_normal_cells', 'FLOAT', 'NULL'],
            ['min_percent_stromal_cells', 'FLOAT', 'NULL'],
            ['min_percent_tumor_cells', 'FLOAT', 'NULL'],
            ['min_percent_tumor_nuclei', 'FLOAT', 'NULL'],
            ['num_portions', 'INT', 'NULL'],
            ['num_slides', 'INT', 'NULL'],
            ['tissue_anatomic_site', 'VARCHAR(55)', 'NULL'],
            ['tissue_anatomic_site_description', 'VARCHAR(45)', 'NULL']
        ],
#         'natural_key_cols': [
#             'SampleBarcode'
#         ],
        'indices_defs': [
            ['endpoint_type'],
            ['sample_gdc_id'],
            ['sample_barcode'],
            ['case_gdc_id'],
            ['case_barcode'],
            ['created_datetime'],
            ['updated_datetime'],
            ['program_name'],
            ['project_short_name'],
            ['pathology_report_uuid'],
            ['sample_type'],
            ['sample_type_code'],
            ['state'],
            ['tumor_code'],
            ['tumor_code_code'],
            ['avg_percent_lymphocyte_infiltration'],
            ['avg_percent_monocyte_infiltration'],
            ['avg_percent_necrosis'],
            ['avg_percent_neutrophil_infiltration'],
            ['avg_percent_normal_cells'],
            ['avg_percent_stromal_cells'],
            ['avg_percent_tumor_cells'],
            ['avg_percent_tumor_nuclei'],
            ['days_to_collection'],
            ['days_to_sample_procurement'],
            ['is_ffpe'],
            ['max_percent_lymphocyte_infiltration'],
            ['max_percent_monocyte_infiltration'],
            ['max_percent_necrosis'],
            ['max_percent_neutrophil_infiltration'],
            ['max_percent_normal_cells'],
            ['max_percent_stromal_cells'],
            ['max_percent_tumor_cells'],
            ['max_percent_tumor_nuclei'],
            ['min_percent_lymphocyte_infiltration'],
            ['min_percent_monocyte_infiltration'],
            ['min_percent_necrosis'],
            ['min_percent_neutrophil_infiltration'],
            ['min_percent_normal_cells'],
            ['min_percent_stromal_cells'],
            ['min_percent_tumor_cells'],
            ['min_percent_tumor_nuclei'],
            ['num_portions'],
            ['num_slides'],
            ['tissue_anatomic_site']
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
            ['file_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['case_barcode', 'VARCHAR(35)', 'NULL'],
            ['sample_gdc_id', 'VARCHAR(45)', 'NULL'],
            ['sample_barcode', 'VARCHAR(45)', 'NULL'],
            ['sample_type_code', 'VARCHAR(2)', 'NULL'],
            ['aliquot_barcode', 'VARCHAR(45)', 'NULL'],
            ['aliquot_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['project_short_name', 'VARCHAR(40)', 'NULL'],
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
            ['datafile_namekey', 'VARCHAR(33)', 'NULL'],
            ['datafile_uploaded', 'VARCHAR(5)', 'NOT NULL'],
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
#             'AliquotBarcode',
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
            ['program_name'],
            ['data_type'],
            ['data_category'],
            ['experimental_strategy'],
            ['type'],
            ['data_format'],
            ['state'],
            ['platform'],
            ['datafile_uploaded'],
            ['endpoint_type'],
            ['analysis_workflow_link'],
            ['analysis_workflow_type']
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
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
        ],
        'indices_defs': [
            ['ParticipantBarcode'],
            ['SampleBarcode'],
            ['Project'],
            ['Program'],
            ['endpoint_type'],
        ],
        'natural_key_cols': ['SampleBarcode'],
#         'foreign_key': [
#             'SampleBarcode',
#         ]
    }
    
    isbcgc_cloudsql_model.ISBCGC_database_helper.metadata_tables = OrderedDict(
        [
            ('metadata_gdc_annotation', metadata_gdc_annotation),
            ('metadata_gdc_program', metadata_gdc_program),
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

