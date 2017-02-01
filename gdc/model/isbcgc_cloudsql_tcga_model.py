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
    TCGA_metadata_project = {
        'table_name': 'TCGA_metadata_project',
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
    
    TCGA_metadata_clinical = {
        'table_name': 'TCGA_metadata_clinical',
        'primary_key_name': 'metadata_clinical_id',
        'columns': [
            ['endpoint_type', 'VARCHAR(10)', 'NOT NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['program_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(30)', 'NOT NULL'],
            ['summary_file_count', 'INT', 'NOT NULL'],
            ['age_at_initial_pathologic_diagnosis', 'INT', 'NULL'],
            ['age_began_smoking_in_years', 'INT', 'NULL'],
            ['anatomic_neoplasm_subdivision', 'VARCHAR(63)', 'NULL'],
            ['batch_number', 'INT', 'NULL'],
            ['bcr', 'VARCHAR(63)', 'NULL'],
            ['bmi', 'FLOAT(4,1)', 'NULL'],
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
            ['other_malignancy_type', 'VARCHAR(90)', 'NULL'],
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
            ['tss_code', 'VARCHAR(2)', 'NULL'],
            ['tumor_tissue_site', 'VARCHAR(100)', 'NULL'],
            ['tumor_type', 'VARCHAR(30)', 'NULL'],
            ['venous_invasion', 'VARCHAR(8)', 'NULL'],
            ['vital_status', 'VARCHAR(8)', 'NULL'],
            ['weight', 'INT', 'NULL'],
            ['year_of_diagnosis', 'INT', 'NULL'],
            ['year_of_tobacco_smoking_onset', 'INT', 'NULL']
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
            ['age_at_initial_pathologic_diagnosis'],
            ['age_began_smoking_in_years'],
            ['anatomic_neoplasm_subdivision'],
            ['batch_number'],
            ['bmi'],
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
            ['gleason_score_combined'],
            ['h_pylori_infection'],
            ['height'],
            ['histological_type'],
            ['history_of_colon_polyps'],
            ['history_of_neoadjuvant_treatment'],
            ['hpv_calls'],
            ['hpv_status'],
            ['icd_10'],
            ['lymphatic_invasion'],
            ['lymphovascular_invasion_present'],
            ['menopause_status'],
            ['mononucleotide_and_dinucleotide_marker_panel_analysis_status'],
            ['neoplasm_histologic_grade'],
            ['new_tumor_event_after_initial_treatment'],
            ['number_of_lymphnodes_examined'],
            ['number_of_lymphnodes_positive_by_he'],
            ['number_pack_years_smoked'],
            ['other_dx'],
            ['pathologic_M'],
            ['pathologic_N'],
            ['pathologic_stage'],
            ['pathologic_T'],
            ['person_neoplasm_cancer_status'],
            ['pregnancies'],
            ['primary_neoplasm_melanoma_dx'],
            ['primary_therapy_outcome_success'],
            ['psa_value'],
            ['race'],
            ['residual_tumor'],
            ['stopped_smoking_year'],
            ['tobacco_smoking_history'],
            ['tumor_tissue_site'],
            ['tumor_type'],
            ['vital_status'],
            ['weight'],
            ['year_of_diagnosis'],
            ['year_of_tobacco_smoking_onset']
        ]
    }
    
    TCGA_metadata_biospecimen = {
        'table_name': 'TCGA_metadata_biospecimen',
        'primary_key_name': 'metadata_biospecimen_id',
        'columns': [
            ['endpoint_type', 'VARCHAR(10)', 'NOT NULL'],
            ['sample_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['sample_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['program_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['pathology_report_uuid', 'VARCHAR(36)', 'NULL'],
            ['sample_type', 'VARCHAR(2)', 'NOT NULL'],
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
            ['preservation_method', 'VARCHAR(10)', 'NULL'],
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
            ['num_slides', 'INT', 'NULL']
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
            ['pathology_report_uuid'],
            ['sample_type'],
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
            ['preservation_method'],
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
            ['num_slides']
        ],
#         'foreign_key': [
#             'case_barcode',
#             'metadata_clinical',
#             'case_barcode'
#         ]
    }
    
    TCGA_metadata_samples = {
        'table_name': 'TCGA_metadata_samples',
        'primary_key_name': 'metadata_samples_id',  # todo: define this?

        'columns': [
            ['case_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['program_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_short_name', 'VARCHAR(30)', 'NOT NULL'],
            ['project_disease_type', 'VARCHAR(30)', 'NOT NULL'],
            ['age_at_initial_pathologic_diagnosis', 'INT', 'NULL'],
            ['gender', 'VARCHAR(15)', 'NULL'],
            ['vital_status', 'VARCHAR(8)', 'NULL'],
            ['tumor_tissue_site', 'VARCHAR(100)', 'NULL'],
            ['days_to_last_known_alive', 'INT', 'NULL'],
            ['person_neoplasm_cancer_status', 'VARCHAR(15)', 'NULL'],
            ['race', 'VARCHAR(50)', 'NULL'],
            ['ethnicity', 'VARCHAR(30)', 'NULL'],
            ['bmi', 'FLOAT(4,1)', 'NULL'],
            ['tobacco_smoking_history', 'VARCHAR(50)', 'NULL'],
            ['menopause_status', 'VARCHAR(120)', 'NULL'],
            ['hpv_status', 'VARCHAR(20)', 'NULL'],
            ['year_of_diagnosis', 'INT', 'NULL'],
            ['pathologic_stage', 'VARCHAR(15)', 'NULL'],
            ['residual_tumor', 'VARCHAR(5)', 'NULL'],
            ['neoplasm_histologic_grade', 'VARCHAR(15)', 'NULL'],
            ['histological_type', 'VARCHAR(120)', 'NULL'],
            ['sample_barcode', 'VARCHAR(40)', 'NOT NULL'],
            ['sample_type', 'VARCHAR(2)', 'NOT NULL'],
            ['preservation_method', 'VARCHAR(10)', 'NULL'],
            ['endpoint_type', 'VARCHAR(10)', 'NOT NULL'],
        ],
        'indices_defs': [
            ['case_barcode'],
            ['program_name'],
            ['project_short_name'],
            ['project_disease_type'],
            ['age_at_initial_pathologic_diagnosis'],
            ['gender'],
            ['vital_status'],
            ['tumor_tissue_site'],
            ['days_to_last_known_alive'],
            ['person_neoplasm_cancer_status'],
            ['race'],
            ['ethnicity'],
            ['bmi'],
            ['tobacco_smoking_history'],
            ['menopause_status'],
            ['hpv_status'],
            ['year_of_diagnosis'],
            ['pathologic_stage'],
            ['residual_tumor'],
            ['neoplasm_histologic_grade'],
            ['histological_type'],
            ['sample_barcode'],
            ['sample_type'],
            ['preservation_method'],
            ['endpoint_type']
        ],
        'natural_key_cols': ['sample_barcode'],
#         'foreign_key': [
#             'sample_barcode',
#         ]
    }
    
    TCGA_metadata_attrs = {
        'table_name': 'TCGA_metadata_attrs',
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

    TCGA_metadata_data = {
        'table_name': 'TCGA_metadata_data',
        'primary_key_name': 'metadata_data_id',
        'columns': [
            ['file_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['case_barcode', 'VARCHAR(35)', 'NOT NULL'],
            ['sample_gdc_id', 'VARCHAR(45)', 'NOT NULL'],
            ['sample_barcode', 'VARCHAR(45)', 'NOT NULL'],
            ['sample_type', 'VARCHAR(2)', 'NOT NULL'],
            ['aliquot_barcode', 'VARCHAR(45)', 'NOT NULL'],
            ['aliquot_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
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
            ['file_name_key', 'VARCHAR(250)', 'NULL'],
            ['file_uploaded', 'VARCHAR(5)', 'NOT NULL'],
            ['data_format', 'VARCHAR(10)', 'NOT NULL'],
            ['md5sum', 'VARCHAR(33)', 'NULL'],
            ['access', 'VARCHAR(10)', 'NOT NULL'],
            ['acl', 'VARCHAR(25)', 'NULL'],
            ['platform', 'VARCHAR(50)', 'NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NULL'],
            ['analysis_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['analysis_workflow_link', 'VARCHAR(60)', 'NULL'],
            ['analysis_workflow_type', 'VARCHAR(60)', 'NULL'],
            ['center_code', 'VARCHAR(8)', 'NULL'],
            ['center_name', 'VARCHAR(50)', 'NULL'],
            ['center_type', 'VARCHAR(8)', 'NULL'],
            ['species', 'VARCHAR(30)', 'NULL'],
            ['archive_file_name', 'VARCHAR(80)', 'NULL'],
            ['archive_gdc_id', 'VARCHAR(36)', 'NULL'],
            ['archive_submitter_id', 'VARCHAR(80)', 'NULL'],
            ['archive_revision', 'INT', 'NULL'],
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
            ('TCGA_metadata_project', TCGA_metadata_project),
            ('TCGA_metadata_clinical', TCGA_metadata_clinical),
            ('TCGA_metadata_biospecimen', TCGA_metadata_biospecimen),
            ('TCGA_metadata_samples', TCGA_metadata_samples),
            ('TCGA_metadata_data', TCGA_metadata_data),
            ('TCGA_metadata_attrs', TCGA_metadata_attrs)
        ]
    )

