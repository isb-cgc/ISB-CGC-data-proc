'''
a mock wrapper to google cloud sql.

Copyright 2015, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
["limitations", "under", "the", "None", "License.", "None"],
'''
class ISBCGC_database_helper():
    """
    this class mocks the cloud sql metadata upload
    """
    self = None
    def __init__(self, config, log):
        pass
        
    @classmethod
    def initialize(cls, config, log):
        pass

    @classmethod
    def select(cls, config, stmt, log, params = []):
        if 'metadata_datadictionary' in stmt:
            return [
                ["age_at_initial_pathologic_diagnosis", "metadata_clinical", "int"],
                ["anatomic_neoplasm_subdivision", "metadata_clinical", "controlled vocabulary text"],
                ["batch_number", "metadata_clinical", "controlled vocabulary text"],
                ["bcr", "metadata_clinical", "controlled vocabulary text"],
                ["clinical_M", "metadata_clinical", "controlled vocabulary text"],
                ["clinical_N", "metadata_clinical", "controlled vocabulary text"],
                ["clinical_stage", "metadata_clinical", "controlled vocabulary text"],
                ["clinical_T", "metadata_clinical", "controlled vocabulary text"],
                ["colorectal_cancer", "metadata_clinical", "controlled vocabulary text"],
                ["country", "metadata_clinical", "controlled vocabulary text"],
                ["days_to_birth", "metadata_clinical", "number"],
                ["days_to_death", "metadata_clinical", "number"],
                ["days_to_initial_pathologic_diagnosis", "metadata_clinical", "number"],
                ["days_to_last_followup", "metadata_clinical", "number"],
                ["days_to_submitted_specimen_dx", "metadata_clinical", "number"],
                ["Study", "metadata_clinical", "controlled vocabulary text"],
                ["ethnicity", "metadata_clinical", "controlled vocabulary text"],
                ["frozen_specimen_anatomic_site", "metadata_clinical", "controlled vocabulary text"],
                ["gender", "metadata_clinical", "controlled vocabulary text"],
                ["gleason_score_combined", "metadata_clinical", "number"],
                ["height", "metadata_clinical", "number"],
                ["histological_type", "metadata_clinical", "controlled vocabulary text"],
                ["history_of_colon_polyps", "metadata_clinical", "controlled vocabulary text"],
                ["history_of_neoadjuvant_treatment", "metadata_clinical", "controlled vocabulary text"],
                ["history_of_prior_malignancy", "metadata_clinical", "controlled vocabulary text"],
                ["hpv_calls", "metadata_clinical", "controlled vocabulary text"],
                ["hpv_status", "metadata_clinical", "controlled vocabulary text"],
                ["icd_10", "metadata_clinical", "controlled vocabulary text"],
                ["icd_o_3_histology", "metadata_clinical", "controlled vocabulary text"],
                ["icd_o_3_site", "metadata_clinical", "controlled vocabulary text"],
                ["lymphatic_invasion", "metadata_clinical", "controlled vocabulary text"],
                ["lymphnodes_examined", "metadata_clinical", "controlled vocabulary text"],
                ["lymphovascular_invasion_present", "metadata_clinical", "controlled vocabulary text"],
                ["menopause_status", "metadata_clinical", "controlled vocabulary text"],
                ["mononucleotide_and_dinucleotide_marker_panel_analysis_status", "metadata_clinical", "controlled vocabulary text"],
                ["mononucleotide_marker_panel_analysis_status", "metadata_clinical", "controlled vocabulary text"],
                ["neoplasm_histologic_grade", "metadata_clinical", "controlled vocabulary text"],
                ["new_tumor_event_after_initial_treatment", "metadata_clinical", "controlled vocabulary text"],
                ["number_of_lymphnodes_examined", "metadata_clinical", "number"],
                ["number_of_lymphnodes_positive_by_he", "metadata_clinical", "int"],
                ["number_pack_years_smoked", "metadata_clinical", "int"],
                ["ParticipantBarcode", "metadata_clinical", "text"],
                ["ParticipantUUID", "metadata_clinical", "UUID"],
                ["pathologic_M", "metadata_clinical", "controlled vocabulary text"],
                ["pathologic_N", "metadata_clinical", "controlled vocabulary text"],
                ["pathologic_stage", "metadata_clinical", "controlled vocabulary text"],
                ["pathologic_T", "metadata_clinical", "controlled vocabulary text"],
                ["person_neoplasm_cancer_status", "metadata_clinical", "controlled vocabulary text"],
                ["pregnancies", "metadata_clinical", "controlled vocabulary text"],
                ["primary_neoplasm_melanoma_dx", "metadata_clinical", "controlled vocabulary text"],
                ["primary_therapy_outcome_success", "metadata_clinical", "controlled vocabulary text"],
                ["prior_dx", "metadata_clinical", "controlled vocabulary text"],
                ["psa_value", "metadata_clinical", "number"],
                ["race", "metadata_clinical", "controlled vocabulary text"],
                ["residual_tumor", "metadata_clinical", "controlled vocabulary text"],
                ["TSSCode", "metadata_clinical", "controlled vocabulary text"],
                ["tobacco_smoking_history", "metadata_clinical", "controlled vocabulary text"],
                ["tumor_tissue_site", "metadata_clinical", "controlled vocabulary text"],
                ["TumorType", "metadata_clinical", "controlled vocabulary text"],
                ["vital_status", "metadata_clinical", "controlled vocabulary text"],
                ["weight", "metadata_clinical", "int"],
                ["weiss_venous_invasion", "metadata_clinical", "controlled vocabulary text"],
                ["year_of_initial_pathologic_diagnosis", "metadata_clinical", "int"],
                ["avg_percent_lymphocyte_infiltration", "metadata_biospecimen", "number"],
                ["avg_percent_monocyte_infiltration", "metadata_biospecimen", "number"],
                ["avg_percent_necrosis", "metadata_biospecimen", "number"],
                ["avg_percent_neutrophil_infiltration", "metadata_biospecimen", "number"],
                ["avg_percent_normal_cells", "metadata_biospecimen", "number"],
                ["avg_percent_stromal_cells", "metadata_biospecimen", "number"],
                ["avg_percent_tumor_cells", "metadata_biospecimen", "number"],
                ["avg_percent_tumor_nuclei", "metadata_biospecimen", "number"],
                ["batch_number", "metadata_biospecimen", "controlled vocabulary text"],
                ["bcr", "metadata_biospecimen", "controlled vocabulary text"],
                ["days_to_collection", "metadata_biospecimen", "number"],
                ["max_percent_lymphocyte_infiltration", "metadata_biospecimen", "number"],
                ["max_percent_monocyte_infiltration", "metadata_biospecimen", "number"],
                ["max_percent_necrosis", "metadata_biospecimen", "number"],
                ["max_percent_neutrophil_infiltration", "metadata_biospecimen", "number"],
                ["max_percent_normal_cells", "metadata_biospecimen", "number"],
                ["max_percent_stromal_cells", "metadata_biospecimen", "number"],
                ["max_percent_tumor_cells", "metadata_biospecimen", "number"],
                ["max_percent_tumor_nuclei", "metadata_biospecimen", "number"],
                ["min_percent_lymphocyte_infiltration", "metadata_biospecimen", "number"],
                ["min_percent_monocyte_infiltration", "metadata_biospecimen", "number"],
                ["min_percent_necrosis", "metadata_biospecimen", "number"],
                ["min_percent_neutrophil_infiltration", "metadata_biospecimen", "number"],
                ["min_percent_normal_cells", "metadata_biospecimen", "number"],
                ["min_percent_stromal_cells", "metadata_biospecimen", "number"],
                ["min_percent_tumor_cells", "metadata_biospecimen", "number"],
                ["min_percent_tumor_nuclei", "metadata_biospecimen", "number"],
                ["ParticipantBarcode", "metadata_biospecimen", "text"],
                ["Project", "metadata_biospecimen", "text"],
                ["SampleBarcode", "metadata_biospecimen", "text"],
                ["SampleUUID", "metadata_biospecimen", "UUID"],
                ["Study", "metadata_biospecimen", "controlled vocabulary text"],
                ["AliquotBarcode", "metadata_data", "text"],
                ["AliquotUUID", "metadata_data", "UUID"],
                ["analysis_id", "metadata_data", "UUID"],
                ["analyte_code", "metadata_data", "single letter code"],
                ["AnnotationCategory", "metadata_data", "controlled vocabulary text"],
                ["AnnotationClassification", "metadata_data", "controlled vocabulary text"],
                ["DataArchiveName", "metadata_data", "filename"],
                ["DataArchiveURL", "metadata_data", "hyperlink"],
                ["DataArchiveVersion", "metadata_data", "text"],
                ["DataCenterCode", "metadata_data", "controlled vocabulary text"],
                ["DataCenterName", "metadata_data", "text"],
                ["DataCenterType", "metadata_data", "controlled vocabulary text"],
                ["DataCGHubID", "metadata_data", "UUID"],
                ["DatafileMD5", "metadata_data", "32 digit hex number"],
                ["DatafileName", "metadata_data", "filename"],
                ["DatafileNameKey", "metadata_data", "GCS path"],
                ["DatafileUploaded", "metadata_data", "controlled vocabulary text"],
                ["DataLevel", "metadata_data", "controlled vocabulary text"],
                ["Datatype", "metadata_data", "controlled vocabulary text"],
                ["Disease Code", "metadata_data", "controlled vocabulary text"],
                ["GenomeReference", "metadata_data", "controlled vocabulary text"],
                ["IncludeForAnalysis", "metadata_data", "controlled vocabulary text"],
                ["last_modified", "metadata_data", "DATE"],
                ["library_strategy", "metadata_data", "controlled vocabulary text"],
                ["MAGETabArchiveName", "metadata_data", "filename"],
                ["MAGETabArchiveURL", "metadata_data", "hyperlink"],
                ["ParticipantBarcode", "metadata_data", "text"],
                ["Pipeline", "metadata_data", "controlled vocabulary text"],
                ["Platform", "metadata_data", "controlled vocabulary text"],
                ["platform_full_name", "metadata_data", "controlled vocabulary text"],
                ["Project", "metadata_data", "controlled vocabulary text"],
                ["reason_for_state", "metadata_data", "text"],
                ["Repository", "metadata_data", "controlled vocabulary text"],
                ["SampleBarcode", "metadata_data", "text"],
                ["SampleTypeCode", "metadata_data", "controlled vocabulary text"],
                ["SDRFFileName", "metadata_data", "filename"],
                ["SDRFFileNameKey", "metadata_data", "GCS path"],
                ["SecurityProtocol", "metadata_data", "controlled vocabulary text"],
                ["Species", "metadata_data", "controlled vocabulary text"],
                ["state", "metadata_data", "controlled vocabulary text"],
                ["Study", "metadata_data", "controlled vocabulary text"],
                ["wasDerivedFrom", "metadata_data", "text list"]
            ]
        elif 'desc metadata_clinical' in stmt:
            return [
                ["metadata_clinical_id", "int(11)", "NO", "PRI", "NULL", "auto_increment"],
                ["adenocarcinoma_invasion", "varchar(10)", "YES", "None", "NULL", "None"],
                ["age_at_initial_pathologic_diagnosis", "int(11)", "YES", "None", "NULL", "None"],
                ["anatomic_neoplasm_subdivision", "varchar(63)", "YES", "None", "NULL", "None"],
                ["batch_number", "int(11)", "YES", "None", "NULL", "None"],
                ["bcr", "varchar(63)", "YES", "None", "NULL", "None"],
                ["clinical_M", "varchar(12)", "YES", "None", "NULL", "None"],
                ["clinical_N", "varchar(12)", "YES", "None", "NULL", "None"],
                ["clinical_stage", "varchar(12)", "YES", "None", "NULL", "None"],
                ["clinical_T", "varchar(12)", "YES", "None", "NULL", "None"],
                ["colorectal_cancer", "varchar(10)", "YES", "None", "NULL", "None"],
                ["country", "varchar(63)", "YES", "None", "NULL", "None"],
                ["country_of_procurement", "varchar(63)", "YES", "None", "NULL", "None"],
                ["days_to_birth", "int(11)", "YES", "MUL", "NULL", "None"],
                ["days_to_death", "int(11)", "YES", "MUL", "NULL", "None"],
                ["days_to_initial_pathologic_diagnosis", "int(11)", "YES", "None", "NULL", "None"],
                ["days_to_last_followup", "int(11)", "YES", "None", "NULL", "None"],
                ["days_to_submitted_specimen_dx", "int(11)", "YES", "None", "NULL", "None"],
                ["Disease_Code", "varchar(6)", "YES", "MUL", "NULL", "None"],
                ["ethnicity", "varchar(20)", "YES", "MUL", "NULL", "None"],
                ["frozen_specimen_anatomic_site", "varchar(63)", "YES", "None", "NULL", "None"],
                ["gender", "varchar(15)", "YES", "MUL", "NULL", "None"],
                ["gleason_score_combined", "int(11)", "YES", "None", "NULL", "None"],
                ["height", "int(11)", "YES", "None", "NULL", "None"],
                ["histological_type", "varchar(63)", "YES", "MUL", "NULL", "None"],
                ["history_of_colon_polyps", "varchar(8)", "YES", "None", "NULL", "None"],
                ["history_of_neoadjuvant_treatment", "varchar(63)", "YES", "None", "NULL", "None"],
                ["history_of_prior_malignancy", "varchar(25)", "YES", "MUL", "NULL", "None"],
                ["hpv_calls", "varchar(20)", "YES", "None", "NULL", "None"],
                ["hpv_status", "varchar(20)", "YES", "None", "NULL", "None"],
                ["icd_10", "varchar(8)", "YES", "MUL", "NULL", "None"],
                ["icd_o_3_histology", "varchar(10)", "YES", "MUL", "NULL", "None"],
                ["icd_o_3_site", "varchar(8)", "YES", "MUL", "NULL", "None"],
                ["lymphatic_invasion", "varchar(8)", "YES", "MUL", "NULL", "None"],
                ["lymphnodes_examined", "varchar(8)", "YES", "None", "NULL", "None"],
                ["lymphovascular_invasion_present", "varchar(63)", "YES", "None", "NULL", "None"],
                ["menopause_status", "varchar(30)", "YES", "None", "NULL", "None"],
                ["mononucleotide_and_dinucleotide_marker_panel_analysis_status", "varchar(20)", "YES", "MUL", "NULL", "None"],
                ["mononucleotide_marker_panel_analysis_status", "varchar(20)", "YES", "MUL", "NULL", "None"],
                ["neoplasm_histologic_grade", "varchar(15)", "YES", "MUL", "NULL", "None"],
                ["new_tumor_event_after_initial_treatment", "varchar(8)", "YES", "MUL", "NULL", "None"],
                ["number_of_lymphnodes_examined", "int(11)", "YES", "None", "NULL", "None"],
                ["number_of_lymphnodes_positive_by_he", "int(11)", "YES", "MUL", "NULL", "None"],
                ["number_pack_years_smoked", "int(11)", "YES", "None", "NULL", "None"],
                ["ParticipantBarcode", "varchar(12)", "NO", "None", "NULL", "None"],
                ["ParticipantUUID", "varchar(36)", "NO", "None", "NULL", "None"],
                ["pathologic_M", "varchar(12)", "YES", "MUL", "NULL", "None"],
                ["pathologic_N", "varchar(12)", "YES", "MUL", "NULL", "None"],
                ["pathologic_stage", "varchar(10)", "YES", "MUL", "NULL", "None"],
                ["pathologic_T", "varchar(12)", "YES", "MUL", "NULL", "None"],
                ["person_neoplasm_cancer_status", "varchar(15)", "YES", "MUL", "NULL", "None"],
                ["pregnancies", "varchar(35)", "YES", "MUL", "NULL", "None"],
                ["primary_neoplasm_melanoma_dx", "varchar(10)", "YES", "MUL", "NULL", "None"],
                ["primary_therapy_outcome_success", "varchar(35)", "YES", "None", "NULL", "None"],
                ["prior_dx", "varchar(50)", "YES", "MUL", "NULL", "None"],
                ["psa_value", "float", "YES", "None", "NULL", "None"],
                ["race", "varchar(30)", "YES", "MUL", "NULL", "None"],
                ["residual_tumor", "varchar(5)", "YES", "None", "NULL", "None"],
                ["tobacco_smoking_history", "varchar(30)", "YES", "MUL", "NULL", "None"],
                ["TSSCode", "varchar(2)", "YES", "MUL", "NULL", "None"],
                ["tumor_tissue_site", "varchar(20)", "YES", "MUL", "NULL", "None"],
                ["tumor_type", "varchar(4)", "YES", "None", "NULL", "None"],
                ["venous_invasion", "varchar(63)", "YES", "None", "NULL", "None"],
                ["vital_status", "varchar(63)", "YES", "MUL", "NULL", "None"],
                ["weight", "varchar(63)", "YES", "None", "NULL", "None"],
                ["year_of_initial_pathologic_diagnosis", "varchar(63)", "YES", "MUL", "NULL", "None"]
            ]
        elif "desc metadata_biospecimen" in stmt:
            return [
                ["metadata_biospecimen_id", "int(11)", "NO", "None", "PRI", "auto_increment"],
                ["ParticipantBarcode", "varchar(12)", "NO", "None", "NULL", "None"],
                ["SampleBarcode", "varchar(16)", "NO", "None", "NULL", "None"],
                ["SampleUUID", "varchar(36)", "YES", "None", "NULL", "None"],
                ["batch_number", "int(11)", "YES", "None", "NULL", "None"],
                ["bcr", "varchar(63)", "YES", "None", "MUL", "None"],
                ["days_to_collection", "int(11)", "YES", "None", "NULL", "None"],
                ["days_to_sample_procurement", "int(11)", "YES", "None", "NULL", "None"],
                ["Disease_Code", "varchar(20)", "YES", "None", "MUL", "None"],
                ["Study", "varchar(20)", "YES", "None", "MUL", "None"],
                ["is_ffpe", "varchar(4)", "YES", "None", "NULL", "None"],
                ["preservation_method", "varchar(20)", "YES", "None", "NULL", "None"],
                ["Project", "varchar(20)", "NO", "None", "NULL", "None"],
                ["tissue_type", "varchar(15)", "YES", "None", "MUL", "None"],
                ["tumor_pathology", "varchar(50)", "YES", "None", "MUL", "None"],
                ["avg_percent_lymphocyte_infiltration", "float", "YES", "None", "NULL", "None"],
                ["avg_percent_monocyte_infiltration", "float", "YES", "None", "NULL", "None"],
                ["avg_percent_necrosis", "float", "YES", "None", "NULL", "None"],
                ["avg_percent_neutrophil_infiltration", "float", "YES", "None", "NULL", "None"],
                ["avg_percent_normal_cells", "float", "YES", "None", "NULL", "None"],
                ["avg_percent_stromal_cells", "float", "YES", "None", "NULL", "None"],
                ["avg_percent_tumor_cells", "float", "YES", "None", "NULL", "None"],
                ["avg_percent_tumor_nuclei", "float", "YES", "None", "NULL", "None"],
                ["max_percent_lymphocyte_infiltration", "float", "YES", "None", "NULL", "None"],
                ["max_percent_monocyte_infiltration", "float", "YES", "None", "NULL", "None"],
                ["max_percent_necrosis", "float", "YES", "None", "NULL", "None"],
                ["max_percent_neutrophil_infiltration", "float", "YES", "None", "NULL", "None"],
                ["max_percent_normal_cells", "float", "YES", "None", "NULL", "None"],
                ["max_percent_stromal_cells", "float", "YES", "None", "NULL", "None"],
                ["max_percent_tumor_cells", "float", "YES", "None", "NULL", "None"],
                ["max_percent_tumor_nuclei", "float", "YES", "None", "NULL", "None"],
                ["min_percent_lymphocyte_infiltration", "float", "YES", "None", "NULL", "None"],
                ["min_percent_monocyte_infiltration", "float", "YES", "None", "NULL", "None"],
                ["min_percent_necrosis", "float", "YES", "None", "NULL", "None"],
                ["min_percent_neutrophil_infiltration", "float", "YES", "None", "NULL", "None"],
                ["min_percent_normal_cells", "float", "YES", "None", "NULL", "None"],
                ["min_percent_stromal_cells", "float", "YES", "None", "NULL", "None"],
                ["min_percent_tumor_cells", "float", "YES", "None", "NULL", "None"],
                ["min_percent_tumor_nuclei", "float", "YES", "None", "NULL", "None"],
            ]
        elif "desc metadata_data" in stmt:
            return [
                ["metadata_data_id", "int(11)", "NO", "None", "PRI", "auto_increment"],
                ["ParticipantBarcode", "varchar(12)", "NO", "None", "MUL", "None"],
                ["SampleBarcode", "varchar(16)", "NO", "None", "MUL", "None"],
                ["AliquotBarcode", "varchar(28)", "NO", "None", "NULL", "None"],
                ["AliquotUUID", "varchar(36)", "YES", "None", "NULL", "None"],
                ["AnnotationCategory", "varchar(100)", "YES", "None", "NULL", "None"],
                ["AnnotationClassification", "varchar(100)", "YES", "None", "NULL", "None"],
                ["DataArchiveName", "varchar(100)", "YES", "None", "NULL", "None"],
                ["DataArchiveURL", "varchar(300)", "YES", "None", "NULL", "None"],
                ["DataArchiveVersion", "varchar(20)", "YES", "None", "NULL", "None"],
                ["DataCenterCode", "varchar(2)", "YES", "None", "MUL", "None"],
                ["DataCenterName", "varchar(20)", "YES", "None", "MUL", "None"],
                ["DataCenterType", "varchar(4)", "YES", "None", "MUL", "None"],
                ["DataCGHubID", "varchar(36)", "YES", "None", "NULL", "None"],
                ["DatafileMD5", "varchar(32)", "YES", "None", "NULL", "None"],
                ["DatafileName", "varchar(100)", "NO", "None", "MUL", "None"],
                ["DatafileNameKey", "varchar(200)", "NO", "None", "NULL", "None"],
                ["DatafileUploaded", "varchar(5)", "NO", "None", "MUL", "None"],
                ["DataLevel", "varchar(7)", "NO", "None", "NULL", "None"],
                ["Datatype", "varchar(30)", "YES", "None", "MUL", "None"],
                ["Disease_Code", "varchar(6)", "YES", "None", "NULL", "None"],
                ["GenomeReference", "varchar(32)", "YES", "None", "NULL", "None"],
                ["IncludeForAnalysis", "varchar(3)", "YES", "None", "NULL", "None"],
                ["MAGETabArchiveName", "varchar(250)", "YES", "None", "NULL", "None"],
                ["MAGETabArchiveURL", "varchar(240)", "YES", "None", "NULL", "None"],
                ["Pipeline", "varchar(45)", "NO", "None", "MUL", "None"],
                ["Platform", "varchar(40)", "NO", "None", "MUL", "None"],
                ["Project", "varchar(30)", "NO", "None", "NULL", "None"],
                ["Repository", "varchar(15)", "YES", "None", "NULL", "None"],
                ["SampleTypeCode", "varchar(2)", "YES", "None", "MUL", "None"],
                ["SDRFFileName", "varchar(75)", "YES", "None", "MUL", "None"],
                ["SDRFFileNameKey", "varchar(200)", "YES", "None", "NULL", "None"],
                ["SecurityProtocol", "varchar(30)", "NO", "None", "NULL", "None"],
                ["Species", "varchar(25)", "NO", "None", "NULL", "None"],
                ["Study", "varchar(20)", "NO", "None", "MUL", "None"],
                ["wasDerivedFrom", "varchar(150)", "YES", "None", "NULL", "None"],
                ["library_strategy", "varchar(10)", "YES", "None", "NULL", "None"],
                ["state", "varchar(12)", "YES", "None", "NULL", "None"],
                ["reason_for_state", "varchar(200)", "YES", "None", "NULL", "None"],
                ["analysis_id", "varchar(36)", "YES", "None", "NULL", "None"],
                ["analyte_code", "varchar(2)", "YES", "None", "MUL", "None"],
                ["last_modified", "varchar(10)", "YES", "None", "NULL", "None"],
                ["platform_full_name", "varchar(30)", "YES", "None", "NULL", "None"],
            ]
        return []
    
    @classmethod
    def insert(cls, config, rows, table, log):
        log.info('\t\tstarting mock insert for %s' % (table))
        field_names = cls.field_names(table)
        cls.column_insert(config, rows, table, field_names, log)
        log.info('\t\tcompleted mock insert')

    @classmethod
    def column_insert(cls, config, rows, table, field_names, log):
        log.info('\t\t\tinsert into %s.%s\n\t(%s)\nvalues\n\t(%s)' % (config['cloudsql']['db'], table, ', '.join(field_names), ', '.join(['%s']*len(field_names))))
        if 0 == len(rows):
            log.warning('\tno rows to insert')
            return
        # now save in batches
        batch = 5
        count = 0
        inserts = []
        for start in range(0, len(rows), batch):
            for index in range(batch):
                if start + index == len(rows):
                    break
                inserts += [rows[start + index]]
            log.info('\t\t\tmock insert rows %s to %s' % (start, start + index))
            if 4 >= count:
                for row in range(batch):
                    log.info('\t\t\t%s' % (','.join(str(insert) for insert in inserts[row])))
            else:
                break
            count += 1
            inserts = []
            
        for start in range(0, len(rows), len(rows)/10):
            for index in range(len(rows)/10):
                if start + index == len(rows):
                    break
            log.info('\t\t\tmock insert rows %s to %s' % (start, start + index))
            

    @classmethod
    def field_names(cls, table):
        if 'metadata_clinical' == table:
            retval = ['adenocarcinoma_invasion','age_at_initial_pathologic_diagnosis','anatomic_neoplasm_subdivision','batch_number','bcr',
            'clinical_M','clinical_N','clinical_stage','clinical_T','colorectal_cancer','country','days_to_birth',
            'days_to_death','days_to_initial_pathologic_diagnosis','days_to_last_followup','days_to_last_known_alive', 'days_to_submitted_specimen_dx',
            'ethnicity','frozen_specimen_anatomic_site','gender','gleason_score_combined','height','histological_type','history_of_colon_polyps',
            'history_of_neoadjuvant_treatment','history_of_prior_malignancy','hpv_calls','hpv_status','icd_10','icd_o_3_histology','icd_o_3_site',
            'lymphatic_invasion','lymphnodes_examined','lymphovascular_invasion_present','menopause_status',
            'mononucleotide_and_dinucleotide_marker_panel_analysis_status','mononucleotide_marker_panel_analysis_status','neoplasm_histologic_grade',
            'new_tumor_event_after_initial_treatment','number_of_lymphnodes_examined','number_of_lymphnodes_positive_by_he',
            'number_pack_years_smoked','ParticipantBarcode','ParticipantUUID','pathologic_M','pathologic_N','pathologic_stage','pathologic_T',
            'person_neoplasm_cancer_status','pregnancies','primary_neoplasm_melanoma_dx','primary_therapy_outcome_success','prior_dx','psa_value',
            'race','residual_tumor','tobacco_smoking_history','TSSCode','tumor_tissue_site','tumor_type',
            'venous_invasion','vital_status','weight','year_of_initial_pathologic_diagnosis']
        elif 'metadata_biospecimen' == table:
            retval = ['ParticipantBarcode','SampleBarcode','SampleUUID','batch_number','bcr','days_to_collection','days_to_sample_procurement',
             'SampleTypeCode', 'SampleType', 'SampleTypeLetterCode', 
             'Study','is_ffpe','preservation_method','Project','tissue_type','tumor_pathology','avg_percent_lymphocyte_infiltration',
             'avg_percent_monocyte_infiltration','avg_percent_necrosis','avg_percent_neutrophil_infiltration','avg_percent_normal_cells',
             'avg_percent_stromal_cells','avg_percent_tumor_cells','avg_percent_tumor_nuclei','max_percent_lymphocyte_infiltration',
             'max_percent_monocyte_infiltration','max_percent_necrosis','max_percent_neutrophil_infiltration','max_percent_normal_cells',
             'max_percent_stromal_cells','max_percent_tumor_cells','max_percent_tumor_nuclei','min_percent_lymphocyte_infiltration',
             'min_percent_monocyte_infiltration','min_percent_necrosis','min_percent_neutrophil_infiltration','min_percent_normal_cells',
             'min_percent_stromal_cells','min_percent_tumor_cells','min_percent_tumor_nuclei']
        elif 'metadata_data' == table:
            retval = ['ParticipantBarcode', 'SampleBarcode', 'AliquotBarcode', 'AliquotUUID', 'AnnotationCategory', 'AnnotationClassification', 
         'DataArchiveName', 'DataArchiveURL', 'DataArchiveVersion', 'DataCenterCode', 'DataCenterName', 'DataCenterType', 'DataCGHubID', 
         'DatafileMD5', 'DatafileName', 'DatafileNameKey', 'DatafileUploaded', 'DataLevel', 'Datatype', 'GenomeReference', 
         'IncludeForAnalysis', 'MAGETabArchiveName', 'MAGETabArchiveURL', 'Pipeline', 'Platform', 'Project', 'Repository', 'SampleType',
         'SampleTypeCode', 'SDRFFileName', 'SDRFFileNameKey', 'SecurityProtocol', 'Species', 'Study', 'wasDerivedFrom', 'library_strategy', 
         'state', 'reason_for_state', 'analysis_id', 'analyte_code', 'last_modified', 'platform_full_name']

        elif 'metadata_samples' == table:
            retval = ['adenocarcinoma_invasion', 'age_at_initial_pathologic_diagnosis', 'anatomic_neoplasm_subdivision',
                      'avg_percent_lymphocyte_infiltration', 'avg_percent_monocyte_infiltration', 'avg_percent_necrosis',
                      'avg_percent_neutrophil_infiltration', 'avg_percent_normal_cells', 'avg_percent_stromal_cells',
                      'avg_percent_tumor_cells', 'avg_percent_tumor_nuclei', 'batch_number', 'bcr', 'clinical_M', 'clinical_N', 'clinical_stage',
                      'clinical_T', 'colorectal_cancer', 'country', 'country_of_procurement', 'days_to_birth', 'days_to_collection',
                      'days_to_death', 'days_to_initial_pathologic_diagnosis', 'days_to_last_followup', 'days_to_submitted_specimen_dx',
                      'Disease_Code', 'ethnicity', 'frozen_specimen_anatomic_site', 'gender', 'gleason_score_combined', 'height',
                      'histological_type', 'history_of_colon_polyps', 'history_of_neoadjuvant_treatment', 'history_of_prior_malignancy',
                      'hpv_calls', 'hpv_status', 'icd_10', 'icd_o_3_histology', 'icd_o_3_site', 'lymph_node_examined_count', 'lymphatic_invasion',
                      'lymphnodes_examined', 'lymphovascular_invasion_present', 'max_percent_lymphocyte_infiltration',
                      'max_percent_monocyte_infiltration', 'max_percent_necrosis', 'max_percent_neutrophil_infiltration',
                      'max_percent_normal_cells', 'max_percent_stromal_cells', 'max_percent_tumor_cells', 'max_percent_tumor_nuclei',
                      'menopause_status', 'min_percent_lymphocyte_infiltration', 'min_percent_monocyte_infiltration', 'min_percent_necrosis',
                      'min_percent_neutrophil_infiltration', 'min_percent_normal_cells', 'min_percent_stromal_cells', 'min_percent_tumor_cells',
                      'min_percent_tumor_nuclei', 'mononucleotide_and_dinucleotide_marker_panel_analysis_status',
                      'mononucleotide_marker_panel_analysis_status', 'neoplasm_histologic_grade', 'new_tumor_event_after_initial_treatment',
                      'number_of_lymphnodes_examined', 'number_of_lymphnodes_positive_by_he', 'number_pack_years_smoked', 'ParticipantBarcode',
                      'pathologic_M', 'pathologic_N', 'pathologic_T', 'pathologic_stage', 'person_neoplasm_cancer_status', 'pregnancies',
                      'preservation_method', 'primary_neoplasm_melanoma_dx', 'primary_therapy_outcome_success', 'prior_dx', 'Project',
                      'psa_value', 'race', 'residual_tumor', 'SampleBarcode', 'Study', 'tissue_type', 'tobacco_smoking_history',
                      'total_number_of_pregnancies', 'tumor_tissue_site', 'tumor_pathology', 'tumor_type', 'venous_invasion', 'vital_status',
                      'weight', 'year_of_initial_pathologic_diagnosis', 'SampleTypeCode', 'has_Illumina_DNASeq', 'has_BCGSC_HiSeq_RNASeq',
                      'has_UNC_HiSeq_RNASeq', 'has_BCGSC_GA_RNASeq', 'has_UNC_GA_RNASeq', 'has_HiSeq_miRnaSeq', 'has_GA_miRNASeq', 'has_RPPA',
                      'has_SNP6', 'has_27k', 'has_450k'
                      ]
        return retval
