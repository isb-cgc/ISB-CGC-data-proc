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
import MySQLdb
import time

class ISBCGC_database_helper():
    """
    this class manages the cloud sql metadata upload
    """
    metadata_clinical = {
        'table_name': 'metadata_clinical',
        'primary_key_name': 'metadata_clinical_id',
        'columns': [
            ['age_at_initial_pathologic_diagnosis', 'INTEGER', 'NULL'],
            ['age_began_smoking_in_years', 'INTEGER', 'NULL'],
            ['anatomic_neoplasm_subdivision', 'VARCHAR(63)', 'NULL'],
            ['batch_number', 'INTEGER', 'NULL'],
            ['bcr', 'VARCHAR(63)', 'NULL'],
            ['BMI', 'FLOAT(3,1)', 'NULL'],
            ['clinical_M', 'VARCHAR(12)', 'NULL'],
            ['clinical_N', 'VARCHAR(12)', 'NULL'],
            ['clinical_stage', 'VARCHAR(12)', 'NULL'],
            ['clinical_T', 'VARCHAR(12)', 'NULL'],
            ['colorectal_cancer', 'VARCHAR(10)', 'NULL'],
            ['country', 'VARCHAR(63)', 'NULL'],
            ['days_to_birth', 'INTEGER', 'NULL'],
            ['days_to_death', 'INTEGER', 'NULL'],
            ['days_to_initial_pathologic_diagnosis', 'INTEGER', 'NULL'],
            ['days_to_last_followup', 'INTEGER', 'NULL'],
            ['days_to_last_known_alive', 'INTEGER', 'NULL'],
            ['days_to_submitted_specimen_dx', 'INTEGER', 'NULL'],
            ['ethnicity', 'VARCHAR(30)', 'NULL'],
            ['gender', 'VARCHAR(15)', 'NULL'],
            ['gleason_score_combined', 'INTEGER', 'NULL'],
            ['h_pylori_infection', 'VARCHAR(10)', 'NULL'],
            ['height', 'INTEGER', 'NULL'],
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
            ['number_of_lymphnodes_examined', 'INTEGER', 'NULL'],
            ['number_of_lymphnodes_positive_by_he', 'INTEGER', 'NULL'],
            ['number_pack_years_smoked', 'INTEGER', 'NULL'],
            ['other_dx', 'VARCHAR(70)', 'NULL'],
            ['other_malignancy_anatomic_site', 'VARCHAR(65)', 'NULL'],
            ['other_malignancy_histological_type', 'VARCHAR(150)', 'NULL'],
            ['other_malignancy_malignancy_type', 'VARCHAR(90)', 'NULL'],
            ['ParticipantBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['ParticipantUUID', 'VARCHAR(36)', 'NOT NULL'],
            ['pathologic_M', 'VARCHAR(12)', 'NULL'],
            ['pathologic_N', 'VARCHAR(12)', 'NULL'],
            ['pathologic_stage', 'VARCHAR(15)', 'NULL'],
            ['pathologic_T', 'VARCHAR(12)', 'NULL'],
            ['person_neoplasm_cancer_status', 'VARCHAR(15)', 'NULL'],
            ['pregnancies', 'VARCHAR(10)', 'NULL'],
            ['primary_neoplasm_melanoma_dx', 'VARCHAR(10)', 'NULL'],
            ['primary_therapy_outcome_success', 'VARCHAR(70)', 'NULL'],
            ['Project', 'VARCHAR(40)', 'NOT NULL'],
            ['psa_value', 'FLOAT', 'NULL'],
            ['race', 'VARCHAR(50)', 'NULL'],
            ['residual_tumor', 'VARCHAR(5)', 'NULL'],
            ['stopped_smoking_year', 'INTEGER', 'NULL'],
            ['Study', 'VARCHAR(40)', 'NOT NULL'],
            ['tobacco_smoking_history', 'VARCHAR(50)', 'NULL'],
            ['TSSCode', 'VARCHAR(2)', 'NULL'],
            ['tumor_tissue_site', 'VARCHAR(100)', 'NULL'],
            ['tumor_type', 'VARCHAR(30)', 'NULL'],
            ['venous_invasion', 'VARCHAR(8)', 'NULL'],
            ['vital_status', 'VARCHAR(8)', 'NULL'],
            ['weight', 'INTEGER', 'NULL'],
            ['year_of_initial_pathologic_diagnosis', 'INTEGER', 'NULL'],
            ['year_of_tobacco_smoking_onset', 'INTEGER', 'NULL']
        ],
#         'natural_key_cols': [
#             'ParticipantBarcode'
#         ],
        'indices_defs': [
            ['age_at_initial_pathologic_diagnosis'],
            ['age_began_smoking_in_years'],
            ['batch_number'],
            ['bcr'],
            ['BMI'],
            ['clinical_M'],
            ['clinical_N'],
            ['clinical_stage'],
            ['clinical_T'],
            ['colorectal_cancer'],
            ['country'],
            ['days_to_birth'],
            ['days_to_death'],
            ['days_to_last_followup'],
            ['days_to_last_known_alive'],
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
            ['icd_o_3_histology'],
            ['icd_o_3_site'],
            ['lymphatic_invasion'],
            ['lymphovascular_invasion_present'],
            ['menopause_status'],
            ['mononucleotide_and_dinucleotide_marker_panel_analysis_status'],
            ['neoplasm_histologic_grade'],
            ['new_tumor_event_after_initial_treatment'],
            ['number_of_lymphnodes_positive_by_he'],
            ['number_pack_years_smoked'],
            ['ParticipantBarcode'],
            ['pathologic_M'],
            ['pathologic_N'],
            ['pathologic_stage'],
            ['pathologic_T'],
            ['primary_therapy_outcome_success'],
            ['Project'],
            ['psa_value'],
            ['race'],
            ['stopped_smoking_year'],
            ['Study'],
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
    
    metadata_biospecimen = {
        'table_name': 'metadata_biospecimen',
        'primary_key_name': 'metadata_biospecimen_id',
        'columns': [
            ['analyte_code', 'VARCHAR(2)', 'NULL'],
            ['avg_percent_lymphocyte_infiltration', 'FLOAT', 'NULL'],
            ['avg_percent_monocyte_infiltration', 'FLOAT', 'NULL'],
            ['avg_percent_necrosis', 'FLOAT', 'NULL'],
            ['avg_percent_neutrophil_infiltration', 'FLOAT', 'NULL'],
            ['avg_percent_normal_cells', 'FLOAT', 'NULL'],
            ['avg_percent_stromal_cells', 'FLOAT', 'NULL'],
            ['avg_percent_tumor_cells', 'FLOAT', 'NULL'],
            ['avg_percent_tumor_nuclei', 'FLOAT', 'NULL'],
            ['batch_number', 'INTEGER', 'NULL'],
            ['bcr', 'VARCHAR(63)', 'NULL'],
            ['days_to_collection', 'INTEGER', 'NULL'],
            ['days_to_sample_procurement', 'INTEGER', 'NULL'],
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
            ['num_portions', 'INTEGER', 'NULL'],
            ['num_samples', 'INTEGER', 'NULL'],
            ['ParticipantBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['Project', 'VARCHAR(40)', 'NOT NULL'],
            ['SampleBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['SampleType', 'VARCHAR(55)', 'NULL'],
            ['SampleTypeCode', 'VARCHAR(2)', 'NULL'],
            ['SampleTypeLetterCode', 'VARCHAR(5)', 'NULL'],
            ['SampleUUID', 'VARCHAR(36)', 'NULL'],
            ['Study', 'VARCHAR(40)', 'NULL'],
            ['tissue_anatomic_site', 'VARCHAR(55)', 'NULL'],
            ['tissue_anatomic_site_description', 'VARCHAR(45)', 'NULL']
        ],
#         'natural_key_cols': [
#             'SampleBarcode'
#         ],
        'indices_defs': [
            ['batch_number'],
            ['bcr'],
            ['ParticipantBarcode'],
            ['Project'],
            ['SampleBarcode'],
            ['SampleType'],
            ['SampleTypeCode'],
            ['SampleTypeLetterCode'],
            ['Study'],
            ['tissue_anatomic_site'],
            ['tissue_anatomic_site_description']
        ],
#         'foreign_key': [
#             'ParticipantBarcode',
#             'metadata_clinical',
#             'ParticipantBarcode'
#         ]
    }
    
    metadata_data = {
        'table_name': 'metadata_data',
        'primary_key_name': 'metadata_data_id',
        'columns': [
            ['ParticipantBarcode', 'VARCHAR(35)', 'NOT NULL'],
            ['SampleBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['AliquotBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['AliquotUUID', 'VARCHAR(36)', 'NULL'],
            ['AnnotationCategory', 'VARCHAR(100)', 'NULL'],
            ['AnnotationClassification', 'VARCHAR(100)', 'NULL'],
            ['DataArchiveName', 'VARCHAR(100)', 'NULL'],
            ['DataArchiveURL', 'VARCHAR(300)', 'NULL'],
            ['DataArchiveVersion', 'VARCHAR(20)', 'NULL'],
            ['DataCenterCode', 'VARCHAR(2)', 'NULL'],
            ['DataCenterName', 'VARCHAR(40)', 'NULL'],
            ['DataCenterType', 'VARCHAR(4)', 'NULL'],
            ['DataCGHubID', 'VARCHAR(36)', 'NULL'],
            ['DatafileMD5', 'VARCHAR(32)', 'NULL'],
            ['DatafileName', 'VARCHAR(250)', 'NOT NULL'],
            ['DatafileNameKey', 'VARCHAR(200)', 'NOT NULL'],
            ['DatafileUploaded', 'VARCHAR(5)', 'NOT NULL'],
            ['DataLevel', 'VARCHAR(7)', 'NOT NULL'],
            ['Datatype', 'VARCHAR(50)', 'NULL'],
            ['GenomeReference', 'VARCHAR(32)', 'NULL'],
            ['IncludeForAnalysis', 'VARCHAR(3)', 'NULL'],
            ['MAGETabArchiveName', 'VARCHAR(250)', 'NULL'],
            ['MAGETabArchiveURL', 'VARCHAR(300)', 'NULL'],
            ['Pipeline', 'VARCHAR(45)', 'NOT NULL'],
            ['Platform', 'VARCHAR(40)', 'NOT NULL'],
            ['Project', 'VARCHAR(40)', 'NOT NULL'],
            ['Repository', 'VARCHAR(15)', 'NULL'],
            ['SampleType', 'VARCHAR(55)', 'NULL'],
            ['SampleTypeCode', 'VARCHAR(2)', 'NULL'],
            ['SDRFFileName', 'VARCHAR(75)', 'NULL'],
            ['SDRFFileNameKey', 'VARCHAR(200)', 'NULL'],
            ['SecurityProtocol', 'VARCHAR(30)', 'NOT NULL'],
            ['Species', 'VARCHAR(25)', 'NOT NULL'],
            ['Study', 'VARCHAR(40)', 'NOT NULL'],
            ['wasDerivedFrom', 'VARCHAR(1000)', 'NULL'],
            ['library_strategy', 'VARCHAR(25)', 'NULL'],
            ['state', 'VARCHAR(25)', 'NULL'],
            ['reason_for_state', 'VARCHAR(250)', 'NULL'],
            ['analysis_id', 'VARCHAR(36)', 'NULL'],
            ['analyte_code', 'VARCHAR(2)', 'NULL'],
            ['last_modified', 'DATE', 'NULL'],
            ['platform_full_name', 'VARCHAR(30)', 'NULL'],
            ['GG_dataset_id', 'VARCHAR(30)', 'NULL'],
            ['GG_readgroupset_id', 'VARCHAR(40)', 'NULL']
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
            ['AnnotationCategory'],
            ['AnnotationClassification'],
            ['DataArchiveName'],
            ['DataArchiveURL'],
            ['DataCenterCode'],
            ['DataCenterName'],
            ['DataCenterType'],
            ['DatafileName'],
            ['DatafileNameKey'],
            ['DatafileUploaded'],
            ['DataLevel'],
            ['Datatype'],
            ['IncludeForAnalysis'],
            ['Pipeline'],
            ['Platform'],
            ['Project'],
            ['Repository'],
            ['SampleType'],
            ['SampleTypeCode'],
            ['SecurityProtocol'],
            ['Species'],
            ['Study'],
            ['state'],
            ['GG_dataset_id']
        ],
#         'foreign_key': [
#             'SampleBarcode',
#             'metadata_biospecimen',
#             'SampleBarcode'
#         ]
    }

    metadata_samples = {
        'table_name': 'metadata_samples',
        'primary_key_name': 'metadata_samples_id',  # todo: define this?

        'columns': [
            ['age_at_initial_pathologic_diagnosis', 'INTEGER', 'NULL'],
            ['anatomic_neoplasm_subdivision', 'VARCHAR(63)', 'NULL'],
            ['age_began_smoking_in_years', 'INTEGER', 'NULL'],
            ['avg_percent_lymphocyte_infiltration', 'FLOAT', 'NULL'],
            ['avg_percent_monocyte_infiltration', 'FLOAT', 'NULL'],
            ['avg_percent_necrosis', 'FLOAT', 'NULL'],
            ['avg_percent_neutrophil_infiltration', 'FLOAT', 'NULL'],
            ['avg_percent_normal_cells', 'FLOAT', 'NULL'],
            ['avg_percent_stromal_cells', 'FLOAT', 'NULL'],
            ['avg_percent_tumor_cells', 'FLOAT', 'NULL'],
            ['avg_percent_tumor_nuclei', 'FLOAT', 'NULL'],
            ['batch_number', 'INTEGER', 'NULL'],
            ['bcr', 'VARCHAR(63)', 'NULL'],
            ['BMI', 'FLOAT(3,1)', 'NULL'],
            ['clinical_M', 'VARCHAR(12)', 'NULL'],
            ['clinical_N', 'VARCHAR(12)', 'NULL'],
            ['clinical_stage', 'VARCHAR(12)', 'NULL'],
            ['clinical_T', 'VARCHAR(12)', 'NULL'],
            ['colorectal_cancer', 'VARCHAR(10)', 'NULL'],
            ['country', 'VARCHAR(63)', 'NULL'],
            ['days_to_birth', 'INTEGER', 'NULL'],
            ['days_to_collection', 'INTEGER', 'NULL'],
            ['days_to_death', 'INTEGER', 'NULL'],
            ['days_to_initial_pathologic_diagnosis', 'INTEGER', 'NULL'],
            ['days_to_last_followup', 'INTEGER', 'NULL'],
            ['days_to_last_known_alive', 'INTEGER', 'NULL'],
            ['days_to_submitted_specimen_dx', 'INTEGER', 'NULL'],
            ['ethnicity', 'VARCHAR(30)', 'NULL'],
            ['gender', 'VARCHAR(15)', 'NULL'],
            ['gleason_score_combined', 'INTEGER', 'NULL'],
            ['h_pylori_infection', 'VARCHAR(10)', 'NULL'],
            ['height', 'INTEGER', 'NULL'],
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
            ['max_percent_lymphocyte_infiltration', 'FLOAT', 'NULL'],
            ['max_percent_monocyte_infiltration', 'FLOAT', 'NULL'],
            ['max_percent_necrosis', 'FLOAT', 'NULL'],
            ['max_percent_neutrophil_infiltration', 'FLOAT', 'NULL'],
            ['max_percent_normal_cells', 'FLOAT', 'NULL'],
            ['max_percent_stromal_cells', 'FLOAT', 'NULL'],
            ['max_percent_tumor_cells', 'FLOAT', 'NULL'],
            ['max_percent_tumor_nuclei', 'FLOAT', 'NULL'],
            ['menopause_status', 'VARCHAR(120)', 'NULL'],
            ['min_percent_lymphocyte_infiltration', 'FLOAT', 'NULL'],
            ['min_percent_monocyte_infiltration', 'FLOAT', 'NULL'],
            ['min_percent_necrosis', 'FLOAT', 'NULL'],
            ['min_percent_neutrophil_infiltration', 'FLOAT', 'NULL'],
            ['min_percent_normal_cells', 'FLOAT', 'NULL'],
            ['min_percent_stromal_cells', 'FLOAT', 'NULL'],
            ['min_percent_tumor_cells', 'FLOAT', 'NULL'],
            ['min_percent_tumor_nuclei', 'FLOAT', 'NULL'],
            ['mononucleotide_and_dinucleotide_marker_panel_analysis_status', 'VARCHAR(20)', 'NULL'],
            ['neoplasm_histologic_grade', 'VARCHAR(15)', 'NULL'],
            ['new_tumor_event_after_initial_treatment', 'VARCHAR(8)', 'NULL'],
            ['num_portions', 'INTEGER', 'NULL'],
            ['num_samples', 'INTEGER', 'NULL'],
            ['number_of_lymphnodes_examined', 'INTEGER', 'NULL'],
            ['number_of_lymphnodes_positive_by_he', 'INTEGER', 'NULL'],
            ['number_pack_years_smoked', 'INTEGER', 'NULL'],
            ['other_dx', 'VARCHAR(70)', 'NULL'],
            ['other_malignancy_anatomic_site', 'VARCHAR(65)', 'NULL'],
            ['other_malignancy_histological_type', 'VARCHAR(150)', 'NULL'],
            ['other_malignancy_malignancy_type', 'VARCHAR(90)', 'NULL'],
            ['ParticipantBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['pathologic_M', 'VARCHAR(12)', 'NULL'],
            ['pathologic_N', 'VARCHAR(12)', 'NULL'],
            ['pathologic_stage', 'VARCHAR(15)', 'NULL'],
            ['pathologic_T', 'VARCHAR(12)', 'NULL'],
            ['person_neoplasm_cancer_status', 'VARCHAR(15)', 'NULL'],
            ['pregnancies', 'VARCHAR(10)', 'NULL'],
            ['primary_neoplasm_melanoma_dx', 'VARCHAR(10)', 'NULL'],
            ['primary_therapy_outcome_success', 'VARCHAR(70)', 'NULL'],
            ['Project', 'VARCHAR(40)', 'NOT NULL'],
            ['psa_value', 'FLOAT', 'NULL'],
            ['race', 'VARCHAR(50)', 'NULL'],
            ['residual_tumor', 'VARCHAR(5)', 'NULL'],
            ['SampleBarcode', 'VARCHAR(45)', 'NOT NULL'],
            ['SampleTypeCode', 'VARCHAR(2)', 'NULL'],
            ['stopped_smoking_year', 'INTEGER', 'NULL'],
            ['Study', 'VARCHAR(40)', 'NOT NULL'],
            ['tissue_anatomic_site', 'VARCHAR(55)', 'NULL'],
            ['tissue_anatomic_site_description', 'VARCHAR(45)', 'NULL'],
            ['tobacco_smoking_history', 'VARCHAR(50)', 'NULL'],
            ['TSSCode', 'VARCHAR(2)', 'NULL'],
            ['tumor_tissue_site', 'VARCHAR(100)', 'NULL'],
            ['tumor_type', 'VARCHAR(30)', 'NULL'],
            ['venous_invasion', 'VARCHAR(8)', 'NULL'],
            ['vital_status', 'VARCHAR(8)', 'NULL'],
            ['weight', 'INTEGER', 'NULL'],
            ['year_of_initial_pathologic_diagnosis', 'INTEGER', 'NULL'],
            ['year_of_tobacco_smoking_onset', 'INTEGER', 'NULL'],
            ['has_Illumina_DNASeq', 'tinyint(4)', 'NULL'],
            ['has_BCGSC_HiSeq_RNASeq', 'tinyint(4)', 'NULL'],
            ['has_UNC_HiSeq_RNASeq', 'tinyint(4)', 'NULL'],
            ['has_BCGSC_GA_RNASeq', 'tinyint(4)', 'NULL'],
            ['has_UNC_GA_RNASeq', 'tinyint(4)', 'NULL'],
            ['has_HiSeq_miRnaSeq', 'tinyint(4)', 'NULL'],
            ['has_GA_miRNASeq', 'tinyint(4)', 'NULL'],
            ['has_RPPA', 'tinyint(4)', 'NULL'],
            ['has_SNP6', 'tinyint(4)', 'NULL'],
            ['has_27k', 'tinyint(4)', 'NULL'],
            ['has_450k', 'tinyint(4)', 'NULL']
        ],
        'natural_key_cols': ['SampleBarcode'],
#         'foreign_key': [
#             'SampleBarcode',
#         ]
        'indices_defs': [
            ['age_at_initial_pathologic_diagnosis'],
            ['age_began_smoking_in_years'],
            ['batch_number'],
            ['bcr'],
            ['BMI'],
            ['clinical_M'],
            ['clinical_N'],
            ['clinical_stage'],
            ['clinical_T'],
            ['colorectal_cancer'],
            ['country'],
            ['days_to_birth'],
            ['days_to_death'],
            ['days_to_last_followup'],
            ['days_to_last_known_alive'],
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
            ['icd_o_3_histology'],
            ['icd_o_3_site'],
            ['lymphatic_invasion'],
            ['lymphovascular_invasion_present'],
            ['menopause_status'],
            ['mononucleotide_and_dinucleotide_marker_panel_analysis_status'],
            ['neoplasm_histologic_grade'],
            ['new_tumor_event_after_initial_treatment'],
            ['number_of_lymphnodes_positive_by_he'],
            ['number_pack_years_smoked'],
            ['ParticipantBarcode'],
            ['pathologic_M'],
            ['pathologic_N'],
            ['pathologic_stage'],
            ['pathologic_T'],
            ['primary_therapy_outcome_success'],
            ['Project'],
            ['psa_value'],
            ['race'],
            ['SampleBarcode'],
            ['SampleTypeCode'],
            ['stopped_smoking_year'],
            ['Study'],
            ['tissue_anatomic_site'],
            ['tissue_anatomic_site_description'],
            ['tobacco_smoking_history'],
            ['tumor_tissue_site'],
            ['tumor_type'],
            ['venous_invasion'],
            ['vital_status'],
            ['weight'],
            ['year_of_initial_pathologic_diagnosis'],
            ['year_of_tobacco_smoking_onset'],
        ]
    }
    
    metadata_tables = {
        'metadata_clinical': metadata_clinical,
        'metadata_biospecimen': metadata_biospecimen,
        'metadata_data': metadata_data,
        'metadata_samples': metadata_samples
    }

    self = None

    ssl_dir = 'ssl/'
    ssl = {
#             'ca': ssl_dir + 'server-ca.pem',
        'cert': ssl_dir + 'client-cert.pem',
        'key': ssl_dir + 'client-key.pem' 
    }

    @classmethod
    def getDBConnection(cls, config, log):
        try:
            db = MySQLdb.connect(host=config['cloudsql']['host'], db=config['cloudsql']['db'], user=config['cloudsql']['user'], passwd=config['cloudsql']['passwd'], ssl = cls.ssl)
        except Exception as e:
            # if connection requests are made too close together over a period of time, the connection attempt might fail
            count = 4
            while count > 0:
                count -= 1
                time.sleep(1)
                log.warning('\n\n!!!!!!sleeping on error to reattempt db connection!!!!!!\n')
                try:
                    db = MySQLdb.connect(host=config['cloudsql']['host'], db=config['cloudsql']['db'], user=config['cloudsql']['user'], passwd=config['cloudsql']['passwd'], ssl = cls.ssl)
                    break
                except Exception as e:
                    if 1 == count:
                        log.exception("failed to reconnect to database")
                        raise e
            
        return db

    def __init__(self, config, log):
        db = None
        cursor = None
        try:
            if not config['process_bio']:
                return
            db = self.getDBConnection(config, log)
            cursor = db.cursor()
            cursor.execute('select table_name from information_schema.tables where table_schema = "%s"' % (config['cloudsql']['db']))
            not_found = dict(self.metadata_tables)
            found = {}
            for next_row in cursor:
                print next_row[0]
                if next_row[0] in self.metadata_tables:
                    found[next_row[0]] = self.metadata_tables[next_row[0]]
                    not_found.pop(next_row[0])
            if 0 != len(found) and config['cloudsql']['update_schema']:
                # need to delete in foreign key dependency order
                found_list = []
                for table_name in self.metadata_tables:
                    if table_name in found:
                        found_list += [found[table_name]]
                self._drop_schema(cursor, config, found_list, log)
                self._create_schema(cursor, config, self.metadata_tables.values(), log)
            elif 0 != len(not_found):
                log.info('\tcreating table(s) %s' % (', '.join(not_found)))
                self._create_schema(cursor, config, not_found.values(), log)
            else:
                log.info('\tpreserving tables')
            log.info('\tconnection successful')
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    @classmethod
    def initialize(cls, config, log):
        if cls.self:
            log.warning('class has already been initialized')
        else:
            cls.self = ISBCGC_database_helper(config, log)
    
    def _drop_schema(self, cursor, config, tables, log):
        drop_schema_template = 'DROP TABLE %s.%s'
        
        for table in tables:
            drop_statement = drop_schema_template % (config['cloudsql']['db'], table['table_name'])
            log.info('\tdropping table %s:\n%s' % (config['cloudsql']['db'], drop_statement))
            try:
                cursor.execute(drop_statement)
            except Exception as e:
                log.exception('\tproblem dropping %s' % (table['table_name']))
                raise e

    def _create_schema(self, cursor, config, tables, log):
        create_table_template = "CREATE TABLE IF NOT EXISTS %s.%s (\n\t%s\n)" 
        primary_key_template = '%s INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n\t'
        create_col_template = '%s %s %s,\n\t'
        index_template = 'INDEX %s (%s),\n\t'
        foreign_key_template = 'CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s),\n\t'

        for table in tables:
            columnDefinitions = primary_key_template % (table['primary_key_name'])
            columnDefinitions += ''.join([create_col_template % (column[0], column[1], column[2]) for column in table['columns']])
            if 'natural_key_cols' in table and 0 < table['natural_key_cols']:
                index_cols = ','.join(table['natural_key_cols'])
                columnDefinitions += 'UNIQUE ' + index_template % (table['table_name'] + '_nkey', index_cols)
            count = 1
            if 'indices_defs' in table and 0 < table['indices_defs']:
                for index_def in table['indices_defs']:
                    index_cols = ','.join(index_def)
                    columnDefinitions += index_template % (table['table_name'] + str(count), index_cols)
                    count += 1
            if 'foreign_key' in table:
                columnDefinitions += foreign_key_template % ('fk_' + table['table_name'] + '_' + table['foreign_key'][1], 
                                            table['foreign_key'][0], table['foreign_key'][1], table['foreign_key'][2])
            columnDefinitions = columnDefinitions[:-3]
            table_statement = create_table_template % (config['cloudsql']['db'], table['table_name'], columnDefinitions)
            log.info('\tcreating table %s:\n%s' % (config['cloudsql']['db'], table_statement))
            try:
                cursor.execute(table_statement)
            except Exception as e:
                log.exception('problem creating %s' % (table['table_name']))
                raise e

    @classmethod
    def select(cls, config, stmt, log, params = [], verbose = True):
        db = None
        cursor = None
        try:
            if verbose:
                log.info('\t\tstarting \'%s\'' % (stmt))
            db = cls.getDBConnection(config, log)
            cursor = db.cursor()
            # now execute the select
            cursor.execute(stmt, params)
            if verbose:
                log.info('\t\tcompleted select.  fetched %s rows', cursor.rowcount)
            retval = [row for row in cursor]
            return retval
        except Exception as e:
            log.exception('\t\tselect failed')
            if cursor:
                cursor.execute("ROLLBACK")
            raise e
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()
        
    @classmethod
    def select_paged(cls, config, stmt, log, countper = 1000, verbose = True):
        db = None
        cursor = None
        try:
            if verbose:
                log.info('\t\tstarting \'%s\'' % (stmt))
            db = cls.getDBConnection(config, log)
            cursor = db.cursor()
            retval = []
            # now execute the select
            curcount = countper
            while 0 < cursor.rowcount:
                curstmt = stmt % (curcount)
                cursor.execute(curstmt, [curcount])
                retval += [row for row in cursor]
                log.info('\t\tcompleted select.  fetched %s rows for %s', cursor.rowcount, curstmt)
                curcount += countper
            return retval
        except Exception as e:
            log.exception('\t\tselect failed')
            if cursor:
                cursor.execute("ROLLBACK")
            raise e
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()
        
    @classmethod
    def update(cls, config, stmt, log, params = [], verbose = True):
        db = None
        cursor = None
        try:
            if verbose:
                log.info('\t\tstarting \'%s\'' % (stmt))
            db = cls.getDBConnection(config, log)
            cursor = db.cursor()
            # now execute the updates
            cursor.execute("START TRANSACTION")
            count = 0
            report = len(params) / 20
            for paramset in params:
                if 0 == count % report:
                    log.info('\t\t\tupdated %s records' % (count))
                count += 1
                try:
                    cursor.execute(stmt, paramset)
                except MySQLdb.OperationalError as oe:
                    if oe.errno == 1205:
                        log.warning('\t\t\tupdate had operation error (%s:%s) on %s, sleeping' % (stmt, count, paramset))
                        time.sleep(1)
                        cursor.execute(stmt, paramset)
                    else:
                        log.exception('\t\t\tupdate had operation error (%s:%s) on %s' % (stmt, count, paramset))
                except Exception as e:
                    log.exception('problem with update(%s): \n\t\t\t%s\n\t\t\t%s' % (count, stmt, paramset))
                    raise e
            cursor.execute("COMMIT")
            if verbose:
                log.info('\t\tcompleted update.  updated %s record', count)
        except Exception as e:
            log.exception('\t\tupdate failed')
            if cursor:
                cursor.execute("ROLLBACK")
            raise e
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()
        
    @classmethod
    def insert(cls, config, rows, table, log):

        field_names = cls.field_names(table)
        cls.column_insert(config, rows, table, field_names, log)


    @classmethod
    def column_insert(cls, config, rows, table, field_names, log):
        db = None
        cursor = None
        try:
            log.info('\t\tstarting insert for %s' % (table))
            insert_stmt = 'insert into %s.%s\n\t(%s)\nvalues\n\t(%s)' % (config['cloudsql']['db'], table, ', '.join(field_names), ', '.join(['%s']*len(field_names)))
            db = cls.getDBConnection(config, log)
            cursor = db.cursor()
            cursor.execute("START TRANSACTION")
            # now save in batches
            batch = 1028
            inserts = []
            for start in range(0, len(rows), batch):
                for index in range(batch):
                    if start + index == len(rows):
                        break
                    inserts += [rows[start + index]]
                log.info('\t\t\tinsert rows %s to %s' % (start, index))
                cursor.executemany(insert_stmt, inserts)
                inserts = []
            
            cursor.execute("COMMIT")
            log.info('\t\tcompleted insert')
            log.info('\t\tchecking counts for insert')
            cursor.execute('select count(*) from %s.%s' % (config['cloudsql']['db'], table))
            log.info('\t\tcounts for insert: submitted--%s saved--%s' % (len(rows), cursor.fetchone()[0]))
        except Exception as e:
            log.exception('\t\tinsert failed')
            if cursor:
                cursor.execute("ROLLBACK")
            raise e
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    @classmethod
    def field_names(cls, table):
        return [field_parts[0] for field_parts in cls.metadata_tables[table]['columns']]
