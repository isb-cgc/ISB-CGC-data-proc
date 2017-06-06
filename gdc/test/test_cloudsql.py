'''
Created on Jan 24, 2017

opyright 2017, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author: michael
'''
import json

from gdc.test.test_setup import GDCTestSetup
from gdc.util.gdc_util import get_map_rows, request
from isbcgc_cloudsql_model import ISBCGC_database_helper

class GDCCloudSQLTest(GDCTestSetup):
    def process_header(self,query):
        if ('*' in query and 'count' not in query) or 'desc' in query:
            return ''
        
        index = query.find('from')
        return '\n' + '\t'.join(column.strip().split(' ')[-1] for column in query[6:index].split(','))
    
    def run_query(self, query, verbose = True):
        rows = ISBCGC_database_helper.select(self.config, query, self.log, [], verbose)
        results = ''
        try:
            for row in rows:
                results += '%s\n' % ('\t'.join(str(item) for item in row))
            if verbose:
                self.log.info('rows for query:%s\n%s\n' % (self.process_header(query), results))
            return rows
        except:
            self.log.exception()

#    def testCloudSQL(self):
    def CloudSQL(self):
        query = "select * from metadata_program order by program_name"
        self.run_query(query)
   
        query = "desc CCLE_metadata_project"
        self.run_query(query)
   
        query = "select count(*), 'CCLE' from CCLE_metadata_project union select count(*), 'TARGET' from TARGET_metadata_project union select count(*), 'TCGA' from TCGA_metadata_project"
        self.run_query(query)
   
        query = "select count(*), 'CCLE' from CCLE_metadata_clinical union select count(*), 'TARGET' from TARGET_metadata_clinical union select count(*), 'TCGA' from TCGA_metadata_clinical"
        self.run_query(query)
   
        query = "select count(*), 'CCLE' from CCLE_metadata_biospecimen union select count(*), 'TARGET' from TARGET_metadata_biospecimen union select count(*), 'TCGA' from TCGA_metadata_biospecimen"
        self.run_query(query)
   
        query = "select sample_type, count(*) ct from CCLE_metadata_biospecimen group by sample_type"
        self.run_query(query)
   
        query = "select * from CCLE_metadata_project order by project_short_name"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_project order by project_short_name"
        self.run_query(query)
   
        query = "select * from TCGA_metadata_project order by project_short_name"
        self.run_query(query)
   
        query = "select * from CCLE_metadata_clinical order by case_barcode limit 50"
        self.run_query(query)
   
        query = "desc TARGET_metadata_clinical"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_clinical order by case_barcode limit 50"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_clinical where endpoint_type = 'legacy' order by case_barcode limit 20"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_clinical where endpoint_type = 'current' order by case_barcode limit 20"
        self.run_query(query)
   
        query = "desc TCGA_metadata_clinical"
        self.run_query(query)
   
        query = "select * from TCGA_metadata_clinical order by case_barcode limit 50"
        self.run_query(query)
   
        query = "select * from TCGA_metadata_clinical where endpoint_type = 'legacy' order by case_barcode limit 20"
        self.run_query(query)
   
        query = "select * from TCGA_metadata_clinical where endpoint_type = 'current' order by case_barcode limit 20"
        self.run_query(query)
####################
        query = 'select count(*) from TCGA_metadata_clinical where age_began_smoking_in_years is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where anatomic_neoplasm_subdivision is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where batch_number is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where bmi is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where clinical_M is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where clinical_N is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where clinical_stage is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where clinical_T is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where colorectal_cancer is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where country is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where days_to_initial_pathologic_diagnosis is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where days_to_last_followup is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where days_to_submitted_specimen_dx is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where gleason_score_combined is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where h_pylori_infection is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where height is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where history_of_colon_polyps is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where hpv_calls is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where hpv_status is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where lymphatic_invasion is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where lymphnodes_examined is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where lymphovascular_invasion_present is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where menopause_status is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where mononucleotide_and_dinucleotide_marker_panel_analysis_status is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where neoplasm_histologic_grade is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where new_tumor_event_after_initial_treatment is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where number_of_lymphnodes_examined is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where number_of_lymphnodes_positive_by_he is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where number_pack_years_smoked is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where other_dx is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where other_malignancy_anatomic_site is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where other_malignancy_histological_type is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where other_malignancy_type is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where pathologic_M is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where pathologic_N is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where pathologic_stage is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where pathologic_T is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where pregnancies is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where primary_neoplasm_melanoma_dx is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where primary_therapy_outcome_success is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where psa_value is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where residual_tumor is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where stopped_smoking_year is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where tobacco_smoking_history is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where tumor_type is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where venous_invasion is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where weight is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_clinical where year_of_tobacco_smoking_onset is not null'
        self.run_query(query)
##############
        query = "select * from CCLE_metadata_biospecimen order by case_barcode limit 50"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_biospecimen order by case_barcode limit 50"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_biospecimen where 'legacy' = endpoint_type order by case_barcode limit 20"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_biospecimen where 'current' = endpoint_type order by case_barcode limit 50"
        self.run_query(query)
   
        query = "desc TCGA_metadata_biospecimen"
        self.run_query(query)
   
        query = "select * from TCGA_metadata_biospecimen order by case_barcode limit 50"
        self.run_query(query)
 
        query = "select * from TCGA_metadata_biospecimen where 'legacy' = endpoint_type order by case_barcode limit 20"
        self.run_query(query)
   
        query = "select * from TCGA_metadata_biospecimen where 'current' = endpoint_type order by case_barcode limit 50"
        self.run_query(query)
   
########################
        query = 'select count(*) from TCGA_metadata_biospecimen where days_to_collection is not null'
        self.run_query(query)
 
        query = 'select count(*) from TCGA_metadata_biospecimen where days_to_sample_procurement is not null'
        self.run_query(query)
 
########################
 
        query = "select * from CCLE_metadata_samples order by case_barcode limit 50"
        self.run_query(query)
   
        query = "desc TARGET_metadata_samples"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_samples order by case_barcode limit 50"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_samples where 'legacy' = endpoint_type order by case_barcode limit 20"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_samples where 'current' = endpoint_type order by case_barcode limit 20"
        self.run_query(query)
#######################
 
        query = "select * from TCGA_metadata_samples order by case_barcode limit 50"
        self.run_query(query)
   
        query = "select * from TCGA_metadata_samples where 'legacy' = endpoint_type order by case_barcode limit 20"
        self.run_query(query)
   
        query = "select * from TCGA_metadata_samples where 'current' = endpoint_type order by case_barcode limit 20"
        self.run_query(query)
#######################
        query = 'select count(*) ct from TCGA_metadata_samples where bmi is not null'
        self.run_query(query)
  
        query = 'select count(*) ct from TCGA_metadata_samples where tobacco_smoking_history is not null'
        self.run_query(query)
  
        query = 'select count(*) ct from TCGA_metadata_samples where menopause_status is not null'
        self.run_query(query)
  
        query = 'select count(*) ct from TCGA_metadata_samples where hpv_status is not null'
        self.run_query(query)
  
        query = 'select count(*) ct from TCGA_metadata_samples where pathologic_stage is not null'
        self.run_query(query)
  
        query = 'select count(*) ct from TCGA_metadata_samples where residual_tumor is not null'
        self.run_query(query)
  
        query = 'select count(*) ct from TCGA_metadata_samples where neoplasm_histologic_grade is not null'
        self.run_query(query)
#######################
 
        query = "select * from CCLE_metadata_attrs"
        self.run_query(query)
   
        query = "select * from TARGET_metadata_attrs"
        self.run_query(query)
   
        query = "select * from TCGA_metadata_attrs"
        self.run_query(query)
  
        query = "select program_name, count(distinct sample_barcode) from CCLE_metadata_samples group by program_name union select program_name, count(distinct sample_barcode) from TARGET_metadata_samples group by program_name union select program_name, count(distinct sample_barcode) from TCGA_metadata_samples group by 1"
        self.run_query(query)
  
        query = "select program_name, endpoint_type, count(distinct sample_barcode) from CCLE_metadata_samples group by 1, 2 union select program_name, endpoint_type, count(distinct sample_barcode) from TARGET_metadata_samples group by 1, 2 union select program_name, endpoint_type, count(distinct sample_barcode) from TCGA_metadata_samples group by 1, 2"
        self.run_query(query)
  
        query = "select program_name, count(distinct case_barcode) from CCLE_metadata_samples group by program_name union select program_name, count(distinct case_barcode) from TARGET_metadata_samples group by program_name union select program_name, count(distinct case_barcode) from TCGA_metadata_samples group by 1"
        self.run_query(query)
  
        query = "select program_name, endpoint_type, count(distinct case_barcode) from CCLE_metadata_samples group by 1, 2 union select program_name, endpoint_type, count(distinct case_barcode) from TARGET_metadata_samples group by 1, 2 union select program_name, endpoint_type, count(distinct case_barcode) from TCGA_metadata_samples group by 1, 2"
        self.run_query(query)
  
        query = "select count(distinct a.case_barcode), count(distinct a.sample_barcode) from TCGA_metadata_samples a left join TCGA_metadata_samples b on a.sample_barcode = b.sample_barcode where a.endpoint_type = 'current' and b.endpoint_type = 'legacy' and b.sample_barcode is null"
        self.run_query(query)
  
        query = "select count(distinct a.case_barcode), count(distinct a.sample_barcode) from TCGA_metadata_samples a where a.endpoint_type = 'current' and a.sample_barcode not in (select b.sample_barcode from TCGA_metadata_samples b where b.endpoint_type = 'legacy')"
        self.run_query(query)
  
        query = "select a.project_short_name, count(distinct a.case_barcode), count(distinct a.sample_barcode) from TCGA_metadata_samples a where a.endpoint_type = 'current' and a.sample_barcode not in (select b.sample_barcode from TCGA_metadata_samples b where b.endpoint_type = 'legacy') group by a.project_short_name order by a.project_short_name"
#         self.run_query(query)
   
        query = \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(distinct file_name) files, count(*) total, 'CCLE19' from CCLE_metadata_data_HG19 group by endpoint_type union " \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(distinct file_name) files, count(*) total, 'TARGET19' from TARGET_metadata_data_HG19 group by endpoint_type union " \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(distinct file_name) files, count(*) total, 'TARGET38' from TARGET_metadata_data_HG38 group by endpoint_type union " \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(distinct file_name) files, count(*) total, 'TCGA19' from TCGA_metadata_data_HG19 group by endpoint_type union " \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(distinct file_name) files, count(*) total, 'TCGA38' from TCGA_metadata_data_HG38 group by endpoint_type " \
            "order by 3, 1"
        self.run_query(query)
  
        query = \
            "select file_uploaded, count(distinct file_name_key) name_keys, count(*) total, 'CCLE19' from CCLE_metadata_data_HG19 group by file_uploaded union " \
            "select file_uploaded, count(distinct file_name_key) name_keys, count(*) total, 'TARGET19' from TARGET_metadata_data_HG19 group by file_uploaded union " \
            "select file_uploaded, count(distinct file_name_key) name_keys, count(*) total, 'TARGET38' from TARGET_metadata_data_HG38 group by file_uploaded union " \
            "select file_uploaded, count(distinct file_name_key) name_keys, count(*) total, 'TCGA19' from TCGA_metadata_data_HG19 group by file_uploaded union " \
            "select file_uploaded, count(distinct file_name_key) name_keys, count(*) total, 'TCGA38' from TCGA_metadata_data_HG38 group by file_uploaded " \
            "order by 3, 1"
        self.run_query(query)
  
        query = \
            "select genomic_build build, isb_label, count(distinct sample_barcode) barcodes, sum(count) total, count(*) ct " \
            "from TCGA_metadata_data_type_availability dt join TCGA_metadata_sample_data_availability sd " \
               "on dt.metadata_data_type_availability_id = sd.metadata_data_type_availability_id " \
            "group by 1, 2"
        self.run_query(query)

    def compare_metadata2gdcportal(self, msg, rows, compare_gdc, compare_index): 
        metadata = {row[0]: int(row[compare_index]) for row in rows}
        gdc = {row[0]: int(row[compare_index]) for row in compare_gdc}
        equal = True
        keys = sorted(gdc.keys())
        matched = ''
        not_matched = ''
        not_metadata = ''
        neither = ''
        for key in keys:
            if 0 == gdc[key]:
                neither += '%s is in neither\n' % (key)
                equal = False
            elif key not in metadata:
                not_metadata += '%s is not in metadata\n' % (key)
                equal = False
            else:
                gdc_count = gdc[key]
                metadata_count = metadata[key]
                if gdc_count == metadata_count:
                    matched += '%s matched counts\n' % (key)
                else:
                    not_matched += '%s mismatch: %d vs. %d\n' % (key, gdc_count, metadata_count)
                    equal = False
        if equal:
            self.log.info('%s compared equal!\n' % (msg))
        else:
            compare = matched + not_matched + not_metadata + neither
            self.log.info('%s\n%s\n' % (msg, compare))


    def getHG19combined_totals(self, query):
        for index, program in enumerate(['TCGA', 'TARGET', 'CCLE']):
            rows = self.run_query(query % (program), False)
            if 0 == index:
                metadata_type2files = {row[0]:int(row[2]) for row in rows}
            else:
                for row in rows:
                    metadata_type2files[row[0]] += int(row[2])
        
        rows = [(data_type, count) for data_type, count in metadata_type2files.iteritems()]
        return rows

    def make_filter_piece(self, field, value, operand):
        if operand in ('is', 'not'):
            filt = {
                'op': operand,
                 'content': {
                     'field': field,
                } 
            }
        else:
            filt = {
                'op': operand,
                 'content': {
                     'field': field,
                     'value': value
                } 
            }
        return filt

    def make_filter(self, fields, values, operands):
        if 1 == len(fields):
            filt = self.make_filter_piece(fields[0], values[0], operands[0])
        else:
            filt = {
                'op': 'and',
                'content': []
            }
            oplist = filt['content']
            for field, value, operand in zip(fields, values, operands):
                oplist += [self.make_filter_piece(field, value, operand)]
        return filt

    def getAPITotal(self, endpt_type, endpt, fields, values, operands, verbose = False):
        filt = self.make_filter(fields, values, operands)

        params = {
            'filters':json.dumps(filt), 
            'sort': '%s:asc' % ('%s_id' % (endpt)),
            'from': 1, 
            'size': 1
        }

        endpt_url = self.config['%ss_endpt' % (endpt)]['%s endpt' % (endpt_type)]
#         query = self.config['%ss_endpt' % (endpt)]['query']
        query = '?expand=cases'
        url = endpt_url + query
        
        response = request(url, params, "query %s for total for %s" % (url, params), self.log, self.config['map_requests_timeout'], verbose)
        response.raise_for_status()
        rj = response.json()
        
        return rj['data']['pagination']['total']
    
    def countCaseOrNot(self, endpt_type, endpt, fields, values, operands):
        filt = self.make_filter(fields, values, operands)
        rows = get_map_rows(self.config, endpt_type, endpt, 'TCGA', filt, self.log)
        sampletype2count = {}
        counts = [0, 0]
        for info in rows.itervalues():
            count = sampletype2count.setdefault(info['file_name'][13:16], 0)
            sampletype2count[info['file_name'][13:16]] = count + 1
            if 'cases' in info and 0 < len(info['cases']):
                counts[1] += 1
            else:
                counts[0] += 1
        return counts, sampletype2count
    
    def add_to_map(self, count_map, count_field, program, case_id, id):
        project2counts = count_map.setdefault(program, {})
        counts = project2counts.setdefault(count_field, [count_field, set(), set()])
        counts[1].add(case_id)
        counts[2].add(id)

    def get_case_and_sample_counts(self, endpt):
        params = {
            'filters':"{}", 
            'sort':'case_id:asc', 
            'from':1, 
            'size':1000}
        curstart = 1
        no_case = 0
        program2case2counts = {}
        program2sample2counts = {}
        while True:
            msg = '\t\tproblem getting filtered map for files'
            response = None
            retries = 4
            while retries:
                retries -= 1
                try:
                    response = request(endpt, params, msg, self.log, None)
                    response.raise_for_status()
                    try:
                        rj = response.json()
                        break
                    except:
                        self.log.exception('problem with response, not json: %s' % (response.text))
                        raise
                finally:
                    if response:
                        response.close
            
            params['from'] = params['from'] + params['size']
            for index in range(len(rj['data']['hits'])):
                themap = rj['data']['hits'][index]
                if 'cases' not in themap:
                    no_case += 1
                    continue
                file_id = themap['file_id']
                for i in range(len(themap['samples'])):
                    case_id = themap['case_id']
                    project_id = themap['project']['project_id']
                    program = project_id.split('-')[0]
                    self.add_to_map(program2case2counts, case_id, program, case_id, file_id)
                    sample_id = themap['samples'][i]['sample_id']
                    self.add_to_map(program2sample2counts, sample_id, program, case_id, sample_id)
            
            curstart += rj['data']['pagination']['count']
            if curstart > rj['data']['pagination']['total']:
                break

        program2case_counts = {}
        program2sample_counts = {}
        for program2field2counts, program2field_counts  in zip([program2case2counts, program2sample2counts], 
                                        [program2case_counts, program2sample_counts]):
            for program, field2counts in program2field2counts.iteritems():
                counts_list = []
                for counts in field2counts.values():
                    counts[1] = len(counts[1])
                    counts[2] = len(counts[2])
                    counts_list += [counts]
                program2field_counts[program] = counts_list
        return program2case_counts, program2sample_counts

    def get_file_counts(self, endpt, filters = '{}'):
        params = {
            'filters': filters, 
            'sort':'file_id:asc', 
            'from':1, 
            'size':200}
        curstart = 1
        no_case = 0
        program2project2counts = {}
        program2data_type2counts = {}
        program2experimental_strategy2counts = {}
        program2access2counts = {}
        while True:
            msg = '\t\tproblem getting filtered map for files'
            response = None
            retries = 4
            while retries:
                retries -= 1
                try:
                    response = request(endpt, params, msg, self.log)
                    response.raise_for_status()
                    try:
                        rj = response.json()
                        break
                    except:
                        self.log.exception('problem with response, not json: %s' % (response.text))
                        raise
                finally:
                    if response:
                        response.close
            
            params['from'] = params['from'] + params['size']
            for index in range(len(rj['data']['hits'])):
                themap = rj['data']['hits'][index]
                if 'cases' not in themap:
                    no_case += 1
                    continue
                file_id = themap['file_id']
                for i in range(len(themap['cases'])):
                    case_id = themap['cases'][i]['case_id']
                    project_id = themap['cases'][i]['project']['project_id']
                    program = project_id.split('-')[0]
                    self.add_to_map(program2project2counts, project_id, program, case_id, file_id)
                    self.add_to_map(program2data_type2counts, themap['data_type'], program, case_id, file_id)
                    if 'experimental_strategy' in themap:
                        self.add_to_map(program2experimental_strategy2counts, themap['experimental_strategy'], program, case_id, file_id)
                    self.add_to_map(program2access2counts, themap['access'], program, case_id, file_id)
            
            curstart += rj['data']['pagination']['count']
            if curstart > rj['data']['pagination']['total']:
                break

        if 0 < no_case:
            self.log.info('found %d records with no cases' % (no_case))
        program2project_counts = {}
        program2data_type_counts = {}
        program2exp_strat_counts = {}
        program2access_counts = {}
        for program2field2counts, program2field_counts  in zip([program2project2counts, program2data_type2counts, program2experimental_strategy2counts, program2access2counts], 
                                        [program2project_counts, program2data_type_counts, program2exp_strat_counts, program2access_counts]):
            for program, field2counts in program2field2counts.iteritems():
                counts_list = []
                for counts in field2counts.values():
                    counts[1] = len(counts[1])
                    counts[2] = len(counts[2])
                    counts_list += [counts]
                program2field_counts[program] = counts_list
        return program2project_counts, program2data_type_counts, program2exp_strat_counts, program2access_counts

#     def testSamplesExists(self):
    def SamplesExists(self):
        filters = {
                    'op': 'in',
                    'content': {
                        'field': 'cases.samples.submitter_id',
                        'value': [
                            'TCGA-PN-A8M9-01A',
                            'TCGA-PN-A8M9-10A',
                            'TCGA-DM-A286-01A',
                            'TCGA-F4-6857-01A',
                            'TCGA-F4-6857-10A',
                            'TCGA-BR-4186-01A',
                            'TCGA-BR-4186-11A',
                            'TCGA-BR-4190-01A',
                            'TCGA-BR-4190-11A',
                            'TCGA-BR-4194-01A',
                            'TCGA-BR-4194-11A',
                            'TCGA-BR-4195-01A',
                            'TCGA-BR-4195-11A',
                            'TCGA-BR-4196-01A',
                            'TCGA-BR-4196-11A',
                            'TCGA-BR-4197-01A',
                            'TCGA-BR-4197-11A',
                            'TCGA-BR-4199-01A',
                            'TCGA-BR-4199-11A',
                            'TCGA-BR-4200-01A',
                            'TCGA-BR-4200-11A',
                            'TCGA-BR-4205-01A',
                            'TCGA-BR-4205-11A',
                            'TCGA-BR-4259-01A',
                            'TCGA-BR-4259-11A',
                            'TCGA-BR-4260-01A',
                            'TCGA-BR-4260-11A',
                            'TCGA-BR-4261-01A',
                            'TCGA-BR-4261-11A',
                            'TCGA-BR-4263-01A',
                            'TCGA-BR-4263-11A',
                            'TCGA-BR-4264-01A',
                            'TCGA-BR-4264-11A',
                            'TCGA-BR-4265-01A',
                            'TCGA-BR-4265-11A',
                            'TCGA-BR-4266-01A',
                            'TCGA-BR-4266-11A',
                            'TCGA-BR-4270-01A',
                            'TCGA-BR-4270-11A',
                            'TCGA-BR-4271-01A',
                            'TCGA-BR-4271-11A',
                            'TCGA-BR-4272-01A',
                            'TCGA-BR-4272-11A',
                            'TCGA-BR-4273-01A',
                            'TCGA-BR-4273-11A',
                            'TCGA-BR-4274-01A',
                            'TCGA-BR-4274-11A',
                            'TCGA-BR-4276-01A',
                            'TCGA-BR-4276-11A',
                            'TCGA-BR-4277-01A',
                            'TCGA-BR-4277-11A',
                            'TCGA-BR-4278-01A',
                            'TCGA-BR-4278-11A',
                            'TCGA-BR-4281-01A',
                            'TCGA-BR-4281-11A',
                            'TCGA-BR-4282-01A',
                            'TCGA-BR-4282-11A',
                            'TCGA-BR-4283-01A',
                            'TCGA-BR-4283-11A',
                            'TCGA-BR-4284-01A',
                            'TCGA-BR-4284-11A',
                            'TCGA-BR-4285-01A',
                            'TCGA-BR-4285-11A',
                            'TCGA-BR-4286-01A',
                            'TCGA-BR-4286-11A',
                            'TCGA-BR-4288-01A',
                            'TCGA-BR-4288-11A',
                            'TCGA-BR-4291-01A',
                            'TCGA-BR-4291-11A',
                            'TCGA-BR-4298-01A',
                            'TCGA-BR-4298-11A',
                            'TCGA-BR-4375-01A',
                            'TCGA-BR-4375-11A',
                            'TCGA-BR-4376-01A',
                            'TCGA-BR-4376-11A',
                            'TCGA-01-0628-11A',
                            'TCGA-01-0630-11A',
                            'TCGA-01-0631-11A',
                            'TCGA-01-0633-11A',
                            'TCGA-01-0636-11A',
                            'TCGA-01-0637-11A',
                            'TCGA-01-0639-11A',
                            'TCGA-01-0642-11A',
                            'TCGA-02-0001-01C',
                            'TCGA-02-0001-10A'
                        ]
                    }
                }
        self.log.info(self.get_file_counts('https://gdc-api.nci.nih.gov/legacy/files?expand=cases,cases.project', json.dumps(filters)))
        
#     def testCloudSQLvsGDC(self):
    def cloudSQLvsGDC(self):
# HG38 compare for target
        query = "select project_short_name, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from TARGET_metadata_data_HG38 group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
#         "files_endpt" = {
#             "current endpt": "https://gdc-api.nci.nih.gov/files",
#             "legacy endpt": "https://gdc-api.nci.nih.gov/legacy/files",
#             "query": "?expand=archive,cases,cases.project.program,cases.samples,cases.samples.portions,cases.samples.portions.analytes,cases.samples.portions.analytes.aliquots,analysis,center,index_files&pretty=true"
#         },
        
        program_counts, data_type_counts, exp_strat_counts, access_counts = self.get_file_counts('https://gdc-api.nci.nih.gov/files?expand=cases,cases.project')

        targetHG38project = [
            ['TARGET-AML', int(923), int(1870)],
            ['TARGET-CCSK', int(13), int(2)],
            ['TARGET-NBL', int(1120), int(2803)],
            ['TARGET-OS', int(384), int(3)],
            ['TARGET-RT', int(75), int(173)],
            ['TARGET-WT', int(663), int(1321)]
        ]
         
        self.compare_metadata2gdcportal('TARGET project cases', rows, program_counts['TARGET'], 1)
        self.compare_metadata2gdcportal('TARGET project files', rows, program_counts['TARGET'], 2)
         
        query = "select data_type, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from TARGET_metadata_data_HG38 group by data_type order by data_type"
        rows = self.run_query(query, False)
 
        targetHG38type = [
            ['Aggregated Somatic Mutation', int(258), int(12)],
            ['Aligned Reads', int(741), int(1586)],
            ['Annotated Somatic Mutation', int(258), int(1071)],
            ['Biospecimen Supplement', int(88), int(3)],
            ['Clinical Supplement', int(1820), int(8)],
            ['Gene Expression Quantification', int(434), int(1443)],
            ['Isoform Expression Quantification', int(436), int(489)],
            ['miRNA Expression Quantification', int(436), int(489)],
            ['Raw Simple Somatic Mutation', int(258), int(1071)],
        ]
         
        self.compare_metadata2gdcportal('TARGET data_type cases', rows, data_type_counts['TARGET'], 1)
        self.compare_metadata2gdcportal('TARGET data_type files', rows, data_type_counts['TARGET'], 2)
         
        query = "select experimental_strategy, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from TARGET_metadata_data_HG38 group by experimental_strategy order by experimental_strategy"
        rows = self.run_query(query, False)
 
        targetHG38strategy = [
            ['miRNA-Seq', int(436), int(1467)],
            ['RNA-Seq', int(457), int(1954)],
            ['WXS', int(281), int(2740)]
        ]
 
        self.compare_metadata2gdcportal('TARGET experimental_strategy cases', rows, exp_strat_counts['TARGET'], 1)
        self.compare_metadata2gdcportal('TARGET experimental_strategy files', rows, exp_strat_counts['TARGET'], 2)
         
        query = "select access, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from TARGET_metadata_data_HG38 group by access order by access"
        rows = self.run_query(query, False)
 
        targetHG38file = [
            ['controlled', int(741), int(3740)],
            ['open', int(1856), int(2432)]
        ]
 
        self.compare_metadata2gdcportal('TARGET access cases', rows, access_counts['TARGET'], 1)
        self.compare_metadata2gdcportal('TARGET access files', rows, access_counts['TARGET'], 2)
         
        query = "select project_short_name, count(distinct case_gdc_id) cases from TARGET_metadata_clinical where endpoint_type = 'current' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        targetHG38case = [
            ['TARGET-AML', int(923)],
            ['TARGET-CCSK', int(13)],
            ['TARGET-NBL', int(1120)],
            ['TARGET-OS', int(384)],
            ['TARGET-RT', int(75)],
            ['TARGET-WT', int(663)]
        ]
 
        self.compare_metadata2gdcportal('TARGET project to cases', rows, targetHG38case, 1)
         
        query = "select project_short_name, count(distinct case_barcode) cases from TARGET_metadata_biospecimen where endpoint_type = 'current' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('TARGET project cases in biospecimen', rows, targetHG38case, 1)
         
        query = "select project_short_name, count(distinct case_barcode) cases from TARGET_metadata_samples where endpoint_type = 'current' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('TARGET project cases in samples', rows, targetHG38case, 1)
         
# HG38 compare for tcga
        query = "select project_short_name, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from TCGA_metadata_data_HG38 group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        tcgaHG38project = [
            ['TCGA-ACC', int(92), int(2108)],
            ['TCGA-BLCA', int(412), int(10193)],
            ['TCGA-BRCA', int(1098), int(27207)],
            ['TCGA-CESC', int(308), int(7350)],
            ['TCGA-CHOL', int(51), int(1157)],
            ['TCGA-COAD', int(463), int(11827)],
            ['TCGA-DLBC', int(58), int(1163)],
            ['TCGA-ESCA', int(185), int(4473)],
            ['TCGA-GBM', int(617), int(9657)],
            ['TCGA-HNSC', int(528), int(12895)],
            ['TCGA-KICH', int(113), int(1853)],
            ['TCGA-KIRC', int(537), int(12272)],
            ['TCGA-KIRP', int(291), int(7368)],
            ['TCGA-LAML', int(200), int(3954)],
            ['TCGA-LGG', int(516), int(12603)],
            ['TCGA-LIHC', int(377), int(9511)],
            ['TCGA-LUAD', int(585), int(14804)],
            ['TCGA-LUSC', int(504), int(13124)],
            ['TCGA-MESO', int(87), int(2050)],
            ['TCGA-OV', int(608), int(13054)],
            ['TCGA-PAAD', int(185), int(4433)],
            ['TCGA-PCPG', int(179), int(4422)],
            ['TCGA-PRAD', int(500), int(12568)],
            ['TCGA-READ', int(172), int(4012)],
            ['TCGA-SARC', int(261), int(6282)],
            ['TCGA-SKCM', int(470), int(11265)],
            ['TCGA-STAD', int(478), int(10835)],
            ['TCGA-TGCT', int(150), int(3636)],
            ['TCGA-THCA', int(507), int(12703)],
            ['TCGA-THYM', int(124), int(2974)],
            ['TCGA-UCEC', int(560), int(13604)],
            ['TCGA-UCS', int(57), int(1364)],
            ['TCGA-UVM', int(80), int(1928)]
        ]
 
        self.compare_metadata2gdcportal('TCGA project cases', rows, tcgaHG38project, 1)
        self.compare_metadata2gdcportal('TCGA project files', rows, tcgaHG38project, 2)
         
        query = "select data_type, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from TCGA_metadata_data_HG38 group by data_type order by data_type"
        rows = self.run_query(query, False)
 
        tcgaHG38type = [
            ['Aggregated Somatic Mutation', int(10429), int(132)],
            ['Aligned Reads', int(10995), int(44402)],
            ['Annotated Somatic Mutation', int(10429), int(44506)],
            ['Biospecimen Supplement', int(11353), int(11353)],
            ['Clinical Supplement', int(11160), int(11160)],
            ['Copy Number Segment', int(10995), int(22376)],
            ['Gene Expression Quantification', int(10237), int(33279)],
            ['Isoform Expression Quantification', int(10165), int(10999)],
            ['Masked Copy Number Segment', int(10995), int(22376)],
            ['Masked Somatic Mutation', int(10429), int(132)],
            ['Methylation Beta Value', int(10979), int(12429)],
            ['miRNA Expression Quantification', int(10165), int(10999)],
            ['Raw Simple Somatic Mutation', int(10429), int(44506)]
        ]
 
        self.compare_metadata2gdcportal('TCGA data_type cases', rows, tcgaHG38type, 1)
        self.compare_metadata2gdcportal('TCGA data_type files', rows, tcgaHG38type, 2)
         
        query = "select experimental_strategy, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from TCGA_metadata_data_HG38 group by experimental_strategy order by experimental_strategy"
        rows = self.run_query(query, False)
 
        tcgaHG38strategy = [
            ['Genotyping Array', int(10995), int(44752)],
            ['Methylation Array', int(10979), int(12429)],
            ['miRNA-Seq', int(10165), int(32997)],
            ['RNA-Seq', int(10239), int(44375)],
            ['WXS', int(10544), int(111583)]
        ]
 
        self.compare_metadata2gdcportal('TCGA experimental_strategy cases', rows, tcgaHG38strategy, 1)
        self.compare_metadata2gdcportal('TCGA experimental_strategy files', rows, tcgaHG38strategy, 2)
         
        query = "select access, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from TCGA_metadata_data_HG38 group by access order by access"
        rows = self.run_query(query, False)
 
        tcgaHG38access = [
            ['controlled', int(10995), int(133546)],
            ['open', int(11353), int(135103)]
        ]
 
        self.compare_metadata2gdcportal('TCGA access cases', rows, tcgaHG38access, 1)
        self.compare_metadata2gdcportal('TCGA access files', rows, tcgaHG38access, 2)
         
        query = "select project_short_name, count(distinct case_gdc_id) cases from TCGA_metadata_clinical where endpoint_type = 'current' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        tcgaHG38case = [
            ['TCGA-ACC', int(92)],
            ['TCGA-BLCA', int(412)],
            ['TCGA-BRCA', int(1098)],
            ['TCGA-CESC', int(308)],
            ['TCGA-CHOL', int(51)],
            ['TCGA-COAD', int(463)],
            ['TCGA-DLBC', int(58)],
            ['TCGA-ESCA', int(185)],
            ['TCGA-GBM', int(617)],
            ['TCGA-HNSC', int(528)],
            ['TCGA-KICH', int(113)],
            ['TCGA-KIRC', int(537)],
            ['TCGA-KIRP', int(291)],
            ['TCGA-LAML', int(200)],
            ['TCGA-LGG', int(516)],
            ['TCGA-LIHC', int(377)],
            ['TCGA-LUAD', int(585)],
            ['TCGA-LUSC', int(504)],
            ['TCGA-MESO', int(87)],
            ['TCGA-OV', int(608)],
            ['TCGA-PAAD', int(185)],
            ['TCGA-PCPG', int(179)],
            ['TCGA-PRAD', int(500)],
            ['TCGA-READ', int(172)],
            ['TCGA-SARC', int(261)],
            ['TCGA-SKCM', int(470)],
            ['TCGA-STAD', int(478)],
            ['TCGA-TGCT', int(150)],
            ['TCGA-THCA', int(507)],
            ['TCGA-THYM', int(124)],
            ['TCGA-UCEC', int(560)],
            ['TCGA-UCS', int(57)],
            ['TCGA-UVM', int(80)]
        ]
 
        self.compare_metadata2gdcportal('TCGA project cases', rows, tcgaHG38case, 1)
         
        query = "select project_short_name, count(distinct case_barcode) cases from TCGA_metadata_biospecimen where endpoint_type = 'current' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('TCGA project cases in biospecimen', rows, tcgaHG38case, 1)
         
        query = "select project_short_name, count(distinct case_barcode) cases from TCGA_metadata_samples where endpoint_type = 'current' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('TCGA project cases in samples', rows, tcgaHG38case, 1)
         
# HG19 compare across all programs
        projects = [
            "TCGA-PCPG",
            "TCGA-SARC",
            "TCGA-UVM",
            "TCGA-LAML",
            "TCGA-SKCM",
            "TCGA-KIRP",
            "TCGA-UCS",
            "TCGA-PAAD",
            "TCGA-LUSC",
            "TCGA-READ",
            "TCGA-COAD",
            "TCGA-MESO",
            "TCGA-GBM",
            "TCGA-LGG",
            "TCGA-TGCT",
            "TCGA-UCEC",
            "TCGA-BLCA",
            "TCGA-HNSC",
            "TCGA-ACC",
            "TCGA-LIHC",
            "TCGA-THCA",
            "TCGA-BRCA",
            "TCGA-OV",
            "TCGA-ESCA",
            "TCGA-CESC",
            "TCGA-PRAD",
            "TCGA-STAD",
            "TCGA-THYM",
            "TCGA-DLBC",
            "TCGA-LUAD",
            "TCGA-KICH",
            "TCGA-KIRC",
            "TCGA-CHOL",
            "TARGET-ALL-P1",
            "TARGET-ALL-P2",
            "TARGET-AML",
            "TARGET-CCSK",
            "TARGET-NBL",
            "TARGET-OS",
            "TARGET-RT",
            "TARGET-WT",
            "CCLE-BLCA",
            "CCLE-BRCA",
            "CCLE-CESC",
            "CCLE-COAD",
            "CCLE-DLBC",
            "CCLE-ESCA",
            "CCLE-HNSC",
            "CCLE-KIRC",
            "CCLE-LCLL",
            "CCLE-LGG",
            "CCLE-LIHC",
            "CCLE-LUSC",
            "CCLE-MESO",
            "CCLE-MM",
            "CCLE-OV",
            "CCLE-PAAD",
            "CCLE-PRAD",
            "CCLE-SARC",
            "CCLE-SKCM",
            "CCLE-STAD",
            "CCLE-THCA",
            "CCLE-UCEC"
        ]
 
#         counts, sampletype2counts = self.countCaseOrNot('legacy', 'file', ['data_type'], ['Diagnostic image'], ['='])
#         self.log.info('%s had no case for %d files and had case for %d files' % ('Diagnostic image', counts[0], counts[1]))
#         sampletypes = sorted(sampletype2counts.keys())
#         self.log.info('sample types:\n\t%s\n' % ('\n\t'.join('%s: %s' % (sampletype, sampletype2counts[sampletype]) for sampletype in sampletypes)))
             
         
        query = "select data_type, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from %s_metadata_data_HG19 group by data_type order by data_type"
        rows = self.getHG19combined_totals(query)
         
        gdc_type2file = [
            ['ABI sequence trace', int(360)],
            ['Aligned reads', int(87700)],
            ['Auxiliary test', int(2920)],
            ['Biospecimen data', int(270)],
            ['Biospecimen Supplement', int(11295)],
            ['Bisulfite sequence alignment', int(47)],
            ['CGH array QC', int(11355)],
            ['Clinical data', int(1276)],
            ['Clinical Supplement', int(11108)],
            ['Copy number estimate', int(68246)],
            ['Copy number QC metrics', int(2346)],
            ['Copy number segmentation', int(96085)],
            ['Copy number variation', int(570)],
            ['Coverage WIG', int(28237)],
            ['Diagnostic image', int(11894)],
            ['Exon junction quantification', int(16069)],
            ['Exon quantification', int(16129)],
            ['Gene expression quantification', int(31255)],
            ['Genotypes', int(23422)],
            ['Intensities', int(8389)],
            ['Intensities Log2Ratio', int(2258)],
            ['Isoform expression quantification', int(22586)],
            ['LOH', int(22)],
            ['Methylation array QC metrics', int(1503)],
            ['Methylation beta value', int(13396)],
            ['Methylation percentage', int(47)],
            ['Microsatellite instability', int(6898)],
            ['miRNA gene quantification', int(24340)],
            ['miRNA isoform quantification', int(23160)],
            ['Normalized copy numbers', int(23092)],
            ['Normalized intensities', int(20635)],
            ['Pathology report', int(11124)],
            ['Protein expression quantification', int(8389)],
            ['Raw intensities', int(86547)],
            ['Sequencing tag', int(32)],
            ['Sequencing tag counts', int(32)],
            ['Simple nucleotide variation', int(62926)],
            ['Simple somatic mutation', int(199)],
            ['Structural variation', int(3045)],
            ['TCGA DCC Archive', int(6603)],
            ['Tissue slide image', int(18485)],
            ['Unaligned reads', int(12360)],
        ]
        self.compare_metadata2gdcportal('HG19 datatype files', rows, gdc_type2file, 1)
         
        api_total2file = []
        for data_type in gdc_type2file:
            total = int(self.getAPITotal('legacy', 'file', ['data_type'], [data_type[0]], ['=']))
            api_total2file += [[data_type[0], total]]
        self.compare_metadata2gdcportal('HG19 datatype files against total', rows, api_total2file, 1)
         
        api_total_projects2file = []
        fields = ['cases.project.project_id'] + ['data_type']
        for data_type in gdc_type2file:
            total = int(self.getAPITotal('legacy', 'file', fields, [projects] + [data_type[0]], ['in', '=']))
            api_total_projects2file += [[data_type[0], total]]
        self.compare_metadata2gdcportal('HG19 datatype files by project against total', rows, api_total_projects2file, 1)
         
        api_total_projects2file = []
        fields = ['cases', 'data_type']
        for data_type in gdc_type2file:
            total = int(self.getAPITotal('legacy', 'file', fields, [projects] + [data_type[0]], ['is', '=']))
            api_total_projects2file += [[data_type[0], total]]
        self.compare_metadata2gdcportal('HG19 datatype files by no project against total', rows, api_total_projects2file, 1)
         
        fields = ['cases', 'data_type']
        values = [projects] + ['Diagnostic image']
        total = int(self.getAPITotal('legacy', 'file', fields, ["TCGA-BRCA", 'Diagnostic image'], ['is', '='], True))
        self.log.info('\nfor %s found %d files with no project info\n' % ('Diagnostic image', total))
         
        total = int(self.getAPITotal('legacy', 'file', ['cases'], [""], ['is'], True))
        self.log.info('\nfor all projects found %d files with no project info using is\n' % (total))
         
        total = int(self.getAPITotal('legacy', 'file', ['cases'], [""], ['not'], True))
        self.log.info('\nfor all projects found %d files with project info using not\n' % (total))
         
        total = int(self.getAPITotal('legacy', 'file', ['cases.project.project_id'], [projects], ['in'], True))
        self.log.info('\nfor all projects found %d files with project info\n' % (total))
 
        query = "select experimental_strategy, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from %s_metadata_data_HG19 group by experimental_strategy order by experimental_strategy"
        rows = self.getHG19combined_totals(query)
 
        gdc_strategy2file = [
            ['AMPLICON', int(1478)],
            ['Bisulfite-Seq', int(229)],
            ['Capillary sequencing', int(366)],
            ['CGH array', int(25409)],
            ['DNA-Seq', int(87590)],
            ['Exon array', int(1338)],
            ['Gene expression array', int(11494)],
            ['Genotyping array', int(234919)],
            ['Methylation array', int(54015)],
            ['miRNA expression array', int(3543)],
            ['miRNA-Seq', int(66002)],
            ['Mixed strategies', int(2920)],
            ['MSI-Mono-Dinucleotide Assay', int(6898)],
            ['Protein expression array', int(38786)],
            ['RNA-Seq', int(116719)],
            ['Total RNA-Seq', int(678)],
            ['VALIDATION', int(12890)],
            ['WGS', int(8741)],
            ['WXS', int(30402)]
        ]
         
        self.compare_metadata2gdcportal('HG19 experimental_strategy files', rows, gdc_strategy2file, 1)
         
        query = "select access, count(distinct case_gdc_id) cases, count( distinct file_gdc_id) files from %s_metadata_data_HG19 group by access order by access"
        rows = self.getHG19combined_totals(query)
 
        gdc_access2file = [
            ['controlled', int(333156)],
            ['open', int(469243)]
        ]
         
        self.compare_metadata2gdcportal('HG19 access files', rows, gdc_access2file, 1)
         
        query = "select project_short_name, count(distinct case_gdc_id) cases from CCLE_metadata_clinical where endpoint_type = 'legacy' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        gdc_ccleHG19case = [
            ['CCLE-BLCA', int(26)],
            ['CCLE-BRCA', int(63)],
            ['CCLE-CESC', int(24)],
            ['CCLE-COAD', int(59)],
            ['CCLE-DLBC', int(60)],
            ['CCLE-ESCA', int(26)],
            ['CCLE-HNSC', int(34)],
            ['CCLE-KIRC', int(25)],
            ['CCLE-LCLL', int(81)],
            ['CCLE-LIHC', int(33)],
            ['CCLE-LGG', int(65)],
            ['CCLE-LUSC', int(186)],
            ['CCLE-MESO', int(1)],
            ['CCLE-MM', int(26)],
            ['CCLE-OV', int(46)],
            ['CCLE-PAAD', int(41)],
            ['CCLE-PRAD', int(7)],
            ['CCLE-SARC', int(40)],
            ['CCLE-SKCM', int(52)],
            ['CCLE-STAD', int(39)],
            ['CCLE-THCA', int(12)],
            ['CCLE-UCEC', int(4)],
        ]
         
        self.compare_metadata2gdcportal('HG19 CCLE cases', rows, gdc_ccleHG19case, 1)
         
        query = "select project_short_name, count(distinct case_barcode) cases from CCLE_metadata_biospecimen where endpoint_type = 'legacy' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('HG19 CCLE cases in biospecimen', rows, gdc_ccleHG19case, 1)
         
        query = "select project_short_name, count(distinct case_barcode) cases from CCLE_metadata_samples where endpoint_type = 'legacy' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('HG19 CCLE cases in samples', rows, gdc_ccleHG19case, 1)
         
        query = "select project_short_name, count(distinct case_gdc_id) cases from TARGET_metadata_clinical where endpoint_type = 'legacy' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        gdc_targetHG19case = [
            ['TARGET-ALL-P1', int(214)],
            ['TARGET-ALL-P2', int(1543)],
            ['TARGET-AML', int(923)],
            ['TARGET-CCSK', int(13)],
            ['TARGET-NBL', int(1120)],
            ['TARGET-OS', int(384)],
            ['TARGET-RT', int(75)],
            ['TARGET-WT', int(663)]
        ]
         
        self.compare_metadata2gdcportal('HG19 TARGET cases', rows, gdc_targetHG19case, 1)
         
        query = "select project_short_name, count(distinct case_barcode) cases from TARGET_metadata_biospecimen where endpoint_type = 'legacy' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('HG19 TARGET cases in biospecimen', rows, gdc_targetHG19case, 1)
         
        query = "select project_short_name, count(distinct case_barcode) cases from TARGET_metadata_biospecimen where endpoint_type = 'legacy' and case_barcode in (select case_barcode from TARGET_metadata_samples) group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('HG19 TARGET cases in samples', rows, gdc_targetHG19case, 1)
         
        query = "select project_short_name, count(distinct case_gdc_id) cases from TCGA_metadata_clinical where endpoint_type = 'legacy' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        gdc_tcgaHG19case = [
            ['TCGA-ACC', int(92)],
            ['TCGA-BLCA', int(412)],
            ['TCGA-BRCA', int(1096)],
            ['TCGA-CESC', int(308)],
            ['TCGA-CHOL', int(51)],
            ['TCGA-COAD', int(463)],
            ['TCGA-DLBC', int(58)],
            ['TCGA-ESCA', int(185)],
            ['TCGA-GBM', int(617)],
            ['TCGA-HNSC', int(528)],
            ['TCGA-KICH', int(113)],
            ['TCGA-KIRC', int(535)],
            ['TCGA-KIRP', int(291)],
            ['TCGA-LAML', int(143)],
            ['TCGA-LIHC', int(377)],
            ['TCGA-LGG', int(516)],
            ['TCGA-LUAD', int(585)],
            ['TCGA-LUSC', int(504)],
            ['TCGA-MESO', int(87)],
            ['TCGA-OV', int(608)],
            ['TCGA-PAAD', int(185)],
            ['TCGA-PCPG', int(179)],
            ['TCGA-PRAD', int(500)],
            ['TCGA-READ', int(172)],
            ['TCGA-SARC', int(261)],
            ['TCGA-SKCM', int(470)],
            ['TCGA-STAD', int(478)],
            ['TCGA-TGCT', int(150)],
            ['TCGA-THCA', int(507)],
            ['TCGA-THYM', int(124)],
            ['TCGA-UCEC', int(560)],
            ['TCGA-UCS', int(57)],
            ['TCGA-UVM', int(80)]
        ]
         
        self.compare_metadata2gdcportal('HG19 TCGA cases', rows, gdc_tcgaHG19case, 1)
 
        query = "select project_short_name, count(distinct case_barcode) cases from TCGA_metadata_biospecimen where endpoint_type = 'legacy' group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('HG19 TCGA cases in biospecimen', rows, gdc_tcgaHG19case, 1)
         
        query = "select project_short_name, count(distinct case_barcode) cases from TCGA_metadata_biospecimen where endpoint_type = 'legacy' and case_barcode in (select case_barcode from TCGA_metadata_samples) group by project_short_name order by project_short_name"
        rows = self.run_query(query, False)
 
        self.compare_metadata2gdcportal('HG19 TCGA cases in samples', rows, gdc_tcgaHG19case, 1)

#         query = "select dcc.participantbarcode, dcc.samplebarcode, gdc.sample_barcode, 1 = instr(samplebarcode, sample_barcode) begins from test.metadata_biospecimen dcc left join 2017_02_17_gdc_dev.CCLE_metadata_biospecimen gdc on dcc.participantbarcode = gdc.case_barcode where dcc.project = 'CCLE' order by 3"
#         rows = self.run_query(query, True)
#         
#         query = "select dcc.participantbarcode, dcc.samplebarcode, gdc.sample_barcode from test.metadata_biospecimen dcc left join 2017_02_17_gdc_dev.CCLE_metadata_biospecimen gdc on dcc.participantbarcode = gdc.case_barcode where dcc.project = 'CCLE' and gdc.sample_barcode is null"
#         rows = self.run_query(query, True)
#         

    def testRandom(self):
        query = 'desc TCGA_metadata_data_type_availability'
        self.run_query(query)
        
        query = 'select count(*) ct from TCGA_metadata_biospecimen'
        self.run_query(query)
        
        query = "select * from TCGA_metadata_data_type_availability order by 1"
        self.run_query(query)
        
        query = 'select metadata_data_type_availability_id from TCGA_metadata_data_type_availability where isb_label = "Somatic_Mutation" and genomic_build = "HG38"'
        self.run_query(query)
        
        query = "select isb_label, s.metadata_data_type_availability_id id, count(distinct sample_barcode) barcodes, sum(count) count, count(*) ct from TCGA_metadata_sample_data_availability s join TCGA_metadata_data_type_availability d on s.metadata_data_type_availability_id = d.metadata_data_type_availability_id where genomic_build = 'HG38' group by 1;"
        self.run_query(query)
