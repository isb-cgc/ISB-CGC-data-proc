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
from gdc.test.test_setup import GDCTestSetup
from isbcgc_cloudsql_model import ISBCGC_database_helper

class GDCCloudSQLTest(GDCTestSetup):
    def process_header(self,query):
        if ('*' in query and 'count' not in query) or 'desc' in query:
            return ''
        
        index = query.find('from')
        return '\n' + '\t'.join(column.strip().split(' ')[-1] for column in query[6:index].split(','))
    
    def run_query(self, query):
        rows = ISBCGC_database_helper.select(self.config, query, self.log, [])
        results = ''
        try:
            for row in rows:
                results += '%s\n' % ('\t'.join(str(item) for item in row))
            self.log.info('rows for query:%s\n%s\n' % (self.process_header(query), results))
        except:
            self.log.exception()

    def testCloudSQL(self):
#         query = "desc CCLE_metadata_project"
#         self.run_query(query)
#  
#         query = "select count(*) 'CCLE' from CCLE_metadata_project union select count(*) 'TARGET' from TARGET_metadata_project union select count(*) 'TCGA' from TCGA_metadata_project"
#         self.run_query(query)
#  
#         query = "select count(*) 'CCLE' from CCLE_metadata_clinical union select count(*) 'TARGET' from TARGET_metadata_clinical union select count(*) 'TCGA' from TCGA_metadata_clinical"
#         self.run_query(query)
#  
#         query = "select count(*) 'CCLE' from CCLE_metadata_biospecimen union select count(*) 'TARGET' from TARGET_metadata_biospecimen union select count(*) 'TCGA' from TCGA_metadata_biospecimen"
#         self.run_query(query)
#  
#         query = "select sample_type, count(*) ct from CCLE_metadata_biospecimen group by sample_type"
#         self.run_query(query)
#  
#         query = "select * from CCLE_metadata_project order by project_short_name"
#         self.run_query(query)
#  
#         query = "select * from TARGET_metadata_project order by project_short_name"
#         self.run_query(query)
#  
#         query = "select * from TCGA_metadata_project order by project_short_name"
#         self.run_query(query)
#  
#         query = "select * from CCLE_metadata_clinical order by case_barcode limit 50"
#         self.run_query(query)
#  
#         query = "desc TARGET_metadata_clinical"
#         self.run_query(query)
#  
#         query = "select * from TARGET_metadata_clinical order by case_barcode limit 50"
#         self.run_query(query)
#  
#         query = "select case_barcode from TARGET_metadata_clinical group by case_barcode order by case_barcode limit 20"
#         self.run_query(query)
#  
#         query = "desc TCGA_metadata_clinical"
#         self.run_query(query)
#  
#         query = "select * from TCGA_metadata_clinical order by case_barcode limit 50"
#         self.run_query(query)
#  
#         query = "select * from CCLE_metadata_biospecimen order by case_barcode limit 50"
#         self.run_query(query)
#  
#         query = "select * from TARGET_metadata_biospecimen order by case_barcode limit 50"
#         self.run_query(query)
#  
#         query = "desc TCGA_metadata_biospecimen"
#         self.run_query(query)
#  
#         query = "select * from TCGA_metadata_biospecimen order by case_barcode limit 50"
#         self.run_query(query)
#  
#         query = "select * from CCLE_metadata_samples order by case_barcode limit 50"
#         self.run_query(query)
#  
#         query = "desc TARGET_metadata_samples"
#         self.run_query(query)
#  
#         query = "select * from TARGET_metadata_samples order by case_barcode limit 50"
#         self.run_query(query)
#  
#         query = "select * from TCGA_metadata_samples order by case_barcode limit 50"
#         self.run_query(query)
# 
#         query = "select * from CCLE_metadata_attrs"
#         self.run_query(query)
#  
#         query = "select * from TARGET_metadata_attrs"
#         self.run_query(query)
#  
#         query = "select * from TCGA_metadata_attrs"
#         self.run_query(query)
# 
#         query = "select program_name, count(distinct sample_barcode) from CCLE_metadata_samples group by program_name union select program_name, count(distinct sample_barcode) from TARGET_metadata_samples group by program_name union select program_name, count(distinct sample_barcode) from TCGA_metadata_samples group by 1"
#         self.run_query(query)
# 
#         query = "select program_name, endpoint_type, count(distinct sample_barcode) from CCLE_metadata_samples group by 1, 2 union select program_name, endpoint_type, count(distinct sample_barcode) from TARGET_metadata_samples group by 1, 2 union select program_name, endpoint_type, count(distinct sample_barcode) from TCGA_metadata_samples group by 1, 2"
#         self.run_query(query)
# 
#         query = "select program_name, count(distinct case_barcode) from CCLE_metadata_samples group by program_name union select program_name, count(distinct case_barcode) from TARGET_metadata_samples group by program_name union select program_name, count(distinct case_barcode) from TCGA_metadata_samples group by 1"
#         self.run_query(query)
# 
#         query = "select program_name, endpoint_type, count(distinct case_barcode) from CCLE_metadata_samples group by 1, 2 union select program_name, endpoint_type, count(distinct case_barcode) from TARGET_metadata_samples group by 1, 2 union select program_name, endpoint_type, count(distinct case_barcode) from TCGA_metadata_samples group by 1, 2"
#         self.run_query(query)
# 
#         query = "select count(distinct a.case_barcode), count(distinct a.sample_barcode) from TCGA_metadata_samples a left join TCGA_metadata_samples b on a.sample_barcode = b.sample_barcode where a.endpoint_type = 'current' and b.endpoint_type = 'legacy' and b.sample_barcode is null"
#         self.run_query(query)
# 
#         query = "select count(distinct a.case_barcode), count(distinct a.sample_barcode) from TCGA_metadata_samples a where a.endpoint_type = 'current' and a.sample_barcode not in (select b.sample_barcode from TCGA_metadata_samples b where b.endpoint_type = 'legacy')"
#         self.run_query(query)
# 
#         query = "select a.project_short_name, count(distinct a.case_barcode), count(distinct a.sample_barcode) from TCGA_metadata_samples a where a.endpoint_type = 'current' and a.sample_barcode not in (select b.sample_barcode from TCGA_metadata_samples b where b.endpoint_type = 'legacy') group by a.project_short_name order by a.project_short_name"
#         self.run_query(query)
  
        query = \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(*) total, 'CCLE19' from CCLE_metadata_data_HG19 group by endpoint_type union " \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(*) total, 'TARGET19' from TARGET_metadata_data_HG19 group by endpoint_type union " \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(*) total, 'TARGET38' from TARGET_metadata_data_HG38 group by endpoint_type union " \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(*) total, 'TCGA19' from TCGA_metadata_data_HG19 group by endpoint_type union " \
            "select endpoint_type, count(distinct case_barcode) cases, count(distinct sample_barcode) samples, count(distinct aliquot_barcode) aliquots, count(*) total, 'TCGA38' from TCGA_metadata_data_HG38 group by endpoint_type " \
            "order by 3, 1"
        self.run_query(query)
 
        query = "select data_type, experimental_strategy, platform, analysis_workflow_type, count(*) ct from TCGA_metadata_data_HG38 " \
            "group by data_type, experimental_strategy, platform, analysis_workflow_type " \
            "order by data_type, experimental_strategy, platform, analysis_workflow_type"
        self.run_query(query)
 
        query = "select experimental_strategy, platform, analysis_workflow_type, count(*) ct from TCGA_metadata_data_HG38 " \
            "group by experimental_strategy, platform, analysis_workflow_type " \
            "order by experimental_strategy, platform, analysis_workflow_type"
        self.run_query(query)
 
        query = "select data_type, platform, analysis_workflow_type, count(*) ct from TCGA_metadata_data_HG38 " \
            "group by data_type, platform, analysis_workflow_type " \
            "order by data_type, platform, analysis_workflow_type"
        self.run_query(query)
 
        query = "select data_type, experimental_strategy, analysis_workflow_type, count(*) ct from TCGA_metadata_data_HG38 " \
            "group by data_type, experimental_strategy, analysis_workflow_type " \
            "order by data_type, experimental_strategy, analysis_workflow_type"
        self.run_query(query)
 
        query = "select data_type, experimental_strategy, platform, count(*) ct from TCGA_metadata_data_HG38 " \
            "group by data_type, experimental_strategy, platform " \
            "order by data_type, experimental_strategy, platform"
        self.run_query(query)
 
        query = "select data_type, experimental_strategy, platform, center_name, count(*) ct from TCGA_metadata_data_HG19 " \
            "group by data_type, experimental_strategy, platform, center_name " \
            "order by data_type, experimental_strategy, platform, center_name"
        self.run_query(query)
 
        query = "select experimental_strategy, platform, center_name, count(*) ct from TCGA_metadata_data_HG19 " \
            "group by experimental_strategy, platform, center_name " \
            "order by experimental_strategy, platform, center_name"
        self.run_query(query)
 
        query = "select data_type, platform, center_name, count(*) ct from TCGA_metadata_data_HG19 " \
            "group by data_type, platform, center_name " \
            "order by data_type, platform, center_name"
        self.run_query(query)
 
        query = "select data_type, experimental_strategy, center_name, count(*) ct from TCGA_metadata_data_HG19 " \
            "group by data_type, experimental_strategy, center_name " \
            "order by data_type, experimental_strategy, center_name"
        self.run_query(query)
 
        query = "select data_type, experimental_strategy, platform, count(*) ct from TCGA_metadata_data_HG19 " \
            "group by data_type, experimental_strategy, platform " \
            "order by data_type, experimental_strategy, platform"
        self.run_query(query)
 
