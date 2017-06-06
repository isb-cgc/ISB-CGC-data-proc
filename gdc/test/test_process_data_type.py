'''
Created on Dec 28, 2016

copyright 2017, Institute for Systems Biology.

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
from bq_wrapper import fetch_paged_results, query_bq_table
from gdc.test.test_setup import GDCTestSetup
from gdc.util.process_data_type import process_data_type, populate_sample_availibility

from isbcgc_cloudsql_model import ISBCGC_database_helper

class GDCProcessDataTypeTest(GDCTestSetup):
    def setUp(self):
        self.log_tag = '_gdc_test_populate_sample_availibility/'
        super(GDCProcessDataTypeTest, self).setUp()

#     def test_process_data_type(self):
#         process_data_type(self.config, 'current', 'TCGA', 'TCGA-UCS', 'Aligned Reads', 'test_aligned_reads')

    def test_populate_sample_availibility(self):
        bqTable2data_type = {
            'Somatic_Mutation': 'Masked Somatic Mutation'
        }
        for bqTable, data_type in bqTable2data_type.iteritems():
            self.log.info('populate_sample_availibility() for %s' % (data_type))
            #remove existing records
            stmt = 'delete from TCGA_metadata_sample_data_availability where metadata_data_type_availability_id = ' \
            '(select metadata_data_type_availability_id from TCGA_metadata_data_type_availability where isb_label = "Somatic_Mutation" and genomic_build = "HG38")'
            ISBCGC_database_helper.update(self.config, stmt, self.log, [[]])
            
            query_results = query_bq_table('select Tumor_Sample_Barcode, Matched_Norm_Sample_Barcode, fileUUID from [isb-cgc:TCGA_hg38_data_v0.{}] group by 1,2,3'.format(bqTable), True, None, self.log)
            page_token = None
            barcode2seen_files = {}
            barcode2infos = {}
            infos = []
            while True:
                # loop through the big query results and get the sample_barcode into the info list as populate_sample_availibility()
                # expects it
                total_rows, rows, page_token = fetch_paged_results(query_results, 200000, None, page_token, self.log)
                for row in rows:
                    tumor = row[0][:16]
#                     normal = row[1][:16]
                    files = row[2].split('|')
                    for curfile in files:
                        if tumor in barcode2seen_files:
                            seen_files = barcode2seen_files[tumor]
                            if row[2] in seen_files:
                                continue
                            seen_files.add(curfile)
                        else:
                            barcode2seen_files[tumor] = set([curfile])
                        samples_tumor = {'submitter_id': tumor}
                        sample_list = [samples_tumor]
                        
                        info = {'access': 'open'}
                        case_list = info.setdefault('cases', [])
                        case_list += [{'samples': sample_list}]
                        barcode2infos[tumor] = barcode2infos.setdefault(tumor, []) + [info]
#         
#                     samples_normal = {'submitter_id': normal}
#                     sample_list = [samples_normal]
#                     
#                     info = {'access': 'open'}
#                     case_list = info.setdefault('cases', [])
#                     case_list += [{'samples': sample_list}]
#                     barcode2infos[normal] = barcode2infos.setdefault(normal, []) + [info]
                infos += [info for curinfos in barcode2infos.itervalues() for info in curinfos]
        
                # create inserts into the metadata data that for big query rows that didn't have a match already in the metadata data table
                if not page_token:
                    self.log.info('\tprocessed total of %s rows for %s' % (total_rows, bqTable))
                    break
            populate_sample_availibility(self.config, 'current', 'TCGA', 'all', data_type, infos, self.log)
            self.log.info('finished populate_sample_availibility() for %s' % (data_type))
