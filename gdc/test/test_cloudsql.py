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


    def run_query(self, query):
        rows = ISBCGC_database_helper.select(self.config, query, [])
        self.log.info('\nrows for query\n\t%s\n' % (query))
        try:
            for row in rows:
                self.log.info('\t%s' % ('\t'.join(str(item) for item in row)))
        except:
            self.log.exception()

    def testCloudSQL(self):
        query = "desc CCLE_metadata_project"
        self.run_query(query)

        query = "select count(*) 'CCLE' from CCLE_metadata_project union select count(*) 'TARGET' from TARGET_metadata_project union select count(*) 'TCGA' from TCGA_metadata_project"
        self.run_query(query)

        query = "select count(*) 'CCLE' from CCLE_metadata_clinical union select count(*) 'TARGET' from TARGET_metadata_clinical union select count(*) 'TCGA' from TCGA_metadata_clinical"
        self.run_query(query)

        query = "select count(*) 'CCLE' from CCLE_metadata_biospecimen union select count(*) 'TARGET' from TARGET_metadata_biospecimen union select count(*) 'TCGA' from TCGA_metadata_biospecimen"
        self.run_query(query)

        query = "select sample_type, count(*) ct from CCLE_metadata_biospecimen group by sample_type"
        self.run_query(query)

        query = "select * from CCLE_metadata_project order by project_short_name"
        self.run_query(query)

        query = "select * from TARGET_metadata_project order by project_short_name"
        self.run_query(query)

        query = "select * from TCGA_metadata_project order by project_short_name"
        self.run_query(query)

        query = "select * from CCLE_metadata_clinical order by case_barcode limit 20"
        self.run_query(query)

        query = "select * from TARGET_metadata_clinical order by case_barcode limit 20"
        self.run_query(query)

        query = "select * from TCGA_metadata_clinical order by case_barcode limit 20"
        self.run_query(query)

        query = "select * from CCLE_metadata_biospecimen order by case_barcode limit 20"
        self.run_query(query)

        query = "select * from TARGET_metadata_biospecimen order by case_barcode limit 20"
        self.run_query(query)

        query = "select * from TCGA_metadata_biospecimen order by case_barcode limit 20"
        self.run_query(query)

