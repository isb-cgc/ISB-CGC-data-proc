'''
Created on Dec 28, 2016

@author: michael
'''

from gdc.etl.methylation import Methylation
from gdc.test.test_setup import GDCTestSetup

class GDCETLTest(GDCTestSetup):
    def setUp(self):
        super(GDCETLTest, self).setUp()
        self.config['process_files']['datatype2bqscript']['Methylation Beta Value']['bq_table'] = 'TCGA_Methylation'
        
    def test_etl_load(self):
        Methylation().finish_etl(self.config, 'TCGA-LAML', 'Methylation Beta Value', 28, self.log)

