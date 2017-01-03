'''
Created on Dec 28, 2016

@author: michael
'''
from gdc.test.test_setup import GDCTestSetup
from gdc.util.process_data_type import process_data_type

class GDCProcessDataTypeTest(GDCTestSetup):
    def setUp(self):
        super(GDCProcessDataTypeTest, self).setUp()
        self.config.update(
            {
                "upload_files": True,
                "upload_etl_files": True
            }
        )

    def test_process_data_type(self):
        process_data_type(self.config, 'current', 'TCGA-LAML', 'Methylation Beta Value', '.', log_name = None)
