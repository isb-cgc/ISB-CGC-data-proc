'''
Created on Dec 28, 2016

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
