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
from gdc.case_postprocessing import ccle_case_postprocessing
from gdc.case_postprocessing import target_case_postprocessing
from gdc.test.test_setup import GDCTestSetup

class GDCETLTest(GDCTestSetup):
    def setUp(self):
        super(GDCETLTest, self).setUp()
        
    def test_target_postprocess(self):
        target_case_postprocessing.postprocess(self.config, 'TARGET-OS', self.log)
        
    def test_ccle_postprocess(self):
        ccle_case_postprocessing.postprocess(self.config, 'CCLE-BLCA', self.log)
        

