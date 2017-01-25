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
from datetime import date
import json
import logging
import unittest

from util import create_log

class GDCTestSetup(unittest.TestCase):
    def setUp(self):
        with open('./gdc/config/uploadGDC_test.json') as configFile:
            self.config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_gdc_upload_run/'
        log_name = create_log(log_dir, 'gdc_upload')
        self.log = logging.getLogger(log_name)

    def tearDown(self):
        pass

