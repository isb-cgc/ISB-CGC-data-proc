'''
Created on Dec 28, 2016

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

