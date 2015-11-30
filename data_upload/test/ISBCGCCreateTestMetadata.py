'''
Created on Nov 30, 2015

Copyright 2015, Institute for Systems Biology.

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
from datetime import date, datetime
import json
import logging
import sys

from isbcgc_cloudsql_model import ISBCGC_database_helper
from util import create_log

def main(configfilename):
    print datetime.now(), 'begin create test metadata'
    with open(configfilename) as configFile:
        config = json.load(configFile)
    
    log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '_test_metadata' + '/'
    log_name = create_log(log_dir, 'top_processing')
    log = logging.getLogger(log_name)
    log.info('begin create test metadata')
    try:
        with open(config['test_barcode_file']) as barcodefile:
            test_barcodes = barcodefile.readlines()
#         ISBCGC_database_helper.initialize(config, log)
#         inswert_stmt = "INSERT newtable SELECT * FROM oldtable where barcode in (...)"
        # first simply get the controlled access paths for the test barcodes and save to a file
        select_stmt = "select DatafileName, DatafileNameKey from metadata_data where barcode = %s and datafilenamekey <> '' and securityprotocol = 'dbGap controlled-access'"
        with open(config['outputfile']) as outputfile:
            for test_barcode in test_barcodes:
                fileinfo = ISBCGC_database_helper.select(config, select_stmt, log, [test_barcode], False)
                outputfile.write('\t'.join(fileinfo) + '\n')
    except Exception as e:
        log.exception('problem creating test metadata')
        raise e
    log.info('finish create test metadata')
    print datetime.now(), 'create test metadata'

if __name__ == '__main__':
    main(sys.argv[1])