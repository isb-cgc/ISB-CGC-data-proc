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
import gcs_wrapper
import json
import logging
import sys

from isbcgc_cloudsql_model import ISBCGC_database_helper
from util import create_log

def generateProtectedFileInfo(config, test_barcodes, log):
    ''' currently not used '''
    # simply get the controlled access paths for the test barcodes and save to a file
    select_stmt = "select ParticipantBarcode, DatafileName, DatafileNameKey from metadata_data where ParticipantBarcode = %s and DatafileNameKey <> '' and securityprotocol = 'dbGap controlled-access' " + \
        "group by ParticipantBarcode, DatafileName, DatafileNameKey"
    with open(config['outputfile'], 'w') as outputfile:
        for test_barcode in test_barcodes:
            test_barcode = test_barcode.strip()
            fileinfos = ISBCGC_database_helper.select(config, select_stmt, log, [test_barcode], False)
            for fileinfo in fileinfos:
                outputfile.write('\t'.join(fileinfo) + '\n')

    try:
        gcs_wrapper.open_connection()
        gcs_wrapper.upload_file(config['outputfile'], config['buckets']['open'], config['buckets']['key_prefix']['protected_filenamekey'] + '/' + config['outputfile'], log)
    except Exception as e:
        log.exception('problem saving %s to GCS' % (config['outputfile']))
        raise e
    finally:
        gcs_wrapper.close_connection()

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
        ISBCGC_database_helper.initialize(config, log)
        log.info('#barcodes: %s' % (len(test_barcodes) - 1))
        for metadata_table in ISBCGC_database_helper.metadata_tables:
            table_create_stmt = 'create table {0} like {1}.{0}'.format(metadata_table, config['cloudsql']['source_db'])
            log.info('table create statement: %s' % (table_create_stmt))
            ISBCGC_database_helper.update(config, table_create_stmt, log, [[]])
            
            insert_stmt = "INSERT {0} SELECT * FROM {1}.{0} where ParticipantBarcode in (%s)".format(metadata_table, config['cloudsql']['source_db'])
            barcode_list = []
            per = 100
            for start in range(1, len(test_barcodes), per):
                barcodes = []
                for index in range(start, start + per):
                    if index == len(test_barcodes):
                        break
                    try:
                        barcodes += ["'" + test_barcodes[index].strip() + "'"]
                    except Exception as e:
                        log.exception('problem setting up barcodes(%s): %s:%s:%s' % (len(test_barcodes), start, index, start + per))
                barcode_list += [barcodes]
            for barcodes in barcode_list:
                log.info('cur length: %s' % (len(barcodes)))
                cur_insert_stmt = insert_stmt % ','.join(barcodes)
                log.info('insert statement: %s' % (cur_insert_stmt))
                ISBCGC_database_helper.update(config, cur_insert_stmt, log, [], False)
    except Exception as e:
        log.exception('problem creating test metadata')
        raise e
    
    log.info('finish create test metadata')
    print datetime.now(), 'create test metadata'

if __name__ == '__main__':
    main(sys.argv[1])
