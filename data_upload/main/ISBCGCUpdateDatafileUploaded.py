'''
Created on Nov 25, 2015

part of solution for mdmiller53/software-engineering-coordination/#798

@author: michael
'''
from datetime import date
from datetime import datetime
import json
import logging
import sys

import gcs_wrapper
import isbcgc_cloudsql_model
from util import create_log

def updateDatafileUploaded(config, uploaded_data_files, log):
    try:
        update_stmt = "update metadata_data set DatafileUploaded = 'true', DatafileNameKey = %s where DatafileName = %s"
        isbcgc_cloudsql_model.ISBCGC_database_helper.update(config, update_stmt, log, uploaded_data_files, False)
    except Exception as e:
        log.exception('\tproblem updating')
        raise e

def main(configfilename):
    print datetime.now(), 'begin update DatafileUploaded'
    with open(configfilename) as configFile:
        config = json.load(configFile)
    
    log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + 'update_datafileuploaded' + '/'
    log_name = create_log(log_dir, 'top_processing')
    log = logging.getLogger(log_name)
    log.info('begin update DatafileUploaded')
    try:
        gcs_wrapper.open_connection()
        for bucket in config['buckets']['update_uploaded']:
            updateDatafileUploaded(config, gcs_wrapper.get_bucket_contents(bucket, log), log)
    except Exception as e:
        log.exception('problem updating DatafileUploaded')
        raise e
    finally:
        if gcs_wrapper:
            gcs_wrapper.close_connection()
    log.info('finish update DatafileUploaded')
    print datetime.now(), 'finish update DatafileUploaded'

if __name__ == '__main__':
    main(sys.argv[1])
