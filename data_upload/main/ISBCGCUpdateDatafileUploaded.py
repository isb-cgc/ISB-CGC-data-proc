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

import isbcgc_cloudsql_model
from util import create_log

def updateDatafileUploaded(config, path_file, log):
    try:
        select_stmt = 'select datafilename from metadata_data where datafilename = ? group by datafilename'
        found_path_names = []
        notfound_path_names = []
        count = 0
        log.info('\tprocessing path/name combinations for existence in the database')
        for path in path_file:
            path = path.strip()
            filename = path[path.rindex('/') + 1:]
            path_nobucket = path[path.index('/', 5):]
            if 0 == count % 8192:
                log.info('\tprocessing %s record: %s:%s' % (count, path, filename))
            count += 1
            # check that the file was actually part of the metadata
            cursor = isbcgc_cloudsql_model.ISBCGC_database_helper.select(config, select_stmt, log, [filename], False)
            cursor = []
            if 0 < len(cursor):
                found_path_names += [[path_nobucket, filename]]
            else:
                notfound_path_names += [[path_nobucket, filename]]
        if 0 == len(notfound_path_names):
            log.info('\tprocessed a total of %s path/name combinations.' % (count))
        else:
            if 100 > notfound_path_names:
                print_notfound = notfound_path_names
            else:
                print_notfound = notfound_path_names[:100] + ['...']
            log.info('\tprocessed a total of %s path/name combinations.  %s files were not found:\n\t\t%s\n' % (count, len(notfound_path_names), '\n\t\t'.join(':'.join(tuple) for tuple in print_notfound)))
        
        update_stmt = 'update metadata_data set DatafileUploaded = \'true\', DatafileNameKey = %s where DatafileName = %s'
        isbcgc_cloudsql_model.ISBCGC_database_helper.update(config, update_stmt, log, found_path_names, False)
    except Exception as e:
        log.exception('\tproblem updating')
        raise e

def main(configfilename):
    print datetime.now(), 'begin update DatafileUploaded'
    with open(configfilename) as configFile:
        config = json.load(configFile)
    
    log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '_update_uploaded' + '/'
    log_name = create_log(log_dir, 'top_processing')
    log = logging.getLogger(log_name)
    log.info('begin update DatafileUploaded')
    try:
        for bucket in config['buckets']['update_uploaded']:
            with open(bucket['outputfile'], 'r') as outfile:
                updateDatafileUploaded(config, outfile, log)
    except Exception as e:
        log.exception('problem updating DatafileUploaded')
        raise e
    log.info('finish update DatafileUploaded')
    print datetime.now(), 'finish update DatafileUploaded'

if __name__ == '__main__':
    main(sys.argv[1])
