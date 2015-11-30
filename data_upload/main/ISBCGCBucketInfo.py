'''
Created on Nov 25, 2015

part of solution for mdmiller53/software-engineering-coordination/#798

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
from datetime import date
from datetime import datetime
import json
import logging
import sys

import gcs_wrapper
from util import create_log

def main(configfilename):
    print datetime.now(), 'begin process bucket info'
    with open(configfilename) as configFile:
        config = json.load(configFile)
    
    log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '_update_uploaded' + '/'
    log_name = create_log(log_dir, 'top_processing')
    log = logging.getLogger(log_name)
    
    log.info('begin process bucket info')
    try:
        gcs_wrapper.open_connection()
        file_exts = config['buckets']['update_exts']
        for bucket in config['buckets']['update_uploaded']:
            bucket_name = bucket['bucket_name']
            with open(bucket['outputfile'], 'w') as outfile:
                upload_files = gcs_wrapper.get_bucket_contents(bucket_name, log)
                for upload_file_pair in upload_files:
                    for file_ext in file_exts:
                        if upload_file_pair[0].endswith(file_ext):
                            outfile.write('\t'.joinupload_file_pair() + '\n')
                            break
    except Exception as e:
        log.exception('problem processing bucket info')
        raise e
    finally:
        gcs_wrapper.close_connection()
    log.info('finish process bucket info')
    print datetime.now(), 'finish process bucket info'

if __name__ == '__main__':
    main(sys.argv[1])
