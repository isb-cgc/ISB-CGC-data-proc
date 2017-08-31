'''
Created on Aug 31, 2017

# Copyright 2017, Google, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

@author: michael
'''
from datetime import date
import logging
import sys

import gcs_wrapper_gcloud as wrapper
from util import create_log

def main(project_name, bucket_name):
    log_dir = str(date.today()).replace('-', '_') + '_bam_report_/'
    log_name = create_log(log_dir, 'top_processing')
    log = logging.getLogger(log_name)
    log.info('begin bam report')
    
    wrapper.open_connection({'cloud_projects': {'open': project_name}}, log)
    files = wrapper.get_bucket_contents(bucket_name, None, log)
    
    count = 0
    id2contents = {}
    for file_name in files:
        if 0 == count % 1024:
            log.info('\tprocessing file {}: {}'.format(count, file_name))
        count += 1
        parts = file_name.split('/')
        id2contents.setdefault(parts[-2], set()).add(parts[-1])
    wrapper.close_connection()
        
    count = 0
    no_index = set()
    ext_pairs2count = {}
    for contents in id2contents.itervalues():
        if 0 == count % 1024:
            log.info('\tprocessing contents {}: {}'.format(count, contents))
        count += 1
        file_prefix = None
        for file_name in contents:
            parts = file_name.split('.')
            if 'bam' in parts[-1]:
                bam_ext = parts[-1]
                file_prefix = '.'.join(parts[:-1])
                break
        
        if not file_prefix:
            log.warn('\tdid not find a bam file for {}'.format(contents))
            continue
        set.remove(file_name)
        if 1 < len(contents):
            log.warn('\tfound  more than 2 files for bam file {}:  {}'.format(file_name, contents))
            continue
        if 0 == len(contents):
            log.warn('\tdidn\'t find an index file for bam file {}'.format(file_name))
            no_index.add(contents.pop())
            continue
        
        index_file = contents.pop()
        index_ext = index_file[len(file_prefix) + 1:]
        count = ext_pairs2count.setdefault((bam_ext, index_ext), 0)
        ext_pairs2count[(bam_ext, index_ext)] = count + 1
        
    if 0 < len(no_index):
        log.info('\tno-index file: \n\t\t{}\n\textension pairs:\n\t\t{}'.format('\n\t\t'.join(no_index), '\n\t\t'.join('{}: {}'.format(pairs, count) for pairs, count in ext_pairs2count.iteritems())))
    else:
        log.info('\textension pairs:\n\t\t{}')
    log.info('bam report completed')

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])