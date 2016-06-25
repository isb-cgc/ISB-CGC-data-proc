'''
Created on Mar 28, 2015

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
import logging

import util

def process_metadata_current(config, run_dir, log_name):
    """
    return type:
        barcode2term2value: for each sample barcode, finds the AliquotUUID and CENTER_CODE values and sets it as the DataCenterCode field
    """
    log = logging.getLogger(log_name)
    log.info('start processing metadata.current.txt')
    barcode2term2value = {}
    
    metadataURL = config['downloads']['metadata_current']
    try:
        metadata = util.getURLData(metadataURL, 'metadata.current.txt', log)
        lines = metadata.split('\n')
        util.post_run_file(run_dir, 'metadata.current.txt', metadata)
    except Exception as e:
        log.exception('problem fetching metadata.current.txt')
        if 'test' == config['mode']:
            metadata = open('metadata.current.txt')
            lines = metadata.read()
            lines = lines.split('\n')
            log.warning('using local copy for testing purposes')
        else:
            raise e
    
    try:
        column2term = config['metadata_locations']['metadata.current.txt']
        headers = lines[0].split('\t')
        column2index = {}
        for column in column2term:
            column2index[column] = headers.index(column)
    except Exception as e:
        log.exception('problem parsing metadata.current.txt header: %s' % (headers))
        raise e
        
    try:
        for line in lines[1:]:
            if not line:
                continue
            fields = line.split('\t')
            term2value = {}
            for column, term in column2term.iteritems():
                term2value[term] = fields[column2index[column]]
            barcode2term2value[fields[1]] = term2value
    except Exception as e:
        log.exception('problem parsing metadata.current.txt: %s' % (line))
        raise e
     
    log.info('finished processing metadata.current.txt')
    return barcode2term2value
