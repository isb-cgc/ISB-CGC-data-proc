'''
Created on Jul 27, 2016

Copyright 2016, Institute for Systems Biology.

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
import os
import requests
import tarfile
import time

from util import create_log, delete_dir_contents, upload_file

def write_response(config, response, start, end, log):
    log.info('\t\tstarting write of gdc files')
    accum = 0
    count = 0
    file_name = config['output_dir'] + config['output_file'] % (start, end - 1)
    with open(file_name, 'wb') as f:
        chunk_size=2048
        for chunk in response.iter_content(chunk_size):
            if chunk: # filter out keep-alive new chunks
                if 0 == count % 8000:
                    log.info('\t\t\twritten %s kb' % (accum / 1024))
                count += 1
                accum += chunk_size
                f.write(chunk)
    log.info('\t\tfinished write of gdc files to %s.  wrote <= %s kb' % (file_name, accum / 1024))

def request_try(config, url, file_ids, start, end, log):
    headers = {
        'Content-Type':'application/json'
    }
    params = {'ids':file_ids[start:end]}
    retries = 0
    while True:
        try:
            response = requests.post(url, data=json.dumps(params), headers=headers, stream=True)
            response.raise_for_status()
            break
        except Exception as e:
            response = None
            if retries < 3:
                retries += 1
                time.sleep(1 * retries)
                log.warn('\t\trequest try %d after error %s' % (retries, e))
                continue
#             if 100 < start - end:
#                 log.error('range too small to continue--%s:%s' % (end, start))
            # divide the interval into 2 segments
            else:
                raise RuntimeError('request failed too many times')
    
    if response:
        write_response(config, response, start, end, log)
    return 


def process_files(config, data_type, log):
    filepath = config['output_dir'] + config['gdc_download_%s_%s.tar.gz']
    with tarfile.open(filepath) as tf:
        log.info('\t\textract files from %s' % (config['gdc_download_%s_%s.tar.gz']))
        tf.extractall(path=config['output_dir'][:config['output_dir'].rindex('/', 0, -1)])
        log.info('\t\tdone extract files from %s' % (config['gdc_download_%s_%s.tar.gz']))
    
    with open(config['output_dir'] + 'MANIFEST.txt') as manifest:
        lines = manifest.read().split('\n')
        paths = []
        filenames = set()
        for line in lines[1:]:
            filepath = line.split('\t')[1]
            paths += filepath
            filenames.add(filepath.split('/')[1])
    
    use_dir_in_name = False if len(paths) == len(filenames) else True
    for path in paths:
        key_name = config['upload_folder'] + (path.replace('/', '_') if use_dir_in_name else path.split('/')[1])
        upload_file(config, path, config['bucket_name'], key_name, log)
        
    delete_dir_contents(config['output_dir'])

def request(config, url, file_ids, log):
    log.info('\tstarting requests fetch of gdc files')
    lines_per = min(config['lines_per'], len(file_ids))
    start = 0
    end = lines_per
    while start < len(file_ids):
        log.info('\t\tfetching range %d:%d' % (start, end))
        request_try(config, url, file_ids, start, end, log)
        process_files(config, log)
        start = end
        end += lines_per
        
    log.info('\tfinished fetch of gdc files')

def upload_files(config, file_ids, data_type, log):
    try:
        log.info('starting upload of gdc files')
        if not os.path.isdir(config['output_dir']):
            os.makedirs(config['output_dir'])
        
        url = 'https://gdc-api.nci.nih.gov/data'
        start = time.clock()
        request(config, url, file_ids, log)
        log.info('finished upload of gdc files in %s minutes' % ((time.clock() - start) / 60))
    except:
        raise

def get_file_ids(config):
    file_ids = []
    with open(config['input_id_file']) as file_id_file:
        for line in file_id_file:
            file_ids += [line.strip()]
    
    return file_ids

if __name__ == '__main__':
    config = {
        'output_dir': '/tmp/',
        'output_file': 'gdc_download_%s_%s.tar.gz',
        'input_id_file': 'gdc/doc/gdc_manifest.2016-09-09_head_5000.tsv',
        'lines_per': 0,
        'bucket_name': 'isb-cgc-scratch',
        'upload_folder': 'gdc/test_gdc_upload/',
        'upload_run_folder': 'gdc/test_gdc_upload_run/'
    }
    log_dir = str(date.today()).replace('-', '_') + '_gdc_upload_run/'
    log_name = create_log(log_dir, 'gdc_upload')
    log = logging.getLogger(log_name)
    file_ids = get_file_ids(config)
    data_type = 'cnv'
    for lines_per in (2000, 1000, 500, 100, 50, 10):
        config['lines_per'] = lines_per
        try:
            upload_files(config, file_ids, data_type, log)
        except:
            log.exception('failed with lines per @ %d' % (lines_per))
