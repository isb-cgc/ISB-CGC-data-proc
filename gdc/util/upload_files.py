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
from collections import OrderedDict
from datetime import date
import json
import logging
import os
import requests
import tarfile
import time

from util import create_log, delete_dir_contents, delete_objects, import_module, upload_file

def write_response(config, response, start, end, outputdir, log):
    try:
        log.info('\t\tstarting write of gdc files')
        accum = 0
        count = 0
        file_name = outputdir + config['download_output_file_template'] % (start, end - 1)
        with open(file_name, 'wb') as f:
            chunk_size=2048
            for chunk in response.iter_content(chunk_size):
                if chunk: # filter out keep-alive new chunks
                    if 0 == count % 8000:
                        log.info('\t\t\twritten %skb' % (accum / 1024))
                    count += 1
                    accum += len(chunk)
                    f.write(chunk)
        log.info('\t\tfinished write of gdc files to %s.  wrote %skb' % (file_name, accum / 1024))
    except:
        log.exception('problem saving file to %s' % (file_name))
        raise

def request_try(config, url, file_ids, start, end, outputdir, log):
    headers = {
        'Content-Type':'application/json'
    }
    params = {'ids':[key.split('/')[0] for key in file_ids[start:end]]}
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
                log.exception('request failed too many times')
                raise
    
    if response:
        write_response(config, response, start, end, outputdir, log)
    return 


def process_files(config, file2info, outputdir, start, end, project, data_type, etl_module, log):
    try:
        filepath = outputdir + config['download_output_file_template'] % (start, end - 1)
        with tarfile.open(filepath) as tf:
            log.info('\t\textract files from %s' % (filepath))
            tf.extractall(outputdir)
            log.info('\t\tdone extract files from %s' % (filepath))
         
        with open(outputdir + 'MANIFEST.txt') as manifest:
            lines = manifest.read().split('\n')
            paths = []
            filenames = set()
            for line in lines[1:]:
                filepath = line.split('\t')[1]
                paths += [filepath]
                filenames.add(filepath.split('/')[1])
        paths.sort(key = lambda path:path.split('/')[1])
         
        if config['upload_files']:
            use_dir_in_name = False if len(paths) == len(filenames) else True
            for path in paths:
                basefolder = config['buckets']['folders']['base_file_folder']
                key_name = basefolder + '%s/%s/' % (project, data_type) + (path.replace('/', '_') if use_dir_in_name else path.split('/')[1])
                log.info('\t\tuploading %s' % (key_name))
                upload_file(config, outputdir + path, config['buckets']['open'], key_name, log)
            
        if config['upload_etl_files'] and data_type in config['process_files']['datatype2bqscript'] and etl_module is not None:
            etl_module.upload_batch_etl(config, outputdir, paths, file2info, project, data_type, log)
        else:
            log.warning('\t\tnot processing files for ETL for project %s and datatype %s%s' % (project, data_type, ' because there is no script specified' if config['upload_etl_files'] else ''))
    except:
        log.exception('problem process file %s for project %s and data_type %s' % (filepath, project, data_type))
        raise
    finally:
        delete_dir_contents(outputdir)

def request(config, url, file2info, outputdir, project, data_type, log):
    log.info('\tstarting requests fetch of gdc files')
    log.info('first set of sorted files:\n\t' + '\n\t'.join(sorted([(info['file_id'] + '/' + info['file_name']) for info in file2info.values()], key = lambda t:t.split('/')[1])[:20]))
    ordered2info = OrderedDict(sorted([(info['file_id'] + '/' + info['file_name'], info) for info in file2info.values()], key = lambda t:t[1]['file_name']))
    download_files_per = min(config['download_files_per'], len(file2info))
    start = 0
    end = download_files_per
    etl_module = None
    if data_type in config['process_files']['datatype2bqscript']:
        etl_module = import_module(config['process_files']['datatype2bqscript'][data_type]['python_script'])
    while start < len(file2info):
        log.info('\t\tfetching range %d:%d' % (start, end))
        request_try(config, url, ordered2info.keys(), start, end, outputdir, log)
        process_files(config, ordered2info, outputdir, start, end, project, data_type, etl_module, log)
        start = end
        end += download_files_per
        
    if config['upload_etl_files'] and data_type in config['process_files']['datatype2bqscript'] and etl_module is not None:
        etl_module.finish_etl(config, project, data_type, log)
    else:
        log.warning('\t\tnot finishing for ETL for project %s and datatype %s%s' % (project, data_type, ' because there is no script specified' if config['upload_etl_files'] else ''))

    log.info('\tfinished fetch of gdc files')

def upload_files(config, file2info, project, data_type, log):
    try:
        log.info('starting upload of gdc files')
        outputdir = config['download_base_output_dir'] + '%s/%s/' % (project, data_type)
        if not os.path.isdir(outputdir):
            os.makedirs(outputdir)
        
        url = 'https://gdc-api.nci.nih.gov/data'
        start = time.clock()
        request(config, url, file2info, outputdir, project, data_type, log)
        log.info('finished upload of gdc files in %s minutes' % ((time.clock() - start) / 60))
    except:
        # clean-up
        log.exception('failed to upload files for project %s and datatype %s' % (project, data_type))
        delete_objects(config, config['buckets']['open'], config['buckets']['folders']['base_file_folder'], log)
        raise

def setup_file_ids(config):
    file_ids = {}
    with open(config['input_id_file']) as file_id_file:
        for line in file_id_file:
            file_ids[line.strip()] = None
    
    return file_ids

if __name__ == '__main__':
    config = {
        "upload_files": True,
        'upload_etl_files': False,
        'download_base_output_dir': '/tmp/%s/%s/',
        'download_output_file_template': 'gdc_download_%s_%s.tar.gz',
        'input_id_file': 'gdc/doc/gdc_manifest.2016-09-09_head_5000.tsv',
        'download_files_per': 0,
        "buckets": {
            "open": "isb-cgc-scratch",
            "controlled": "62f2c827-test-a",
            "folders": {
                "base_file_folder": "gdc/test_gdc_upload/",
                "base_run_folder": "gdc/test_gdc_upload_run/"
            }
        },
        'upload_run_folder': 'gdc/test_gdc_upload_run/'
    }
    log_dir = str(date.today()).replace('-', '_') + '_gdc_upload_run/'
    log_name = create_log(log_dir, 'gdc_upload')
    log = logging.getLogger(log_name)
    file_ids = setup_file_ids(config)
    project = 'tcga-ucs'
    data_type = 'cnv'
    for download_files_per in (50):
        config['download_files_per'] = download_files_per
        try:
            upload_files(config, file_ids, project, data_type, log)
        except:
            log.exception('failed with lines per @ %d' % (download_files_per))
