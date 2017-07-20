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
import json
from os import listdir, makedirs, path, remove
import requests
from shutil import rmtree
import tarfile
import time

from gdc_util import instantiate_etl_class
from util import delete_dir_contents, delete_objects, flatten_map, upload_file

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
                log.warn('\t\trequest try %d after error %s for %s' % (retries, e, params))
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


def process_files(config, endpt_type, file2info, outputdir, start, end, program_name, project, data_type, etl_class, log):
    try:
        filepath = outputdir + config['download_output_file_template'] % (start, end - 1)
        with tarfile.open(filepath) as tf:
            log.info('\t\textract tar files from %s' % (filepath))
            tf.extractall(outputdir)
            log.info('\t\tdone extract tar files from %s' % (filepath))
     
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
            for path in paths:
                basefolder = config['buckets']['folders']['base_file_folder']
                
                metadata = flatten_map(file2info[path], config[program_name]['process_files']['data_table_mapping'])
                keypath_template = config[program_name]['process_files']['bucket_path_template']
                key_path_components = []
                for part in config[program_name]['process_files']['bucket_path']:
                    fields = part.split(':')
                    if 1 == len(fields):
                        if 'endpoint_type' == part:
                            key_path_components += [endpt_type]
                        else:
                            key_path_components += [metadata[0][part]]
                    elif 'alt' == fields[0]:
                        if fields[1] in metadata[0] and metadata[0][fields[1]]:
                            key_path_components += [metadata[0][fields[1]]]
                        else:
                            key_path_components += [metadata[0][fields[2]]]
                
                key_name = basefolder + (keypath_template % tuple(key_path_components))
                log.info('\t\tuploading %s' % (key_name))
                upload_file(config, outputdir + path, config['buckets']['open'], key_name, log)
        else:
            log.info('\t\t\tnot uploading files for %s:%s' % (project, data_type))
            
        etl_uploaded = False
        if config['upload_etl_files'] and data_type in config[program_name]['process_files']['datatype2bqscript'] and etl_class is not None:
            etl_uploaded = etl_class.upload_batch_etl(config, outputdir, paths, file2info, endpt_type, program_name, project, data_type, log)
        else:
            log.warning('\t\tnot processing files for ETL for project %s and datatype %s%s' % (project, data_type, ' because there is no script specified' if config['upload_etl_files'] else ''))
        return etl_uploaded
    except:
        log.exception('problem process file %s for project %s and data_type %s' % (filepath, project, data_type))
        raise
    finally:
        if 'delete_dir_contents' not in config or config['delete_dir_contents']:
            delete_dir_contents(outputdir)

def request(config, endpt_type, url, file2info, outputdir, program_name, project, data_type, log):
    log.info('\tstarting requests fetch of gdc files')
    log.info('first set of sorted files of %d:\n\t' % (len(file2info)) + '\n\t'.join(sorted([(info['file_id'] + '/' + info['file_name']) for info in file2info.values()], key = lambda t:t.split('/')[1])[:20]))
    ordered2info = OrderedDict(sorted([(info['file_id'] + '/' + info['file_name'], info) for info in file2info.values()], key = lambda t:t[1]['file_name']))
    download_files_per = min(config['download_files_per'], len(file2info))
    start = 0
    end = download_files_per
    etl_class = instantiate_etl_class(config, program_name, data_type, log)
    
    batch_count = 0
    while start < len(file2info):
        log.info('\t\tfetching range %d:%d' % (start, end))
        request_try(config, url, ordered2info.keys(), start, end, outputdir, log)
        etl_uploaded = process_files(config, endpt_type, ordered2info, outputdir, start, end, program_name, project, data_type, etl_class, log)
        if etl_uploaded:
            batch_count += 1
        start = end
        end += download_files_per
        
    if config['upload_etl_files'] and data_type in config[program_name]['process_files']['datatype2bqscript'] and etl_class is not None:
        etl_class.finish_etl(config, endpt_type, program_name, project, data_type, batch_count, log)
    else:
        log.warning('\t\tnot finishing for ETL for project %s and datatype %s%s' % (project, data_type, ' because there is no script specified' if config['upload_etl_files'] else ''))

    log.info('\tfinished fetch of gdc files')

def upload_files(config, endpt_type, file2info, program_name, project, data_type, log):
    try:
        if not (config['upload_files'] or config['upload_etl_files']):
            log.info('\n\t====================\n\tnot downloading files this run!\n\t====================')
            return

        log.info('starting upload of gdc files')
        outputdir = config['download_base_output_dir'] + '%s/%s/' % (project, data_type)
        if not path.isdir(outputdir):
            makedirs(outputdir)
        else:
            files = listdir(outputdir)
            for file_name in files:
                if path.isdir(outputdir + file_name):
                    rmtree(outputdir + file_name)
                else:
                    remove(outputdir + file_name)

        url = config['data_endpt']['%s endpt' % (endpt_type)]
        start = time.clock()
        request(config, endpt_type, url, file2info, outputdir, program_name, project, data_type, log)
        log.info('finished upload of gdc files in %s minutes' % ((time.clock() - start) / 60))
    except:
        # clean-up
        log.exception('failed to upload files for project %s and datatype %s' % (project, data_type))
        log.warning('cleaning up GCS for failed project %s and datatype %s' % (project, data_type))
        delete_objects(config, config['buckets']['open'], config['buckets']['folders']['base_file_folder'], log)
        log.warning('finished cleaning up GCS for failed project %s and datatype %s' % (project, data_type))
        raise
