'''
Created on Jun 26, 2016

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
import json
import logging

from gdc.util.gdc_util import get_ids, get_filtered_map, get_filtered_map_rows, insert_rows

from util import create_log

def save2db(config, file2info, log):
    log.info('\tbegin save cases to db')
    values = file2info.values()
    insert_rows(config, 'metadata_gdc_data', values, config['process_files']['data_table_mapping'], log)
    log.info('\tfinished save cases to db')

def get_file_map_rows(config, data_type, project_id, log):
    log.info('\tbegin select files')
    count = 0
    endpt = config['files_endpt']['endpt']
    query = config['files_endpt']['query']
    url = endpt + query
    mapfilter = config['process_files']['filter_result']
    filt = { 
              'op': 'and',
              'content': [
                 {
                     'op': '=',
                     'content': {
                         'field': 'data_type',
                         'value': [data_type]
                      }
                  },
                  {
                     'op': '=',
                     'content': {
                         'field': 'cases.project.project_id',
                         'value': [project_id]
                      }
                  } 
              ]
           } 

    file2info = get_filtered_map_rows(url, 'file_id', filt, mapfilter, 'file', log, config['process_files']['fetch_count'], 3)
    
    log.info('\tfinished select files.  processed %s files for %s' % (count, 'files'))
    return file2info

def get_file_maps(config, data_type, project_id, file_ids, log):
    log.info('\tbegin select files')
    count = 0
    endpt = config['files_endpt']['endpt']
    query = config['files_endpt']['query']
    url = endpt + query
    file2info = {}
    for file_id in file_ids:
        filt = {'op':'=', 
            'content':{
                'field':'file_id', 
                'value':[file_id]}}
        file2info[file_id] = get_filtered_map(url, file_id, filt, config['process_files']['filter_result'], project_id, count, log)
        count += 1
    
    log.info('\tfinished select files.  processed %s files for %s' % (count, project_id))
    return file2info

def get_file_ids(config, project_id, data_type, log):
    try:
        log.info('\tbegin get_file_ids for %s for %s' % (data_type, project_id))
        file_ids = []
        
        filt = { 
                  'op': 'and',
                  'content': [
                     {
                         'op': '=',
                         'content': {
                             'field': 'data_type',
                             'value': [data_type]
                          }
                      },
                      {
                         'op': '=',
                         'content': {
                             'field': 'cases.project.project_id',
                             'value': [project_id]
                          }
                      } 
                  ]
               } 

        size = config['process_files']['file_fetch_size']
        params = { 
                   'filters': json.dumps(filt),
                   'fields': 'file_id',
                   'sort': 'file_id:asc',
                   'size': size 
                 }
        file_ids = get_ids(config['files_endpt']['endpt'], 'file_id', project_id, params, 'file', log)
        log.info('\tfinished get_file_ids for %s for %s' % (data_type, project_id))
        return file_ids
    except:
        log.exception('problem getting file_ids for %s' % (project_id))
        raise
    
def process_data_type(config, project_id, data_type, log_dir, log_name):
    try:
        log_name = create_log(log_dir, log_name)
        log = logging.getLogger(log_name)
        log.info('begin process_data_type %s for %s' % (data_type, project_id))
        if config['process_files']['call_map_rows']:
            file2info = get_file_map_rows(config, data_type, project_id, log)
        else:
            file_ids = get_file_ids(config, project_id, data_type, log)
            file2info = get_file_maps(config, data_type, project_id, file_ids, log)
        save2db(config, file2info, log)
        log.info('finished process_data_type %s for %s' % (data_type, project_id))
    except:
        log.exception('problem processing data_type %s for %s' % (data_type, project_id))
        raise
