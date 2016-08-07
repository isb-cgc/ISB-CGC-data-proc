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
import logging

from gdc.util.gdc_util import get_filtered_map_rows, insert_rows

from util import create_log

def save2db(config, file2info, log):
    log.info('\tbegin save files to db')
    insert_rows(config, 'metadata_gdc_data', file2info.values(), config['process_files']['data_table_mapping'], log)
    log.info('\tfinished save files to db')

def get_file_map_rows(config, data_type, project_id, log):
    log.info('\tbegin select files')
    data_types_legacy2use_project = config['data_types_legacy2use_project']
    use_project = data_type not in data_types_legacy2use_project or data_types_legacy2use_project[data_type]
    count = 0
    endpt = config['files_endpt']['endpt']
    query = config['files_endpt']['query']
    url = endpt + query
    mapfilter = config['process_files']['filter_result']
    if use_project:
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
    else:
        filt = { 
                  'op': 'and',
                  'content': [
                     {
                         'op': '=',
                         'content': {
                             'field': 'data_type',
                             'value': [data_type]
                          }
                     }
                  ]
               } 

    file2info = get_filtered_map_rows(url, 'file_id', filt, mapfilter, 'file', log, config['process_files']['fetch_count'], 3)
    
    log.info('\tfinished select files.  processed %s files for %s' % (count, 'files'))
    return file2info

def process_data_type(config, project_id, data_type, file_count, log_dir, log_name):
    try:
        log_name = create_log(log_dir, log_name)
        log = logging.getLogger(log_name)
        log.info('begin process_data_type %s for %s' % (data_type, project_id))
        file2info = get_file_map_rows(config, data_type, project_id, log)
        save2db(config, file2info, log)
        log.info('finished process_data_type %s for %s' % (data_type, project_id))
        return file2info
    except:
        log.exception('problem processing data_type %s for %s' % (data_type, project_id))
        raise
