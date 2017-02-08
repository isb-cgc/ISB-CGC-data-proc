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

from gdc.util.gdc_util import get_map_rows, save2db, update_cloudsql_from_bigquery
from util import close_log, create_log
from upload_files import upload_files

def get_filter(config, data_type, project_id):
    data_types_legacy2use_project = config['data_types_legacy2use_project']
    use_project = data_type not in data_types_legacy2use_project or data_types_legacy2use_project[data_type]
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
    return filt

def process_data_type(config, endpt_type, program_name, project_id, data_type, log_dir, log_name = None):
    try:
        if log_name:
            log_name = create_log(log_dir, log_name)
        else:
            log_name = create_log(log_dir, project_id + '_' + data_type.replace(' ', ''))
        log = logging.getLogger(log_name)
        
        log.info('begin process_data_type %s for %s' % (data_type, project_id))
        file2info = get_map_rows(config, endpt_type, 'file', program_name, get_filter(config, data_type, project_id), log)
        save2db(config, endpt_type, '%s_metadata_data_%s' % (program_name, config['endpt2genomebuild'][endpt_type]), file2info, config[program_name]['process_files']['data_table_mapping'], log)
        upload_files(config, endpt_type, file2info, project_id, data_type, log)
        log.info('finished process_data_type %s for %s' % (data_type, project_id))

        return file2info
    except:
        log.exception('problem processing data_type %s for %s' % (data_type, project_id))
        raise
    finally:
        close_log(log)

def set_uploaded_path(config, log):
    log.info('begin set_uploaded_path')
    postproc_config = config['postprocess_keypath']
    for cloudsql_table in postproc_config['postproc_cloudsql_tables']:
        update_cloudsql_from_bigquery(config, postproc_config, None, cloudsql_table, log)
    log.info('finished set_uploaded_path')
# LOAD DATA INFILE 'employee8.txt' 
#  INTO TABLE employee 
#  FIELDS TERMINATED BY ','
#  (id, name, salary);
    
def populate_data_availibility(config, log):
    pass