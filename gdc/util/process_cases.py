'''
Created on Jun 23, 2016

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

def save2db(config, case2info, log):
    log.info('\tbegin save cases to db')
    values = case2info.values()
    insert_rows(config, 'metadata_gdc_clinical', values, config['process_cases']['clinical_table_mapping'], log)
    insert_rows(config, 'metadata_gdc_biospecimen', values, config['process_cases']['sample_table_mapping'], log)
    log.info('\tfinished save cases to db')

def get_case_map_rows(config, project_name, log):
    log.info('\tbegin select cases')
    count = 0
    endpt = config['cases_endpt']['endpt']
    query = config['cases_endpt']['query']
    url = endpt + query
    mapfilter = config['process_cases']['filter_result']
    filt = { 'op': '=',
             'content': {
                 'field': 'project.project_id',
                 'value': [project_name]
              } 
           }
    
    case2info = get_filtered_map_rows(url, 'case_id', filt, mapfilter, 'case', log, config['process_cases']['fetch_count'], 3)
    
    log.info('\tfinished select cases.  processed %s cases for %s' % (count, 'cases'))
    return case2info

def process_cases(config, project_name, log_dir):
    try:
        log_name = create_log(log_dir, project_name + '_cases')
        log = logging.getLogger(log_name)

        log.info('begin process_cases(%s)' % (project_name))
        case2info = get_case_map_rows(config, project_name, log)
        save2db(config, case2info, log)
        log.info('finished process_cases(%s)' % (project_name))

        return case2info
    except:
        log.exception('problem processing cases(%s):' % (project_name))
        raise
