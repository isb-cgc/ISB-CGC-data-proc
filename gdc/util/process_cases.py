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
import json
import logging
import requests

from gdc.util.gdc_util import get_map_rows, request, save2db
from util import create_log

def get_omf_map_rows(config, project_name, log):
    filt = { 
        "op":"in",
        "content":{ 
            "field":"files.tags",
            "value":[ 
                "omf"
            ]
        } 
    }
    curstart = 0
    size = 20
    
    params = {
        'filters':json.dumps(filt), 
        'sort': '%s:asc' % ("case_id"),
        'from': curstart, 
        'size': size
    }

def get_filter(project_name):
    return {
                'op': '=',
                 'content': {
                     'field': 'project.project_id',
                     'value': [project_name]
              } 
           }
    
def process_cases(config, project_name, log_dir):
    try:
        log_name = create_log(log_dir, project_name + '_cases')
        log = logging.getLogger(log_name)

        log.info('begin process_cases(%s)' % (project_name))
        case2info = get_map_rows(config, 'case', get_filter(project_name), log)
        save2db(config, 'metadata_gdc_clinical', case2info, config['process_cases']['clinical_table_mapping'], log)
        save2db(config, 'metadata_gdc_biospecimen', case2info, config['process_cases']['sample_table_mapping'], log)
        log.info('finished process_cases(%s)' % (project_name))

#         log.info('begin process_cases(%s) for omf files' % (project_name))
#         omf2info = get_omf_map_rows(config, project_name, log)
#         save2db(config, 'metadata_gdc_clinical', case2info, config['process_cases']['clinical_table_mapping'], log)
#         log.info('finished process_cases(%s) for omf files' % (project_name))

        return case2info
    except:
        log.exception('problem processing cases(%s):' % (project_name))
        raise
