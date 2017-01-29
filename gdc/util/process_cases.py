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

from gdc.util.gdc_util import get_map_rows, save2db
from util import close_log, create_log, import_module

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

def remove_null_samples(case2info, log):
    sampleless = set()
    for case, info in case2info.iteritems():
        if 'samples' not in info:
            log.info('\tfound %s had no samples' % case)
            sampleless.add(case)
            
    for case in sampleless:
        case2info.pop(case)
    
def process_cases(config, endpt_type, program_name, project_name, log_dir):
    try:
        log_name = create_log(log_dir, project_name + '_cases')
        log = logging.getLogger(log_name)

        log.info('begin process_cases(%s)' % (project_name))
        case2info = get_map_rows(config, endpt_type, 'case', program_name, get_filter(project_name), log)
        save2db(config, endpt_type, '%s_metadata_clinical' % (program_name), case2info, config[program_name]['process_cases']['clinical_table_mapping'], log)
        
        remove_null_samples(case2info, log)
        save2db(config, endpt_type, '%s_metadata_biospecimen' % (program_name), case2info, config[program_name]['process_cases']['sample_table_mapping'], log)

        # fill uin the rest of the metadata depending on the program
        if 0 < len(case2info.values()):
            postproc_module = import_module(config[program_name]['process_cases']['postproc_case']['postproc_module'])
            postproc_module.postprocess(config, project_name, endpt_type, log)
        
        log.info('finished process_cases(%s)' % (project_name))

#         log.info('begin process_cases(%s) for omf files' % (project_name))
#         omf2info = get_omf_map_rows(config, project_name, log)
#         save2db(config, 'metadata_gdc_clinical', case2info, config['process_cases']['clinical_table_mapping'], log)
#         log.info('finished process_cases(%s) for omf files' % (project_name))

        return case2info
    except:
        log.exception('problem processing cases(%s):' % (project_name))
        raise
    finally:
        close_log(log)
