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

from util import create_log, filter_map, print_list_synopsis

def get_case_ids(config, project_name, log):
    try:
        log.info('\tbegin get_case_ids(%s)' % (project_name))
        case_ids = []
        
        filt = { 'op': '=',
                 'content': {
                     'field': 'project.project_id',
                     'value': [project_name]
                  } 
               } 

        endpt =  config['cases_endpt']['endpt']
        curstart = 1
        size = config['process_cases']['case_fetch_size']
        log.info("\t\tget request loop starting.  url:%s" % (endpt))
        while True:
            params = { 'filters': json.dumps(filt),
                       'fields': 'case_id', 
                       'sort': 'case_id:asc',
                       'from': curstart, 
                       'size': size 
                     }
            response = requests.get(endpt, params=params)
            rj = response.json()
            print_list_synopsis(rj['data']['hits'], '\t\tcases for %s' % (project_name), log, 5)
            for index in range(len(rj['data']['hits'])):
                case_ids += [rj['data']['hits'][index]['case_id']]

            curstart += rj['data']['pagination']['count']
            if curstart >= rj['data']['pagination']['total']:
                break

        log.info('\tfinished get_case_ids(%s)' % (project_name))
        return case_ids
    except:
        log.exception('problem getting case_ids for %s' % (project_name))
        raise

def process_cases(config, project_name, log_dir):
    try:
        log_name = create_log(log_dir, project_name + '_bio')
        log = logging.getLogger(log_name)

        log.info('begin process_cases(%s)' % (project_name))
        
        case_ids = get_case_ids(config, project_name, log)

        log.info('\tbegin select cases')
        count = 0
        endpt =  config['cases_endpt']['endpt']
        query = config['cases_endpt']['query']
        url = endpt + query
        case2info = {}
        for case_id in case_ids:
            filt = { 'op': '=',
                     'content': {
                         'field': 'case_id',
                         'value': [case_id]
                      } 
                   }
            params = { 'filters': json.dumps(filt),
                   'from': 1,
                   'size': 100
            }
            response = requests.get(url, params=params)
            rj = response.json()
            if 1 != len(rj['data']['hits']):
                raise ValueError('unexpected number of hits for %s: %s' % (case_id, len(rj['data']['hits'])))
            filteredmap = filter_map(rj['data']['hits'][0], config['process_cases']['filter_result'])
            case2info[case_id] = filteredmap
            if 0 == count % 100:
                print_list_synopsis([filteredmap], '\t\tprocessing case %d with id %s, for %s.  filtered map:' % (count, case_id, project_name), log, 1)
            count += 1
            
        log.info('\tfinished select cases.  processed %s cases for %s' % (count, project_name))
        
        log.info('finished process_cases(%s)' % (project_name))
        return case2info
    except:
        log.exception('problem processing cases(%s):' % (project_name))
        raise
