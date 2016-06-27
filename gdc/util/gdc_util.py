'''
Created on Jun 27, 2016

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
import requests
import time

from util import filter_map, flatten_map, import_module, print_list_synopsis

def request(url, params, msg, log):
    response = requests.get(url, params=params)
    try:
        response.raise_for_status()
    except:
        retry_count = 1
        while True:
            # try again a few times with a brief pause
            log.exception('%s, retry %d...' % (msg, retry_count))
            time.sleep(1)
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                break
            except:
                if 4 == retry_count:
                    log.exception('%s, giving up...' % (msg))
                    raise
                retry_count += 1 
    
    return response

def get_ids(endpt, typeID, project_name, params, label, log):
    ids = []
    curstart = 1
    log.info("\t\tget request loop starting.  url:%s" % (endpt))
    msg = '\t\tproblem getting ids for %s' % (project_name)
    while True:
        params['from'] = curstart
        response = request(endpt, params, msg, log)
        rj = response.json()
        print_list_synopsis(rj['data']['hits'], '\t\t%s for %s' % (label, project_name), log, 5)
        for index in range(len(rj['data']['hits'])):
            ids += [rj['data']['hits'][index][typeID]]
        
        curstart += rj['data']['pagination']['count']
        if curstart >= rj['data']['pagination']['total']:
            break
    
    return ids

def get_filtered_map(url, ident, filt, mapfilter, project_name, count, log):
    params = {'filters':json.dumps(filt), 
        'from':1, 
        'size':100}
    msg = '\t\tproblem getting filtered map for %s' % (project_name)
    response = request(url, params, msg, log)
        
    rj = response.json()
    if 1 != len(rj['data']['hits']):
        raise ValueError('unexpected number of hits for %s: %s' % (ident, len(rj['data']['hits'])))
    filteredmap = filter_map(rj['data']['hits'][0], mapfilter)
    if 0 == count % 100:
        print_list_synopsis([filteredmap], '\t\tprocessing id %d with id %s, for %s.  filtered map:' % (count, ident, project_name), log, 1)
    return filteredmap

def insert_rows(config, tablename, values, mapfilter, log):
    module = import_module(config['database_module'])
    maps = []
    for value in values:
        maps += flatten_map(value, mapfilter)
    print_list_synopsis(maps, '\t\trows to save for %s' % (tablename), log)

    fieldnames = module.ISBCGC_database_helper.field_names(tablename)
    rows = []
    for nextmap in maps:
        row = []
        for fieldname in fieldnames:
            if fieldname in nextmap:
                row += [nextmap[fieldname]]
            else:
                row += [None]
        
        rows += [row]
    
    module.ISBCGC_database_helper.column_insert(config, rows, tablename, fieldnames, log)
