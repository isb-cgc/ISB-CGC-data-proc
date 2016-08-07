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

def request(url, params, msg, log, timeout = None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    except:
        retry_count = 1
        while True:
            # try again a few times with a brief pause
            log.exception('%s, retry %d...' % (msg, retry_count))
            time.sleep(1)
            try:
                if timeout:
                    response = requests.get(url, params=params, timeout = timeout)
                else:
                    response = requests.get(url, params=params)
                response.raise_for_status()
                break
            except:
                if 4 == retry_count:
                    log.exception('%s, giving up...' % (msg))
                    raise
                retry_count += 1 
    
    return response

def get_filtered_map_rows(url, idname, filt, mapfilter, activity, log, size = 100, timeout = None):
    count = 0
    id2map = {}
    curstart = 1
    while True:
        params = {
            'filters':json.dumps(filt), 
            'sort': '%s:asc' % (idname),
            'from': curstart, 
            'size': size
        }
        msg = '\t\tproblem getting filtered map for %s' % (activity)
        response = request(url, params, msg, log, timeout)
            
        rj = response.json()
        for index in range(len(rj['data']['hits'])):
            themap = rj['data']['hits'][index]
            id2map[themap[idname]] = filter_map(themap, mapfilter)
            if 0 == count % size:
                print_list_synopsis([themap], '\t\tprocessing id %d with id %s, for %s.  unfiltered map:' % (count, themap[idname], activity), log, 1)
                print_list_synopsis([id2map[themap[idname]]], '\t\tprocessing id %d with id %s, for %s.  filtered map:' % (count, themap[idname], activity), log, 1)
            count += 1
        
        curstart += rj['data']['pagination']['count']
        if curstart >= rj['data']['pagination']['total']:
            break

    return id2map

def addrow(fieldnames, row2map):
    row = []
    for fieldname in fieldnames:
        if fieldname in row2map:
            row += [row2map[fieldname]]
        else:
            row += [None]
    return [row]

def insert_rows(config, tablename, values, mapfilter, log):
    maps = []
    for value in values:
        maps += flatten_map(value, mapfilter)
    print_list_synopsis(maps, '\t\trows to save for %s' % (tablename), log)

    module = import_module(config['database_module'])
    fieldnames = module.ISBCGC_database_helper.field_names(tablename)
    rows = []
    for nextmap in maps:
        rows += addrow(fieldnames, nextmap)
    
    module.ISBCGC_database_helper.column_insert(config, rows, tablename, fieldnames, log)

def request_facets_results(url, facet_query, facet, log, page_size = 0, params = None):
    facet_query = facet_query % (facet, page_size)
    response = request(url + facet_query, params, 'requesting facet %s from %s' % (facet, url), log)

    rj = response.json()
    buckets = rj['data']['aggregations'][facet]['buckets']
    retval = {}
    for bucket in buckets:
        retval[bucket['key']] = bucket['doc_count']
    return retval
