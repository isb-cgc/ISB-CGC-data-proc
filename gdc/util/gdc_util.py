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

from bq_wrapper import fetch_paged_results, query_bq_table
from isbcgc_cloudsql_model import ISBCGC_database_helper
from util import filter_map, flatten_map, import_module, print_list_synopsis

def request(url, params, msg, log, timeout = 1, verbose = True):
    try:
        if verbose:
            log.info('\t\tstart request for %s\n%s' % (url, params))
#         time.sleep(timeout)
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
    except Exception as e:
        retry_count = 1
        while True:
            # try again a few times with a brief pause
            log.warning('%s, retry %d(%d) because of %s:%s...' % (msg, retry_count, timeout if retry_count == 0 else timeout * (retry_count - 1) * 2, type(e).__name__, e))
            time.sleep(timeout * retry_count * 2)
            try:
                response = requests.get(url, params=params, timeout=timeout * retry_count * 2)
                response.raise_for_status()
                log.info('%s, retry %d successful' % (msg, retry_count))
                break
            except:
                if 4 == retry_count:
                    log.exception('%s, giving up...' % (msg))
                    raise
                retry_count += 1 
    if verbose:
        log.info('\t\tfinished request for %s' % (url))
    
    return response

def __addrow(endpt_type, fieldnames, row2map, log):
    row = []
    
    for fieldname in fieldnames:
        if 'endpoint_type' == fieldname:
            row += [endpt_type]
        elif 'species' == fieldname:
            row += ['Homo sapiens']
        elif 'file_uploaded' == fieldname:
            row += ['false']
        elif 'preservation_method' == fieldname:
            if 'is_ffpe' in row2map:
                row += ['FFPE' if row2map['is_ffpe'] else 'frozen']
            else:
                row += [None]
        elif 'disease_code' == fieldname:
            if 'project_short_name' in row2map:
                row += [row2map['project_short_name'].split('-')[1]]
            else:
                if 'case_gdc_id' in row2map:
                    log.warning('problem setting disease_code for %s' % row2map['case_gdc_id'])
                else:
                    log.warning('problem setting disease_code for %s' % row2map['file_gdc_id'])
                row += [None]
        elif 'aliquot_gdc_id' == fieldname:
            if 'aliquot_gdc_id' in row2map:
                row += [row2map['aliquot_gdc_id']]
            else:
                if 'portion_barcode' in row2map:
                    row += [row2map['portion_gdc_id']]
                else:
                    row += [None]
        elif 'aliquot_barcode' == fieldname:
            if 'aliquot_barcode' in row2map:
                row += [row2map['aliquot_barcode']]
            else:
                if 'portion_barcode' in row2map:
                    row += [row2map['portion_barcode']]
                else:
                    row += [None]
        elif fieldname in row2map:
            if [row2map[fieldname]] is not None:
                row += [row2map[fieldname]]
            else:
                row += [None]
        else:
            row += [None]
    return [row]

def __insert_rows(config, endpt_type, tablename, values, mapfilter, log):
    maps = []
    for value in values:
        maps += flatten_map(value, mapfilter)
    print_list_synopsis(maps, '\t\trows to save for %s' % (tablename), log)

    module = import_module(config['database_module'])
    fieldnames = module.ISBCGC_database_helper.field_names(tablename)
    rows = []
    for nextmap in maps:
        rows += __addrow(endpt_type, fieldnames, nextmap, log)
    if config['update_cloudsql']:
#     def select(cls, config, stmt, log, params = [], verbose = True):
        wherelist = []
        for fieldname in fieldnames:
            wherelist += ['%s = %%s' % (fieldname)]
        stmt = 'select %s from %s where %s' % (fieldnames[0], tablename, ' and '.join(wherelist))
        count = 0
        for index in range(8):
            if len(rows) == index:
                break
            result = module.ISBCGC_database_helper.select(config, stmt, log, rows[index])
            count += 1 if len(result) > 0 else 0
        if count == min(len(rows), 8):
            log.warning('\n\t====================\n\tfirst %d records already saved for %s, skipping\n\t====================' % (count, tablename))
            return
        elif 0 < count:
            raise ValueError('only some of the first %d records were saved for %s' % (count, tablename))
        module.ISBCGC_database_helper.column_insert(config, rows, tablename, fieldnames, log)
    else:
        log.warning('\n\t====================\n\tnot saving to cloudsql to %s this run!\n\t====================' % (tablename))

def save2db(config, endpt_type, table, endpt2info, table_mapping, log):
    if 0 == len(endpt2info.values()):
        log.warning('no rows to save for %s!' % (table))
    else:
        log.info('\tbegin save rows to db for %s' % table)
        __insert_rows(config, endpt_type, table, endpt2info.values(), table_mapping, log)
        log.info('\tfinished save rows to db for %s' % table)

def __get_filtered_map_rows(url, idname, filt, mapfilter, activity, log, size = 100, timeout = None):
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
        response = None
        retries = 4
        while retries:
            retries -= 1
            try:
                response = request(url, params, msg, log, timeout)
                response.raise_for_status()
                    
                try:
                    rj = response.json()
                    break
                except:
                    log.exception('problem with response, not json: %s' % (response.text))
                    raise
            finally:
                if response:
                    response.close
        
        for index in range(len(rj['data']['hits'])):
            themap = rj['data']['hits'][index]
            id2map[themap[idname]] = filter_map(themap, mapfilter)
            if 0 == count % size:
                print_list_synopsis([themap], '\t\tprocessing id %d with id %s, for %s.  unfiltered map:' % (count, themap[idname], activity), log, 1)
                print_list_synopsis([id2map[themap[idname]]], '\t\tprocessing id %d with id %s, for %s.  filtered map:' % (count, themap[idname], activity), log, 1)
            count += 1
        
        curstart += rj['data']['pagination']['count']
        if curstart > rj['data']['pagination']['total']:
            break

    return id2map

def get_map_rows(config, endpt_type, endpt, program_name, filt, log):
    log.info('\tbegin select %s %ss' % (endpt_type, endpt))
    endpt_url = config['%ss_endpt' % (endpt)]['%s endpt' % (endpt_type)]
    query = config['%ss_endpt' % (endpt)]['query']
    url = endpt_url + query
    
    if program_name:
        mapfilter = config[program_name]['process_%ss' % (endpt)]['filter_result']
        endpt2info = __get_filtered_map_rows(url, '%s_id' % (endpt), filt, mapfilter, endpt, log, config[program_name]['process_%ss' % (endpt)]['fetch_count'], config['map_requests_timeout'])
    else:
        mapfilter = config['process_%ss' % (endpt)]['filter_result']
        endpt2info = __get_filtered_map_rows(url, '%s_id' % (endpt), filt, mapfilter, endpt, log, config['process_%ss' % (endpt)]['fetch_count'], config['map_requests_timeout'])
    
    log.info('\tfinished select %s.  processed %s %ss' % (endpt, len(endpt2info), endpt))
    return endpt2info

def request_facets_results(url, facet_query, facet, log, page_size = 0, params = None):
    response = None
    try:
        facet_query = facet_query % (facet, page_size)
        response = request(url + facet_query, params, 'requesting facet %s from %s' % (facet, url), log, 4)
    
        rj = response.json()
        buckets = rj['data']['aggregations'][facet]['buckets']
        retval = {}
        for bucket in buckets:
            retval[bucket['key']] = bucket['doc_count']
    finally:
        if response:
            response.close()
    return retval

def update_cloudsql_from_bigquery(config, postproc_config, project_name, cloudsql_table, log, data_type = None, endpt_type = None):
    update_stmt = 'update %s\nset \n\t%s\nwhere %s = %%s' % (cloudsql_table, '\n\t'.join('%s = %%s,' % (postproc_config['postproc_columns'][key]) for key in postproc_config['postproc_columns'].keys())[:-1], postproc_config['postproc_key_column'])
    if project_name:
        if data_type: # assumes that endpt_type is also supplied
            query_results = query_bq_table(postproc_config['postproc_query'] % (', '.join(postproc_config['postproc_columns'].keys()), endpt_type, project_name, data_type), False, postproc_config['postproc_project'], log)
        else:
            query_results = query_bq_table(postproc_config['postproc_query'] % (', '.join(postproc_config['postproc_columns'].keys()), project_name), False, postproc_config['postproc_project'], log)
    else:
        query_results = query_bq_table(postproc_config['postproc_query'] % (', '.join(postproc_config['postproc_columns'].keys())), False, postproc_config['postproc_project'], log)
    page_token = None
    log.info('\t\t\tupdate_stmt\n%s' % (update_stmt))
    update_count = 0
    while True:
        total_rows, rows, page_token = fetch_paged_results(query_results, postproc_config['postproc_fetch_count'], project_name, page_token, log)
        if 0 < total_rows:
            log.info('\t\t\ttotal rows: %s\n\t%s\n\t\t...\n\t%s' % (total_rows, str(rows[0]), str(rows[-1])))
        else:
            log.info('\t\t\tno rows')
            return
        if config['update_cloudsql']:
            ISBCGC_database_helper.update(config, update_stmt, log, rows, True)
        update_count += len(rows)
        log.info('\t\t\tupdated %s so far%s' % (update_count, ' for ' + project_name if project_name else ''))
        if not page_token:
            log.info('\t\t\tupdated total of %s rows%s' % (update_count, ' for ' + project_name if project_name else ''))
            return

def instantiate_etl_class(config, program_name, data_type, log):
    etl_class = None
    if data_type in config[program_name]['process_files']['datatype2bqscript']:
        log.info('\t\t\tinstantiating etl class %s' % (config[program_name]['process_files']['datatype2bqscript'][data_type]['class']))
        etl_module_name = config[program_name]['process_files']['datatype2bqscript'][data_type]['python_module']
        module = import_module(etl_module_name)
        etl_class_name = config[program_name]['process_files']['datatype2bqscript'][data_type]['class']
        Etl_class = getattr(module, etl_class_name)
        etl_class = Etl_class(config)
    return etl_class
