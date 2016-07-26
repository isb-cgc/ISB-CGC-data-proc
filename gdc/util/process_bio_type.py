'''
Created on Jul 4, 2016

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
from copy import copy
import logging

from gdc.util.gdc_util import addrow, get_filtered_map_rows

from util import create_log, flatten_map, import_module

def save2db(config, file2info, case_map,  project_id, data_type, log):
    log.info('\tbegin save bio files to db for %s for %s' % (data_type, project_id))
    
    module = import_module(config['database_module'])
    flat_map = config['process_biofiles']['data_table_mapping']
    fieldnames = module.ISBCGC_database_helper.field_names('metadata_gdc_data')
    rows = []
    count = 0
    for info in file2info.itervalues():
        for case_info in info['cases']:
            case = case_info['tcga_barcode']
            if case in case_map:
                samples2aliquots = case_map[case]
                flattened_info = flatten_map(info, flat_map)
                if len(flattened_info) > 1:
                    raise ValueError('unexpected length from flatten_map(%s): %s' % (flattened_info, len(flattened_info)))
                for sample, aliquots in samples2aliquots.iteritems():
                    for aliquot_info in aliquots:
                        infocopy = copy(flattened_info[0])
                        infocopy['SampleBarcode'] = sample
                        infocopy['AliquotBarcode'] = aliquot_info['tcga_barcode']
                        infocopy['AliquotUUID'] = aliquot_info['aliquot_id']
                        rows += addrow(fieldnames, infocopy)
                        if 0 == count % 100:
                            log.info('\t\tprocessing metadata for bio data %d for %s for %s' % (count, data_type, project_id))
                        count += 1
            else:
                log.warning('didn\'t find samples/aliqouts for %s' % (case))
                
    log.info('\tfinished save bio files to db for %s for %s.  total of %s rows' % (data_type, project_id, count))

def get_file_map_rows(config, bio_data_type, project_id, data_type, log):
    log.info('\tbegin select bio files for %s for %s' % (data_type, project_id))
    count = 0
    endpt = config['files_endpt']['endpt']
    query = config['files_endpt']['query']
    url = endpt + query
    mapfilter = config['process_biofiles']['filter_result']
    filt = { 
              'op': 'and',
              'content': [
                 {
                     'op': '=',
                     'content': {
                         'field': 'data_type',
                         'value': [bio_data_type]
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

    file2info = get_filtered_map_rows(url, 'file_id', filt, mapfilter, 'file', log, config['process_biofiles']['fetch_count'], 3)
    
    log.info('\tfinished select bio files.  processed %s files for %s for %s' % (count, data_type, project_id))
    return file2info

def process_file2info(config, project_id, data_type, file2info, log):
    log.info('\tbegin process_file2info(%s:%s) for bio' % (data_type, project_id))
    case2samples2aliquots = {}
    for info in file2info.values():
        for case in info['cases']:
            samples2aliquots = case2samples2aliquots.setdefault(case['tcga_barcode'], {})
            for sample_info in case['samples']:
                samplebarcode = sample_info['tcga_barcode']
                for portion_info in sample_info['portions']:
                    for analytes_info in portion_info['analytes']:
                        for aliguot_info in analytes_info['aliquots']:
                            aliquots = samples2aliquots.setdefault(samplebarcode, [])
                            aliquots += [{'aliquot_id': aliguot_info['aliquot_id'], 'tcga_barcode': aliguot_info['tcga_barcode']}]
    log.info('\tfinished process_file2info(%s:%s) for bio' % (data_type, project_id))
    return case2samples2aliquots

def process_bio_type(config, project_id, data_type, file2info, file_count, log_dir, log_name):
    try:
        log_name = create_log(log_dir, log_name)
        log = logging.getLogger(log_name)
        log.info('begin process_bio_type() for %s for %s' % (data_type, project_id))
        case_map = process_file2info(config, project_id, data_type, file2info, log)
        
        bio_data_type = 'Clinical Supplement'
        file2info = get_file_map_rows(config, bio_data_type, project_id, data_type, log)
        save2db(config, file2info, case_map,  project_id, data_type, log)
        
        log.info('finished process_bio_type() for %s for %s' % (data_type, project_id))
        return file2info
    except:
        log.exception('problem processing bio for %s for %s' % (data_type, project_id))
        raise
