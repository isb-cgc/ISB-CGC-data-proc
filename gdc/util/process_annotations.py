'''
Created on Jun 28, 2016

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

from gdc.util.gdc_util import get_ids, get_filtered_map, get_filtered_map_rows, insert_rows
from util import create_log

def save2db(config, annotation2info, log):
    log.info('\tbegin save annotations to db')
    insert_rows(config, 'metadata_gdc_annotation', annotation2info.values(), config['process_annotations']['annotation_table_mapping'], log)
    log.info('\tfinished save annotations to db')

def get_annotation_map_rows(config, log):
    log.info('\tbegin select annotations')
    count = 0
    endpt = config['annotations_endpt']['endpt']
    query = config['annotations_endpt']['query']
    url = endpt + query
    mapfilter = config['process_annotations']['filter_result']
    
    annotation2info = get_filtered_map_rows(url, 'annotation_id', {}, mapfilter, 'annotation', log, config['process_annotations']['fetch_count'])
    
    log.info('\tfinished select annotations.  processed %s annotations for %s' % (count, 'annotations'))
    return annotation2info

def get_annotation_maps(config, annotation_ids, log):
    log.info('\tbegin select annotations')
    count = 0
    endpt = config['annotations_endpt']['endpt']
    query = config['annotations_endpt']['query']
    url = endpt + query
    mapfilter = config['process_annotations']['filter_result']
    
    annotation2info = {}
    for annotation_id in annotation_ids:
        filt = {'op':'=', 
                'content':{
                    'field':'annotation_id', 
                    'value':[annotation_id]}}
        annotation2info[annotation_id] = get_filtered_map(url, annotation_id, filt, mapfilter, 'annotations', count, log)
        count += 1
    
    log.info('\tfinished select annotations.  processed %s annotations for %s' % (count, 'annotations'))
    return annotation2info

def get_annotation_ids(config, log):
    try:
        log.info('\tbegin get_annotation_ids')
        
        size = config['process_annotations']['annotation_fetch_size']
        params = {
                   'fields': 'annotation_id',
                   'sort': 'annotation_id:asc',
                   'size': size
        }

        ids = get_ids(config['annotations_endpt']['endpt'], 'annotation_id', 'annotations', params, 'annotation', log)

        log.info('\tfinished get_annotation_ids')
        return ids
    except:
        log.exception('problem getting annotations')
        raise

def process_annotations(config, log_dir):
    try:
        log_name = create_log(log_dir, 'annotations')
        log = logging.getLogger(log_name)

        log.info('begin process_annotations')
        if config['process_annotations']['call_map_rows']:
            annotation2info = get_annotation_map_rows(config, log)
        else:
            annotation_ids = get_annotation_ids(config, log)
            annotation2info = get_annotation_maps(config, annotation_ids, log)
        save2db(config, annotation2info, log)
        log.info('finished process_annotations')

        return annotation2info
    except:
        log.exception('problem processing annotations:')
        raise
