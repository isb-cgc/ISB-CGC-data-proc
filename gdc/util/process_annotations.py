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

from gdc.util.gdc_util import get_map_rows, save2db
from util import close_log, create_log

def get_filter():
    return {}

def process_annotations(config, endpt_type, log_dir):
    try:
        log_name = create_log(log_dir, '%s_annotations' % (endpt_type))
        log = logging.getLogger(log_name)

        log.info('begin process_annotations')
        annotation2info = get_map_rows(config, endpt_type, 'annotation', get_filter(), log)
        save2db(config, endpt_type, 'metadata_gdc_annotation', annotation2info, config['process_annotations']['annotation_table_mapping'], log)
        log.info('finished process_annotations')

        return annotation2info
    except:
        log.exception('problem processing annotations:')
        raise
    finally:
        close_log(log)
