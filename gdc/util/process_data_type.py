'''
Created on Jun 26, 2016

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

from gdc.util.gdc_util import get_map_rows, save2db, update_cloudsql_from_bigquery
from isbcgc_cloudsql_model import ISBCGC_database_helper
from upload_files import upload_files
from util import close_log, create_log, flatten_map

def get_filter(config, data_type, project_id):
    data_types_legacy2use_project = config['data_types_legacy2use_project']
    use_project = data_type not in data_types_legacy2use_project or data_types_legacy2use_project[data_type]
    if use_project:
        filt = { 
                  'op': 'and',
                  'content': [
                     {
                         'op': '=',
                         'content': {
                             'field': 'data_type',
                             'value': [data_type]
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
    else:
        filt = { 
                  'op': 'and',
                  'content': [
                     {
                         'op': '=',
                         'content': {
                             'field': 'data_type',
                             'value': [data_type]
                          }
                     }
                  ]
               } 
    return filt

def filter_null_samples(config, file2info, project_id, data_type, log):
    retval = {}
    for fileid, info in file2info.iteritems():
        if 'samples' in info['cases'][0] or data_type in config['postproc_data_availability']['data_type_exclude']:
            retval[fileid] = info
        else:
            log.info('found null sample for %s:%s:%s' % (info['cases'][0]['case_id'], data_type, project_id))
    return retval
    
def populate_data_availibility(config, endpt_type, program_name, project_id, data_type, infos, log):
    log.info('\tbegin populate_data_availibility() for %s:%s' % (project_id, data_type))
    
    # iterate through the gdc info and put together the counts for the sample barcodes
    sample_barcode2count = {}
    for info in infos:
        mapping = config[program_name]['process_files']['data_table_mapping']
        flattened = flatten_map(info, mapping)
        for index in range(len(flattened)):
            if (data_type in ('Simple somatic mutation', 'Masked Somatic Mutation') and 'controlled ' == flattened[index]['access']) or \
                (data_type in ('Aligned reads') and 'open' == flattened[index]['access']):
                continue
            sample_barcode = flattened[index]['sample_barcode']
            count = sample_barcode2count.setdefault(sample_barcode, 0)
            sample_barcode2count[sample_barcode] = count + 1
        
    # read in the appropriate data availability row to get the foreign key
    isb_label = config['data_type2isb_label'][data_type]
    stmt = 'select metadata_data_type_availability_id from %s_metadata_data_type_availability where genomic_build = %%s and isb_label = %%s' % (program_name)
    foreign_key = ISBCGC_database_helper.select(config, stmt, log, [config['endpt2genomebuild'][endpt_type], isb_label])[0][0]
    
    params = []
    for sample_barcode, count in sample_barcode2count.iteritems():
        params += [[foreign_key, sample_barcode, count]]
    
    ISBCGC_database_helper.column_insert(config, params, '%s_metadata_sample_data_availability' % (program_name), ['metadata_data_type_availability_id', 'sample_barcode', 'count'], log)
    
    log.info('\tfinished populate_data_availibility() for %s:%s' % (project_id, data_type))

def set_uploaded_path(config, endpt_type, program_name, project_id, data_type, log):
    log.info('\tbegin set_uploaded_path()')
    postproc_config = config['postprocess_keypath']
    # first set file_name_key to null
    cloudsql_table = '%s_metadata_data_%s' % (program_name, config['endpt2genomebuild'][endpt_type])
    ISBCGC_database_helper.update(config, postproc_config['postproc_file_name_key_null_update'] % (cloudsql_table, project_id, data_type), log, [[]])
    # now use the BigQuery table to set file_name_key
    update_cloudsql_from_bigquery(config, postproc_config, project_id, cloudsql_table, log, data_type)
    ISBCGC_database_helper.update(config, postproc_config['postproc_file_uploaded_update'] % (cloudsql_table), log, [[]])
    log.info('\tfinished set_uploaded_path()')

def process_data_type(config, endpt_type, program_name, project_id, data_type, log_dir, log_name = None):
    try:
        if log_name:
            log_name = create_log(log_dir, log_name)
        else:
            log_name = create_log(log_dir, project_id + '_' + data_type.replace(' ', ''))
        log = logging.getLogger(log_name)
        
        log.info('begin process_data_type %s for %s' % (data_type, project_id))
        file2info = get_map_rows(config, endpt_type, 'file', program_name, get_filter(config, data_type, project_id), log)
        file2info = filter_null_samples(config, file2info, project_id, data_type, log)
        if data_type in config['data_type_gcs']:
            save2db(config, endpt_type, '%s_metadata_data_%s' % (program_name, config['endpt2genomebuild'][endpt_type]), file2info, config[program_name]['process_files']['data_table_mapping'], log)
            if config['process_paths']:
                set_uploaded_path(config, endpt_type, program_name, project_id, data_type, log)
        if config['process_data_availability'] and data_type not in ('Clinical Supplement', 'Biospecimen Supplement'):
            populate_data_availibility(config, endpt_type, program_name, project_id, data_type, file2info.values(), log)
        upload_files(config, endpt_type, file2info, program_name, project_id, data_type, log)
        log.info('finished process_data_type %s for %s' % (data_type, project_id))

        return file2info
    except:
        log.exception('problem processing data_type %s for %s' % (data_type, project_id))
        raise
    finally:
        close_log(log)

