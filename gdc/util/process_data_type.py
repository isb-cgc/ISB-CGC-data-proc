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
from util import close_log, create_log

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

def process_data_type(config, endpt_type, program_name, project_id, data_type, log_dir, log_name = None):
    try:
        if log_name:
            log_name = create_log(log_dir, log_name)
        else:
            log_name = create_log(log_dir, project_id + '_' + data_type.replace(' ', ''))
        log = logging.getLogger(log_name)
        
        log.info('begin process_data_type %s for %s' % (data_type, project_id))
        file2info = get_map_rows(config, endpt_type, 'file', program_name, get_filter(config, data_type, project_id), log)
        save2db(config, endpt_type, '%s_metadata_data_%s' % (program_name, config['endpt2genomebuild'][endpt_type]), file2info, config[program_name]['process_files']['data_table_mapping'], log)
        upload_files(config, endpt_type, file2info, project_id, data_type, log)
        log.info('finished process_data_type %s for %s' % (data_type, project_id))

        return file2info
    except:
        log.exception('problem processing data_type %s for %s' % (data_type, project_id))
        raise
    finally:
        close_log(log)

def set_file_uploaded(config, log):
    log.info('\tbegin set_file_uploaded')
    postproc_config = config['postprocess_keypath']
    for cloudsql_table in postproc_config['postproc_cloudsql_tables']:
        ISBCGC_database_helper.update(config, postproc_config['postproc_file_uploaded_update'] % (cloudsql_table), log, [[]])
    log.info('\tfinished set_file_uploaded')
    

def set_uploaded_path(config, log):
    log.info('\tbegin set_uploaded_path()')
    postproc_config = config['postprocess_keypath']
    for cloudsql_table in postproc_config['postproc_cloudsql_tables']:
        update_cloudsql_from_bigquery(config, postproc_config, None, cloudsql_table, log)
    log.info('\tfinished set_uploaded_path()')
    set_file_uploaded(config, log)
    
def populate_data_availibility(config, log):
    log.info('\tbegin populate_data_availibility()')
    postproc_config = config['postproc_data_availability']
    columns = postproc_config['columns']
    column_order = postproc_config['column_order']
    column_list = ', '.join(postproc_config['select_columns'])
    insert_columns = [columns[column] for column in column_order] + [postproc_config['display_column_name'], postproc_config['deprecated_column_name']]
    display_name_mappings = postproc_config['display_name_map']
    deprecations = postproc_config['deprecated']
    
    for target_table in postproc_config['target2source_query']:
        rows = []
        for source_table in postproc_config['target2source_query'][target_table]:
            stmt = 'select %s from %s group by %s' % (column_list, source_table, column_list)
            genome_build = source_table[-4:]
            rows = map(lambda row: [genome_build] + list(row), ISBCGC_database_helper.select(config, stmt, log, params = []))
            complete_rows = []
            for row in rows:
                complete_row = [None] * (len(columns) + 2)
                display_name = []
                for index, column in enumerate(column_order):
                    value = row[index]
                    if value:
                        if column in display_name_mappings:
                            display_name += [display_name_mappings[column][value]]
                        else:
                            display_name += [value]
                        complete_row[index] = value
                complete_row[-2] = ':'.join(display_name)
                complete_row[-1] = 1
                complete_rows += [complete_row]
            ISBCGC_database_helper.column_insert(config, complete_rows, target_table, insert_columns, log)
            
            for row, complete_row in zip(rows, complete_rows):
                params = ['%s' % ('%s = \'%s\'' % (column, value) if value else column + ' is null') for column, value in zip(postproc_config['select_columns'], row[1:])]
                stmt = 'select sample_barcode from %s where %s group by sample_barcode' % (source_table, ' and '.join(params))
                log.info('')
                sample_barcodes = ISBCGC_database_helper.select(config, stmt, log, [])
                
                params = ['%s' % ('%s = \'%s\'' % (column, value) if value else column + ' is null') for column, value in zip([columns[column] for column in column_order], row)]
                stmt = 'select metadata_data_type_availability_id from %s where %s' % (target_table, ' and '.join(params))
                log.info('')
                ids = ISBCGC_database_helper.select(config, stmt, log, [])
                
                associations = []
                for barcode in sample_barcodes:
                    associations += [[ids[0][0], barcode[0]]]
                field_names = ['metadata_data_type_availability_id', 'sample_barcode']
                ISBCGC_database_helper.column_insert(config, associations, '%s_metadata_sample_data_availability' % (target_table.split('_')[0]), field_names, log)

        log.info('\t\tcompleted %s:\n\t\t\t%s\n\t\t\t\t...\n\t\t\t%s' % (target_table, complete_rows[0], complete_rows[-1]))
    
    log.info('\tfinished populate_data_availibility()')
