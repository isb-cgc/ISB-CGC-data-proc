'''
Created on Mar 5, 2017

Copyright 2017, Institute for Systems Biology.

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
from bq_wrapper import fetch_paged_results, query_bq_table
from isbcgc_cloudsql_model import ISBCGC_database_helper
from util import print_list_synopsis

def process_image_updates(config, image_config, rows, log):
    '''
    based on the config maps, checks to see if the config-specified metadata data table can be updated with the GCS file path
    for the sample, file_name combination for each of the rows that has a match
    
    parameters:
        config: configuration mappings
        image_config: section of the config file with specific mappings for these image types
        rows: rows selected from the BigQuery table
        log: where to write progress and other messages
    
    returns:
        rows: rows that weren't mapped to existing rows from BigQuery 
    '''
    data_columns = image_config['data_columns'] if 'data_columns' in image_config else None
    data_select_template = image_config['data_select_template'].format(','.join(data_columns)) if 'data_select_template' in image_config else None
    if data_select_template:
        # select matching rows from the metadata data for the bigquery rows
        data_select = data_select_template % (','.join(["'" + bq_row[0] + "'" for bq_row in rows]))
        data_rows = ISBCGC_database_helper.select(config, data_select, log, [])
        
        # get the parameters from row info from the bigquery select for the update
        data_rows_map = {tuple(row):None for row in data_rows}
        update_rows = [row for row in rows if (row[0], row[1]) in data_rows_map]
        log.info('\t\tfound %d row(s) for updating file_name_key after filtering' % (len(update_rows)))
        updates = []
        for update_row in update_rows:
            update = []
            for update_column in image_config['update_columns']:
                update += [update_row[update_column]]
            
            updates += [update]
        
        # do the update
        ISBCGC_database_helper.update(config, image_config['data_update_template'], log, updates)
        
        # get the bigquery rows that weren't updated and return them
        rows = [row for row in rows if (row[0], row[1]) not in data_rows_map]
        log.info('\t\t%d row(s) to insert after filtering' % (len(rows)))
    return rows


def process_image_inserts(config, image_config, not_barcodes, rows, log):
    '''
    based on either the case_barcode (for radiology images) or the sample_barcode (for tissue or diagnostic images),
    creates a new metadata data record in the config-specified metadata data table
    
    parameters:
        config: configuration mappings
        image_config: section of the config file with specific mappings for these image types
        not_barcodes: an output parameter for barcodes that weren't found in the underlying clinical or biospecimen table
        rows: rows selected from the BigQuery table
        log: where to write progress and other messages
    '''
    # get the information from the config mapping
    clinical_columns = image_config['clinical_columns']
    clinical_select_template = image_config['clinical_select_template'].format(','.join(clinical_columns))
    file_columns = image_config['file_columns']
    file_column_names = sorted(file_columns.keys())
    literal_columns = image_config['literal_columns']
    literal_columns_names = sorted(literal_columns.keys())
    literal_values = [literal_columns[column] for column in literal_columns_names]
    insert_columns = clinical_columns + file_column_names + literal_columns_names
    barcode_index = image_config['barcode_column_index']

    if 0 < len(rows):
        # select info from the metadata clinical table (for radiology) or the biospecimen (for tissue or diagnostic)
        clinical_select = clinical_select_template % (','.join(["'" + bq_row[0] + "'" for bq_row in rows]))
        clinical_rows = ISBCGC_database_helper.select(config, clinical_select, log, [])
        barcode2info = {}
        for row in clinical_rows:
            barcode2info[row[barcode_index]] = row
    
    insert_rows = []
    for bq_row in rows:
        # match up the rows that exist in BigQuery table that are in the database 
        if bq_row[0] not in barcode2info:
            not_barcodes.add(bq_row[0])
        else:
            file_values = []
            for column in file_column_names:
                fields = file_columns[column].split(':')
                if 1 == len(fields):
                    file_values += [bq_row[int(fields[0])]]
                elif 'split' == fields[1]:
                    file_values += [bq_row[int(fields[0])].split(fields[2])[int(fields[3])]]
                else:
                    raise ValueError('unknown operation: %s' % (fields))
            
            insert_rows += [list(barcode2info[bq_row[0]]) + file_values + literal_values]
    
    print_list_synopsis(insert_rows, '%s:\n%s' % (image_config['data_insert_table'], insert_columns), log, 1)
    if 0 < len(insert_rows):
        # insert the matching rows in the metadata data table
        ISBCGC_database_helper.column_insert(config, insert_rows, image_config['data_insert_table'], insert_columns, log)

def process_image_type(config, image_type, log):
    '''
    based on the configuration map loaded from the configFileName, loads the DCC data into GCS.  also
    obtains metadata based on file paths, SDRF values, and CGHub manifest values
    
    parameters:
        config: configuration mappings
        image_type: the type of image (radiology, tissue or diagnostic)
        log: where to write progress and other messages
    '''
    programs = config['program_names_for_images']
    for program in programs:
        # for programs with images, select the appropriate section from the config file
        image_config = config[program]['process_files']['images'][image_type]
        not_barcodes = set()

        # query the big query table
        bq_select_template = image_config['bq_select_template']
        bq_columns = image_config['bq_columns']
        query_results = query_bq_table(bq_select_template.format(','.join(bq_columns)), image_config['use_legacy'], None, log)
        page_token = None
        while True:
            # loop through the big query results
            total_rows, rows, page_token = fetch_paged_results(query_results, image_config['fetch_count'], None, page_token, log)
            # process updates to the metadata data table
            rows = process_image_updates(config, image_config, rows, log)

            # create inserts into the metadata data that for big query rows that didn't have a match already in the metadata data table
            process_image_inserts(config, image_config, not_barcodes, rows, log)
            if not page_token:
                log.info('\t\tcreated total of %s rows' % (total_rows))
                break

        log.info('\tbig_query %s barcodes(%s) not in the metadata table for %s:\n\t%s\n\t\t...\n%s\n' % (image_type, len(not_barcodes), program, '\n\t'.join(sorted(list(not_barcodes)))[:5], '\n\t'.join(sorted(list(not_barcodes)))[-5:]))

def process_radiology_images(config, log):
    '''
    process the radiology images based on rows in metadata.TCGA_radiology_images
    
    parameters:
        config: configuration mappings
        log: where to write progress and other messages
    '''
    log.info('start process_radiology_images()')
    process_image_type(config, 'radiology', log)
    log.info('finished process_radiology_images()')

def process_diagnostic_images(config, log):
    '''
    process the diaqgnostic images based on rows in metadata.TCGA_slide_images where slide_id begins with 'DX'
    
    parameters:
        config: configuration mappings
        log: where to write progress and other messages
    '''
    log.info('start process_diagnostic_images()')
    process_image_type(config, 'diagnostic', log)
    log.info('finished process_diagnostic_images()')

def process_tissue_images(config, log):
    '''
    process the tissue images based on rows in metadata.TCGA_slide_images where slide_id doesn't begin with 'DX'
    
    parameters:
        config: configuration mappings
        log: where to write progress and other messages
    '''
    log.info('start process_tissue_images()')
    process_image_type(config, 'tissue', log)
    log.info('finished process_tissue_images()')

def process_images(config, log):
    '''
    process the image files based on the BigQuery tables metadata.TCGA_radiology_images and metadata.TCGA_slide_images
    
    parameters:
        config: configuration mappings
        log: where to write progress and other messages
    '''
#     process_radiology_images(config, log)
    process_diagnostic_images(config, log)
    process_tissue_images(config, log)

