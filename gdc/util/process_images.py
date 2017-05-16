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

def process_data_image_records(config, image_config, image_type, rows, log):
    '''
    based on either the case_barcode (for radiology images) or the sample_barcode (for tissue or diagnostic images),
    either updates or creates a new metadata data record in the config-specified metadata data table
    
    parameters:
        config: configuration mappings
        image_config: section of the config file with specific mappings for these image types
        not_barcodes: an output parameter for barcodes that weren't found in the underlying clinical or biospecimen table
        rows: rows selected from the BigQuery table
        log: where to write progress and other messages

        file_gdc_id: ?
        case_gdc_id: ?
        case_barcode: row[2]
        sample_gdc_id: ?
        sample_barcode: row[0]
        sample_type: sample_barcode[13:15]
        aliquot_barcode: row[3]
        aliquot_gdc_id: ?
        project_short_name: row[4].split('/')[6]
        disease_code: project_short_name.split('-')[1]
        program_name: 'TCGA'
        data_type: image_type
        data_category: 'Clinical'
        type: 'file'
        file_name: row[1]
        data_format: 'SVS'
        access: 'open'
        acl: 'open'
        platform: 'Clinical'
        file_name_key: row[4]
        file_uploaded: 'true'
        endpoint_type: 'legacy'
        species: 'Homo sapien'
    '''
    # get the information from the config mapping
    log.info('\tchecking data records')
    barcode2row = dict([(row[0], row) for row in rows])
    data_select_template = image_config['data_select_template']
    data_rows = ISBCGC_database_helper.select(config, data_select_template % ("'" + "','".join(barcode2row) + "'"), log, [])
    barcode_db = set([data_row[0] for data_row in data_rows])
    new_barcodes = set(barcode2row) - barcode_db
    if 0 < len(new_barcodes):
        log.info('\t\tinserting {} new data records'.format(len(new_barcodes)))
        rows = []
        for barcode in new_barcodes:
            row = barcode2row[barcode]
            project_short_name = row[4].split('/')[6]
            rows += [[row[2], row[0], row[0][13:15], row[3], row[4].split('/')[6], project_short_name.split('-')[1], 'TCGA', config['image_type2data_type']['image_type'], 
                      'Clinical', 'file', row[1], 'SVS', 'open', 'open', 'Clinical', row[4], 'true', 'legacy', 'Homo sapien']]
        ISBCGC_database_helper.column_insert(config, rows, image_config['data_table'], image_config['data_columns'], log)
    else:
        log.info('\t\tno rows to insert for biospecimen records')
    if 0 < len(barcode_db):
        log.info('\t\tupdating {} existing data records'.format(len(barcode_db)))
        rows = []
        for barcode in new_barcodes:
            row = barcode2row[barcode]
            rows += [[row[4], row[0], row[row[1]]]]
            ISBCGC_database_helper.update(config, image_config['data_update_template'], log, rows)
    else:
        log.info('\t\tno rows to update for biospecimen records')
    
def process_sanple_image_records(config, image_config, rows, log):
    '''
    based on either the case_barcode (for radiology images) or the sample_barcode (for tissue or diagnostic images),
    creates a new metadata data record in the config-specified metadata data table
    
    parameters:
        config: configuration mappings
        image_config: section of the config file with specific mappings for these image types
        not_barcodes: an output parameter for barcodes that weren't found in the underlying clinical or biospecimen table
        rows: rows selected from the BigQuery table
        log: where to write progress and other messages

        endpoint_type: 'legacy'
        sample_gdc_id: ?
        sample_barcode: row[0]
        case_gdc_id: ?
        case_barcode: row[2]
        program_name: 'TCGA'
        disease_code: project_name.split('-')[1]
        project_short_name: row[4].split('/')[6]
    '''
    log.info('\tchecking samples records')
    barcode2row = dict([(row[0], row) for row in rows])
    samples_select_template = image_config['samples_select_template']
    samples_rows = ISBCGC_database_helper.select(config, samples_select_template % ("'" + "','".join(barcode2row) + "'"), log, [])
    barcode_db = set([samples_row[0] for samples_row in samples_rows])
    new_barcodes = set(barcode2row) - barcode_db
    if 0 < len(new_barcodes):
        log.info('\t\tinserting {} new samples records'.format(len(new_barcodes)))
        rows = []
        for barcode in new_barcodes:
            row = barcode2row[barcode]
            project_short_name = row[4].split('/')[6]
            rows += [['current', row[0], row[0][13:15], row[2], 'TCGA', project_short_name.split('-')[1], project_short_name]]
        ISBCGC_database_helper.column_insert(config, rows, image_config['samples_table'], image_config['biospecimen_columns'], log)
    else:
        log.info('\t\tno rows to insert for samples records')

def process_biospecimen_image_records(config, image_config, rows, log):
    '''
    based on either the sample_barcode (for tissue or diagnostic images),
    creates a new metadata clinical record if necessary in the config-specified metadata clinical table
    
    parameters:
        config: configuration mappings
        image_config: section of the config file with specific mappings for these image types
        rows: rows selected from the BigQuery table
        log: where to write progress and other messages

        endpoint_type: 'legacy'
        sample_gdc_id: ?
        sample_barcode: row[0]
        sample_type: row[0][13:15]
        case_gdc_id: ?
        case_barcode: row[2]
        program_name: 'TCGA'
        disease_code: project_name.split('-')[1]
        project_short_name: row[4].split('/')[6]
    '''
    # get the information from the config mapping
    log.info('\tchecking biospecimen records')
    barcode2row = dict([(row[0], row) for row in rows])
    biospecimen_select_template = image_config['biospecimen_select_template']
    biospecimen_rows = ISBCGC_database_helper.select(config, biospecimen_select_template % ("'" + "','".join(barcode2row) + "'"), log, [])
    barcode_db = set([biospecimen_row[0] for biospecimen_row in biospecimen_rows])
    new_barcodes = set(barcode2row) - barcode_db
    if 0 < len(new_barcodes):
        log.info('\t\tinserting {} new biospecimen records'.format(len(new_barcodes)))
        rows = []
        for barcode in new_barcodes:
            row = barcode2row[barcode]
            project_short_name = row[4].split('/')[6]
            rows += [['legacy', row[0], row[0][13:15], row[2], 'TCGA', project_short_name.split('-')[1], project_short_name]]
            rows += [['current', row[0], row[0][13:15], row[2], 'TCGA', project_short_name.split('-')[1], project_short_name]]
        ISBCGC_database_helper.column_insert(config, rows, image_config['biospecimen_table'], image_config['biospecimen_columns'], log)
    else:
        log.info('\t\tno rows to insert for biospecimen records')

def process_clinical_image_records(config, image_config, rows, log):
    '''
    based on either the case_barcode (for radiology images) or the sample_barcode (for tissue or diagnostic images),
    creates a new metadata data record in the config-specified metadata data table
    
    parameters:
        config: configuration mappings
        image_config: section of the config file with specific mappings for these image types
        not_barcodes: an output parameter for barcodes that weren't found in the underlying clinical or biospecimen table
        rows: rows selected from the BigQuery table
        log: where to write progress and other messages

        endpoint_type: 'legacy'
        case_gdc_id: ?
        case_barcode: row[2]
        program_name: 'TCGA'
        disease_code: project_name.split('-')[1]
        project_short_name: row[4].split('/')[6]
    '''
    # get the information from the config mapping
    log.info('\tchecking clinical records')
    barcode2row = dict([(row[2], row) for row in rows])
    clinical_select_template = image_config['clinical_select_template']
    clinical_rows = ISBCGC_database_helper.select(config, clinical_select_template % ("'" + "','".join(barcode2row) + "'"), log, [])
    barcode_db = set([clinical_row[0] for clinical_row in clinical_rows])
    new_barcodes = set(barcode2row) - barcode_db
    if 0 < len(new_barcodes):
        log.info('\t\tinserting {} new clinical records'.format(len(new_barcodes)))
        
        rows = []
        for barcode in new_barcodes:
            row = barcode2row[barcode]
            project_short_name = row[4].split('/')[6]
            rows += [['legacy', row[2], 'TCGA', project_short_name.split('-')[1], project_short_name]]
            rows += [['current', row[2], 'TCGA', project_short_name.split('-')[1], project_short_name]]
        ISBCGC_database_helper.column_insert(config, rows, image_config['clinical_table'], image_config['clinical_columns'], log)
    else:
        log.info('\t\tno rows to insert for clinical records')

def process_image_records(config, image_config, rows, log):
    '''
    based on either the case_barcode (for radiology images) or the sample_barcode (for tissue or diagnostic images),
    processes the BigQuery record to make sure there is a clinical, biospecimen (and samples), and data record for
    that image
    
    parameters:
        config: configuration mappings
        image_config: section of the config file with specific mappings for these image types
        rows: rows selected from the BigQuery table
        log: where to write progress and other messages

    '''
    # call through each of the tables that have potential inserts or updates
    process_clinical_image_records(config, image_config, rows, log)
    process_biospecimen_image_records(config, image_config, rows, log)
    process_sanple_image_records(config, image_config, rows, log)
    process_data_image_records(config, image_config, rows, log)
    
def process_image_type(config, image_type, log):
    '''
    based on the configuration map loaded from the configFileName, loads the DCC metadata into CloudSQL.  also
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
        # query the big query table
        bq_select_template = image_config['bq_select_template']
        bq_columns = image_config['bq_columns']
        query_results = query_bq_table(bq_select_template.format(','.join(bq_columns)), image_config['use_legacy'], None, log)
        page_token = None
        while True:
            # loop through the big query results
            total_rows, rows, page_token = fetch_paged_results(query_results, image_config['fetch_count'], None, page_token, log)
            # process updates to the metadata data table
            rows = process_image_records(config, image_config, rows, log)
    
            # create inserts into the metadata data that for big query rows that didn't have a match already in the metadata data table
            if not page_token:
                log.info('\t\tchecked total of %s rows' % (total_rows))
                break
    

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
#     process_tissue_images(config, log)

