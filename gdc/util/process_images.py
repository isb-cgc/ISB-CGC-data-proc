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
import sys
from time import sleep

from bq_wrapper import fetch_paged_results, query_bq_table
from isbcgc_cloudsql_model import ISBCGC_database_helper

def verify_barcodes_filenames(config, program, image_config, image_type, rows, log):
    barcode_bq = set()
    if 'Radiology' == image_type:
        barcode_bq = set([(row[0], row[1].split('/')[-1]) for row in rows])
    else:
        barcode_bq = set([(row[0], row[1]) for row in rows])
    
    data_select_template = image_config[image_type]['data_verify_select_template']
    data_rows = ISBCGC_database_helper.select(config, data_select_template, log, [])
    barcode_db = set([(data_row[0], data_row[1]) for data_row in data_rows])
    
    log.info('\n\tBQ length:{}\tSQL length:{}\n\tbarcode/file combinations in BQ and not SQL:{}\n\tbarcode/file combinations in SQL and not BQ:{}'
             .format(len(barcode_bq), len(barcode_db), len(barcode_bq-barcode_db), len(barcode_db-barcode_bq)))
    if 0 < len(barcode_bq - barcode_db):
        log.info('\n\tfirst barcodes in bq only: {}'.format(list(barcode_bq - barcode_db)[:20]))
    if 0 < len(barcode_db - barcode_bq):
        log.info('first barcodes in sql only: {}'.format(list(barcode_db - barcode_bq)[:20]))

def process_data_availability_records(config, program, image_config, image_type, rows, log):
    '''
    NOTE: this assumes at the start of the run, that no sample barcode is associated with and Image ISB label.  it is
    possible that a sample_barcode is added and then a subsequent batch will contain that barcode associated with 
    more files
    '''
    if 'Radiology' == image_type:
        return
    
    stmt = 'select metadata_data_type_availability_id from TCGA_metadata_data_type_availability where %s = isb_label and %s = genomic_build'
    image_rows = ISBCGC_database_helper.select(config, stmt, log, ["Pathology_Image", "HG19"])
    if 1 != len(image_rows):
        raise ValueError('unexpected number of rows returned for availability id {}'.format(image_rows))
    data_type_availability_id = image_rows[0][0]
    
    # get the current count for a barcode based on previous runs
    stmt = 'select sample_barcode, count(*) from {}_metadata_sample_data_availability where metadata_data_type_availability_id = %s group by 1'.format(program)
    db_rows = ISBCGC_database_helper.select(config, stmt, log, [data_type_availability_id,])
    db_barcode2count = {}
    for db_row in db_rows:
        db_barcode2count[db_row[0]] = db_row[1]
    if len(db_rows) != len(set(db_rows)):
        raise ValueError('sample_barcode should only appear once per isb_label')
    
    # get the current count from the current batch for each sample barcode
    barcode2count = {}
    for row in rows:
        barcode2count[row[0]] = barcode2count.setdefault(row[0], 0) + 1
        
    new_barcodes = set(barcode2count) - set(db_rows)
    new_params = []
    for new_barcode in new_barcodes:
        new_params += [[data_type_availability_id, new_barcode, barcode2count[new_barcode]]]
    ISBCGC_database_helper.column_insert(config, new_params, 'TCGA_metadata_sample_data_availability', ['metadata_data_type_availability_id', 'sample_barcode', 'count'], log)
    
    update_barcodes = set(barcode2count) & set(db_rows)
    update_params = []
    for update_barcode in update_barcodes:
        update_params += [[barcode2count[update_barcode] + db_barcode2count[update_barcode], data_type_availability_id, update_barcode]]
    stmt = 'update TCGA_metadata_sample_data_availability set count = %s where metadata_data_type_availability_id = %s and sample_barcode = %s'
    ISBCGC_database_helper.update(config, stmt, log, update_params, False)

na_barcodes = {}
def setup_radiology_row(row, program, image_config, image_type):
    project_short_name = row[1].split('/')[5]
    if 'NA' == project_short_name:
        na_barcodes[row[1].split('/')[6]] = na_barcodes.setdefault(row[1].split('/')[6], 0) + 1
        return None
    try:
        dbrow = [row[0], project_short_name, project_short_name.split('-')[1], program, image_config['image_tag2data_type'][image_type], 
                  'Clinical', 'file', row[1].split('/')[-1], 'ZIP', 'open', 'open', 'Clinical', row[1], 'true', 'legacy', 'Homo sapien']
    except:
        raise
    return [dbrow]

def setup_svs_row(row, program, image_config, image_type):
    project_short_name = row[4].split('/')[6]
    return [[row[2], row[0], row[0][13:15], row[3], row[4].split('/')[6], project_short_name.split('-')[1], program, image_config['image_tag2data_type'][image_type], 
              'Clinical', 'file', row[1], 'SVS', 'open', 'open', 'Clinical', row[4], 'true', 'legacy', 'Homo sapien']]

def process_data_image_records(config, program, image_config, image_type, rows, log):
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
    barcode2rows = {}
    for row in rows:
        if 'Radiology' == image_type:
            bcrows = barcode2rows.setdefault((row[0], row[1].split('/')[-1]), [])
            bcrows += [row]
        else:
            bcrows = barcode2rows.setdefault((row[0], row[1]), [])
            bcrows += [row]
    data_select_template = image_config[image_type]['data_select_template']
    if 0 == len(data_select_template):
        barcode_db = set()
    else:
        barcodes = ''
        for barcode, file_name in barcode2rows:
            barcodes += '("{}", "{}")'.format(barcode, file_name)
        barcodes = barcodes[:-1]
        data_rows = ISBCGC_database_helper.select(config, data_select_template % (','.join('("{}", "{}")'.format(barcode, file_name) for barcode, file_name in barcode2rows)), log, [])
        barcode_db = set([(data_row[0], data_row[1]) for data_row in data_rows])

    new_barcodes = set(barcode2rows) - barcode_db
    if 0 < len(new_barcodes):
        log.info('\t\tinserting {} new data records'.format(len(new_barcodes)))
        db_rows = []
        for barcode in new_barcodes:
            for row in barcode2rows[barcode]:
                row_method = image_config['image_tag2row_method'][image_type]
                next_row = getattr(sys.modules[__name__], row_method)(row, program, image_config, image_type)
                if next_row is not None:
                    db_rows += next_row
        ISBCGC_database_helper.column_insert(config, db_rows, image_config['data_table'], image_config[image_type]['data_columns'], log)
    else:
        log.info('\t\tno rows to insert for data records')

    if 0 < len(barcode_db):
        log.info('\t\tupdating {} existing data records'.format(len(barcode_db)))
        rows = []
        for barcode in barcode_db:
            for row in barcode2rows[barcode]:
                if 'Radiology' == image_type:
                    rows += [[row[1], row[0], row[1].split('/')[5], image_config['image_tag2data_type'][image_type]]]
                else:
                    rows += [[row[4], row[0], row[1], image_config['image_tag2data_type'][image_type]]]
        ISBCGC_database_helper.update(config, image_config[image_type]['data_update_template'], log, rows)
    else:
        log.info('\t\tno rows to update for data records')
    
def process_sanple_image_records(config, program, image_config, image_type, rows, log):
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
    barcode2row = dict([(row[image_config[image_type]['sample_barcode_index']], row) for row in rows])
    log.info('\tbacodes--{}:{}'.format(len(set(barcode2row)), len(barcode2row)))
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
        ISBCGC_database_helper.column_insert(config, rows, image_config['samples_table'], image_config['samples_columns'], log)
    else:
        log.info('\t\tno rows to insert for samples records')

def process_biospecimen_image_records(config, program, image_config, image_type, rows, log):
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
    barcode2row = dict([(row[image_config[image_type]['sample_barcode_index']], row) for row in rows])
    log.info('\tbacodes--{}:{}'.format(len(set(barcode2row)), len(barcode2row)))
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

def process_clinical_image_records(config, program, image_config, image_type, rows, log):
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
    log.info('\tchecking clinical records.')
    barcode2row = dict([(row[image_config[image_type]['case_barcode_index']], row) for row in rows])
    log.info('\tbacodes--{}:{}'.format(len(set(barcode2row)), len(barcode2row)))
    clinical_select_template = image_config['clinical_select_template']
    clinical_rows = ISBCGC_database_helper.select(config, clinical_select_template % ("'" + "','".join(barcode2row) + "'"), log, [])
    barcode_db = set([clinical_row[0] for clinical_row in clinical_rows])
    new_barcodes = set(barcode2row) - barcode_db
    if 0 < len(new_barcodes):
        log.info('\t\tinserting {} new clinical records'.format(len(new_barcodes)))
        
        rows = []
        for barcode in new_barcodes:
            row = barcode2row[barcode]
            if 'Radiology' == image_type:
                project_short_name = row[1].split('/')[5]
            else:
                project_short_name = row[4].split('/')[6]
            if 'NA' == project_short_name:
                continue
            rows += [['legacy', row[2], program, project_short_name.split('-')[1], project_short_name]]
            rows += [['current', row[2], program, project_short_name.split('-')[1], project_short_name]]
        ISBCGC_database_helper.column_insert(config, rows, image_config['clinical_table'], image_config['clinical_columns'], log)

    else:
        log.info('\t\tno rows to insert for clinical records')

def process_image_records(config, program, image_config, image_type, rows, log):
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
    process_clinical_image_records(config, program, image_config, image_type, rows, log)
    sleep(30)
    if image_type != 'Radiology':
        process_biospecimen_image_records(config, program, image_config, image_type, rows, log)
        sleep(30)
        process_sanple_image_records(config, program, image_config, image_type, rows, log)
        sleep(30)
    process_data_image_records(config, program, image_config, image_type, rows, log)
    sleep(30)
    process_data_availability_records(config, program, image_config, image_type, rows, log)
        
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
        image_config = config[program]['process_files']['images']
        # query the big query table
        bq_select_template = image_config[image_type]['bq_select_template']
        bq_columns = image_config[image_type]['bq_columns']
        query_results = query_bq_table(bq_select_template.format(','.join(bq_columns)), image_config[image_type]['use_legacy'], None, log)
        page_token = None
        combined_rows = []
        while True:
            # loop through the big query results
            total_rows, rows, page_token = fetch_paged_results(query_results, image_config['fetch_count'], None, page_token, log)
            combined_rows += rows 
            # process updates to the metadata data table
            rows = process_image_records(config, program, image_config, image_type, rows, log)
    
            # create inserts into the metadata data that for big query rows that didn't have a match already in the metadata data table
            if not page_token:
                log.info('\tchecked total of %s rows' % (total_rows))
                break
        verify_barcodes_filenames(config, program, image_config, image_type, combined_rows, log)
    

def process_radiology_images(config, log):
    '''
    process the radiology images based on rows in metadata.TCGA_radiology_images
    
    parameters:
        config: configuration mappings
        log: where to write progress and other messages
    '''
    log.info('start process_radiology_images()')
    process_image_type(config, 'Radiology', log)
    log.info('\tbarcodes({}) for NA projects:\n\t\t{}'.format(len(na_barcodes), 
        '\n\t\t'.join('{}\t{}'.format(na_barcode, count) for na_barcode, count in na_barcodes.iteritems())))
    log.info('finished process_radiology_images()')

def process_diagnostic_images(config, log):
    '''
    process the diaqgnostic images based on rows in metadata.TCGA_slide_images where slide_id begins with 'DX'
    
    parameters:
        config: configuration mappings
        log: where to write progress and other messages
    '''
    log.info('start process_diagnostic_images()')
    process_image_type(config, 'Diagnostic', log)
    log.info('finished process_diagnostic_images()')

def process_tissue_images(config, log):
    '''
    process the tissue images based on rows in metadata.TCGA_slide_images where slide_id doesn't begin with 'DX'
    
    parameters:
        config: configuration mappings
        log: where to write progress and other messages
    '''
    log.info('start process_tissue_images()')
    process_image_type(config, 'Tissue', log)
    log.info('finished process_tissue_images()')

def process_images(config, log):
    '''
    process the image files based on the BigQuery tables metadata.TCGA_radiology_images and metadata.TCGA_slide_images
    
    parameters:
        config: configuration mappings
        log: where to write progress and other messages
    '''
    process_radiology_images(config, log)
    process_diagnostic_images(config, log)
    process_tissue_images(config, log)
