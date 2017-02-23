'''
Created on Jun 28, 2016

Copyright 2016, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License ");
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

from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.utils.gcutils import read_mysql_query
from gdc.etl.etl import Etl
from gdc.model.isbcgc_cloudsql_gdc_model import ISBCGC_database_helper
from gdc.util.gdc_util import get_map_rows, save2db
from util import close_log, create_log

def etl(config, log):
    log.info('\tbegin creating annotation big query table')
    etl_config = config['TCGA']['process_annotations']['etl']
    columns = ', '.join(etl_config['use_columns'])
    sql = etl_config['sql_template'] % (columns)
    log.info("\t\tselect annotation records from db")
    metadata_df = read_mysql_query(config['cloudsql']['host'], config['cloudsql']['db'], config['cloudsql']['user'], config['cloudsql']['passwd'], sql)
    log.info("\t\tFound {0} rows, columns." .format(str(metadata_df.shape)))

    log.info("\tupload data to GCS.")
    project_id = config['cloud_projects']['open']
    bucket_name = config['buckets']['open']
    gcs_file_path = 'gs://' + bucket_name + '/' + config['buckets']['folders']['base_run_folder'] + 'etl/annotation'
    gcs = GcsConnector(project_id, bucket_name)
    gcs.convert_df_to_njson_and_upload(metadata_df, gcs_file_path + 'annotation.json')
    
    log.info('create the BigQuery table')
    Etl().load(project_id, [etl_config['bq_dataset']], [etl_config['bq_table']], [etl_config['schema_file']], [gcs_file_path], [etl_config['write_dispositions']], 1, log)
    
    log.info('\tfinished creating annotation big query table')

def associate_metadata2annotation(config, program_name, build, log):
    # now save the associations
    if not config['update_cloudsql']:
        return
    
    data_associate_statements = [
        "insert into %s_metadata_annotation2data_%s " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_data_%s s on " \
            "s.aliquot_barcode = a.entity_barcode " \
        "where status = 'Approved'",

        "insert into %s_metadata_annotation2data_%s " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_data_%s s on " \
            "left(s.aliquot_barcode, 15) = a.entity_barcode " \
        "where status = 'Approved'",


        "insert into %s_metadata_annotation2data_%s " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_data_%s s on " \
            "left(s.aliquot_barcode, 19) = a.entity_barcode " \
        "where status = 'Approved'",

        "insert into %s_metadata_annotation2data_%s " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_data_%s s on " \
            "left(s.aliquot_barcode, 20) = a.entity_barcode " \
        "where status = 'Approved'",

        "insert into %s_metadata_annotation2data_%s " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_data_%s s on " \
            "left(s.aliquot_barcode, 23) = a.entity_barcode " \
        "where status = 'Approved'",

        "insert into %s_metadata_annotation2data_%s " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_data_%s s on " \
            "left(s.aliquot_barcode, 24) = a.entity_barcode " \
        "where status = 'Approved'",

        "insert into %s_metadata_annotation2data_%s " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_data_%s s on " \
            "left(s.aliquot_barcode, 27) = a.entity_barcode " \
        "where status = 'Approved'",

        "insert into %s_metadata_annotation2data_%s " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_data_%s s on " \
            "s.sample_barcode = a.entity_barcode " \
        "where status = 'Approved'",

        "insert into %s_metadata_annotation2data_%s " \
            "(metadata_annotation_id, metadata_data_id) " \
        "select a.metadata_annotation_id, s.metadata_data_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_data_%s s on " \
            "s.case_barcode = a.entity_barcode " \
        "where status = 'Approved'",
    ]
    
    for statement in data_associate_statements:
        try:
            ISBCGC_database_helper.update(config, statement % (program_name, build, build), log, [[]], True)
        except:
            log.exception('problem executing:\n\t%s' % (statement % (program_name, build, build)))

    bio_associate_statements = [
        "insert into %s_metadata_annotation2clinical " \
            "(metadata_annotation_id, metadata_clinical_id) " \
        "select a.metadata_annotation_id, s.metadata_clinical_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_clinical s on " \
            "s.case_barcode = a.entity_barcode " \
        "where status = 'Approved'",

        "insert into %s_metadata_annotation2biospecimen "  \
            "(metadata_annotation_id, metadata_biospecimen_id) " \
        "select a.metadata_annotation_id, s.metadata_biospecimen_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_biospecimen s on " \
            "0 < instr(s.sample_barcode, a.entity_barcode) " \
        "where status = 'Approved'",

        "insert into %s_metadata_annotation2samples " \
            "(metadata_annotation_id, metadata_samples_id) " \
        "select a.metadata_annotation_id, s.metadata_samples_id " \
        "from TCGA_metadata_annotation a join TCGA_metadata_samples s on " \
            "0 < instr(s.sample_barcode, a.entity_barcode) " \
        "where status = 'Approved'"
    ]
    
    for statement in bio_associate_statements:
        try:
            ISBCGC_database_helper.update(config, statement % (program_name), log, [[]], True)
        except:
            log.exception('problem executing:\n\t%s' % (statement % (program_name)))

def call_metadata2annotation(config, log):
    # now save the associations
    for program_name in config['program_names_for_annotation']:
        for build in config['genomic_builds_for_annotation']:
            associate_metadata2annotation(config, program_name, build, log)
    
def get_filter():
    return {}

def add_barcodes(annotation2info):
    for info in annotation2info.itervalues():
        if 28 == len(info['entity_submitter_id']):
            info['aliquot_barcode'] = info['entity_submitter_id']
        if 15 < len(info['entity_submitter_id']):
            info['sample_barcode'] = info['entity_submitter_id'][:16]

def process_annotations(config, endpt_type, log_dir):
    try:
        log_name = create_log(log_dir, '%s_annotations' % (endpt_type))
        log = logging.getLogger(log_name)

        for program_name in config['program_names_for_annotation']:
            log.info('begin process_annotations for %s' % (program_name))
            annotation2info = get_map_rows(config, endpt_type, 'annotation', program_name, get_filter(), log)
            add_barcodes(annotation2info)
            save2db(config, endpt_type, '%s_metadata_annotation' % program_name, annotation2info, config['%s' % (program_name)]['process_annotations']['annotation_table_mapping'], log)
            log.info('finished process_annotations %s' % (program_name))

        return annotation2info
    except:
        log.exception('problem processing annotations:')
        raise
    finally:
        close_log(log)
