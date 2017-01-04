'''
Created on Dec 19, 2016

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
from cPickle import dump, load, HIGHEST_PROTOCOL
from datetime import date
import json
import logging
from multiprocessing import Lock
from os import listdir, makedirs, path, remove
import pandas as pd
from shutil import copy
from subprocess import check_call
from sys import argv

from cStringIO import StringIO

from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.transform.tools import cleanup_dataframe

from gdc.etl.isoform_expression_quantification import Isoform_expression_quantification

from util import create_log, flatten_map

lock = Lock()

class miRNA_matrix(Isoform_expression_quantification):
    
    def __init__(self, config):
        '''
        Constructor

        download the mirna isoform files and create the isoform matrix
        
        eventually (next etl run), this should be incorporated into the Isoform expression quantification run.
        
        invoking the main routine will currently do the entire matrix creation 
        '''

    def upload_batch_etl(self, config, outputdir, paths, file2info, project, data_type, log):
        if not config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['only_matrix']:
            super(miRNA_matrix, self).upload_batch_etl(config, outputdir, paths, file2info, project, data_type, log)
        else:
            log.info('not calling upload_batch_etl() for %s:%s' % (project, data_type))
        
        # copy files to common location cross all projects, flattening the directory names into the file names
        input_dir = config['download_base_output_dir'] + '%s/%s/' % (project, data_type)
        common_dir = config['download_base_output_dir'] + config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_subdir']

        log.info('\tcopy files for %s:%s for mirna isoform matrix' % (data_type, project))
        contents = listdir(input_dir)
        for content in contents:
            if path.isdir(input_dir + content):
                files = listdir(input_dir + content)
                for file_name in files:
                    full_name = content + '_' + file_name
                    if path.exists(common_dir + full_name):
                        raise ValueError('file already exists: %s' % (full_name))
                    copy(input_dir + content + '/' + file_name, common_dir + full_name)
        log.info('\tcopied files for %s: %s for mirna isoform matrix' % (data_type, project))
        
        # first time this is called, safe off the file2info, transformed into aliquot2info, for use in finalize
        mapfile_name = project + "_aliquotinfo.txt"
        mapfile_path = config['download_base_output_dir'] + config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_persist_subdir'] + mapfile_name
        if not path.exists(mapfile_path):
            log.info('\tcreate metadata file for %s:%s for mirna isoform matrix' % (data_type, project))
            # create the aliquot centric map
            file_name2info = {}
            for value in file2info.values():
                flattened = flatten_map(value, config['process_files']['data_table_mapping'])[0]
                info = file_name2info.setdefault('_'.join([flattened['file_gdc_id'], flattened['file_name']]), {})
                info['aliquot_barcode'] = flattened['aliquot_barcode']
                info['project_short_name'] = flattened['project_short_name']
                program_name = flattened['program_name']
                info['program_name'] = program_name
                sample_type_code = flattened['aliquot_barcode'][config['sample_code_position'][program_name]['start']:config['sample_code_position'][program_name]['end']]
                info['sample_type_code'] = sample_type_code
                info['file_name'] = flattened['file_name']
                info['file_gdc_id'] = flattened['file_gdc_id']
                info['case_gdc_id'] = flattened['case_gdc_id']
                info['sample_gdc_id'] = flattened['sample_gdc_id']
                info['aliquot_gdc_id'] = flattened['aliquot_gdc_id']
            with open(mapfile_path, 'wb') as mapfile:
                dump(file_name2info, mapfile, protocol = HIGHEST_PROTOCOL)
            log.info('\tsaved metadata file for %s:%s for mirna isoform matrix' % (data_type, project))
    
    def finish_etl(self, config, project, data_type, batch_count, log):
        if not config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['only_matrix']:
            super(miRNA_matrix, self).finish_etl(config, project, data_type, batch_count, log)
        else:
            log.info('not calling finish_etl() for %s:%s' % (project, data_type))
            
    def initialize(self, config, log): 
        common_dir = config['download_base_output_dir'] + config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_subdir']
        mapfile_dir = config['download_base_output_dir'] + config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_persist_subdir']
        with lock:
            if not path.exists(common_dir):
                makedirs(common_dir)
            else:
                files = listdir(common_dir)
                for file_name in files:
                    remove(common_dir + file_name)
    
            if not path.exists(mapfile_dir):
                makedirs(mapfile_dir)
            else:
                files = listdir(mapfile_dir)
                for file_name in files:
                    remove(mapfile_dir + file_name)
    
    def get_aliquot_info(self, config):
        mapfile_path = config['download_base_output_dir'] + config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_persist_subdir']
        mapfiles = listdir(mapfile_path)
        master_file2info = {}
        for mapfile in mapfiles:
            with open(mapfile_path + mapfile, 'rb') as pickle:
                curmap = load(pickle)
            master_file2info.update(curmap)
        return master_file2info

    def melt_matrix(self, matrix_file, platform, file2info, config, log):
        """
        # melt matrix
        """
        log.info('\t\t\tbegin melt matrix: \'%s\'' % (matrix_file))
        # begin parsing the data
        data_df2 = pd.read_csv(matrix_file, delimiter='\t', header=0)
        data_df2 = data_df2.set_index(["Gene"])
        
        # create a StingIO object with this info
        # call utils.convert_file_to_dataframe(buffer, sep=",")
        # call tools.cleanup_dataframe()
        # gcs.convert_df_to_njson_and_upload()
        log.info('\t\t\t\tstart processing saved matrix.  size: %s' % (len(data_df2)))
        mod = int(len(data_df2) / 20)
        count = 0
        total_count = 0
        buf = StringIO()
        buf.write("sample_barcode	mirna_id	mirna_accession	normalized_count	platform	project_short_name	program_name	sample_type_code" +
                  "	file_name	file_gdc_id	aliquot_barcode	case_barcode	case_gdc_id	sample_gdc_id	aliquot_gdc_id\n")
        for i,j in data_df2.T.iteritems():
            for k,m in j.iteritems():
                aliquot =  file2info[k]['aliquot_barcode']
                SampleBarcode = "-".join(aliquot.split("-")[0:4])
                ParticipantBarcode = "-".join(aliquot.split("-")[0:3])
                SampleTypeCode = aliquot.split("-")[3][0:2]
                info = file2info[k]
                line = "\t".join(map(str,(SampleBarcode, i.split(".")[0], i.split(".")[1],  m, platform, info['project_short_name'], info['program_name'], SampleTypeCode, 
                    info['file_name'], info['file_gdc_id'], aliquot, ParticipantBarcode, info['case_gdc_id'], info['sample_gdc_id'], info['aliquot_gdc_id']))) + '\n'
                buf.write(line)
                total_count += 1
            if 0 == count % mod:
                log.info('\t\t\t\t\tprocessed %s lines:\n%s' % (count, line))
                file_name = matrix_file.split('/')[-1] + '_' + count
                log.info('\t\t\t\tsave %s to GCS' % file_name)
                buf.seek(0)
                df = convert_file_to_dataframe(buf)
                df = cleanup_dataframe(df, log)
                gcs = GcsConnector(config['cloud_projects']['open'], config['buckets']['open'])
                gcs.convert_df_to_njson_and_upload(df, config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['gcs_output_path'] + file_name, logparam=log)
                buf = StringIO()
                buf.write("sample_barcode	mirna_id	mirna_accession	normalized_count	platform	project_short_name	program_name	sample_type_code" +
                          "	file_name	file_gdc_id	aliquot_barcode	case_barcode	case_gdc_id	sample_gdc_id	aliquot_gdc_id\n")
            count += 1
        log.info('\t\t\t\tprocessed %s total lines created %s records' % (count, total_count))
                
        log.info('\t\t\t\tcompleted save to GCS')
        log.info('\t\t\tfinished melt matrix')
    

    def load_isoform_matrix(self, config, write_disposition, log):
        bq_dataset = config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['bq_dataset']
        bq_table = config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_bq_table']
        schema_file = config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_schema_file']
        bucket_name = config['buckets']['open']
        object_path = config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['gcs_output_path']
        gcs_file_path = 'gs://' + bucket_name + '/' + object_path[:-1]
        self.load(config['cloud_projects']['open'], [bq_dataset], [bq_table], [schema_file], [gcs_file_path], [write_disposition], 1, log)

    def finalize(self, config, log): 
        log.info('\tstart creating isoform matrix')
        
        log.info('\t\trunning expression_matrix_mimat.pl')
        # TODO: split this into HiSeq and GA platforms when metadata is available at the gdc
        matrix_script = config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_script']
        matrix_adf = config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_adf']
        matrix_output_dir = config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_output_dir']
        common_dir = config['download_base_output_dir'] + config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_subdir']
        check_call([matrix_script, '-m', matrix_adf, '-o', matrix_output_dir, '-p', common_dir, '-n', 'IlluminaHiSeq_miRNASeq'])
        log.info('\t\tcompleted expression_matrix_mimat.pl')
        
        log.info('\t\trunning melt isoform matrix')
        matrix_filename = config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_filename'] % ('IlluminaHiSeq')
        file2info = self.get_aliquot_info(config)
        self.melt_matrix(matrix_output_dir + '/' + matrix_filename, 'IlluminaHiSeq', file2info, config, log)
        log.info('\t\tcompleted melt isoform matrix')
        
        log.info('\t\trunning load isoform matrix into bigquery')
        write_disposition = 'WRITE_EMPTY'
        self.load_isoform_matrix(config, write_disposition, log)
        log.info('\t\tcompleted load isoform matrix into bigquery')
        
        log.info('\tfinished creating isoform matrix')
        
def main(config_filename):
    try:
        with open(config_filename) as configFile:
            config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_gdc_upload_run/'
        log_name = create_log(log_dir, 'gdc_upload')
        log = logging.getLogger(log_name)
        log.info('begin melting miRNA isoform matrix')
        test_file = './tcga_etl_pipeline/mirna_isoform_matrix/hiseq/expn_matrix_mimat_norm_IlluminaHiSeq_miRNASeq_small.txt'
        log.info('finished melting miRNA isoform matrix')
    except:
        raise


if __name__ == '__main__':
    main(argv[1])