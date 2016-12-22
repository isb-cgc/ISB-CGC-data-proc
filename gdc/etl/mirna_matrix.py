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

from util import create_log

lock = Lock()

class miRNA_matrix(Isoform_expression_quantification):
    
    def __init__(self, config):
        '''
        Constructor

        download the mirna isoform files and create the isoform matrix
        
        eventually (next etl run), this should be incorporated into the Isoform expression quantification run.
        
        invoking the main routine will currently do the entire matrix creation 
        '''

    def melt_matrix(self, matrix_file, Platform, studies_map, config, log):
        """
        # melt matrix
        """
        log.info('\tbegin melt matrix: \'%s\'' % (matrix_file))
        # begin parsing the data
        data_df2 = pd.read_csv(matrix_file, delimiter='\t', header=0)
        data_df2 = data_df2.set_index(["Gene"])
        
        # create a StingIO object with this info
        # call utils.convert_file_to_dataframe(buffer, sep=",")
        # call tools.cleanup_dataframe()
        # gcs.convert_df_to_njson_and_upload()
        log.info('\t\tstart processing saved matrix.  size: %s' % (len(data_df2)))
        mod = int(len(data_df2) / 20)
        count = 0
        buf = StringIO()
        buf.write("case_barcode	sample_barcode	aliquot_barcode	sample_type_letter_code	project_short_name	platform	mirna_id	mirna_accession	normalized_count\n")
        for i,j in data_df2.T.iteritems():
            if 0 == count % mod:
                log.info('\t\t\tprocessed %s lines' % (count))
            count += 1
            for k,m in j.iteritems():
                aliquot =  k.strip(".mirbase20")
                aliquot =  aliquot.strip(".hg19")
                SampleBarcode = "-".join(aliquot.split("-")[0:4])
                ParticipantBarcode = "-".join(aliquot.split("-")[0:3])
                SampleTypeLetterCode = config["sample_code2letter"][aliquot.split("-")[3][0:2]]
                Study = studies_map[aliquot].upper()
                buf.write("\t".join(map(str,(ParticipantBarcode, SampleBarcode, aliquot, SampleTypeLetterCode,  Study, Platform, i.split(".")[0], i.split(".")[1],  m))) + '\n')
        log.info('\t\tprocessed %s total lines' % (count))
                
        file_name = matrix_file.split('/')[-1]
        log.info('\t\tsave %s to GCS' % file_name)
        buf.seek(0)
        df = convert_file_to_dataframe(buf)
        df = cleanup_dataframe(df)
        gcs = GcsConnector(config['project_id'], config['buckets']['open'])
        gcs.convert_df_to_njson_and_upload(df, config['mirna_isoform_matrix'][Platform]['output_dir'] + file_name)
        log.info('\t\tcompleted save to GCS')
        log.info('\tfinished melt matrix')
    
    def upload_batch_etl(self, config, outputdir, paths, file2info, project, data_type, log):
        if not config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['only_matrix']:
            super(miRNA_matrix, self).upload_batch_etl(config, outputdir, paths, file2info, project, data_type, log)
            
        # copy files to common location cross all projects, flattening the directory names into the file names
        input_dir = config['download_base_output_dir'] + '%s/%s/' % (project, data_type)
        common_dir = config['download_base_output_dir'] + config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_subdir']

        contents = listdir(input_dir)
        for content in contents:
            if path.isdir(input_dir + content):
                files = listdir(input_dir + content)
                for file_name in files:
                    full_name = content + '_' + file_name
                    if path.exists(common_dir + full_name):
                        raise ValueError('file already exists: %s' % (full_name))
                    copy(input_dir + content + '/' + file_name, common_dir + full_name)
    
    def finish_etl(self, config, project, data_type, batch_count, log):
        if not config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['only_matrix']:
            super(miRNA_matrix, self).finish_etl(config, project, data_type, batch_count, log)
            
    def initialize(self, config, log): 
        common_dir = config['download_base_output_dir'] + config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_subdir']
        with lock:
            if not path.exists(common_dir):
                makedirs(common_dir)
            else:
                files = listdir(common_dir)
                for file_name in files:
                    remove(common_dir + file_name)
    
    def finalize(self, config, file2info, log): 
        log.info('\tstart creating isoform matrix')
        
        log.info('\t\trunning expression_matrix_mimat.pl')
        common_dir = config['download_base_output_dir'] + config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_subdir']
        check_call(['./tcga_etl_pipeline/mirna_isoform_matrix/expression_matrix_mimat.pl', '-m', './tcga_etl_pipeline/mirna_isoform_matrix/tcga_mirna_bcgsc_hg19.adf', 
                    '-o', './tcga_etl_pipeline/mirna_isoform_matrix/hiseq', '-p', common_dir, '-n' 'IlluminaHiSeq_miRNASeq])'])
        log.info('\t\tcompleted expression_matrix_mimat.pl')

        log.info('\tfinished creating isoform matrix')
        
def main(config_filename):
    try:
        with open(config_filename) as configFile:
            config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_gdc_upload_run/'
        log_name = create_log(log_dir, 'gdc_upload')
        log = logging.getLogger(log_name)
        log.info('begin creating miRNA isoform matrix')

        log.info('finished creating miRNA isoform matrix')
    except:
        raise


if __name__ == '__main__':
    main(argv[1])