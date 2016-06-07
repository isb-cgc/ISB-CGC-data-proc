import pandas as pd
import sys
import os
import sqlite3
import json

from cStringIO import StringIO

from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.transform.tools import cleanup_dataframe
from bigquery_etl.utils.logging_manager import configure_logging

# python mirna_isoform_matrix/melt_matrix.py mirna_isoform_matrix/hiseq/expn_matrix_mimat_norm_IlluminaHiSeq_miRNASeq.txt IlluminaHiSeq config/data_etl.json
# python mirna_isoform_matrix/melt_matrix.py mirna_isoform_matrix/ga/expn_matrix_mimat_norm_IlluminaGA_miRNASeq.txt IlluminaGA config/data_etl.json

def melt_matrix(matrix_file, Platform, studies_map, config, log):
    """
    # melt matrix
    """
    log.info('\tbegin melt matrix')
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

if __name__ == '__main__':

    matrix_file = sys.argv[1]
    Platform = sys.argv[2]
    config_file = sys.argv[3]
    log_filename = 'etl_upload_isoform_matrix_%s.log' % (Platform)
    log_name = 'etl_upload_isoform_matrix'
    log = configure_logging(log_name, log_filename)
    log.info('begin uploading isoform %s matrix' % (Platform))

    try:
        config = json.load(open(config_file))
        # get file info
        conn = sqlite3.connect(config['mirna_isoform_matrix']['isoform_file_db'])
        sql = 'SELECT * from {0}'.format('task_queue')
        data_library = pd.read_sql_query(sql, conn)
        conn.close()
        
        data_library = data_library.set_index('AliquotBarcode')
        studies_map = data_library['Study'].to_dict()
    
        melt_matrix(matrix_file, Platform, studies_map, config, log)
    except Exception as e:
        log.exception('problem uploading matrix for %s' % (Platform))
        raise e
    log.info('finished uploading isoform %s matrix' % (Platform))

