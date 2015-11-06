import pandas as pd
import sys
import os
import sqlite3
import json

# python melt_matrix.py hiseq/expn_matrix_mimat_norm_IlluminaHiSeq_miRNASeq.txt IlluminaHiSeq ../config/data_etl.json
# python melt_matrix.py ga/expn_matrix_mimat_norm_IlluminaGA_miRNASeq.txt IlluminaGA ../config/data_etl.json

def melt_matrix(matrix_file, Platform, studies_map, config):
    """
    # melt matrix
    """
    # begin parsing the data
    data_df2 = pd.read_csv(matrix_file, delimiter='\t', header=0)
    data_df2 = data_df2.set_index(["Gene"])
    
    for i,j in data_df2.T.iteritems():
        for k,m in j.iteritems():
           ele = [[ k, i.split(".")[0],i.split(".")[1],  m]]
           aliquot =  k.strip(".hg19")
           SampleBarcode = "-".join(aliquot.split("-")[0:4])
           ParticipantBarcode = "-".join(aliquot.split("-")[0:3])
           SampleTypeLetterCode = config["sample_code2letter"][aliquot.split("-")[3][0:2]]
           Study = studies_map[aliquot].upper()
           print ",".join(map(str, (ParticipantBarcode, SampleBarcode, aliquot, SampleTypeLetterCode,  Study, Platform, i.split(".")[0], i.split(".")[1],  m)))

if __name__ == '__main__':

    matrix_file = sys.argv[1]
    Platform = sys.argv[2]
    config_file = sys.argv[3]

    # get file info
    conn = sqlite3.connect('../etl-mirna-isoform.db')
    sql = 'SELECT * from {0}'.format('task_queue')
    data_library = pd.read_sql_query(sql, conn)
    conn.close()
    
    data_library = data_library.set_index('AliquotBarcode')
    studies_map = data_library['Study'].to_dict()

    melt_matrix(matrix_file, Platform, studies_map, json.load(open(config_file)))


