import sys
import re
import json
import os
import json
from StringIO import StringIO
from gcloud import storage
import logging
from os.path import basename
import pprint
from collections import defaultdict
import pandas as pd
import concurrent.futures
import time
import tempfile
from pandas import ExcelWriter
import sqlite3
from bigquery_etl.execution import process_manager
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
import os.path
#------------------------------------
# parse mirna
#------------------------------------
def download_file(project_id, bucket_name, blobname, outfilename, dummy):
#    gcs.download_to_filename("/mnt/datadisk-3/isoform_files/" + Study + "/" + basename(blobname))
    print 'start'
    client = storage.Client(project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(blobname)
    blob.download_to_filename(outfilename)
    print 'Donwload'
    return True

def submit_to_queue(queue_df, conn, table_name):
    """
     Submit to queue - sqllite database
    """
    queue_df.to_sql(con=conn, name=table_name, if_exists='replace', index=False)
    print 'Inserted ' + str(len(queue_df)) + ' records to the task_queue'

#-----------------------------------------------------------------------------
def main(config):

#    etl = util.DataETL("isb-cgc", "isb-cgc-open") # this starts a new connectioni
    project_id = config['project_id']
    bucket_name = config['buckets']['open']
   
    # connect to bucket to get files 
    gcs = GcsConnector(project_id, bucket_name)
    isoform_file = re.compile("^.*.isoform.quantification.txt.json$")
    data_library = gcs.search_files(search_patterns=['.isoform.quantification.txt'], regex_search_pattern=isoform_file, prefixes=['tcga/intermediary/mirna/isoform/'])
    # we are eliminating bad files - size 0; could be hg18 etc
    data_library.loc[:, 'basefilename'] = data_library['filename'].map(lambda x: os.path.splitext(os.path.basename(x))[0].replace('.json', ''))
    data_library = data_library.query('size > 0')
    

    conn = sqlite3.connect('../etl-mirna-isoform.db')
    sql = 'SELECT * from {0}'.format('task_queue')
    all_files_df = pd.read_sql_query(sql, conn)
    conn.close()

    with open('downloadedfiles.txt') as f:
        lines = f.read().splitlines()

    all_files_df = all_files_df[ (all_files_df.DatafileName.isin(data_library.basefilename))]
    all_files_df = all_files_df[ ~ (all_files_df.DatafileName.isin(lines))]
    data_library = all_files_df
    print data_library
    
    conn = sqlite3.connect('etl-isoform-download.db')
    submit_to_queue(data_library, conn, 'task_queue')
    queue_df = data_library

     # restart ETL; this gets the diff; also takes care of errors
    try:
        sql = 'SELECT * from task_queue_status where errors="None"'
        queue_df2 = pd.read_sql_query(sql, conn)
        print 'completed: ', len(queue_df2)
        queue_df = queue_df[ ~(queue_df.DatafileNameKey.isin(queue_df2.DatafileNameKey))]
        print 'Not completed: ',  len(queue_df)
    except Exception, e:
        pass


    # -----------------------------------------------------
    # thread this with concurrent futures
    #------------------------------------------------------
    pm = process_manager.ProcessManager(max_workers=200, db='isoform_download', table='task_queue_status')
    for i, df in data_library.iterrows():
        row = df.to_dict()
        print  row['DatafileName']
        outfilename = "/mnt/datadisk-3/isoform_files/" + row['Platform'] + "/" + row['DatafileName'] 
        future = pm.submit(download_file, project_id, bucket_name, row['DatafileNameKey'], outfilename, '')
        time.sleep(0.2)

    pm.start()


if __name__ == "__main__":
    config = json.load(open(sys.argv[1]))
    main(config)

