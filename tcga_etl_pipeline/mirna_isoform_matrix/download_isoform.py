#!/usr/bin/env python

# Copyright 2015, Institute for Systems Biology.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
import sys
import re
from gcloud import storage
import json
import pandas as pd
import time
import sqlite3
from bigquery_etl.execution import process_manager
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.utils.logging_manager import configure_logging
import os.path
#------------------------------------
# parse mirna
#------------------------------------
def download_file(project_id, bucket_name, blobname, outfilename, dummy, log):
#    gcs.download_to_filename("/mnt/datadisk-3/isoform_files/" + Study + "/" + basename(blobname))
    log.info('\tstart download of %s' % outfilename)
    client = storage.Client(project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(blobname)
    blob.download_to_filename(outfilename)
    log.info('\tfinished download of %s' % outfilename)
    return True

def submit_to_queue(queue_df, conn, table_name, log):
    """
     Submit to queue - sqllite database
    """
    queue_df.to_sql(con=conn, name=table_name, if_exists='replace', index=False)
    log.info('\tInserted ' + str(len(queue_df)) + ' records to the task_queue')

#-----------------------------------------------------------------------------
def main(config):

    log_filename = 'etl_download_isoform.log'
    log_name = 'etl_download_isoform'
    log = configure_logging(log_name, log_filename)
    log.info('begin downloading isoform files')
#    etl = util.DataETL("isb-cgc", "isb-cgc-open") # this starts a new connection
    project_id = config['project_id']
    bucket_name = config['buckets']['open']
   
    # connect to bucket to get files 
    gcs = GcsConnector(project_id, bucket_name)
    isoform_file = re.compile("^.*.isoform.quantification.txt.json$")
    data_library = gcs.search_files(search_patterns=['.isoform.quantification.txt'], regex_search_pattern=isoform_file, prefixes=['tcga/intermediary/mirna/isoform/'])
    # we are eliminating bad files - size 0; could be hg18 etc
    data_library.loc[:, 'basefilename'] = data_library['filename'].map(lambda x: os.path.splitext(os.path.basename(x))[0].replace('.json', ''))
    data_library = data_library.query('size > 0')
    

    log.info('\tbegin selecting isoform files from sql-lite isoform db')
    conn = sqlite3.connect(config['mirna_isoform_matrix']['isoform_file_db'])
    sql = 'SELECT * from {0}'.format('task_queue')
    all_files_df = pd.read_sql_query(sql, conn)
    conn.close()
    log.info('\tfinished selecting isoform files')

    log.info('\tbegin reading from down loaded files')
    with open('downloadedfiles.txt') as f:
        lines = f.read().splitlines()
    log.info('\tfinished reading from down loaded files')

    log.info('filter files.\n\tfiles in cloud storage: %s\n\tfiles previously marked to download: %s\n%s\n' % (len(data_library), len(all_files_df), data_library))
    all_files_df = all_files_df[ (all_files_df.DatafileName.isin(data_library.basefilename))]
    all_files_df = all_files_df[ ~ (all_files_df.DatafileName.isin(lines))]
    data_library = all_files_df
    log.info('finished filter files: %s\n%s\n' % (len(data_library), data_library))
    
    conn = sqlite3.connect(config['mirna_isoform_matrix']['isoform_file_db'])
    submit_to_queue(data_library, conn, 'task_queue', log)
    queue_df = data_library

    # restart ETL; this gets the diff; also takes care of errors
    try:
        sql = 'SELECT * from task_queue_status where errors="None"'
        queue_df2 = pd.read_sql_query(sql, conn)
        log.info('\tso far completed: ' % (len(queue_df2)))
        queue_df = queue_df[ ~(queue_df.DatafileNameKey.isin(queue_df2.DatafileNameKey))]
        log.info('\tso far not completed: ' % (len(queue_df)))
    except Exception:
        log.error('\n++++++++++++++++++++++\n\tproblem filtering completed jobs, ignoring\n++++++++++++++++++++++\n')


    # -----------------------------------------------------
    # thread this with concurrent futures
    #------------------------------------------------------
    log.info('\tsubmit jobs to process manager')
    pm = process_manager.ProcessManager(max_workers=200, db='isoform_download', table='task_queue_status', log=log)
    for _, df in data_library.iterrows():
        row = df.to_dict()
        print  row['DatafileName']
        outfilename = config['mirna_isoform_matrix']['isoform_download_dir'] + row['Platform'] + "/" + row['DatafileName'] 
        pm.submit(download_file, project_id, bucket_name, row['DatafileNameKey'], outfilename, '')
        time.sleep(0.2)

    log.info('\tstart process manager completion check')
    pm.start()

    log.info('finished downloading isoform files')

if __name__ == "__main__":
    config = json.load(open(sys.argv[1]))
    main(config)

