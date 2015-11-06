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

"""Pipeline script
"""
import os
import json
import sys
import protein.extract
import mirna.mirna.extract
import mirna.isoform.extract
import methylation.extract
import cnv.extract
import mrna.bcgsc.extract
import mrna.unc.extract
import protein.transform
import mirna.mirna.transform
import mirna.isoform.transform
import methylation.transform
import cnv.transform
import mrna.bcgsc.transform
import mrna.unc.transform
import sqlite3
import time
from bigquery_etl.execution import process_manager
from bigquery_etl.tests import tests
import pandas as pd
import argparse
import random
from bigquery_etl.utils.logging_manager import configure_logging

# extract functions
extract_functions = {
    'protein': protein.extract.identify_data,
    'mirna_mirna': mirna.mirna.extract.identify_data,
    'mirna_isoform': mirna.isoform.extract.identify_data,
    'methylation': methylation.extract.identify_data,
    'cnv': cnv.extract.identify_data,
    'mrna_bcgsc': mrna.bcgsc.extract.identify_data,
    'mrna_unc':mrna.unc.extract.identify_data
}
# transform functions
transform_functions = {
    'protein': protein.transform.parse_protein,
    'mirna_mirna':  mirna.mirna.transform.parse_mirna,
    'mirna_isoform': mirna.isoform.transform.parse_isoform,
    'methylation': methylation.transform.parse_methylation,
    'cnv': cnv.transform.parse_cnv,
    'mrna_bcgsc': mrna.bcgsc.transform.parse_bcgsc,
    'mrna_unc': mrna.unc.transform.parse_unc
}

def submit_to_queue(queue_df, conn, table_name):
    """
     Submit to queue - sqllite database
    """
    queue_df.to_sql(con=conn, name=table_name, if_exists='replace', index=False)
    print 'Inserted ' + str(len(queue_df)) + ' records to the task_queue'


def validate_and_get_diff(conn, queue_df, table_task_queue_status):

    sql = 'SELECT * FROM sqlite_master WHERE name ="{0}" and type="table";'.format(table_task_queue_status)
    if len(pd.read_sql_query(sql, conn)) == 1:
        sql = 'SELECT * from task_queue_status where errors="None"'
        queue_status_df = pd.read_sql_query(sql, conn)
        print 'completed: ', len(queue_status_df)
        queue_df = queue_df[~(queue_df.DatafileNameKey.isin(queue_status_df.DatafileNameKey))]
        print 'Not completed: ', len(queue_df)
    else:
        queue_status_df = pd.DataFrame({})
        print 'completed: ', len(queue_status_df)
        print 'Not completed: ', len(queue_df)
    return queue_df

def main(datatype, config_file, max_workers, dry_run, create_new, debug):
    """
    Pipeline
    """
    # config params
    config = json.load(open(config_file))
    project_id = config['project_id']
    bucket_name = config['buckets']['open']
    table_task_queue = 'task_queue'
    table_task_queue_status = 'task_queue_status' 
    db_filename = 'etl-{0}.db'.format(datatype)
    log_filename = 'etl_{0}.log'.format(datatype)
    log_name = 'etl_{0}'.format(datatype)

    # check if the table exists and issue warning
    if os.path.exists(db_filename):
       log.warning('Using the already available database file - {0}'.format(db_filename))
       time.sleep(2)
    
    # connect to the database
    conn = sqlite3.connect(db_filename, check_same_thread=False)
   
    #-------------------------------
    # Submit to task queue
    #-------------------------------
    print "="*30 + "\nQuerying Google Cloud SQL metadata_data table"
    queue_df = extract_functions[datatype](config)
    submit_to_queue(queue_df, conn, table_task_queue)

    #--------------
    # Tests
    #--------------
    tests.assert_notnull_property(queue_df, columns_list=['SampleTypeCode', 'SampleTypeLetterCode',\
                     'Study', 'Platform', 'SampleBarcode', 'OutDatafileNameKey',\
                                         'ParticipantBarcode', 'DatafileNameKey', 'AliquotBarcode'])

    if create_new:
         # delete the old queue(task_queue_status) and re-run
        conn.execute('DROP TABLE IF EXISTS {0}'.format(table_task_queue_status))

    # Validate and get diff ; restart ETL; also takes care of errors
    queue_df = validate_and_get_diff(conn, queue_df, table_task_queue_status)

    if debug:
        # debug mode runs top 30 rows
        log.debug('Running in debug mode (first 30 records)')
        queue_df = queue_df.head(30)

    if dry_run:
        sys.exit()


    #--------------------------------------------
    # Execution
    #------------------------------------------------------
    pmr = process_manager.ProcessManager(max_workers=max_workers, db=db_filename, table=table_task_queue_status)
    for index, row in queue_df.iterrows():
        metadata = row.to_dict()
        inputfilename = metadata['DatafileNameKey']
        outputfilename = metadata['OutDatafileNameKey']
        # transform
        #transform_functions[datatype]( project_id, bucket_name,\
        #                inputfilename, outputfilename, metadata)
        future = pmr.submit(transform_functions[datatype], project_id, bucket_name,\
                        inputfilename, outputfilename, metadata)
    
        time.sleep(0.1 + 0.5  * random.random())
        if index % 100 == 0:
            time.sleep(5)

    pmr.start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'datatype', 
        help='Name of the datatype to run ETL pipeline; ex: protein, maf, mirna_isoform')
    parser.add_argument(
        'config_file', 
        help='Path to the config file for the job')
    parser.add_argument(
        '-w', '--max_workers',
        help='Number of threads to execute calls (default: 100)',
        type=int,
        default=100)
    parser.add_argument(
        '--dry_run',
        help='Doesnt run the job, just returns statistics about the job such as how many files are\
              in the queue, total errors, files in the bucket etc. (default: False)',
        action='store_true',
        default=False)
    parser.add_argument(
        '--create_new',
        help='Set create disposition to True. This deletes old task queue, db etc\
                WARNING: This deletes old files, database, task queue etc.',
        action='store_true',
        default=False)
    parser.add_argument(
        '--debug',
        help='Runs the pipeline on first 30 records  and then exists. Usefull to run\
             after the dry run to test the workflow. (default: False)',
        action='store_true',
        default=False)

    args = parser.parse_args()

    log_filename = 'etl_{0}.log'.format(args.datatype)
    log_name = 'etl_{0}'.format(args.datatype)
    log = configure_logging(log_name, log_filename)


    results = main(
        args.datatype,
        args.config_file,
        args.max_workers,
        args.dry_run,
        args.create_new,
        args.debug
    )
