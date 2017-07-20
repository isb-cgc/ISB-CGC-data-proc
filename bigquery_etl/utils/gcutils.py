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

"""Script to parse MAF file
"""
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.transform.tools import cleanup_dataframe
from datetime import datetime
import MySQLdb
import pandas as pd
import os

def logit(log, message, level):
    if None == log:
        print '%s--%s: %s' % (datetime.now(), level.upper(), message)
    else:
        if 'INFO' == level.upper():
            log.info(message)
        elif 'WARNING' == level.upper():
            log.warning(message)
        elif 'ERROR' == level.upper():
            log.error(message)
        elif 'EXCEPTION' == level.upper():
            log.exception(message)

def convert_blob_to_dataframe(gcs, project_id, bucket_name, filename, skiprows=0, log = None):
    """
    Function to connect to google cloud storage, download the file,
    and convert to a dataframe
    """

    try:
        logit(log, 'calling download_blob_to_file() for %s' % (filename), 'info')
        filebuffer = gcs.download_blob_to_file(filename)
        logit(log, 'done calling download_blob_to_file() for %s' % (filename), 'info')
    
        # convert blob into dataframe
        logit(log, 'calling convert_file_to_dataframe() for %s' % (filename), 'info')
        data_df = convert_file_to_dataframe(filebuffer, skiprows=skiprows)
        logit(log, 'done calling convert_file_to_dataframe() for %s' % (filename), 'info')
    
        # clean-up dataframe
        logit(log, 'calling cleanup_dataframe() for %s' % (filename), 'info')
        data_df = cleanup_dataframe(data_df)
        logit(log, 'done calling cleanup_dataframe() for %s' % (filename), 'info')
    except Exception as e:
        logit(log, 'problem in convert_blob_to_dataframe(%s): %s' % (filename, e), 'exception')

    return data_df

def read_mysql_query(host, database, user, passwd, sqlquery):
    """Reads CloudSQL database to get the metadata info for the
       files in the bucket
       Returns a dataframe with the metadata info
    """
    # connect db
    SSL_DIR = os.environ.get('SSL_DIR', '~/ssl_dir')

    ssl = {
        'ca': SSL_DIR + 'server-ca.pem',
        'cert': SSL_DIR + 'client-cert.pem',
        'key': SSL_DIR + 'client-key.pem'
    }

    mysql_connection = MySQLdb.connect(host=host, db=database, user=user, passwd=passwd, ssl=ssl)

    df_rows = pd.read_sql_query(sqlquery, mysql_connection)
    return df_rows
