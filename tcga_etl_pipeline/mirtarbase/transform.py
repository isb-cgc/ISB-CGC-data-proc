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

"""Script to parse mirtarbase file
To run: python transform.py config_file
"""

from urllib2 import Request, urlopen
import json
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
import sys
from bigquery_etl.transform.tools import cleanup_dataframe
import pandas as pd

def parse_mirtarbase(config):
    """
    Download mirtarbase file with urllib2.Request
    Convert to a dataframe
    """
    # use Request to get the data (change the link to get the most recent file)
    request = Request(config['mirtarbase']['input_file'])
    socket = urlopen(request)

    # load into a pandas dataframe
    xlsd = pd.ExcelFile(socket, dtype=object)
    df_sheet1 = xlsd.parse(0) # parse first sheet (converts ina dataframe)
    print 'Found {0} rows in the file'. format(len(df_sheet1))

    # clean up the dataframe to upload to BigQuery
    data_df = cleanup_dataframe(df_sheet1)

    replace_column_names = {
        'Species_miRNA': 'miRNA_Species',
        'Species_Target_Gene': 'Target_Gene_Species',
        'Target_Gene_Entrez_Gene_ID': 'Target_Gene_EntrezID',
        'miRTarBase_ID': 'miRTarBaseID'
    }
    data_df.columns = [replace_column_names[x] if x in replace_column_names\
                                               else x for x in data_df.columns]
    # convert to int
    data_df['Target_Gene_EntrezID'] = data_df['Target_Gene_EntrezID']\
                                        .map(lambda x: str(int(float(x))), na_action='ignore')

    #-------
    # upload
    #-------
    # connect to gcloud wrapper
    project_id = config['project_id']
    bucket_name = config['buckets']['open']
    # connect to google storage
    gcs = GcsConnector(project_id, bucket_name)

    # upload the contents of the dataframe in njson format
    status = gcs.convert_df_to_njson_and_upload(data_df, config['mirtarbase']['output_file'])
    print status


if __name__ == '__main__':
    parse_mirtarbase(json.load(open(sys.argv[1])))
