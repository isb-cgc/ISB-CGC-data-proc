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

"""Script to parse mrna/bcgsc files
"""
from bigquery_etl.utils import gcutils
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.utils.logging_manager import configure_logging
import sys
import numpy as np
import pandas as pd

def parse_bcgsc(project_id, bucket_name, filename, outfilename, metadata):
    """Download and convert blob into dataframe
       Transform the file: includes data cleaning
       Add Metadata information
    """
    # setup logging
    configure_logging('mrna.bcgsc', "logs/" + metadata['AliquotBarcode'] + '.log')

    # connect to the cloud bucket
    gcs = GcsConnector(project_id, bucket_name)

    #main steps: download, convert to df, cleanup, transform, add metadata
    data_df = gcutils.convert_blob_to_dataframe(gcs, project_id, bucket_name, filename)
    data_df = additional_changes(data_df)
    data_df = add_metadata(data_df, metadata)

    # upload the contents of the dataframe in njson format
    status = gcs.convert_df_to_njson_and_upload(data_df, outfilename)
    return status

def additional_changes(data_df):
    """Make additional data transformations on the dataframe
    """
    # the file must be docs/" #hardcoded link, could be moved to config
    hgnc_approved_symbols_file = 'docs/hgnc_approved_symbols.txt'
    hgnc_approved_symbols_df = pd.read_table(hgnc_approved_symbols_file, sep="|", dtype='object')
    hgnc_approved_symbols = hgnc_approved_symbols_df.set_index('entrez_id')['symbol'].to_dict()

    for line_num, segments in enumerate(data_df['gene']):
        elements = segments.split("|")
        gene_addenda = np.nan

        data_df.loc[line_num, 'original_gene_symbol'] = elements[0]
        gene_id = elements[1].replace("_calculated", "").replace("?", "")
        if (not gene_id) or (gene_id == ''):
           gene_id = np.nan
        data_df.loc[line_num, 'gene_id'] = gene_id

        if len(elements) == 3:
           data_df.loc[line_num, 'gene_addenda'] = elements[2].replace("_calculated", "").replace("?", "")
        else:
           data_df.loc[line_num, 'gene_addenda'] = gene_addenda

    del data_df['gene']
    data_df['HGNC_gene_symbol'] = data_df['gene_id'].map(lambda x: hgnc_approved_symbols.get(str(x), np.nan))

    return data_df

def add_metadata(data_df, metadata):
    """Add metadata info to the dataframe
    """
    data_df['AliquotBarcode'] = metadata['AliquotBarcode']
    data_df['SampleBarcode'] = metadata['SampleBarcode']
    data_df['ParticipantBarcode'] = metadata['ParticipantBarcode']
    data_df['Study'] = metadata['Study'].upper()
    data_df['SampleTypeLetterCode'] = metadata['SampleTypeLetterCode']
    data_df['Platform'] = metadata['Platform']
    data_df['Pipeline'] = metadata['Pipeline']
    data_df['Center'] = metadata['DataCenterName']

    return data_df

if __name__ == '__main__':
    project_id = sys.argv[1]
    bucket_name = sys.argv[2]
    filename = sys.argv[3]
    outfilename = sys.argv[4]
    metadata = {'AliquotBarcode':'test', 'SampleBarcode':'t', 'ParticipantBarcode':'t', 'Study':'e', 'SampleTypeLetterCode':'f', 'Platform':'r'}
    parse_bcgsc(project_id, bucket_name, filename, outfilename, metadata)
