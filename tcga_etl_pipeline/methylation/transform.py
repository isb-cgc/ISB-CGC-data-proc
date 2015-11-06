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

"""Script to parse methylation file
"""
from bigquery_etl.utils import gcutils
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.utils.logging_manager import configure_logging

def parse_methylation(project_id, bucket_name, filename, outfilename, metadata):
    """Download and convert blob into dataframe
       Transform the file: includes data cleaning
       Add Metadata information
    """
    # setup logging
    configure_logging('mirna.isoform', "logs/" + metadata['AliquotBarcode'] + '.log')

    # connect to the cloud bucket
    gcs = GcsConnector(project_id, bucket_name)

    #main steps: download, convert to df, cleanup, transform, add metadata
    data_df = gcutils.convert_blob_to_dataframe(gcs, project_id, bucket_name, filename, skiprows=1)
    data_df.columns = ['Probe_Id', "Beta_Value", "Gene_Symbol", "Chromosome", "Genomic_Coordinate"]

    data_df = add_metadata(data_df, metadata)
    data_df = additional_changes(data_df)

    # upload the contents of the dataframe in njson format
    df_string = data_df.to_csv(index=False, header=False, float_format='%.2f')
    status = gcs.upload_blob_from_string(outfilename, df_string)

    return status

def additional_changes(data_df):
    """Make additional data transformations on the dataframe
    """
    # filer based on the beta_value to remove NULL values
    data_df = data_df[data_df.Beta_Value.notnull()]
    data_df.loc[:, "Beta_Value"] = data_df["Beta_Value"]\
                           .map(lambda beta_value: "{0:.2f}".format(float(beta_value)))
    filter_columns = ['ParticipantBarcode', 'SampleBarcode', 'SampleTypeLetterCode',\
                                 'AliquotBarcode', 'Platform', 'Study', 'Probe_Id', "Beta_Value"]
#    filter_columns = ['ParticipantBarcode']
    data_df = data_df[filter_columns]
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
    return data_df
