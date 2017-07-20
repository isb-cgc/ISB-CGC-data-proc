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

"""Script to parse CNV files
"""
from bigquery_etl.utils import gcutils
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.utils.logging_manager import configure_logging

def parse_cnv(project_id, bucket_name, filename, outfilename, metadata):
    """Download and convert blob into dataframe
       Transform the file: includes data cleaning
       Add Metadata information
    """
    # setup logging
    log = configure_logging('cnv', "logs/cnv_transform_" + metadata['AliquotBarcode'] + '.log')

    # connect to the cloud bucket
    try:
        log.info('start transform of %s' % (metadata['AliquotBarcode']))
        gcs = GcsConnector(project_id, bucket_name)
    
        #main steps: download, convert to df, cleanup, transform, add metadata
        log.info('\tadd changes and metadata for %s' % (metadata['AliquotBarcode']))
        data_df = gcutils.convert_blob_to_dataframe(gcs, project_id, bucket_name, filename, log=log)
        data_df = additional_changes(data_df)
        data_df = add_metadata(data_df, metadata)
    
        # upload the contents of the dataframe in njson format
        status = gcs.convert_df_to_njson_and_upload(data_df, outfilename)
        log.info('finished transform of %s' % (metadata['AliquotBarcode']))
    except Exception as e:
        log.exception('problem transforming %s' % (metadata['AliquotBarcode']))
        raise e
    return status

def additional_changes(data_df):
    """Make additional data transformations on the dataframe
    """
    data_df['Segment_Mean'] = data_df['Segment_Mean'].map(lambda x: "{0:.4f}".format(float(x)))
    data_df['Num_Probes'] = data_df['Num_Probes'].map(lambda x: str(int(float(x))))
    data_df['Start'] = data_df['Start'].map(lambda x: str(int(float(x))))
    data_df['End'] = data_df['End'].map(lambda x: str(int(float(x))))

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
   # project_id = sys.argv[1]
   # bucket_name = sys.argv[2]
   # filename = sys.argv[3]
   # outfilename = sys.argv[4]
   # metadata = {'AliquotBarcode':'test', 'SampleBarcode':'t', 'ParticipantBarcode':'t', 'Study':'e', 'SampleTypeLetterCode':'f', 'Platform':'r'}
    parse_isoform(project_id, bucket_name, filename, outfilename, metadata)
