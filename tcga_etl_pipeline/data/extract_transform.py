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

"""
Script to extract records from the metadata_data table and save to BigQuery.  this must be
run stand alone since it combines the extract and transform steps into one.
"""
import sys
import json

from bigquery_etl.utils.logging_manager import configure_logging
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.utils.gcutils import read_mysql_query

def identify_data(config):
    """Gets the metadata info from database
    """
    log = configure_logging('data_et', 'logs/data_et.log')
    log.info('start data extract and transform')
    # cloudSql connection params
    host = config['cloudsql']['host']
    database = config['cloudsql']['db']
    user = config['cloudsql']['user']
    passwd = config['cloudsql']['passwd']

    log.info("\tselect data records from db")
    sqlquery = """
        SELECT 
            ParticipantBarcode,
            SampleBarcode,
            AliquotBarcode,
            AliquotUUID,
            DataArchiveName,
            DataArchiveURL,
            DataArchiveVersion,
            DataCenterCode,
            DataCenterName,
            DataCenterType,
            DatafileMD5,
            DatafileName,
            DatafileNameKey,
            DatafileUploaded,
            DataLevel,
            Datatype,
            GenomeReference,
            IncludeForAnalysis,
            MAGETabArchiveName,
            MAGETabArchiveURL,
            Pipeline,
            Platform,
            Project,
            Repository,
            SampleType,
            SampleTypeCode,
            SDRFFileName,
            SDRFFileNameKey,
            SecurityProtocol,
            Species,
            Study,
            wasDerivedFrom,
            library_strategy,
            state,
            reason_for_state,
            analysis_id,
            analyte_code,
            last_modified,
            platform_full_name,
            GG_dataset_id,
            GG_readgroupset_id
        FROM metadata_data
        WHERE Project = 'TCGA'
            AND study = '%s'
            AND DatafileUploaded='true'
            AND DatafileNameKey is not null
            AND IncludeForAnalysis='yes'
        """

    studies = config['all_tumor_types']
    for study in studies:
        # connect to db and get results in a dataframe
        log.info("\tselect data records from db for %s" % (study))
        metadata_df = read_mysql_query(host, database, user, passwd, sqlquery % (study))
        log.info("\tFound {0} rows, columns." .format(str(metadata_df.shape)))
    
        project_id = config['project_id']
        bucket_name = config['buckets']['open']
        gcs = GcsConnector(project_id, bucket_name)
        log.info("\tupload data to GCS.")
        gcs.convert_df_to_njson_and_upload(metadata_df, config['data']['output_dir'] + config['data']['bq_table'] + '_%s.json' % (study))
    
    log.info('finished extract and transform')

if __name__ == '__main__':
    identify_data(json.load(open(sys.argv[1])))

