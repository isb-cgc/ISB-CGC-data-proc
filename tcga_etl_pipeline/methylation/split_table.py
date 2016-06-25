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
'''
    Split methylation BigQuery table into chromosomes
'''
import pprint
import sys
import json
from apiclient.errors import HttpError
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from oauth2client.client import AccessTokenRefreshError 
import time

from bigquery_etl.utils.logging_manager import configure_logging
# TODO change this to single auth script (GOOGLE_APPLICATION_CREDENTIALS)
# still is still in progress
def get_service():
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)

    return bigquery

def split_table_by_chr(chromosome, project_id, dataset_id, log):
    # this is a new connection to the new project
    bigquery_service = get_service()
    jobCollection = bigquery_service.jobs()

    try:
        query_request = bigquery_service.jobs()
        # maybe there is a nice way to format this one?
        query = """\
            SELECT data.ParticipantBarcode AS ParticipantBarcode, data.SampleBarcode AS SampleBarcode, data.SampleTypeLetterCode AS SampleTypeLetterCode, \
                data.AliquotBarcode AS AliquotBarcode, data.Platform AS Platform, data.Study AS Study, data.Probe_Id AS Probe_Id, data.Beta_Value as Beta_Value 
            FROM \
                ( \
                  SELECT IlmnID \
                  FROM [platform_reference.methylation_annotation] \
                  WHERE ( CHR == "{0}")\
                ) AS ids \
            JOIN EACH \
                (\
                  SELECT * \
                  FROM [{1}.Methylation] \
                ) AS data \
            ON ids.IlmnID == data.Probe_Id""".format(chromosome, dataset_id)

        log.info('importing chromosome %s\n%s' % (chromosome, query))
#        query_data = {'query': query}
        query_data = {
              'configuration': {
              'query': {
                'query': query,
                'useQueryCache': False,
                'destinationTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': 'Methylation_chr{0}'.format(chromosome)
                },
                'createDisposition': 'CREATE_IF_NEEDED',
                'writeDisposition': 'WRITE_EMPTY',
                'allowLargeResults': True
              }
            }
        }
        insertResponse = query_request.insert(projectId=project_id, body=query_data).execute()
        # Ping for status until it is done, with a short pause between calls.
        while True:
            result = jobCollection.get(projectId=project_id,
                                     jobId=insertResponse['jobReference']['jobId']).execute()
            status = result['status']
            if 'DONE' == status['state']:
                if 'errorResult' in status and status['errorResult']:
                    log.error('an error occurred completing import at \'%s\': %s \'%s\' for chormosome %s' % 
                        (status['errorResult']['location'], status['errorResult']['reason'], status['errorResult']['message'], chromosome))
                else:
                    log.info('completed import chromosome %s' % (chromosome))
                break
            if 'errors' in status and status['errors'] and 0 < len(status['errors']):
                for error in status['errors']:
                    log.warning('\terror while importing chromosome %s: %s' % (chromosome, error))
            log.info('\tWaiting for the import to complete for chromosome %s...' % (chromosome))
            time.sleep(20)

    except HttpError as err:
        print 'Error:', pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
           "the application to re-authorize")

def main(config, log):
    log.info('start splitting methylation data by chromosome')
    project_id = config['project_id']
    dataset_id = config['bq_dataset']
    chromosomes = map(str,range(1,23)) + ['X', 'Y']
#    chromosomes = map(lambda orig_string: 'chr' + orig_string, chr_nums)
    for chromosome in chromosomes:
        split_table_by_chr(chromosome, project_id, dataset_id, log)
    log.info('done splitting methylation data by chromosome')

if __name__ == '__main__':
    # setup logging
    with open(sys.argv[1]) as configfile:
        config = json.load(configfile)
    log = configure_logging('methylation_split', "logs/methylation_transform_split" + '.log')
    main(config, log)
