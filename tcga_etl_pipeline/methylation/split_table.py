import httplib2
import pprint
import sys
from apiclient.errors import HttpError
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import time

# TODO change this to single auth script (GOOGLE_APPLICATION_CREDENTIALS)
# still is still in progress
def get_service():
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)

    return bigquery

def split_table_by_chr(chromosome, project_id, dataset_id):
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

        print query
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
#        # Ping for status until it is done, with a short pause between calls.
#        while True:
#            status = jobCollection.get(projectId=project_id,
#                                     jobId=insertResponse['jobReference']['jobId']).execute()
#            if 'DONE' == status['status']['state']:
#                break
#            print 'Waiting for the import to complete...'
        time.sleep(20)

    except HttpError as err:
        print 'Error:', pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
           "the application to re-authorize")

if __name__ == '__main__':

    project_id = 'isb-cgc'
    dataset_id = 'tcga_data_staging'
    chromosomes = map(str,range(1,23)) + ['X', 'Y']
#    chromosomes = map(lambda orig_string: 'chr' + orig_string, chr_nums)
    for chromosome in chromosomes:
        print chromosome
        split_table_by_chr(chromosome, project_id, dataset_id)
