'''
Created on Oct 13, 2016

Copyright 2016, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author: michael
'''
import pprint
import time

from apiclient.errors import HttpError
from googleapiclient import discovery
from oauth2client.client import AccessTokenRefreshError 
from oauth2client.client import GoogleCredentials

from gdc.etl import etl

class Methylation(etl.Etl):
    
    def __init__(self):
        '''
        Constructor

        columns in the GDC file:
        Composite Element REF	Beta_value	Chromosome	Start	End	Gene_Symbol	Gene_Type	Transcript_ID	Position_to_TSS	CGI_Coordinate	Feature_Type
        
        columns for the BQ table:
        sample_barcode
        probe_id (Composite Element REF)
        beta_value (Beta_value)
        project_short_name
        program_name
        sample_type_code
        file_name
        file_id
        aliquot_barcode
        case_barcode
        case_gdc_id
        sample_gdc_id
        aliquot_gdc_id

        '''
        self.__aliquot2platforms = None
        

    def set_aliquot2platforms(self, file2info):
        # put together the map just in time
        self.__aliquot2platforms = {}
        for curinfo in file2info.values():
            curplatforms = self.__aliquot2platforms.setdefault(curinfo['cases'][0]['samples'][0]['portions'][0]['analytes'][0]['aliquots'][0]['submitter_id'], [])
            curplatforms += [curinfo['platform']]

    def skip_file(self, config, path, file2info, info, log):
        if 'Illumina Human Methylation 27' == info['platform']:
            if not self.__aliquot2platforms:
                self.set_aliquot2platforms(file2info)
                
            if 1 < len(self.__aliquot2platforms[info['cases'][0]['samples'][0]['portions'][0]['analytes'][0]['aliquots'][0]['submitter_id']]):
                log.info('\tskipping %s for etl' % (path))
                return True
            
        return False

    def get_service(self):
        # Grab the application's default credentials from the environment.
        credentials = GoogleCredentials.get_application_default()
    
        # Construct the service object for interacting with the BigQuery API.
        bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
    
        return bigquery
    
    def split_table_by_chr(self, chromosome, project_id, dataset_id, table_name, log):
        # this is a new connection to the new project
        bigquery_service = self.get_service()
        jobCollection = bigquery_service.jobs()
    
        try:
            query_request = bigquery_service.jobs()
            # maybe there is a nice way to format this one?
            query = """
                SELECT data.sample_barcode AS sample_barcode, data.probe_id AS probe_id, data.beta_value AS beta_value, data.project_short_name AS project_short_name, data.program_name AS program_name, 
                    data.sample_type_code AS sample_type_code, data.file_name AS file_name, data.file_gdc_id as file_gdc_id, data.aliquot_barcode as aliquot_barcode, data.case_barcode as case_barcode, 
                    data.case_gdc_id as case_gdc_id, data.sample_gdc_id as sample_gdc_id, data.aliquot_gdc_id as aliquot_gdc_id
                FROM 
                    ( 
                      SELECT CpG_probe_id 
                      FROM [platform_reference.GDC_hg38_methylation_annotation] 
                      WHERE ( chromosome == "chr{0}")
                    ) AS ids 
                JOIN EACH 
                    (
                      SELECT * 
                      FROM [{1}.{2}] 
                    ) AS data 
                ON ids.CpG_probe_id == data.probe_id""".format(chromosome, dataset_id, table_name)
    
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
                        'tableId': '{0}_chr{1}'.format(table_name, chromosome)
                    },
                    'createDisposition': 'CREATE_IF_NEEDED',
                    'writeDisposition': 'WRITE_APPEND',
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
    
    def finish_etl(self, config, project, data_type, log):
        super(Methylation, self).finish_etl(config, project, data_type, log)
        # now create the tables per chromosome
        log.info('start splitting methylation data by chromosome')
        project_id = config['cloud_projects']['open']
        dataset_id = config['process_files']['datatype2bqscript'][data_type]['bq_dataset']
        table_name = config['process_files']['datatype2bqscript'][data_type]['bq_table']
        chromosomes = map(str,range(1,23)) + ['X', 'Y']
    #    chromosomes = map(lambda orig_string: 'chr' + orig_string, chr_nums)
        for chromosome in chromosomes:
            self.split_table_by_chr(chromosome, project_id, dataset_id, table_name, log)
        log.info('done splitting methylation data by chromosome')
        

