'''
Created on Nov 16, 2016

@author: michael
'''
from bigquery_etl.load import load_data_from_file
import json

class etl(object):
    '''
    a base class to gather common functionality for performing etl and loading to BigQuery
    '''

    def __init__(self):
        '''
        Constructor
        '''
        pass

    def load(self, project_id, bq_datasets, bq_tables, schema_files, gcs_file_paths, write_dispositions, log):
        """
        Load the bigquery table
        load_data_from_file accepts following params:
        project_id, dataset_id, table_name, schema_file, data_path,
              source_format, write_disposition, poll_interval, num_retries
        """
        log.info('\tbegin load of %s data into bigquery' % (gcs_file_paths))
        sep = ''
        for index in range(len(bq_datasets)):
            log.info("%s\t\tLoading %s table into BigQuery.." % (sep, bq_datasets[index]))
            load_data_from_file.run(project_id, bq_datasets[index], bq_tables[index], schema_files[index], 
                                gcs_file_paths[index] + '/*', 'NEWLINE_DELIMITED_JSON', write_dispositions[index])
            sep = '\n\t\t"*"*30\n'
    
        log.info('done load %s of data into bigquery' % (gcs_file_paths))
