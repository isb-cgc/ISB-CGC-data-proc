'''
Created on Nov 16, 2016

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
import gzip
import pandas as pd

from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.load import load_data_from_file
from bigquery_etl.transform.tools import cleanup_dataframe
from util import flatten_map

class Etl(object):
    '''
    a base class to gather common functionality for performing etl and loading to BigQuery
    '''

    def __init__(self):
        '''
        Constructor
        '''
        pass

    def data_type_specific(self, config, file_df):
        return
    
    def add_metadata(self, file_df, data_type, info, program_name, project, config):
        """Add metadata info to the dataframe
        """
        metadata_list = flatten_map(info, config[program_name]['process_files']['data_table_mapping'])
        metadata = metadata_list[0]
        for next_metadata in metadata_list[1:]:
            metadata.update(next_metadata)
    
        metadata_columns = config[program_name]['process_files']['datatype2bqscript'][data_type]['add_metadata_columns']
        for metadata_column in metadata_columns:
            if 'sample_type_code' == metadata_column:
                program = project.split('-')[0]
                start_samplecode = config['sample_code_position'][program]['start']
                end_samplecode = config['sample_code_position'][program]['end']
                file_df['sample_type_code'] = metadata['sample_barcode'][start_samplecode:end_samplecode]
            else:
                file_df[metadata_column] = metadata[metadata_column]
        
    
        return file_df
    
    def process_file(self, config, outputdir, data_type, path, info, program_name, project, log):
        if config[program_name]['process_files']['datatype2bqscript'][data_type]['file_compressed']:
            with gzip.open(outputdir + path) as input_file:
                file_df = convert_file_to_dataframe(input_file)
        else:
            with open(outputdir + path) as input_file:
                file_df = convert_file_to_dataframe(input_file)
        
        #now filter down to the desired columns
        use_columns = config[program_name]['process_files']['datatype2bqscript'][data_type]['use_columns']
        file_df = file_df[use_columns.keys()]
        #modify to BigQuery desired names, checking for columns that will be split in the next step
        new_names = []
        for colname in file_df.columns:
            fields = use_columns[colname].split('~')
            if 1 == len(fields):
                new_names += [use_columns[colname]]
            else:
                new_names += [colname]
        file_df.columns = new_names
                
        # now process the splits
        for colname in use_columns:
            fields = use_columns[colname].split('~')
            if 2 == len(fields) and 'split' == fields[0]:
                extracted_df = file_df[colname].str.extract(fields[1], expand = True)
                file_df = pd.concat([file_df, extracted_df], axis=1)
        # add the metadata columns
        file_df = self.add_metadata(file_df, data_type, info, program_name, project, config)
        # allow subclasses to make final updates
        self.data_type_specific(config, file_df)
        # and reorder them
        file_df = file_df[config[program_name]['process_files']['datatype2bqscript'][data_type]['order_columns']]
        
        return file_df
    
    def skip_file(self, config, data_type, path, program_name, file2info, info, log):
        etl_config = config[program_name]['process_files']['datatype2bqscript'][data_type]
        if 'experimental_strategy' in etl_config and info['experimental_strategy'] != etl_config['experimental_strategy']:
            return True
        return False
    
    def process_paths(self, config, outputdir, data_type, paths, program_name, project, file2info, log):
        count = 0
        complete_df = None
        log.info('\tprocessing %d paths for %s:%s' % (len(paths), data_type, project))
        for path in paths:
            count += 1
            fields = path.split('/')
            info = file2info[fields[-2] + '/' + fields[-1]]
            if self.skip_file(config, data_type, path, program_name, file2info, info, log):
                continue
            file_df = self.process_file(config, outputdir, data_type, path, info, program_name, project, log)
            if complete_df is None:
                complete_df = file_df
            else:
                complete_df = pd.concat([complete_df, file_df], ignore_index = True)
            if 0 == count % 128:
                log.info('\t\tprocessed %s path: %s' % (count, path))
        log.info('\tdone processing %d paths for %s:%s' % (len(paths), data_type, project))
        
        # clean-up dataframe
        if complete_df is not None:
            log.info('\t\tcalling cleanup_dataframe() for %s' % (paths))
            complete_df = cleanup_dataframe(complete_df, log)
            log.info('\t\tdone calling cleanup_dataframe() for %s' % (paths))
            log.info('\tcomplete data frame(%d):\n%s\n%s' % (len(complete_df), complete_df.head(3), complete_df.tail(3)))
        else:
            log.info('\tno complete data frame created')
        return complete_df
    
    def upload_batch_etl(self, config, outputdir, paths, file2info, endpt_type, program_name, project, data_type, log):
        log.info('\tstart upload_batch_etl() for %s and %s' % (project, data_type))
        try:
            complete_df = self.process_paths(config, outputdir, data_type, paths, program_name, project, file2info, log)
            if complete_df is not None:
                etl_uploaded = True
                gcs = GcsConnector(config['cloud_projects']['open'], config['buckets']['open'])
                keyname = config['buckets']['folders']['base_run_folder'] + 'etl/%s/%s/%s/%s' % (endpt_type, project, data_type, paths[0].replace('/', '_'))
                log.info('\t\tstart convert and upload %s to the cloud' % (keyname))
                gcs.convert_df_to_njson_and_upload(complete_df, keyname, logparam = log)
                log.info('\t\tfinished convert and upload %s to the cloud' % (keyname))
            else:
                etl_uploaded = False
                log.info('\t\tno upload for this batch of files')
        except Exception as e:
            log.exception('problem finishing the etl: %s' % (e))
            raise
        log.info('\tfinished upload_batch_etl() for %s and %s' % (project, data_type))
        return etl_uploaded
    
    def load(self, project_id, bq_datasets, bq_tables, schema_files, gcs_file_paths, write_dispositions, batch_count, log):
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
            load_data_from_file.run(project_id, batch_count, bq_datasets[index], bq_tables[index], schema_files[index], 
                                gcs_file_paths[index] + '/*', 'NEWLINE_DELIMITED_JSON', write_dispositions[index])
            sep = '\n\t\t"*"*30\n'
    
        log.info('done load %s of data into bigquery' % (gcs_file_paths))

    def finish_etl(self, config, endpt_type, program_name, project, data_type, batch_count, log):
        log.info('\tstart finish_etl() for %s %s' % (project, data_type))
        try:
            etl_config = config[program_name]['process_files']['datatype2bqscript'][data_type]
            if endpt_type in etl_config['endpt_types'] and 0 < batch_count:
                log.info('\t\tprocessing finish_etl() for %s %s %s' % (endpt_type, project, data_type))
                bq_dataset = etl_config['bq_dataset']
                bq_table = etl_config['bq_table']
                schema_file = etl_config['schema_file']
                gcs_file_path = 'gs://' + config['buckets']['open'] + '/' + config['buckets']['folders']['base_run_folder'] + 'etl/%s/%s/%s' % (endpt_type, project, data_type)
                write_disposition = etl_config['write_disposition']
                self.load(config['cloud_projects']['open'], [bq_dataset], [bq_table], [schema_file], [gcs_file_path], [write_disposition], batch_count, log)
        except Exception as e:
            log.exception('problem finishing the etl: %s' % (e))
            raise
        log.info('\tfinished finish_etl() for %s %s' % (project, data_type))

    def initialize(self, config, program_name, log): 
        pass
    
    def finalize(self, config, program_name, log): 
        pass
