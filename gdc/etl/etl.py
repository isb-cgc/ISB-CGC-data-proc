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

    def add_metadata(self, file_df, data_type, info, project, config):
        """Add metadata info to the dataframe
        """
        metadata_list = flatten_map(info, config['process_files']['data_table_mapping'])
        metadata = metadata_list[0]
        for next_metadata in metadata_list[1:]:
            metadata.update(next_metadata)
    
        metadata_columns = config['process_files']['datatype2bqscript'][data_type]['add_metadata_columns']
        for metadata_column in metadata_columns:
            if 'sample_type_code' == metadata_column:
                program = project.split('-')[0]
                start_samplecode = config['sample_code_position'][program]['start']
                end_samplecode = config['sample_code_position'][program]['end']
                file_df['sample_type_code'] = metadata['sample_barcode'][start_samplecode:end_samplecode]
            else:
                file_df[metadata_column] = metadata[metadata_column]
        
    
        return file_df
    
    def process_file(self, config, outputdir, data_type, path, info, project, log):
        if config['process_files']['datatype2bqscript'][data_type]['file_compressed']:
            with gzip.open(outputdir + path) as input_file:
                file_df = convert_file_to_dataframe(input_file)
        else:
            with open(outputdir + path) as input_file:
                file_df = convert_file_to_dataframe(input_file)
        
        #now filter down to the desired columns
        use_columns = config['process_files']['datatype2bqscript'][data_type]['use_columns']
        file_df = file_df[use_columns.keys()]
        #modify to BigQuery desired names
        new_names = []
        for colname in file_df.columns:
            new_names += [use_columns[colname]]
        file_df.columns = new_names
        # add the metadata columns
        file_df = self.add_metadata(file_df, data_type, info, project, config)
        # and reorder them
        file_df = file_df[config['process_files']['datatype2bqscript'][data_type]['order_columns']]
        
        return file_df
    
    def skip_file(self, config, path, file2info, info, log):
        return False
    
    def process_paths(self, config, outputdir, data_type, paths, project, file2info, log):
        count = 0
        complete_df = None
        for path in paths:
            count += 1
            fields = path.split('/')
            info = file2info[fields[-2] + '/' + fields[-1]]
            if self.skip_file(config, path, file2info, info, log):
                continue
            file_df = self.process_file(config, data_type, outputdir, path, info, project, log)
            if complete_df is None:
                complete_df = file_df
            else:
                complete_df = pd.concat([complete_df, file_df], ignore_index = True)
            if 0 == count % 128:
                log.info('\t\tprocessed %s path: %s' % (count, path))
        
        # clean-up dataframe
        if complete_df is not None:
            log.info('\t\tcalling cleanup_dataframe() for %s' % (paths))
            complete_df = cleanup_dataframe(file_df, log)
            log.info('\t\tdone calling cleanup_dataframe() for %s' % (paths))
            log.info('\tcomplete data frame(%d):\n%s\n%s' % (len(complete_df), complete_df.head(3), complete_df.tail(3)))
        else:
            log.info('\tno complete data frame created')
        return complete_df
    
    def upload_batch_etl(self, config, outputdir, paths, file2info, project, data_type, log):
        log.info('\tstart upload_batch_etl() for %s and %s' % (project, data_type))
        try:
            complete_df = self.process_paths(config, outputdir, data_type, paths, project, file2info, log)
            if complete_df is not None:
                gcs = GcsConnector(config['cloud_projects']['open'], config['buckets']['open'])
                keyname = config['buckets']['folders']['base_run_folder'] + 'etl/%s/%s/%s' % (project, data_type, paths[0].split('/')[1][:-3])
                log.info('start convert and upload %s to the cloud' % (keyname))
                gcs.convert_df_to_njson_and_upload(complete_df, keyname)
                log.info('finished convert and upload %s to the cloud' % (keyname))
            else:
                log.info('no upload for this batch of files')
        except Exception as e:
            log.exception('problem finishing the etl: %s' % (e))
            raise
        log.info('\tfinished upload_batch_etl() for %s and %s' % (project, data_type))
    
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

    def finish_etl(self, config, project, data_type, log):
        log.info('\tstart finish_etl() for %s %s' % (project, data_type))
        try:
            bq_dataset = config['process_files']['datatype2bqscript'][data_type]['bq_dataset']
            bq_table = config['process_files']['datatype2bqscript'][data_type]['bq_table']
            schema_file = config['process_files']['datatype2bqscript'][data_type]['schema_file']
            gcs_file_path = 'gs://' + config['buckets']['open'] + '/' + config['buckets']['folders']['base_run_folder'] + 'etl/%s/%s' % (project, data_type)
            write_disposition = config['process_files']['datatype2bqscript'][data_type]['write_disposition']
            self.load(config['cloud_projects']['open'], [bq_dataset], [bq_table], [schema_file], [gcs_file_path], [write_disposition], log)
        except Exception as e:
            log.exception('problem finishing the etl: %s' % (e))
            raise
        log.info('\tfinished finish_etl() for %s %s' % (project, data_type))

if __name__ == '__main__':
    config = {
        'project_id': 'isb-cgc',
        'cloud_projects': {
            'open': 'isb-cgc'
        },
        "buckets": {
            "open": "isb-cgc-scratch",
            "controlled": "62f2c827-test-a",
            "folders": {
                "base_file_folder": "gdc/test_local_gdc_upload/",
                "base_run_folder": "gdc/test_local_gdc_upload_run/"
            }
        },
        "process_files": {
            "datatype2bqscript": {
                "Gene Expression Quantification": {
                    "python_module":"gdc.etl.gene_expression_quantification",
                    "class":"gene_expression_quantification",
                    "file_compressed": True,
                    "bq_dataset": "test",
                    "bq_table": "TCGA_GeneExpressionQuantification_local_test",
                    "schema_file": "gdc/schemas/geq.json",
                    "write_disposition": "WRITE_APPEND",
                    "analysis_types": [
                        "HTSeq - FPKM-UQ",
                        "HTSeq - FPKM",
                        "HTSeq - Counts"
                    ]
                },
                "Methylation Beta Value": {
                    "python_module":"gdc.etl.methylation",
                    "class":"methylation",
                    "file_compressed": False,
                    "bq_dataset": "test",
                    "bq_table": "TCGA_Methylation_local_test",
                    "schema_file": "gdc/schemas/meth.json",
                    "write_disposition": "WRITE_APPEND",
                    "use_columns": {
                        "Composite Element REF": "probe_id",
                        "Beta_value": "beta_value"
                    },
                }
            }
        }
    }
    
    from datetime import date
    import logging
    from methylation import Methylation
    from util import create_log
    log_dir = str(date.today()).replace('-', '_') + '_gdc_etl_load_run/'
    log_name = create_log(log_dir, 'gdc_upload')
    log = logging.getLogger(log_name)
    Methylation().finish_etl(config, 'TCGA-UCS', 'Methylation Beta Value', log)