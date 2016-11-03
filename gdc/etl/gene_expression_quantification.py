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
import gzip
import pandas as pd

from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.transform.tools import cleanup_dataframe
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from . import load
from util import flatten_map

def add_metadata(data_df, info, config):
    """Add metadata info to the dataframe
    """
    metadata_list = flatten_map(info, config['process_files']['data_table_mapping'])
    metadata = metadata_list[0]
    for next_metadata in metadata_list[1:]:
        metadata.update(next_metadata)

    data_df['FileID'] = metadata['FileID']
    data_df['AliquotsAliquotID'] = metadata['AliquotsAliquotID']
    data_df['AliquotsSubmitterID'] = metadata['AliquotsSubmitterID']
    data_df['SampleID'] = metadata['SampleID']
    data_df['SamplesSubmitterID'] = metadata['SamplesSubmitterID']
    data_df['CaseID'] = metadata['CaseID']
    data_df['CasesSubmitterID'] = metadata['CasesSubmitterID']
    data_df['ProgramName'] = metadata['ProgramName'].upper()
    data_df['ProjectID'] = metadata['ProjectID'].upper()
    data_df['SampleTypeLetterCode'] = config['sample_code2letter'][metadata['SamplesSubmitterID'][13:15]]
    data_df['DataType'] = metadata['DataType']
    data_df['ExperimentalStrategy'] = metadata['ExperimentalStrategy']

    return data_df


def process_per_sample_files(config, outputdir, associated_paths, types, info, log):
    dfs = [None] * 3
    curindex = 0
    for associated_path in associated_paths:
        # convert blob into dataframe
        log.info('\t\tcalling convert_file_to_dataframe() for %s' % (associated_path))
        dfs[curindex] = convert_file_to_dataframe(gzip.open(outputdir + associated_path), header=None)
        dfs[curindex].columns = ['Ensembl_versioned_gene_ID', types[curindex]]
        add_metadata(dfs[curindex], info, config)
        if 'HTSeq - Counts' == types[curindex]:
            dfs[curindex] = dfs[curindex].drop(dfs[curindex].index[[60483, 60484, 60485, 60486, 60487]])
        log.info('\t\tdone calling convert_file_to_dataframe() for %s' % (associated_path))
        curindex += 1
    
    merge_df = dfs[0]
    for df in dfs[1:]:
        merge_df = merge_df.merge(df, how='inner', on=[
                'Ensembl_versioned_gene_ID', 
                'FileID',
                'AliquotsAliquotID',
                'AliquotsSubmitterID',
                'SampleID',
                'SamplesSubmitterID', 
                'CaseID',
                'CasesSubmitterID', 
                'ProgramName', 
                'ProjectID', 
                'SampleTypeLetterCode', 
                'DataType', 
                'ExperimentalStrategy'])
    
    log.info('merge workflow(%d):\n%s\n\t...\n%s' % (len(merge_df), merge_df.head(3), merge_df.tail(3)))
    return merge_df

def process_paths(config, outputdir, paths, file2info, log):
    types = config['process_files']['datatype2bqscript']['Gene Expression Quantification']['analysis_types']
    idents = set()
    count = 0
    associated_paths = [None] * 3
    complete_df = None
    for path in paths:
        fields = path.split('/')
        info = file2info[fields[-2] + '/' + fields[-1]]
        type_index = types.index(info['analysis']['workflow_type'])
        if associated_paths[type_index]:
            raise ValueError('files in bad order, found two of the same type for %s:%s' % (associated_paths[type_index], fields[1]))
        idents.add(fields[1].split('.')[0])
        if 1 < len(idents):
            raise ValueError('files in bad order, found two different identifiers %s:%s' % (associated_paths[type_index], fields[1]))
        associated_paths[type_index] = path
        count += 1
        if 0 == count % 3:
            merge_df = process_per_sample_files(config, outputdir, associated_paths, types, info, log)
            if complete_df is None:
                complete_df = merge_df
            else:
                complete_df = pd.concat([complete_df, merge_df], ignore_index = True)
            idents = set()
            count = 0
            associated_paths = [None] * 3
    
    # add unversioned gene column
    complete_df['Ensembl_gene_ID'] = complete_df['Ensembl_versioned_gene_ID'].str.split('.').str[0]
    
    # clean-up dataframe
    log.info('\t\tcalling cleanup_dataframe() for %s' % (paths))
    complete_df = cleanup_dataframe(complete_df, log)
    log.info('\t\tdone calling cleanup_dataframe() for %s' % (paths))

    log.info('\tcomplete data frame(%d):\n%s\n%s' % (len(complete_df), complete_df.head(3), complete_df.tail(3)))
    return complete_df

def upload_batch_etl(config, outputdir, paths, file2info, project, data_type, log):
    log.info('\tstart upload_batch_etl() for gene expression quantification for %s and %s' % (project, data_type))
    try:
        if 0 != len(paths) % 3:
            raise RuntimeError('need to process the three RNA files per sample together.  adjust the configuration option \'download_files_per\' accordingly')
        complete_df = process_paths(config, outputdir, paths, file2info, log)
        gcs = GcsConnector(config['cloud_projects']['open'], config['buckets']['open'])
        keyname = config['buckets']['folders']['base_run_folder'] + 'etl/%s/%s/%s' % (project, data_type, paths[0].split('/')[1][:-3])
        log.info('start convert and upload %s to the cloud' % (keyname))
        gcs.convert_df_to_njson_and_upload(complete_df, keyname)
        log.info('finished convert and upload %s to the cloud' % (keyname))
    except Exception as e:
        log.exception('problem finishing the etl: %s' % (e))
        raise
    log.info('\tfinished upload_batch_etl() for gene expression quantification for %s and %s' % (project, data_type))

def finish_etl(config, project, data_type, log):
    log.info('\tstart finish_etl() for gene expression quantification')
    try:
        bq_dataset = config['process_files']['datatype2bqscript']['Gene Expression Quantification']['bq_dataset']
        bq_table = config['process_files']['datatype2bqscript']['Gene Expression Quantification']['bq_table']
        schema_file = config['process_files']['datatype2bqscript']['Gene Expression Quantification']['schema_file']
        gcs_file_path = 'gs://' + config['buckets']['open'] + '/' + config['buckets']['folders']['base_run_folder'] + 'etl/%s/%s' % (project, data_type)
        write_disposition = config['process_files']['datatype2bqscript']['Gene Expression Quantification']['write_disposition']
        load(config['cloud_projects']['open'], [bq_dataset], [bq_table], [schema_file], [gcs_file_path], [write_disposition], log)
    except Exception as e:
        log.exception('problem finishing the etl: %s' % (e))
        raise
    log.info('\tfinished finish_etl() for gene expression quantification')
