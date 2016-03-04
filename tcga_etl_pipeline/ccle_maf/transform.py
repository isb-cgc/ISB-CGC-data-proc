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

""" Script to merge Oncotator file
"""
#### -*- coding: utf-8 -*-
import sys
import time
import pandas as pd
import json
from gcloud import storage
from cStringIO import StringIO
import re
import os
from os.path import basename
import string
import check_duplicates
from bigquery_etl.utils import gcutils, convert_gbq_to_df
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.utils.logging_manager import configure_logging
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.transform.tools import cleanup_dataframe, remove_duplicates
from bigquery_etl.execution import process_manager
import logging

log = logging.getLogger(__name__)

#--------------------------------------
# Format Oncotator output before merging
#--------------------------------------
def format_oncotator_columns(df):
    
    df.columns = map(lambda x: x.replace('1000', '_1000'), df.columns)
    df.columns = map(lambda x: x.replace('gencode', 'GENCODE'), df.columns)
    df.columns = map(lambda x: '_'.join([''.join(i[0].upper() + i[1:]) for i in x.split('_')]), df.columns)

    # adjust columns
    replace_columns = { 
         'Tumor_Sample_Barcode' : 'CCLE_name'
        ,'Matched_Norm_Sample_Barcode' : 'Normal_AliquotBarcode'
        ,'Match_Norm_Seq_Allele1' : 'Normal_Seq_Allele1'
        ,'Match_Norm_Seq_Allele2' : 'Normal_Seq_Allele2'  
        ,'Match_Norm_Validation_Allele1' : 'Normal_Validation_Allele1'
        ,'Match_Norm_Validation_Allele2' : 'Normal_Validation_Allele2'
        ,'Gc_Content' : 'GC_Content'
        ,'CDNA_Change' : 'cDNA_Change'
    }

    for i in replace_columns:
       df.columns = map(lambda x: x.replace(i, replace_columns[i]), df.columns)

    return df

#------------------------------------------
# this adds news columns and does a few checks on the columns
#------------------------------------------
def add_columns(df, sample_code2letter, study):
    ## Add new columns
    df['Center'] = df['Center'].map(lambda x: ';'.join(sorted(x.split(';'))) if ';' in x else x)
    df['Study'] = study
    df['NCBI_Build'] = 37

    return df


#----------------------------------------
# this is the main function to process oncotator MAF files
# 1. this merges the different files by disease type
# 2. selects the columns to load into BigQuery
# 3. format the oncotator columns
# 4. adds new columns
# 5. removes any duplicate aliqouts
#----------------------------------------
def process_oncotator_output(project_id, bucket_name, data_library, bq_columns, sample_code2letter):
    study = data_library['Study'].iloc[0]

    #------------------------------
    # get CCLE_sample_info
    # Grab SampleBarcode_DNA column from the CCLE_sample_info table 
    #   and merge with the CCLE MAF file
    #-------------------------------
    query = 'SELECT * FROM {0}.{1}'.format('test', 'CCLE_sample_info')
    barcodes_info_df = convert_gbq_to_df.run('isb-cgc', query)
    barcodes_info_df = cleanup_dataframe(barcodes_info_df)


    #------------------------------
    # We are merging the MAF files by disease type
    file_count = 0
   
    # create an empty dataframe. we use this to merge dataframe
    disease_bigdata_df = pd.DataFrame()
    # iterate over the selected files
    for oncotator_file in data_library['filename']:
        file_count+= 1

        log.info('-'*10 + "{0}: Processing file {1}".format(file_count, oncotator_file) + '-'*10)

        try:
           gcs = GcsConnector(project_id, 'isb-cgc-scratch')
           # covert the file to a dataframe
           filename =  oncotator_file

           ## TODO remove the hard coded link
           df = pd.read_table('ccle_onco_output.txt', comment='#') # convert into dataframe
           df = cleanup_dataframe(df) # clean-up dataframe

#           df = gcutils.convert_blob_to_dataframe(gcs, project_id, bucket_name, filename)
        except Exception as e:
           print e
           raise
           
        if df.empty:
           log.debug('empty dataframe for file: ' + str(oncotator_file))
           continue

        #------------------------------
        # different operations on the frame
        #------------------------------
        # get only the required BigQuery columns
        df = df[bq_columns]
       
        # format oncotator columns; name changes etc 
        df = format_oncotator_columns(df)

        # add new columns
        df = add_columns(df, sample_code2letter, study)

        disease_bigdata_df = disease_bigdata_df.append(df, ignore_index = True)
            
    #----------------------------

    disease_bigdata_df = disease_bigdata_df[['CCLE_name']]
    print disease_bigdata_df
    barcodes_info_df = barcodes_info_df[['CCLE_name', 'SampleBarcode_DNA']]

    # merge the CCLE dataset with the CCLE_sample_info table
    merged_df = pd.merge(disease_bigdata_df, barcodes_info_df, on='CCLE_name', how='left')

    # this is a merged dataframe
    if not disease_bigdata_df.empty:
        
        # convert the df to new-line JSON and the upload the file
        gcs = GcsConnector(project_id, 'isb-cgc-open')
        gcs.convert_df_to_njson_and_upload(disease_bigdata_df, "tcga/intermediary/ccle_maf/bigquery_data_files/{0}.json".format(study))

    else:
        raise Exception('Empty dataframe!')
    return True

if __name__ == '__main__':

    config = json.load(open(sys.argv[1]))
  
    project_id = config['project_id']
    bucket_name = config['buckets']['open']
    sample_code2letter = config['sample_code2letter']
 
    # get disease_codes/studies( TODO this must be changed to get the disease code from the file name)
    df = convert_file_to_dataframe(open(sys.argv[2]))
    df = cleanup_dataframe(df)
    studies = list(set(df['Study'].tolist()))

    # get bq columns ( this allows the user to select the columns
    # , without worrying about the index, case-sensitivenes etc
    selected_columns = pd.read_table(sys.argv[3], names=['bq_columns'])
    transposed = selected_columns.T
    transposed.columns = transposed.loc['bq_columns']
    transposed = cleanup_dataframe(transposed)
    bq_columns = transposed.columns.values

    # submit threads by disease  code
    pm = process_manager.ProcessManager(max_workers=33, db='maf.db', table='task_queue_status')
    for idx, df_group in df.groupby(['Study']):
        #future = pm.submit(process_oncotator_output, project_id, bucket_name, df_group, bq_columns, sample_code2letter)
        process_oncotator_output( project_id, bucket_name, df_group, bq_columns, sample_code2letter)
        time.sleep(0.2)
    pm.start()
