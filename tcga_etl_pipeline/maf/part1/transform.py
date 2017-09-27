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

"""Script to parse MAF file
"""
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.transform.tools import *
import json
from google.cloud import storage
from bigquery_etl.execution import process_manager
import os.path
from pandas import ExcelWriter

def generate_oncotator_inputfiles(project_id, bucket_name, filename, outputfilename, oncotator_columns):

    print (filename)
    
    # NEW connection
    gcs = GcsConnector(project_id, bucket_name)

    filebuffer = gcs.download_blob_to_file(filename)

    # convert blob into dataframe
    try:
        maf_df = convert_file_to_dataframe(filebuffer)
    except:
        print 'problem converting %s to a dataframe' % (filename)
        raise

    # clean-up dataframe
    maf_df = cleanup_dataframe(maf_df)

    print maf_df.columns
    # lowercase the column names (WHY?)
    maf_df.columns = map(lambda x: x.lower(), maf_df.columns) 
    
    #--------------------------------------------
    # data - manipulation
    #--------------------------------------------
    maf_df["ncbi_build"] = maf_df["ncbi_build"].replace({ 'hg19': '37'
                                  ,'GRCh37': '37'
                                  ,'GRCh37-lite': '37'
                                 })

   
    #---------------------------------------------
    ## Filters
    ## remember all the column names are lowercase
    #---------------------------------------------
    filters = {
        "chromosome" : map(str,range(1,23)) + ['X', 'Y']
        ,"mutation_status": ['somatic', 'Somatic']
        ,"sequencer": ['Illumina HiSeq', 'Illumina GAIIx', 'Illumina MiSeq']
        ,"ncbi_build" : ['37']
    }

    filter_checklist_df = maf_df.isin(filters)
    
    filter_string = (
                       (filter_checklist_df["chromosome"] == True)
                        &   
                       (filter_checklist_df["mutation_status"] == True)
                        &
                       (filter_checklist_df["sequencer"] == True)
                        &
                       (filter_checklist_df["ncbi_build"] == True)
                    )

    maf_df = maf_df[filter_string]

    #---------------------
    #Oncotator part: generate intermediate files for Oncotator
    #---------------------
   
    # oncotator needs these columns
    replace_column_names = {
        "ncbi_build" : 'build'
       ,'chromosome' : 'chr'
       ,'start_position' : 'start'
       ,'end_position' : 'end'
       ,'reference_allele' : 'ref_allele'
       ,'tumor_seq_allele1' : 'tum_allele1'
       ,'tumor_seq_allele2' : 'tum_allele2'
       ,'tumor_sample_barcode': 'tumor_barcode'
       ,'matched_norm_sample_barcode': 'normal_barcode'
    }

    # replace columns with new headings; just name change
    for rcol in replace_column_names:
        maf_df.columns = [replace_column_names[x] if x==rcol else x for x in maf_df.columns]
        oncotator_columns = [replace_column_names[y] if y==rcol else y for y in oncotator_columns]         

    # remove/mangle any duplicate columns ( we are naming line a, a.1, a.2 etc)
    maf_df.columns = mangle_dupe_cols(maf_df.columns.values)

    #---------------------      
    #Oncotator part: generate intermediate files for Oncotator
    #---------------------

    oncotator_df = maf_df[oncotator_columns]

    print "df_columns", len(oncotator_df.columns)
   
    df_stringIO =  oncotator_df.to_csv(sep='\t', index=False, columns= oncotator_columns)

    # upload the file
    gcs.upload_blob_from_string(outputfilename, df_stringIO)
    
    return True
