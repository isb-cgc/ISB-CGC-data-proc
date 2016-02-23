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

""" Parse CCLE GCT file
"""
from bigquery_etl.utils import gcutils
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.transform.tools import cleanup_dataframe
from bigquery_etl.load import load_data_from_file
import pandas as pd
from pandas import ExcelWriter
import numpy as np
import hgnc_validation

def main():
    """Parse GCT file, merge with barcodes info, melt(tidy) 
        and load to Google Storage and BigQuery
    """

    project_id = ''
    bucket_name = ''
    # example file in bucket
    filename = 'ccle/mRNA-gene-exp/CCLE_Expression_Entrez_2012-09-29.gct'
    outfilename = 'test'
    writer = ExcelWriter('ccle.xlsx')

    # connect to the google cloud bucket
    gcs = GcsConnector(project_id, bucket_name)

    #------------------------------
    # load the GCT file 
    #  * the file has duplicate samples (columns)
    #------------------------------
    # To remove duplicates, load the first few lines of the file only. Get columns, unique and select dataframe
    # this is a hack, but I cant find a elegant way to remove duplciates
    gct_df = pd.read_table('CCLE_Expression_Entrez_2012-09-29.gct', sep='\t', skiprows=2, mangle_dupe_cols=False, nrows=2)
    unqiue_columns = np.unique(gct_df.columns)
    gct_df = pd.read_table('CCLE_Expression_Entrez_2012-09-29.gct', sep='\t', skiprows=2, mangle_dupe_cols=True)
    # clean-up the dataset/dataframe
    gct_df = cleanup_dataframe(gct_df) 
    gct_df = gct_df[unqiue_columns]

    # remove any gene_id starting with 'AFFX-'
    gct_df[gct_df['Name'].str.startswith('AFFX-')].to_excel(writer, sheet_name="affy_info")    
    gct_df = gct_df[~gct_df['Name'].str.startswith('AFFX-')]

    #------------------------------
    # HGNC validation
    #-----------------------------
    hgnc_df = hgnc_validation.get_hgnc_map()
    hgnc_df.to_excel(writer, sheet_name="hgnc_info")
    hgnc_dict = dict(zip(hgnc_df.entrez_id, hgnc_df.symbol))
    gct_df['HGNC_gene_symbol'] = gct_df['Name'].map(lambda gene_id: hgnc_dict.get(gene_id.replace('_at', ''), np.nan))
    gct_df[['HGNC_gene_symbol', 'Name', 'Description']].to_excel(writer, sheet_name="gene_info")
    gct_df['Name'] = gct_df['Name'].map(lambda gene_id: gene_id.replace('_at', ''))
 
    #------------------------------
    # barcodes info
    #------------------------------
    barcodes_filename = 'ccle/mRNA-gene-exp/mRNA_names.out.tsv'
    filebuffer = gcs.download_blob_to_file(barcodes_filename)
    barcodes_df = pd.read_table(filebuffer,  header=None, names=['ParticipantBarcode', 'SampleBarcode', 'CCLE_long_name']) # convert into dataframe
    barcodes_df = cleanup_dataframe(barcodes_df) # clean-up dataframe

    #------------------------------
    # ignore (drop) all of the columns from the gene-expression matrix 
    #that don't have corresponding Participant and Sample barcodes, 
    #------------------------------
    columns_df = pd.DataFrame(unqiue_columns)
    columns_df.columns =['CCLE_long_name']
    samples_map_df = pd.merge(columns_df, barcodes_df, on='CCLE_long_name', how='inner')
    samples_map_df.to_excel(writer, sheet_name="sample_info")

    # select columns that are overlapping
    overlapping_samples = samples_map_df['CCLE_long_name'].tolist()
    overlapping_samples = overlapping_samples + ['Name', 'Description', 'HGNC_gene_symbol']
    gct_df = gct_df[overlapping_samples]
    print gct_df    
    
    # melt the matrix
    value_vars = [ col for col in gct_df.columns if col not in ['Name', 'Description', 'HGNC_gene_symbol']]
    melted_df = pd.melt(gct_df, id_vars=['Name', 'Description', 'HGNC_gene_symbol'], value_vars=value_vars)
    melted_df = melted_df.rename(
                    columns={'Name': 'gene_id', 'Description': 'original_gene_symbol', 
                        'variable': 'CCLE_long_name', 'value': 'RMA_normalized_expression'})

    
    # merge to get barcode information
    # changed from outer join to inner join. In this case it shouldnt matter, since we already did a inner join
    # while select the samples above.
    data_df = pd.merge(melted_df, samples_map_df, on='CCLE_long_name', how='inner')
    data_df['Platform'] = "Affymetrix U133 Plus 2.0"

    # reorder columns
    col_order = ["ParticipantBarcode", "SampleBarcode", "CCLE_long_name", "gene_id", "HGNC_gene_symbol", "original_gene_symbol", "Platform", "RMA_normalized_expression"]    
    data_df = data_df[col_order]

    # upload the contents of the dataframe in CSV format
    print "Convert to CSV"
    outfilename = "tcga/intermediary/CCLE_mrna_expr/bq_data_files/ccle_mrna_expr.csv"
    df_string = data_df.to_csv(index=False, header=False)
    status = gcs.upload_blob_from_string(outfilename, df_string)
    print status

    # save the excel file 
    writer.save()

if __name__ == '__main__':
    main()
