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

"""Script to parse Protein files
"""
from bigquery_etl.utils import gcutils
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.utils.logging_manager import configure_logging
from bigquery_etl.transform.tools import split_df_column_values_into_multiple_rows
import sys
import pandas as pd
import numpy as np
from bigquery_etl.tests import tests
import re

def parse_protein(project_id, bucket_name, filename, outfilename, metadata):
    """Download and convert blob into dataframe
       Transform the file: includes data cleaning
       Add Metadata information
    """
    # setup logging
    log = configure_logging('protein', "logs/protein_transform" + metadata['AliquotBarcode'] + '.log')
    try:
        log.info('start transform of %s' % (metadata['AliquotBarcode']))
        # connect to the cloud bucket
        gcs = GcsConnector(project_id, bucket_name)
    
        #main steps: download, convert to df, cleanup, transform, add metadata
        data_df = gcutils.convert_blob_to_dataframe(gcs, project_id, bucket_name, filename, skiprows=1, log=log)
        log.info('\tadd changes and metadata for %s' % (metadata['AliquotBarcode']))
        data_df = additional_changes(data_df)
        data_df = add_metadata(data_df, metadata)
    
        # validation
        tests.assert_notnull_property(data_df, columns_list=['Protein_Name'])
    
        # upload the contents of the dataframe in njson format
        status = gcs.convert_df_to_njson_and_upload(data_df, outfilename, metadata=metadata)
        log.info('finished transform of %s' % (metadata['AliquotBarcode']))
    except Exception as e:
        log.exception('problem transforming %s' % (metadata['AliquotBarcode']))
        raise e
    return status

def additional_changes(data_df):
    """Make additional data transformations on the dataframe
    """
    # need to change the curated file loading logic
    # antibody annotation mapping
    manual_curated_file = 'protein/antibody-gene-protein-map/antibody-gene-protein-map.xlsx' # bad hardcoded link
    aa_df = pd.read_csv(manual_curated_file, delimiter='\t', header=0, keep_default_na=False)
    aa_df = aa_df.set_index('composite_element_ref')

    aa_mapping_geneName = aa_df['final_gene_name'].to_dict()
    aa_mapping_proteinName = aa_df['final_protein_name'].to_dict()

    # add gene and protein name
    data_df['Gene_Name'] = data_df['Composite_Element_REF'].map(lambda x: aa_mapping_geneName.get(x, np.nan))
    data_df['Protein_Name'] = data_df['Composite_Element_REF'].map(lambda x: aa_mapping_proteinName.get(x, np.nan))

    # split the gene column values into multiple rows
    data_df = split_df_column_values_into_multiple_rows(data_df, 'Gene_Name')

    protein_info = data_df['Protein_Name'].str.split('_p', n=1, expand=True)
    data_df['Protein_Basename'] = protein_info[0]
    data_df['Phospho'] = protein_info[1].map(lambda x: "p"+ str(x))
    data_df['Phospho'] = data_df['Phospho'].replace('pNone', np.nan)
    # get protein suffixes
    data_df['antibodySource'] = map(lambda x: get_protein_suffixes(x).strip('-').split('-')[0], data_df['Composite_Element_REF'])
    data_df['validationStatus'] = map(lambda x: get_protein_suffixes(x).strip('-').split('-')[1], data_df['Composite_Element_REF'])
    return data_df

def add_metadata(data_df, metadata):
    """Add metadata info to the dataframe
    """
    data_df['AliquotBarcode'] = metadata['AliquotBarcode']
    data_df['SampleBarcode'] = metadata['SampleBarcode']
    data_df['ParticipantBarcode'] = metadata['ParticipantBarcode']
    data_df['Study'] = metadata['Study'].upper()
    data_df['SampleTypeLetterCode'] = metadata['SampleTypeLetterCode']
    data_df['Platform'] = metadata['Platform']
    data_df['Pipeline'] = metadata['Pipeline']
    data_df['Center'] = metadata['DataCenterName']
    return data_df


def get_protein_suffixes(element_ref):
    antibodySource = ['M', 'R', 'G']
    validationStatus = ['V', 'C', 'NA', 'E', 'QC']
    substrings = ["-".join(["", ads, vds + ".*"]) for ads in antibodySource for vds in validationStatus]
    protein_substrings = re.compile("|".join(substrings))
    suffix_search = re.search(protein_substrings, element_ref)
    if suffix_search:
        suffix = suffix_search.group(0)
    else:
        suffix = np.nan
    return suffix

if __name__ == '__main__':
    project_id = sys.argv[1]
    bucket_name = sys.argv[2]
    filename = sys.argv[3]
    outfilename = sys.argv[4]
    metadata = {
        'AliquotBarcode':'AliquotBarcode',
        'SampleBarcode':'SampleBarcode',
        'ParticipantBarcode':'ParticipantBarcode',
        'Study':'Study',
        'SampleTypeLetterCode':'SampleTypeLetterCode',
        'Platform':'Platform'
    }
#    parse_protein(project_id, bucket_name, filename, outfilename, metadata)
    print get_protein_suffixes('PARP_cleaved-M-QC').strip('-').split('-')[0]
