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
import pandas as pd

from gdc.etl import etl

class Isoform_expression_quantification(etl.Etl):
    
    def __init__(self, config):
        '''
        Constructor

        columns in the GDC file:
        miRNA_ID	isoform_coords	read_count	reads_per_million_miRNA_mapped	cross-mapped	miRNA_region
         
        columns for the BQ table:
        sample_barcode
        miRNA_ID
        read_count
        reads_per_million_miRNA_mapped
        genomic_build (from isoform_coords)
        chromosome (from isoform_coords)
        start (from isoform_coords)
        end (from isoform_coords)
        strand (from isoform_coords)
        cross_mapped
        mirna_accession (from miRNA_region)
        mirna_transcript (from miRNA_region)
        project_short_name
        program_name
        sample_type_code
        file_name
        file_gdc_id
        aliquot_barcode
        case_barcode
        case_gdc_id
        sample_gdc_id
        aliquot_gdc_id

        '''
        pass

    def data_type_specific(self, config, file_df):
        # combine the two versions of the transcript fields (one with an accession, the other without)
        file_df['mirna_transcript'] = file_df[['mirna_transcript', 'mirna_trans']].apply(lambda x: x[0] if pd.isnull(x[1]) else x[1], axis=1)
