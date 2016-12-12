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
from gdc.etl import etl
from util import flatten_map

class Mirna_expression_quantification(etl.Etl):
    
    def __init__(self):
        '''
        Constructor
        '''
        '''
        Constructor

        columns in the GDC file:
        miRNA_ID	read_count	reads_per_million_miRNA_mapped	cross-mapped
         
        columns for the BQ table:
        sample_barcode
        miRNA_ID
        read_count
        reads_per_million_miRNA_mapped
        cross-mapped
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
        pass
