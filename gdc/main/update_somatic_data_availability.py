'''
Created on May 15, 2017

Copyright 2017, Institute for Systems Biology.

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
def update_somantic_data_availbility():
    bq_query = 'SELECT sample_barcode FROM `isb-cgc.GDC_metadata.rel5_aliquot2caseIDmap` join `isb-cgc.TCGA_hg38_data_v0.Somatic_Mutation` ' \
            'on aliquot_barcode = Tumor_Sample_Barcode or aliquot_barcode = Matched_Norm_Sample_Barcode ' \
            'union all ' \
            'SELECT sample_barcode FROM `isb-cgc.GDC_metadata.rel5_aliquot2caseIDmap` join `isb-cgc.TCGA_hg38_data_v0.Somatic_Mutation`' \
             '  on aliquot_barcode = Matched_Norm_Sample_Barcode ' \
            'order by 1'

if __name__ == '__main__':
    pass