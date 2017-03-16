'''
Created on Dec 27, 2016

opyright 2017, Institute for Systems Biology.

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
import unittest

from gdc.test.test_setup import GDCTestSetup
from gdc.util.gdc_util import instantiate_etl_class
from gdc.util.upload_files import upload_files
from util import import_module

class GDCTestUpload(GDCTestSetup):
    def setUp(self):
        super(GDCTestUpload, self).setUp()
        config = {
            "upload_files": True,
            'upload_etl_files': True,
            'download_files_per': 0,
            'gcs_wrapper': 'gcs_wrapper_gcloud',
            'cloud_projects': {
                'open': 'isb-cgc'
            },
            "buckets": {
                "open": "isb-cgc-scratch",
                "controlled": "62f2c827-test-a",
                "folders": {
                    "base_file_folder": "gdc/NCI-GDC_local/",
                    "base_run_folder": "gdc/NCI-GDC_local_run/"
                }
            }
        }

        self.config.update(config)
        
    def setup_file_ids(self, config, data_type):
        data_type2experimental_strategy = {
            'Gene Expression Quantification': 'RNA-Seq',
            'Methylation Beta Value': 'Methylation Array',
            'Masked Somatic Mutation': 'WXS',
            'Isoform Expression Quantification': 'miRNA-Seq',
            'Aligned Reads': 'WXS',
            'Masked Copy Number Segment': 'Genotyping Array',
            'Raw Simple Somatic Mutation': 'WXS',
            'Aggregated Somatic Mutation': 'WXS',
            'Aligned Reads': 'miRNA-Seq',
            'miRNA Expression Quantification': 'miRNA-Seq',
            'Annotated Somatic Mutation': 'WXS',
            'Aligned Reads': 'RNA-Seq',
            'Copy Number Segment': 'Genotyping Array'
        }
        data_type2data_category = {
            'Aggregated Somatic Mutation': 'Simple Nucleotide Variation',
            'Isoform Expression Quantification': 'Transcriptome Profiling',
            'Annotated Somatic Mutation': 'Simple Nucleotide Variation',
            'miRNA Expression Quantification': 'Transcriptome Profiling',
            'Methylation Beta Value': 'DNA Methylation',
            'Aligned Reads': 'Raw Sequencing Data',
            'Masked Copy Number Segment': 'Copy Number Variation',
            'Clinical Supplement': 'Clinical',
            'Copy Number Segment': 'Copy Number Variation',
            'Masked Somatic Mutation': 'Simple Nucleotide Variation',
            'Biospecimen Supplement': 'Biospecimen',
            'Raw Simple Somatic Mutation': 'Simple Nucleotide Variation',
            'Gene Expression Quantification': 'Transcriptome Profiling',
        }
        
        file_ids = {}
        with open(config['input_id_file']) as file_id_file:
            for line in file_id_file:
                info = {
                    'data_type': data_type, 
                    'experimental_strategy': data_type2experimental_strategy[data_type],
                    'data_category': data_type2data_category[data_type],
                    'analysis': {
                    },
                    'cases': [
                        {
                            'case_id': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
                            'project': {
                                'project_id': 'TCGA-UCS',
                                'program': {
                                        'name': 'TCGA'
                                }
                            },
                            'samples': [
                                {
                                    'sample_id': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx', 
                                    'portions': [
                                        {
                                            'analytes': [{
                                                'aliquots': [{
                                                    'submitter_id': 'TCGA-00-0000-01A-01D-A385-10', 
                                                    'aliquot_id': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' 
                                                }]
                                            }]
                                        }
                                    ]
                                 }
                            ]
                        }
                    ]
                }
                fields = line.strip().split('\t')
                aliquot_barcode = fields[0]
                info['cases'][0]['submitter_id'] = aliquot_barcode[:12]
                info['cases'][0]['samples'][0]['submitter_id'] = aliquot_barcode[:16]
                info['cases'][0]['samples'][0]['portions'][0]['analytes'][0]['aliquots'][0]['submitter_id'] = aliquot_barcode
                info['file_id'] = fields[1]
                info['file_name'] = fields[2]
                if 'Methylation27' in fields[2]:
                    info['platform'] = 'Illumina Human Methylation 27'
                    info['cases'][0]['samples'][0]['portions'][0]['analytes'][0]['aliquots'][0]['submitter_id'] = fields[2].split('.')[5]
                elif 'Methylation450' in fields[2]:
                    info['platform'] = 'Illumina Human Methylation 450'
                    info['cases'][0]['samples'][0]['portions'][0]['analytes'][0]['aliquots'][0]['submitter_id'] = fields[2].split('.')[5]
                
                if data_type == 'Gene Expression Quantification':
                    file_fields = fields[2].split('.')
                    if 'htseq' == file_fields[1]:
                        info['analysis']['workflow_type'] = 'HTSeq - Counts'
                    else:
                        info['analysis']['workflow_type'] = 'HTSeq - ' + file_fields[1]
                
                file_ids[fields[1]] = info
        
        return file_ids

    def run_upload(self, config, project, data_type, log):
        file_ids = self.setup_file_ids(config, data_type)
        instantiate_etl_class(config, 'TCGA', data_type, log).initialize(config, log)
        for download_files_per in [17]:
            config['download_files_per'] = download_files_per
            try:
                upload_files(config, 'current', file_ids, project, data_type, log)
            except:
                log.exception('failed with lines per @ %d' % (download_files_per))
        
        instantiate_etl_class(config, 'TCGA', data_type, log).finalize(config, log)

    def test_mrna_expression_quantification(self):
        pass

    def test_methylation(self):
        pass

    def test_mirna_expression_quantification(self):
        pass

    def test_mirna_isoform(self):
        self.config['input_id_file'] = 'gdc/doc/gdc_manifest_mirnaiso.2016-12-12_test_40.tsv'
        module = import_module(self.config['gcs_wrapper'])
        module.open_connection(self.config, self.log)
    
        try:
            project = 'TCGA-UCS'
            data_type = 'Isoform Expression Quantification'
            self.run_upload(self.config, project, data_type, self.log)
        finally:
            module.close_connection()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()