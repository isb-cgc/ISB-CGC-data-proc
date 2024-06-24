'''
Created on Jun 22, 2017

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
from datetime import date, datetime
from logging import getLogger
from json import dumps
from time import sleep
import traceback

from bq_wrapper import fetch_paged_results, query_bq_table
from gdc.test.test_setup import GDCTestSetup
from gdc.util.ThreadLauncher import launch_threads
from gdc.util.gdc_util import request
from isbcgc_cloudsql_model import ISBCGC_database_helper
from util import create_log

class CCLE_datasets:
    def bio_sql2bq(self):
        return {
            'CCLE_metadata_clinical': ['[CCLE_bioclin_v0.clinical_v0]', 'case_barcode', None],
            'CCLE_metadata_biospecimen': [None, 'case_barcode', 'sample_barcode']
        }
        
    def gcs_datasets(self):
        return {
            "Aligned_Reads": ["Aligned reads"]
        }

    def bq_datasets(self):
        return {
        }
    
class TARGET_datasets:
    def bio_sql2bq(self):
        return {
            'TARGET_metadata_clinical': ['[isb-cgc:TARGET_bioclin_v0.Clinical]', 'case_barcode', None],
            'TARGET_metadata_biospecimen': ['[isb-cgc:TARGET_bioclin_v0.Biospecimen]', 'case_barcode', 'sample_barcode']
        }

    def gcs_datasets(self):
        return {
            "Aligned_Reads": ["Aligned reads", "Aligned Reads"]
        }

    # isb_label, bq table, sample_barcode
    def bq_datasets(self):
        return {
#             "miRNA Expression Quantification": [
#                 "miRNA_Gene_Quantification",
#                 "[isb-cgc:TARGET_hg38_data_v0.miRNAseq_Expression]",
#                 "sample_barcode",
#                 True
#             ],
#             "Isoform Expression Quantification": [
#                 "miRNA_Isoform_Quantification",
#                 "[isb-cgc:TARGET_hg38_data_v0.miRNAseq_Isoform_Expression]",
#                 "sample_barcode",
#                 True
#             ],
            "miRNA Expression Quantification": [
                "miRNA_Gene_Quantification",
                "[isb-cgc:test.TARGET_miRNAExpressionQuantification_HG38_170828]",
                "sample_barcode",
                True
            ],
            "Isoform Expression Quantification": [
                "miRNA_Isoform_Quantification",
                "[isb-cgc:test.TARGET_miRNAIsoformQuantification_HG38]",
                "sample_barcode",
                True
            ],
#             "Gene Expression Quantification": [
#                 "mRNA_Gene_Quantification",
#                 "[isb-cgc:TARGET_hg38_data_v0.RNAseq_Gene_Expression]",
#                 "sample_barcode",
#                 True
#             ]
        }
        
    
class TCGA_datasets:
    def bio_sql2bq(self):
        return {
            'TCGA_metadata_annotation': ['[isb-cgc:TCGA_bioclin_v0.Annotations]', 'case_barcode', None],
            'TCGA_metadata_clinical': ['[isb-cgc:TCGA_bioclin_v0.Clinical]', 'case_barcode', None],
            'TCGA_metadata_biospecimen': ['[isb-cgc:TCGA_bioclin_v0.Biospecimen]', 'case_barcode', 'sample_barcode']
        }

    def gcs_datasets(self):
        return {
            "Aligned_Reads": ["Aligned reads", "Aligned Reads"],
            "Pathology_Image": ["Diagnostic image", "Tissue slide image"],
            "Genotypes": ["Genotypes"],
            "DNA_Variation_VCF": ["Simple nucleotide variation"]
        }

    def bq_datasets(self):
        return {
#             "Copy number segmentation": [
#                 "Copy_Number_Segment_Masked",
#                 "[isb-cgc:TCGA_hg19_data_v0.Copy_Number_Segment_Masked]",
#                 "sample_barcode",
#                 False
#             ],
#             "Methylation beta value": [
#                 "DNA_Methylation_Beta",
#                 "[isb-cgc:TCGA_hg19_data_v0.DNA_Methylation]",
#                 "sample_barcode",
#                 False
#             ],
#             "miRNA gene quantification": [
#                 "miRNA_Gene_Quantification",
#                 "[isb-cgc:TCGA_hg19_data_v0.miRNAseq_Expression]",
#                 "sample_barcode",
#                 True
#             ],
#             "miRNA isoform quantification": [
#                 "miRNA_Isoform_Quantification",
#                 "[isb-cgc:TCGA_hg19_data_v0.miRNAseq_Isoform_Expression]",
#                 "sample_barcode",
#                 True
#             ],
            "miRNA gene quantification": [
                "miRNA_Gene_Quantification",
                "[isb-cgc:test.TCGA_miRNAExpressionQuantification_HG38_170828]",
                "sample_barcode",
                True
            ],
            "miRNA isoform quantification": [
                "miRNA_Isoform_Quantification",
                "[isb-cgc:test.TCGA_miRNAIsoformQuantification_HG38_170828]",
                "sample_barcode",
                True
            ],
            "Gene expression quantification": [
                "mRNA_Gene_Quantification",
                "[isb-cgc:TCGA_hg19_data_v0.RNAseq_Gene_Expression_UNC_RSEM]",
                "sample_barcode",
                False
            ],
            "Protein expression quantification": [
                "Protein_Quantification",
                "[isb-cgc:TCGA_hg19_data_v0.Protein_Expression]",
                "sample_barcode",
                False
            ],
            "Simple somatic mutation": [
                "Somatic_Mutation",
                ["[isb-cgc:TCGA_hg19_data_v0.Somatic_Mutation_DCC]", "[isb-cgc:TCGA_hg19_data_v0.Somatic_Mutation_MC3]"],
                ["sample_barcode_tumor", "sample_barcode_normal"],
                False
            ],
            "Masked Copy Number Segment": [
                "Copy_Number_Segment_Masked",
                "[isb-cgc:TCGA_hg38_data_v0.Copy_Number_Segment_Masked]",
                "sample_barcode",
                True
            ],
            "Methylation Beta Value": [
                "DNA_Methylation_Beta",
                "[isb-cgc:TCGA_hg38_data_v0.DNA_Methylation]",
                "sample_barcode",
                True
            ],
            "miRNA Expression Quantification": [
                "miRNA_Gene_Quantification",
                "[isb-cgc:TCGA_hg38_data_v0.miRNAseq_Expression]",
                "sample_barcode",
                True
            ],
            "Isoform Expression Quantification": [
                "miRNA_Isoform_Quantification",
                "[isb-cgc:TCGA_hg38_data_v0.miRNAseq_Isoform_Expression]",
                "sample_barcode",
                True
            ],
            "Gene Expression Quantification": [
                "mRNA_Gene_Quantification",
                "[isb-cgc:TCGA_hg38_data_v0.RNAseq_Gene_Expression]",
                "sample_barcode",
                True
            ],
            "Protein expression quantification": [
                "Protein_Quantification",
                "[isb-cgc:TCGA_hg38_data_v0.Protein_Expression]",
                "sample_barcode",
                False
            ],
            "Masked Somatic Mutation": [
                "Somatic_Mutation",
                "[isb-cgc:TCGA_hg38_data_v0.Somatic_Mutation]",
                ["sample_barcode_tumor", "sample_barcode_normal"],
                False
            ],
        }
        
class GDCTestCloudSQLBQBarcodes(GDCTestSetup):
    '''
        for each of the datatypes either in cloudsql or in bigquery, checks the
        consistency between the GDC, the datastore and the sample data availibility 
        of the sample and case barcodes
    '''
    def __init__(self, param):
        super(GDCTestCloudSQLBQBarcodes, self).__init__(param)
    
    def setUp(self):
        super(GDCTestCloudSQLBQBarcodes, self).setUp()

    def merge_set_lists(self, dict1, dict2):
        keyset1 = set(dict1.keys())
        keyset2 = set(dict2.keys())
        merged = {}
        for key in keyset1 - keyset2:
            merged[key] = dict1[key]
        for key in keyset2 - keyset1:
            merged[key] = dict2[key]
        for key in keyset1 & keyset2:
            merged[key] = dict1[key] | dict2[key]
        
        return merged

    def request_response(self, endpt, params, msg):
        response = None
        retries = 5
        while retries:
            retries -= 1
            try:
                response = request(endpt, params, msg, self.log)
                response.raise_for_status()
                try:
                    rj = response.json()
                    break
                except:
                    self.log.exception('problem with response, not json: %s' % (response.text))
                    raise
            except:
                if 0 == retries:
                    self.log.exception('giving up')
                    raise
                sleep(10)
                self.log.warning('retrying request...')
            finally:
                if response:
                    response.close
        return rj

    def request_gdc_barcode_info(self, batch, program_name, start, chunk, total):
        endpt = 'https://api.gdc.cancer.gov/legacy/cases?expand=project,samples'
        params = {
            'filters': dumps({
                "op":"in",
                "content":{ 
                    "field":"project.program.name",
                    "value":[ 
                        program_name
                    ]
                } 
            }), 
            'sort':'case_id:asc', 
            'from':start, 
            'size':chunk
        }
        curstart = 1
        case_barcode2info = {}
        while True:
            msg = '\t\tproblem getting filtered map for cases'
            rj = self.request_response(endpt, params, msg)
            
            params['from'] = params['from'] + params['size']
            for index in range(len(rj['data']['hits'])):
                themap = rj['data']['hits'][index]
                case_barcode = themap['submitter_id'].strip()
                project_id = themap['project']['project_id'].strip()
                sample_barcodes = set()
                info = {
                    'case_barcode': case_barcode,
                    'project': project_id,
                    'sample_barcodes': sample_barcodes,
                    'total': rj['data']['pagination']['total']
                }
                case_barcode2info[case_barcode] = info
                if 'samples' in themap:
                    for j in range(len(themap['samples'])):
                        sample_barcode = themap['samples'][j]['submitter_id'].strip()
                        sample_barcodes.add(sample_barcode)
        
            curstart += rj['data']['pagination']['count']
            if curstart >= total or params['from'] >= rj['data']['pagination']['total']:
                break

        return case_barcode2info

    def get_gdc_barcode_info(self, program_name, log_dir):
        log = getLogger(create_log(log_dir, 'barcode_info'))
        log.info('processing {} for barcode information'.format(program_name))

        # get the total count to parallelize barcode fetches
        barcode2info = self.request_gdc_barcode_info(program_name, program_name, 1, 1, 1)
        # divide into into batches based on total
        info = barcode2info.popitem()[1]
        total = info['total']
        log.info('\tfetching {} cases for {}'.format(total, info))
        batch = total / 20
        log.info('\tlooking at batches of {} repeated 20 times for {}'.format(batch, program_name))
        params = []
        cur_start = 1

        for i in range(21):
            params += [[program_name + '_%s' % (i), program_name, cur_start, min(batch, 200), batch]]
            log.info('\t\tbatch {}: {}'.format(i, params[-1]))
            cur_start += batch
        
        calls = {
            'fn': self.request_gdc_barcode_info,
            'batches': {
                'params': params
            }
        }
        barcode2info = launch_threads(self.config, 'batches', calls, self.log)
        samples = set()
        for info in barcode2info.itervalues():
#             if 0 != len(set(cursamples) & set(samples)):
#                 raise ValueError('saw repeated barcode: {}'.format(set(cursamples) & set(cursamples)))
            samples |= set(info['sample_barcodes'])
        
        log.info('\tfinished {} for barcode information.  found {} case and {} samples'.format(program_name, len(barcode2info), len(samples)))
        return set(barcode2info.keys()), samples
    
    def get_sql_barcodes(self, tables, case = 'case_barcode', sample = 'sample_barcode'):
        table2cases = {}
        table2samples = {}
        for table, info in tables.iteritems():
            if not info[2]:
                sql = 'select {}, "" from {}'.format(case, table)
            elif not info[1]:
                sql = 'select "", {} from {}'.format(sample, table)
            else:
                sql = 'select {}, {} from {}'.format(case, sample, table)
            rows = ISBCGC_database_helper().select(self.config, sql, self.log, [])
            
            cases = set()
            samples = set()
            for row in rows:
                cases.add(row[0])
                samples.add(row[1])
            table2cases[table] = cases if 1 < len(cases) else set()
            table2samples[table] = samples if 1 < len(samples) else set()
        return table2cases, table2samples
    
    def get_bq_barcodes(self, bq_tables, case = 'case_barcode', sample = 'sample_barcode', where = None):
        bq2cases = {}
        bq2samples = {}
        bq2files = {}
        for table in bq_tables:
            if not table[0]:
                continue
            if not table[2]:
                sql = 'select {}, "" from {}'.format(case, table[0])
            elif not table[1]:
                sql = 'select "", {} from {}'.format(sample, table[0])
            else:
                sql = 'select {}, {} from {}'.format(case, sample, table[0])
            
            self.log.info('\tstart select for {} from bq{}'.format(table[0], ' where {}'.format(where) if where else ''))
            results = query_bq_table(sql, True, 'isb-cgc', self.log)
            count = 0
            page_token = None
            cases = set()
            samples = set()
            while True:
                total, rows, page_token = fetch_paged_results(results, 1000, None, page_token, self.log)
                count += 1000
                for row in rows:
                    cases.add(row[0])
                    samples.add(row[1])
                
                if not page_token:
                    self.log.info('\tfinished select from {}.  select {} total rows'.format(table, total))
                    break
                
            bq2cases[table[0]] = cases if 1 < len(cases) else set()
            bq2samples[table[0]] = samples if 1 < len(samples) else set()
        return bq2cases, bq2samples

    def diff_barcodes(self, barcodes1, tag1, barcodes2, tag2, log):
        diffs = ''
        one_vs_two = barcodes1 - barcodes2
        one_vs_two.discard(None)
        if 0 < len(barcodes2) and 0 < len(one_vs_two):
#             if 10 >= len(one_vs_two):
            if 500 >= len(one_vs_two):
                barcodes = ', '.join(sorted(one_vs_two))
            else:
                diff = sorted(one_vs_two)
                barcodes = '{}...{}'.format(', '.join(diff[:5]), ', '.join(diff[-5:]))
            diffs += '\t\t{} barcodes in {} than in {}--{}\n'.format(len(one_vs_two), tag1, tag2, barcodes)
        else:
            diffs += '\t\tall {} barcodes in {}\n'.format(tag1, tag2)
 
        two_vs_one = barcodes2 - barcodes1
        two_vs_one.discard(None)
        if 0 < len(barcodes1) and 0 < len(two_vs_one):
            if 10 >= len(two_vs_one):
                try:
                    barcodes = ', '.join(sorted(two_vs_one))
                except:
                    log.exception('problem printing barcode diff:\n\t{}'.format(two_vs_one))
                    barcodes = two_vs_one
            else:
                diff = sorted(two_vs_one)
                barcodes = '{}...{}'.format(', '.join(diff[:5]), ', '.join(diff[-5:]))
            diffs += '\t\t!!!{} barcodes in {} than in {}--{}!!!\n'.format(len(two_vs_one), tag2, tag1, barcodes)
        else:
            diffs += '\t\tall {} barcodes in {}\n'.format(tag2, tag1)
        return diffs

    def compare_barcodes(self, program_name, table, barcode_type, api, sql, label1, bq, label2, log):
        diffs = ''
        diffs += self.diff_barcodes(api, 'api', sql, label1, log)
        diffs += self.diff_barcodes(api, 'api', bq, label2, log)
        diffs += self.diff_barcodes(sql, label1, bq, label2, log)

        retval = '{} compares for {}-{}:\n{}'.format(barcode_type, program_name, table, diffs)
        log.info(retval)

        return retval

    def process_bio(self, program_name, program, log_dir):
        log_dir = log_dir + 'bio' + '/'
        log = getLogger(create_log(log_dir, 'bio'))
        
        log.info('processing {} for bio'.format(program_name))
        bio_storage2source2barcodes = {}
        
        cases, samples = self.get_gdc_barcode_info(program_name, log_dir)
        source2barcodes = bio_storage2source2barcodes.setdefault('gdc', {})
        source2barcodes['api'] = (cases, samples)
        
        sql2bq = program().bio_sql2bq()
        sql2cases, sql2samples = self.get_sql_barcodes(sql2bq)
        
        bq2cases, bq2samples = self.get_bq_barcodes(sql2bq.values())
        for table, sqlcases in sql2cases.iteritems():
            if sql2bq[table][0]:
                bqcases = bq2cases[sql2bq[table][0]]
            else:
                bqcases = set()

            sqlsamples = sql2samples[table]
            if sql2bq[table][0]:
                bqsamples = bq2samples[sql2bq[table][0]]
            else:
                bqsamples = set()
        
            source2barcodes = bio_storage2source2barcodes.setdefault('sql', {})
            source2barcodes[table] = (sqlcases, sqlsamples)
            
            source2barcodes = bio_storage2source2barcodes.setdefault('bq', {})
            source2barcodes[table] = (bqcases, bqsamples)
        
        log.info('finished {} for bio'.format(program_name))
        return bio_storage2source2barcodes

    def get_api_data_type_barcodes(self, program_name, data_type, legacy=True, log = None):
        endpt = 'https://api.gdc.cancer.gov/{}files?expand=cases,cases.project,cases.samples'.format('legacy/' if legacy else '')
        params = {
            'filters': dumps({
                "op": "and", 
                "content": [
                    {
                        "op":"=",
                        "content":{ 
                            "field":"data_type",
                            "value":[ 
                                data_type
                            ]
                        }
                    },
                    {
                        "op":"=",
                        "content":{ 
                            "field":"cases.project.program.name",
                            "value":[ 
                                program_name
                            ]
                        }
                    }
                ]
            }), 
            'sort':'file_id:asc', 
            'from':1, 
            'size':70
        }
        curstart = 1
        project2cases = {}
        project2samples = {}
        project2files = {}
        while True:
            msg = '\t\tproblem getting filtered map for files'
            rj = self.request_response(endpt, params, msg)
            
            params['from'] = params['from'] + params['size']
            for index in range(len(rj['data']['hits'])):
                themap = rj['data']['hits'][index]
                if 'cases' not in themap:
                    continue
                project_id = themap['cases'][0]['project']['project_id'].strip()
                file_barcodes = project2files.setdefault(project_id, set())
                file_barcodes.add(themap['file_id'])
                for i in range(len(themap['cases'])):
                    case_barcode = themap['cases'][i]['submitter_id'].strip()
                    case_barcodes = project2cases.setdefault(project_id, set())
                    case_barcodes.add(case_barcode)
                    if 'samples' in themap['cases'][i]:
                        for j in range(len(themap['cases'][i]['samples'])):
                            sample_barcode = themap['cases'][i]['samples'][j]['submitter_id'].strip()
                            sample_barcodes = project2samples.setdefault(project_id, set())
                            sample_barcodes.add(sample_barcode)
            
            curstart += rj['data']['pagination']['count']
            if curstart > rj['data']['pagination']['total'] or params['from'] > rj['data']['pagination']['total']:
                self.log.info('retrieved total of {} cases and {} samples for {} files for {}:{}'.format(
                    len([case for cases in project2cases.itervalues() for case in cases]), len([sample for samples in project2samples.itervalues() for sample in samples]), 
                    rj['data']['pagination']['total'], program_name, data_type))
                break

        return project2cases, project2samples, project2files


    def get_api_data_types_barcodes(self, program_name, data_types, log):
        log.info('\t\tgetting gcs data types barcodes {}-{} for gcs'.format(program_name, '"{}"'.format(', '.join(data_types))))

        try:
            total_project2cases = {}
            total_project2samples = {}
            total_project2files = {}
            for data_type in data_types:
                project2cases, project2samples, project2files = self.get_api_data_type_barcodes(program_name, data_type, False if data_type in ('Aligned Reads', 
                                                                                                                                 'miRNA Expression Quantification', 
                                                                                                                                 'Isoform Expression Quantification',
                                                                                                                                 'Gene Expression Quantification',
                                                                                                                                 'Masked Copy Number Segment',
                                                                                                                                 'Methylation Beta Value',
                                                                                                                                 'Masked Somatic Mutation') 
                                                                                 else True, log)
                total_project2cases = self.merge_set_lists(total_project2cases, project2cases)
                total_project2samples = self.merge_set_lists(total_project2samples, project2samples)
                total_project2files = self.merge_set_lists(total_project2files, project2files)
                log.info('\t\tget_api_data_types_barcodes(): {}-{} cumulative counts, cases={}, samples={}'.format(
                    program_name, data_type, sum(len(cases) for cases in total_project2cases.itervalues()), sum(len(samples) for samples in total_project2samples.itervalues())))
                
            log.info('\t\tfinished gcs data types barcodes {}-{} for gcs'.format(program_name, '"{}"'.format(', '.join(data_types))))
            return total_project2cases, total_project2samples, total_project2files
        except:
            log.exception('problem in get_api_data_types_barcodes()')
            raise
        
    def get_gcs_data_types_barcodes(self, program_name, data_types, log):
        log.info('\t\tgetting gcs data types barcodes {}-{} for gcs'.format(program_name, '"{}"'.format(', '.join(data_types))))

        try:
            stmt = 'select project_short_name, case_barcode, sample_barcode, file_gdc_id from {} where data_type in ({})'
            project2cases = {}
            project2samples = {}
            project2files = {}
            for data_type in data_types:
                build = 'HG38' if data_type in ('Aligned Reads', \
                                                'miRNA Expression Quantification', \
                                                'Isoform Expression Quantification', \
                                                'Gene Expression Quantification', \
                                                'Masked Copy Number Segment', \
                                                'Methylation Beta Value', \
                                                'Masked Somatic Mutation') \
                        else 'HG19'
                rows = ISBCGC_database_helper.select(self.config, stmt.format('{}_metadata_data_{}'.format(program_name, build), '"{}"'.format(data_type)), log, [])
                for row in rows:
                    cases = project2cases.setdefault(row[0], set())
                    cases.add(row[1])
                    samples = project2samples.setdefault(row[0], set())
                    samples.add(row[2])
                    files = project2files.setdefault(row[0], set())
                    files.add(row[3])
                log.info('\t\tget_gcs_data_types_barcodes(): {}-{} cumulative counts, cases={}, samples={}'.format(
                    program_name, data_type, sum(len(cases) for cases in project2cases.itervalues()), sum(len(samples) for samples in project2samples.itervalues())))
        
            log.info('\t\tfinished gcs data types barcodes {}-{} for gcs'.format(program_name, '"{}"'.format(', '.join(data_types))))
            return project2cases, project2samples, project2files
        except:
            log.exception('problem in get_gcs_data_types_barcodes()')
            raise

    def get_gcs_isb_label_barcodes(self, program_name, isb_label, log):
        log.info('\t\tgetting isb_label barcodes {}-{} for gcs'.format(program_name, isb_label))

        try:
            project2cases = {}
            project2samples = {}
            stmt = 'select bs.project_short_name, bs.case_barcode, sa.sample_barcode ' \
                'from {0}_metadata_sample_data_availability sa join {0}_metadata_data_type_availability da on sa.metadata_data_type_availability_id = da.metadata_data_type_availability_id ' \
                'join {0}_metadata_biospecimen bs on sa.sample_barcode = bs.sample_barcode ' \
                'where isb_label = %s group by 1, 2, 3'.format(program_name)
            rows = ISBCGC_database_helper().select(self.config, stmt, log, [isb_label])
            for row in rows:
                cases = project2cases.setdefault(row[0], set())
                cases.add(row[1])
                samples = project2samples.setdefault(row[0], set())
                samples.add(row[2])
            log.info('\t\tget_bq_isb_label_barcodes(): {}-{} cumulative counts, cases={}, samples={}'.format(
                program_name, isb_label, sum(len(cases) for cases in project2cases.itervalues()), sum(len(samples) for samples in project2samples.itervalues())))
        
            log.info('\t\tfinished get_gcs_isb_label_barcodes() {}-{} for gcs'.format(program_name, isb_label))
            return project2cases, project2samples
        except:
            log.exception('problem in get_bq_isb_label_barcodes()')
            raise
        
        log.info('\t\tfinished isb_label barcodes {}-{} for gcs'.format(program_name, isb_label))

    def compare_isb_label_gcs(self, program_name, isb_label, data_types, results, log_dir):
        log_dir = log_dir + isb_label + '/gcs/'
        log = getLogger(create_log(log_dir, '{}_{}_gcs'.format(program_name, isb_label)))
        log.info('\tprocessing {}-{} for gcs'.format(program_name, isb_label))

        api_project2cases, api_project2samples, api_project2files = self.get_api_data_types_barcodes(program_name, data_types, log)
        gcs_project2cases, gcs_project2samples, gcs_project2files = self.get_gcs_data_types_barcodes(program_name, data_types, log)
        label_project2cases, label_project2samples = self.get_gcs_isb_label_barcodes(program_name, isb_label, log)
        
        project2barcodes = results.setdefault(isb_label, {})
        
        api_cases = set(case for cases in api_project2cases.itervalues() for case in cases)
        gcs_cases = set(case for cases in gcs_project2cases.itervalues() for case in cases)
        label_cases = set(case for cases in label_project2cases.itervalues()  for case in cases)
        api_samples = set(sample for samples in api_project2samples.itervalues() for sample in samples)
        gcs_samples = set(sample for samples in gcs_project2samples.itervalues() for sample in samples)
        label_samples = set(sample for samples in label_project2samples.itervalues() for sample in samples)
        api_files = set(nextfile for files in api_project2files.itervalues() for nextfile in files)
        gcs_files = set(nextfile for files in gcs_project2files.itervalues() for nextfile in files)
        project2barcodes['all'] = (api_cases, gcs_cases, label_cases, api_samples, gcs_samples, label_samples, api_files, gcs_files)
        
        for project, api_cases in api_project2cases.iteritems():
            try:
                gcs_cases = gcs_project2cases[project]
            except:
                log.info('no cases for gcs {}'.format(project))
                gcs_cases = set()
            try:
                label_cases = label_project2cases[project]
            except:
                log.info('no cases for label {}'.format(project))
                label_cases = set()
            
            api_samples = api_project2samples[project]
            try:
                gcs_samples = gcs_project2samples[project]
            except:
                log.info('no samples for gcs {}'.format(project))
                gcs_samples = set()
            try:
                label_samples = label_project2samples[project]
            except:
                log.info('no samples for label {}'.format(project))
                label_samples = set()
            
            api_files = api_project2files[project]
            try:
                gcs_files = gcs_project2files[project]
            except:
                log.info('no files for gcs {}'.format(project))
                gcs_files = set()
            project2barcodes[project] = (api_cases, gcs_cases, label_cases, api_samples, gcs_samples, label_samples, api_files, gcs_files)
            
        
        log.info('\tfinished {}-{} for gcs'.format(program_name, isb_label))
        return {}

    def process_gcs(self, program_name, program, results, log_dir):
        log_dir = log_dir + 'gcs' + '/'
        log = getLogger(create_log(log_dir, '{}_gcs'.format(program_name)))
        log.info('processing {} for gcs'.format(program_name))
        
        isb_label2tables = program().gcs_datasets()
        params = []
        for isb_label, data_types in isb_label2tables.iteritems():
            params += [[program_name, isb_label, data_types, results, log_dir]]
        calls = {
            'fn': self.compare_isb_label_gcs,
            'labels': {
                'params': params
            }
        }
        launch_threads(self.config, 'labels', calls, self.log)

        log.info('finished {} for gcs'.format(program_name))

    def get_bq_data_type_barcodes(self, program_name, bq_table, sample_barcode, has_file, log):
        log.info('\t\tgetting bq data type barcodes {}-{} for gcs'.format(program_name, bq_table))

        try:
            if 'Methylation' in bq_table:
                project = '"ALL"'
            else:
                project = 'project_short_name'
            if has_file:
                stmt = 'select {}, case_barcode, {}, file_gdc_id from {} group by 1, 2, 3, 4'.format(project, sample_barcode, bq_table)
            else:
                stmt = 'select {}, case_barcode, {}, "" from {} group by 1, 2, 3'.format(project, sample_barcode, bq_table)
            project2cases = {}
            project2samples = {}
            project2files = {}
            results = query_bq_table(stmt, True, 'isb-cgc', self.log)
            count = 0
            page_token = None
            while True:
                total, rows, page_token = fetch_paged_results(results, 1000, None, page_token, self.log)
                count += 1000
                for row in rows:
                    cases = project2cases.setdefault(row[0], set())
                    cases.add(row[1])
                    samples = project2samples.setdefault(row[0], set())
                    samples.add(row[2])
                    files = project2files.setdefault(row[0], set())
                    if 0 < len(files):
                        files.add(row[3])
                
                if not page_token:
                    self.log.info('\tfinished select from {}.  selected {} total rows'.format(bq_table, total))
                    break
        
            log.info('\t\tfinished bq data type barcodes {}-{} for gcs'.format(program_name, bq_table))
            return project2cases, project2samples, project2files
        except:
            log.exception('problem in get_bq_data_type_barcodes()')
            raise

    def compare_isb_label_bq(self, program_name, data_type, isb_label, bq_table, sample_barcode, has_file, bq_results, log_dir):
        log_dir = log_dir + isb_label + '/'
        log = getLogger(create_log(log_dir, '{}_{}_bq'.format(program_name, data_type)))
        log.info('\tprocessing {}-{} for bq'.format(program_name, isb_label))

        api_project2cases, api_project2samples, api_project2files = self.get_api_data_types_barcodes(program_name, [data_type], log)
        if 'somatic' not in data_type.lower():
            bq_project2cases, bq_project2samples, bq_project2files = self.get_bq_data_type_barcodes(program_name, bq_table, sample_barcode, has_file, log)
        else:
            if "Simple somatic mutation" == data_type:
                bq_project2cases1, bq_project2samples1, bq_project2files1 = self.get_bq_data_type_barcodes(program_name, bq_table[0], sample_barcode[0], has_file, log)
                bq_project2cases_normal1, bq_project2samples_normal1, bq_project2files_normal1 = self.get_bq_data_type_barcodes(program_name, bq_table[0], sample_barcode[1], has_file, log)

                bq_project2cases2, bq_project2samples2, bq_project2files2 = self.get_bq_data_type_barcodes(program_name, bq_table[1], sample_barcode[0], has_file, log)
                bq_project2cases_normal2, bq_project2samples_normal2, bq_project2files_normal2 = self.get_bq_data_type_barcodes(program_name, bq_table[1], sample_barcode[1], has_file, log)
                
                bq_project2cases = self.merge_set_lists(bq_project2cases1, bq_project2cases2)
                bq_project2samples = self.merge_set_lists(bq_project2samples1, bq_project2samples2)
                bq_project2files = self.merge_set_lists(bq_project2files1, bq_project2files2)
            else:
                bq_project2cases, bq_project2samples, bq_project2files = self.get_bq_data_type_barcodes(program_name, bq_table, sample_barcode[0], has_file, log)
                bq_project2cases_normal, bq_project2samples_normal, bq_project2files_normal = self.get_bq_data_type_barcodes(program_name, bq_table, sample_barcode[1], has_file, log)
        label_project2cases, label_project2samples = self.get_gcs_isb_label_barcodes(program_name, isb_label, log)
        
        project2barcodes = bq_results.setdefault(isb_label, {})
        api_cases = set(case for cases in api_project2cases.itervalues() for case in cases)
        bq_cases = set(case for cases in bq_project2cases.itervalues() for case in cases)
        label_cases = set(case for cases in label_project2cases.itervalues()  for case in cases)
        api_samples = set(sample for samples in api_project2samples.itervalues() for sample in samples)
        bq_samples = set(sample for samples in bq_project2samples.itervalues() for sample in samples)
        label_samples = set(sample for samples in label_project2samples.itervalues() for sample in samples)
        api_files = set(file for files in api_project2files.itervalues() for file in files)
        bq_files = set(file for files in bq_project2files.itervalues() for file in files)
        project2barcodes['all'] = (api_cases, bq_cases, label_cases, api_samples, bq_samples, label_samples, api_files, bq_files)
        
        for project, api_cases in api_project2cases.iteritems():
            try:
                bq_cases = bq_project2cases[project]
            except:
                log.info('no cases for gcs {}'.format(project))
                bq_cases = set()
            try:
                label_cases = label_project2cases[project]
            except:
                log.info('no cases for label {}'.format(project))
                label_cases = set()
            
            api_samples = api_project2samples[project]
            try:
                bq_samples = bq_project2samples[project]
            except:
                log.info('no samples for gcs {}'.format(project))
                bq_samples = set()
            try:
                label_samples = label_project2samples[project]
            except:
                log.info('no samples for label {}'.format(project))
                label_samples = set()

            api_files = api_project2files[project]
            try:
                bq_files = bq_project2files[project]
            except:
                log.info('no files for gcs {}'.format(project))
                bq_files = set()

            project2barcodes[project] = (api_cases, bq_cases, label_cases, api_samples, bq_samples, label_samples, api_files, bq_files)
        
        log.info('\tfinished {}-{} for gcs'.format(program_name, isb_label))
        return {}

    def process_bq(self, program_name, program, bq_results, log_dir):
        log_dir = log_dir + 'bq' + '/'
        log = getLogger(create_log(log_dir, '{}_bq'.format(program_name)))
        # data type: isb_label, bq table, sample_barcode
        isb_label2tables = program().bq_datasets()
        params = []
        for data_type, info in isb_label2tables.iteritems():
            params += [[program_name, data_type, info[0], info[1], info[2], info[3], bq_results, log_dir]]
        calls = {
            'fn': self.compare_isb_label_bq,
            'labels': {
                'params': params
            }
        }
        launch_threads(self.config, 'labels', calls, self.log)

        log.info('processing {} bq'.format(program_name))
        log.info('finished {} bq'.format(program_name))

    def process_program(self, program_name, program, log_dir):
        try:
            log_dir = log_dir + program_name + '/'
            log = getLogger(create_log(log_dir, program_name))
            log.info('processing {}'.format(program_name))
            
            output_bio_compare = 'case and sample compare:\n'
            bio_storage2source2barcodes = self.process_bio(program_name, program, log_dir)
            cases, samples = bio_storage2source2barcodes['gdc']['api']
            for sql_source, barcodes in bio_storage2source2barcodes['sql'].iteritems():
                sqlcases, sqlsamples = barcodes
                sources = sorted(bio_storage2source2barcodes['bq'].keys())
                for bq_source in sources:
                    barcodes = bio_storage2source2barcodes['bq'][bq_source]
                    bqcases, bqsamples = barcodes
                    output_bio_compare += self.compare_barcodes(program_name, 'sql-{}:bq-{}'.format(sql_source, bq_source), 'case', cases, sqlcases, 'sql', bqcases, 'bq', log) + '\n'
                    output_bio_compare += self.compare_barcodes(program_name, 'sql-{}:bq-{}'.format(sql_source, bq_source), 'sample', samples, sqlsamples, 'sql', bqsamples, 'bq', log) + '\n{}\n'

            output_bio_counts = 'Case and Sample compares for {} clinical and biospecimen\n\nGDC Case API:\ncases\tsamples\n{}\t{}\n\nCloud SQL\n'.format(program_name, len(cases), len(samples))
            for source, barcodes in bio_storage2source2barcodes['sql'].iteritems():
                sqlcases, sqlsamples = barcodes
                output_bio_counts += '{}:\ncases\tsamples\n{}\t{}\n\n'.format(source, len(sqlcases), len(sqlsamples))
            
            output_bio_counts += 'BigQuery\n'
            sources = sorted(bio_storage2source2barcodes['bq'].keys())
            for source in sources:
                bqcases, bqsamples = bio_storage2source2barcodes['bq'][source]
                output_bio_counts += '{}:\ncases\tsamples\n{}\t{}\n\n'.format(source, len(bqcases), len(bqsamples))
    
            gcs_results = {}
            self.process_gcs(program_name, program, gcs_results, log_dir)
            output_gcs_compare = 'case, sample and file compare for gcs vs. isb_label:\n'
            output_gcs_counts = ''
            for isb_label in gcs_results:
                for project, barcodes in gcs_results[isb_label].iteritems():
                    output_gcs_compare += self.compare_barcodes(program_name, '{0}:project-{1}:label-{2}'.format(program_name, project, isb_label), 'case', barcodes[0], barcodes[1], 'gcs', barcodes[2], 'label', log) + '\n'
                    output_gcs_compare += self.compare_barcodes(program_name, '{0}:project-{1}:label-{2}'.format(program_name, project, isb_label), 'sample', barcodes[3], barcodes[4], 'gcs', barcodes[5], 'label', log) + '\n'
                    output_gcs_compare += self.compare_barcodes(program_name, '{0}:project-{1}:label-{2}'.format(program_name, project, isb_label), 'file', barcodes[6], barcodes[7], 'gcs', set(), 'label', log) + '\n{}\n'
                    if 'all' == project:
                        output_gcs_counts = '{}Case and Sample compares for {} Google Cloud Storage\n\nTotals:\ncases\napi\tgcs\tisb_label\n{}\t{}\t{}\nsamples\napi\tgcs\tisb_label\n{}\t{}\t{}\nfiles\napi\tgcs\n{}\t{}\n\n' \
                            .format('{}\n'.format('*' * 20), program_name, len(barcodes[0]), len(barcodes[1]), len(barcodes[2]), len(barcodes[3]), len(barcodes[4]), len(barcodes[5]), len(barcodes[6]), len(barcodes[7]))
 
            bq_results = {}
            self.process_bq(program_name, program, bq_results, log_dir)
            output_bq_compare = 'case, sample and file compare for bq vs. isb_label:\n'
            output_bq_counts = ''
            for isb_label in bq_results:
                for project, barcodes in bq_results[isb_label].iteritems():
                    output_bq_compare += self.compare_barcodes(program_name, '{0}:project-{1}:label-{2}'.format(program_name, project, isb_label), 'case', barcodes[0], barcodes[1], 'bq', barcodes[2], 'label', log) + '\n'
                    output_bq_compare += self.compare_barcodes(program_name, '{0}:project-{1}:label-{2}'.format(program_name, project, isb_label), 'sample', barcodes[3], barcodes[4], 'bq', barcodes[5], 'label', log) + '\n'
                    output_bq_compare += self.compare_barcodes(program_name, '{0}:project-{1}:label-{2}'.format(program_name, project, isb_label), 'file', barcodes[6], barcodes[7], 'bq', set(), 'label', log) + '\n'
                    if 'all' == project:
                        output_bq_counts = '{}Case and Sample compares for {} Google BigQuery\n\nTotals:\ncases\napi\tbq\tisb_label\n{}\t{}\t{}\nsamples\napi\tbq\tisb_label\n{}\t{}\t{}\nfiles\napi\tbq\n{}\t{}\n\n' \
                            .format('{}\n'.format('*' * 20), program_name, len(barcodes[0]), len(barcodes[1]), len(barcodes[2]), len(barcodes[3]), len(barcodes[4]), len(barcodes[5]), len(barcodes[6]), len(barcodes[7]))
            
            with open('gdc/doc/' + str(date.today()).replace('-', '_') + '_{}_validate_bq_gcs_label.txt'.format(program_name), 'w') as out:
                out.writelines(['Validity Report\n\n', output_bio_counts, output_bio_compare, output_gcs_counts, output_gcs_counts, output_bq_counts, output_bq_compare])
                out.write('Differences:\n\tapi\tgcs\tisb_label\tbq\t\napi\t{}\n')
            
            log.info('finished {}'.format(program_name))
        except:
            log.exception('problem processing {}'.format(program_name))
            raise
        return {}
        
    def test_gcs_bq_validity(self):
        log_dir = str(date.today()).replace('-', '_') + '_validate/'
        calls = {
            'fn': self.process_program,
            'validity': {
                'params': [
                    ['CCLE', CCLE_datasets, log_dir],
                    ['TARGET', TARGET_datasets, log_dir],
                    ['TCGA', TCGA_datasets, log_dir]
                ]
            }
        }
#         self.process_gcs('TARGET', TARGET_datasets, log_dir + 'TARGET/')
        launch_threads(self.config, 'validity', calls, self.log)
