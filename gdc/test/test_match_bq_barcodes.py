'''
Created on May 3, 2017

@author: michael
'''
import json
from time import sleep

from bq_wrapper import fetch_paged_results, query_bq_table
from gdc.test.test_setup import GDCTestSetup
from gdc.util.gdc_util import request
from isbcgc_cloudsql_model import ISBCGC_database_helper

class GDCTestCloudSQLBQBarcodeMatchup(GDCTestSetup):
    def __init__(self, param):
        super(GDCTestCloudSQLBQBarcodeMatchup, self).__init__(param)
    
    def setUp(self):
        self.config = './gdc/config/uploadGDC_test_data.json'
        super(GDCTestCloudSQLBQBarcodeMatchup, self).setUp()


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

    def get_case_counts(self):
        endpt = 'https://gdc-api.nci.nih.gov/legacy/cases?expand=project,samples'
        params = {
            'filters': {}, 
            'sort':'case_id:asc', 
            'from':1, 
            'size':200}
        curstart = 1
        program2case_barcodes = {}
        program2case_no_samples_barcodes = {}
        program2sample_barcodes = {}
        while True:
            msg = '\t\tproblem getting filtered map for cases'
            rj = self.request_response(endpt, params, msg)
            
            params['from'] = params['from'] + params['size']
            for index in range(len(rj['data']['hits'])):
                themap = rj['data']['hits'][index]
                case_barcode = themap['submitter_id'].strip()
                project_id = themap['project']['project_id'].strip()
                program = project_id.split('-')[0]
                case_barcodes = program2case_barcodes.setdefault(program, set())
                no_sample_barcodes = program2case_no_samples_barcodes.setdefault(program, set())
                case_barcodes.add(case_barcode)
                if 'samples' not in themap:
                    no_sample_barcodes.add(case_barcode)
                else:
                    for j in range(len(themap['samples'])):
                        sample_barcode = themap['samples'][j]['submitter_id'].strip()
                        sample_barcodes = program2sample_barcodes.setdefault(program, set())
                        sample_barcodes.add(sample_barcode)
        
            curstart += rj['data']['pagination']['count']
            if curstart > rj['data']['pagination']['total']:
                break

        if 0 < len(program2case_no_samples_barcodes):
            for program in program2case_no_samples_barcodes:
                self.log.info('\t\tfound %d records with no samples for %s' % (len(program2case_no_samples_barcodes[program]), program))
        
        return program2case_barcodes, program2case_no_samples_barcodes, program2sample_barcodes

    def get_file_counts(self):
        endpt = 'https://gdc-api.nci.nih.gov/legacy/files?expand=cases,cases.project,cases.samples'
        params = {
            'filters': {}, 
            'sort':'file_id:asc', 
            'from':1, 
            'size':20}
        curstart = 1
        no_case = 0
        program2case_barcodes = {}
        program2case_no_samples_barcodes = {}
        program2sample_barcodes = {}
        while True:
            msg = '\t\tproblem getting filtered map for files'
            rj = self.request_response(endpt, params, msg)
            
            params['from'] = params['from'] + params['size']
            for index in range(len(rj['data']['hits'])):
                themap = rj['data']['hits'][index]
                if 'cases' not in themap:
                    no_case += 1
                    continue
                for i in range(len(themap['cases'])):
                    case_barcode = themap['cases'][i]['submitter_id'].strip()
                    project_id = themap['cases'][i]['project']['project_id'].strip()
                    program = project_id.split('-')[0].strip()
                    case_barcodes = program2case_barcodes.setdefault(program, set())
                    no_sample_barcodes = program2case_no_samples_barcodes.setdefault(program, set())
                    case_barcodes.add(case_barcode)
                    if 'samples' not in themap['cases'][i]:
                        no_sample_barcodes.add(case_barcode)
                    else:
                        for j in range(len(themap['cases'][i]['samples'])):
                            sample_barcode = themap['cases'][i]['samples'][j]['submitter_id'].strip()
                            sample_barcodes = program2sample_barcodes.setdefault(program, set())
                            sample_barcodes.add(sample_barcode)
            
            curstart += rj['data']['pagination']['count']
            if curstart > rj['data']['pagination']['total']:
                break

        if 0 < no_case:
            self.log.info('found %d records with no cases' % (no_case))
        
        if 0 < len(program2case_no_samples_barcodes):
            for program in program2case_no_samples_barcodes:
                self.log.info('\t\tfound %d records with no samples for %s' % (len(program2case_no_samples_barcodes[program]), program))
        
        return program2case_barcodes, program2case_no_samples_barcodes, program2sample_barcodes

    def get_gdc_case_info(self, barcode_type, barcodes, tag):
        barcode2infos = {}
        endpt_template = 'https://gdc-api.nci.nih.gov/{}cases?expand=samples,platform'
        for endpt_type in ('legacy/', ''):
            endpt = endpt_template.format(endpt_type)
            barcodes = list(barcodes)
            start = 0
            batch = 20
            curstart = 1
            while start < len(barcodes):
                self.log.info('\t\t\tfetching batch %d:%d of total %d for %s' % (start, start + batch, len(barcodes), tag))
                filt = {
                    'op':'in', 
                    'content':{
                        'field':barcode_type, 
                        'value':barcodes[start:start + batch]}}
                params = {
                    'filters':json.dumps(filt), 
                    'sort':'file_id:asc', 
                    'from':1, 
                    'size':20}
                start += batch
                while True:
                    msg = '\t\tproblem getting filtered map for files'
                    rj = self.request_response(endpt, params, msg)
                    params['from'] = params['from'] + params['size']
                    for index in range(len(rj['data']['hits'])):
                        try:
                            themap = rj['data']['hits'][index]
                            notfound = True
                            case_barcode = themap['submitter_id'].strip()
                            if case_barcode in barcodes:
                                barcode2infos[case_barcode] = barcode2infos.setdefault(case_barcode, []) + [[case_barcode]]
                                notfound = False
                                break
                            if 'samples' in themap:
                                for j in range(len(themap['samples'])):
                                    sample_barcode = themap['samples'][j]['submitter_id'].strip()
                                    if sample_barcode in barcodes:
                                        barcode2infos[sample_barcode] = barcode2infos.setdefault(sample_barcode, []) + [[sample_barcode]]
                                        notfound = False
                                        break
                            if notfound:
                                raise ValueError('unexpected mismatch of return with barcodes:\n{}\n'.format(', '.join(barcodes), json.dumps(themap, indent=2)))
                        except:
                            self.log.exception('problem with parsing returned json: \n{}'.format(json.dumps(rj, indent=2)))
                    
                    curstart += rj['data']['pagination']['count']
                    if curstart > rj['data']['pagination']['total']:
                        break
        
        return barcode2infos

    seen_gdc_barcodes = set()
    def get_gdc_file_info(self, barcode_type, barcodes, tag):
        if barcodes in self.seen_gdc_barcodes:
            self.log.info('barcodes seen already, skipping repeat')
        self.seen_gdc_barcodes.add(tuple(barcodes))

        barcode2infos = {}
        endpt_template = 'https://gdc-api.nci.nih.gov/{}files?expand=cases,cases.samples,platform,analysis'
        for endpt_type in ('legacy/', ''):
            endpt = endpt_template.format(endpt_type)
            barcodes = list(barcodes)
            start = 0
            batch = 20
            curstart = 1
            while start < len(barcodes):
                self.log.info('\t\t\tfetching batch %d:%d of total %d for %s' % (start, start + batch, len(barcodes), tag))
                filt = {
                    'op':'in', 
                    'content':{
                        'field':barcode_type, 
                        'value':barcodes[start:start + batch]}}
                params = {
                    'filters':json.dumps(filt), 
                    'sort':'file_id:asc', 
                    'from':1, 
                    'size':20}
                start += batch
                while True:
                    msg = '\t\tproblem getting filtered map for files'
                    rj = self.request_response(endpt, params, msg)
                    params['from'] = params['from'] + params['size']
                    for index in range(len(rj['data']['hits'])):
                        try:
                            themap = rj['data']['hits'][index]
                            data_type = themap['data_type'].strip() if 'data_type' in themap else 'none'
                            data_format = themap['data_format'].strip() if 'data_format' in themap else 'none'
                            if 'data_type' not in themap:
                                acopy = themap.copy()
                                acopy.pop('cases')
                                self.log.info('data_type not in the map:\n{}'.format(json.dumps(acopy, indent=2)))
                            experimental_strategy = themap['experimental_strategy'].strip() if 'experimental_strategy' in themap else 'none'
                            platform = themap['platform'] if 'platform' in themap else 'none'
                            workflow_type = themap['analysis']['workflow_type'] if 'analysis' in themap else 'none'
                            notfound = True
                            for i in range(len(themap['cases'])):
                                case_barcode = themap['cases'][i]['submitter_id'].strip()
                                if case_barcode in barcodes:
                                    barcode2infos[case_barcode] = barcode2infos.setdefault(case_barcode, []) + [[data_type, data_format, experimental_strategy, platform, workflow_type]]
                                    notfound = False
                                    break
                                if 'samples' in themap['cases'][i]:
                                    for j in range(len(themap['cases'][i]['samples'])):
                                        sample_barcode = themap['cases'][i]['samples'][j]['submitter_id'].strip()
                                        if sample_barcode in barcodes:
                                            barcode2infos[sample_barcode] = barcode2infos.setdefault(sample_barcode, []) + [[data_type, data_format, experimental_strategy, platform, workflow_type]]
                                            notfound = False
                                            break
                            if notfound:
                                raise ValueError('unexpected mismatch of return with barcodes:\n{}\n'.format(', '.join(barcodes), json.dumps(themap, indent=2)))
                        except:
                            self.log.exception('problem with parsing returned json: \n{}'.format(json.dumps(rj, indent=2)))
                    
                    curstart += rj['data']['pagination']['count']
                    if curstart > rj['data']['pagination']['total']:
                        break
        
        return barcode2infos

    seen_bq_barcodes = set()
    def get_bq_case_info(self, barcode_type, barcodes, tag):
        barcode_type = barcode_type[6:]
        return self.get_gdc_case_info(barcode_type, barcodes, tag)
        

    def get_bq_file_info(self, barcode_type, barcodes, tag):
        return self.get_gdc_file_info(barcode_type, barcodes, tag)

    seen_sql_barcodes = set()
    def get_sql_file_info(self, program, barcode_type, barcodes, tag):
        barcode2infos = {}


    def log_barcode2info(self, barcode2info):
        if 0 < len(barcode2info):
            barcode_infos = []
            for barcode, infos in barcode2info.iteritems():
                unique = set()
                for info in infos:
                    unique.add(tuple(info))
                
                barcode_infos += ['{}\n\t\t\t{}'.format(barcode, '\n\t\t\t'.join([', '.join(info) for info in set(unique)]))]
            
            self.print_partial_list('barcodes with info', barcode_infos, 20)
            info2count = {}
            for infos in barcode2info.itervalues():
                unique = set()
                for info in infos:
                    unique.add(tuple(info))
                
                for info in unique:
                    info2count[info] = info2count.setdefault(info, 0) + 1
            
            self.print_partial_list('distinct info', ['{}, {}'.format(', '.join(info), count) for (info, count) in info2count.iteritems()], 30)
        else:
            self.log.info('\n\tno file info for barcodes')

    def get_barcode_info(self, program, barcode_type, barcodes, tag1, tag2):
        barcode2info = {}
        if 'endpoint' in tag1:
            barcode2info = self.get_gdc_file_info(barcode_type, barcodes, '{}{}{}'.format(tag1, ' vs. ', tag2))
            self.log.info('\t\tfound %d records for %d barcodes of type %s for %s' % (len(barcode2info), len(barcodes), barcode_type, '{}{}{}'.format(tag1, ' vs. ', tag2)))
            self.log_barcode2info(barcode2info)
        elif ('bq' in tag1 or 'sql' in tag1) and 'endpoint' in tag2:
            barcode2info = self.get_bq_case_info(barcode_type, barcodes, '{}{}{}'.format(tag1, ' vs. ', tag2))
            self.log.info('\t\tfound %d records for %d barcodes of type %s for %s' % (len(barcode2info), len(barcodes), barcode_type, '{}{}{}'.format(tag1, ' vs. ', tag2)))
            self.log_barcode2info(barcode2info)
            barcode2info = self.get_gdc_file_info(barcode_type, barcodes, '{}{}{}'.format(tag1, ' vs. ', tag2))
            self.log.info('\t\tfound %d records for %d barcodes of type %s for %s' % (len(barcode2info), len(barcodes), barcode_type, '{}{}{}'.format(tag1, ' vs. ', tag2)))
            self.log_barcode2info(barcode2info)

    def select_clinical_bq_barcodes(self, program):
        self.log.info('start select %s bq cases' % (program.lower()))
        if 'CCLE' == program:
            clinical_query = 'SELECT case_barcode FROM [isb-cgc:%s_bioclin_v0.clinical_v0]' % (program)
        else:
            clinical_query = 'SELECT case_barcode FROM [isb-cgc:%s_bioclin_v0.Clinical]' % (program)

        clinical_results = query_bq_table(clinical_query, True, 'isb-cgc', self.log)
            
        page_token = None
        clinical_case_barcodes = set()
        dup_barcodes = set()
        while True:
            total_rows, rows, page_token = fetch_paged_results(clinical_results, 10000, None, page_token, self.log)
            for row in rows:
                case_barcode = row[0].strip()
                if case_barcode in clinical_case_barcodes:
                    dup_barcodes.add(case_barcode)
                else:
                    clinical_case_barcodes.add(case_barcode)
            
            if not page_token:
                self.log.info('\tselected total of %s case_barcodes' % (total_rows))
                break
            else:
                self.log.info('\t\tselect %d barcodes' % (len(rows)))
                
        if len(dup_barcodes) > 0:
            self.print_partial_list('duplicate case barcodes in BQ (%s)' % (len(dup_barcodes)), dup_barcodes)
        
        return clinical_case_barcodes

    def get_project_counts(self, table, column, values):
        self.log.info('start get project counts for %s:%s' % (table, column))
        clinical_query = 'SELECT project_short_name, count(*) FROM %s where %s in (\'%s\') group by 1' % (table, column, '\', \''.join(values))
        results = query_bq_table(clinical_query, True, 'isb-cgc', self.log)
        page_token = None
        output = '\tproject\tcount\n'
        while True:
            _, rows, page_token = fetch_paged_results(results, 50, None, page_token, self.log)
            for row in rows:
                output += '\t%s\t%d\n' % (row[0], row[1])
            
            if not page_token:
                self.log.info('project counts:\n%s' % (output))
                break
            else:
                pass

    def select_sample_bq_barcodes(self, program):
        self.log.info('start select %s bq samples' % (program.lower()))
        biospecimen_query = 'SELECT case_barcode, sample_barcode FROM [isb-cgc:%s_bioclin_v0.Biospecimen]' % (program)
        biospecimen_results = query_bq_table(biospecimen_query, True, 'isb-cgc', self.log)
        page_token = None
        sample_case_barcodes = set()
        sample_sample_barcodes = set()
        while True:
            total_rows, rows, page_token = fetch_paged_results(biospecimen_results, 10000, None, page_token, self.log)
            for row in rows:
                case_barcode = row[0].strip()
                sample_barcode = row[1].strip()
                if sample_barcode in sample_sample_barcodes:
                    raise ValueError('found duplicate sample entry: %s' % (sample_barcode))
                sample_case_barcodes.add(case_barcode)
                sample_sample_barcodes.add(sample_barcode)
            
            if not page_token:
                self.log.info('\tselected total of %s sample_barcodes' % (total_rows))
                break
            else:
                self.log.info('\t\tselect %d sample barcodes' % (len(rows)))
        
        return sample_sample_barcodes, sample_case_barcodes

    def select_clinical_sql_barcodes(self, program):
        self.log.info('start select %s sql cases' % (program.lower()))
        clinical_query = 'SELECT case_barcode FROM %s_metadata_clinical' % (program)
        rows = ISBCGC_database_helper.select(self.config, clinical_query, self.log, params = [])
        clinical_case_barcodes = set()
        duplicates = list()
        for row in rows:
            case_barcode = row[0].strip()
            if case_barcode in clinical_case_barcodes:
                duplicates += [case_barcode]
#                 raise ValueError('found duplicate case entry: %s' % (row[0]))
                continue
            clinical_case_barcodes.add(case_barcode)
        if 0 < len(duplicates):
            self.print_partial_list('found case duplicates', duplicates)
            query = 'select l.endpoint_type, c.endpoint_type, count(distinct l.case_barcode), count(distinct c.case_barcode) ' \
                    'from {0}_metadata_clinical l left join {0}_metadata_clinical c ' \
                    'on l.case_barcode = c.case_barcode ' \
                    'group by 1, 2'.format(program)
            rows = ISBCGC_database_helper.select(self.config, query, self.log, params = [])
            self.log.info('\nendpoint\tendpoint\tlegacy count\tcurrent count\n%s' % ('\n'.join('%s\t%s\t%d\t%d' % (row[0], row[1], row[2], row[3]) for row in rows)))
        return clinical_case_barcodes

    def select_sample_sql_barcodes(self, program):
        self.log.info('start select %s sql cases' % (program.lower()))
        sample_query = 'SELECT case_barcode, sample_barcode FROM %s_metadata_biospecimen' % (program)
        rows = ISBCGC_database_helper.select(self.config, sample_query, self.log, params = [])
        sample_case_barcodes = set()
        sample_sample_barcodes = set()
        duplicates = list()
        for row in rows:
            case_barcode = row[0].strip()
            sample_barcode = row[1].strip()
            if sample_barcode in sample_sample_barcodes:
                duplicates += [sample_barcode]
            sample_case_barcodes.add(case_barcode)
            sample_sample_barcodes.add(sample_barcode)
        if 0 < len(duplicates):
            self.print_partial_list('found sample duplicates', duplicates)
            query = 'select l.endpoint_type, c.endpoint_type, count(distinct l.sample_barcode), count(distinct c.sample_barcode) ' \
                    'from {0}_metadata_biospecimen l left join {0}_metadata_biospecimen c ' \
                    'on l.sample_barcode = c.sample_barcode ' \
                    'group by 1, 2'.format(program)
            rows = ISBCGC_database_helper.select(self.config, query, self.log, params = [])
            self.log.info('\nendpoint\tendpoint\tlegacy count\tcurrent count\n%s\n' % ('\n'.join('%s\t%s\t%d\t%d' % (row[0], row[1], row[2], row[3]) for row in rows)))
        return sample_sample_barcodes, sample_case_barcodes

    def print_partial_list(self, msg, barcodes, count = 10):
        barcodes = sorted(list(barcodes))
        if (count * 2) + 1 > len(barcodes):
            self.log.info("\n\t%s\n\t\t%s\n" % (msg, '\n\t\t'.join(barcodes)))
        else:
            self.log.info("\n\t%s: size %d\n\t\t%s\n\t\t\t...\n\t\t%s\n\n" % (msg, len(barcodes), '\n\t\t'.join(list(barcodes)[:count]), '\n\t\t'.join(list(barcodes)[-count:])))

    def write_barcodes(self, curfile, curbarcodes, curmsg):
        curbarcodes = sorted(curbarcodes)
        batch = len(curbarcodes) / 20
        start = 0
        self.log.info(curmsg)
        while start < len(curbarcodes):
            self.log.info('\t\twriting batch %d' % (start))
            curfile.write('\n'.join(curbarcodes[start:start + batch]) + '\n')
            start += batch
        
        self.log.info('finished %s' % (curmsg))

    def read_barcodes(self, curfile, curmsg):
        self.log.info(curmsg)
        barcodes = [barcode.strip() for barcode in curfile]
        self.log.info('finished {}.  read %d'.format(curmsg), len(barcodes))
        return barcodes
    

    def compare_barcodes(self, program, first_set, first_tag, second_set, second_tag, barcode_type):
        if 0 < len(first_set - second_set):
            self.print_partial_list('barcodes in {} not in {}'.format(first_tag, second_tag), first_set - second_set)
            self.get_barcode_info(program, barcode_type, first_set - second_set, first_tag, second_tag)
        else:
            self.log.info('barcodes in {} found in {}'.format(first_tag, second_tag))
        if 0 < len(second_set - first_set):
            self.print_partial_list('barcodes in {} not in {}'.format(second_tag, first_tag), second_set - first_set)
            self.get_barcode_info(program, barcode_type, second_set - first_set, second_tag, first_tag)
        else:
            self.log.info('barcodes in {} found in {}'.format(second_tag, first_tag))

    def diff_program_barcodes(self):
# these are running fine
#         _, _, _ = self.get_case_counts()
#         _, _, _ = self.get_file_counts()
        
        program2case_endpoint_case_barcodes = {}
        program2case_endpoint_case_no_samples_barcodes = {}
        program2case_endpoint_sample_barcodes = {}
        program2file_endpoint_case_barcodes = {}
        program2file_endpoint_case_no_samples_barcodes = {}
        program2file_endpoint_sample_barcodes = {}
        for program in ('CCLE', 'TARGET', 'TCGA'):
#         for program in ('TCGA',):
            self.log.info('\n=======endpoint differences for %s=======' % program)
            with open('data/%s_case_endpt_case.txt' % program, 'r') as cc, \
                 open('data/%s_case_endpt_case_no.txt' % program, 'r') as ccno, \
                 open('data/%s_case_endpt_sample.txt' % program, 'r') as cs, \
                 open('data/%s_file_endpt_case.txt' % program, 'r') as fc, \
                 open('data/%s_file_endpt_case_no.txt' % program, 'r') as fcno, \
                 open('data/%s_file_endpt_sample.txt' % program, 'r') as fs:
                program2case_endpoint_case_barcodes[program] = set(self.read_barcodes(cc, '\treading case endpoint cases for %s' % (program)))
                program2case_endpoint_case_no_samples_barcodes[program] = set(self.read_barcodes(ccno, '\treading case endpoint no samples for %s' % (program)))
                program2case_endpoint_sample_barcodes[program] = set(self.read_barcodes(cs, '\treading case endpoint samples for %s' % (program)))
                program2file_endpoint_case_barcodes[program] = set(self.read_barcodes(fc, '\treading file endpoint cases for %s' % (program)))
                program2file_endpoint_case_no_samples_barcodes[program] = set(self.read_barcodes(fcno, '\treading file endpoint no samples for %s' % (program)))
                program2file_endpoint_sample_barcodes[program] = set(self.read_barcodes(fs, '\treading case endpoint samples for %s' % (program)))
         
            case_endpoint_case_barcodes = program2case_endpoint_case_barcodes[program]
            file_endpoint_case_barcodes = program2file_endpoint_case_barcodes[program]
            case_endpoint_sample_barcodes = program2case_endpoint_sample_barcodes[program]
            file_endpoint_sample_barcodes = program2file_endpoint_sample_barcodes[program]
            clinical_case_bq_barcodes = self.select_clinical_bq_barcodes(program)
            clinical_case_sql_barcodes = self.select_clinical_sql_barcodes(program)
            sample_sample_sql_barcodes, sample_case_sql_barcodes = self.select_sample_sql_barcodes(program)
            if 'CCLE' != program:
                sample_sample_bq_barcodes, sample_case_bq_barcodes = self.select_sample_bq_barcodes(program)

            self.compare_barcodes(program, case_endpoint_case_barcodes, 'case endpoint case', file_endpoint_case_barcodes, 'file endpoint case', 'cases.submitter_id')
            self.compare_barcodes(program, case_endpoint_case_barcodes, 'case endpoint case', clinical_case_bq_barcodes, 'clinical case bq', 'cases.submitter_id')
            if 'CCLE' != program:
                self.compare_barcodes(program, case_endpoint_case_barcodes, 'case endpoint case', sample_case_bq_barcodes, 'sample case bq', 'cases.submitter_id')
            self.compare_barcodes(program, case_endpoint_case_barcodes, 'case endpoint case', clinical_case_sql_barcodes, 'clinical case sql', 'cases.submitter_id')
            self.compare_barcodes(program, case_endpoint_case_barcodes, 'case endpoint case', sample_case_sql_barcodes, 'sample case sql', 'cases.submitter_id')
            self.compare_barcodes(program, file_endpoint_case_barcodes, 'file endpoint case', clinical_case_bq_barcodes, 'clinical case bq', 'cases.submitter_id')
            if 'CCLE' != program:
                self.compare_barcodes(program, file_endpoint_case_barcodes, 'file endpoint case', sample_case_bq_barcodes, 'sample case bq', 'cases.submitter_id')
            self.compare_barcodes(program, file_endpoint_case_barcodes, 'file endpoint case', clinical_case_sql_barcodes, 'clinical case sql', 'cases.submitter_id')
            self.compare_barcodes(program, file_endpoint_case_barcodes, 'file endpoint case', sample_case_sql_barcodes, 'sample case sql', 'cases.submitter_id')
            if 'CCLE' != program:
                self.compare_barcodes(program, clinical_case_bq_barcodes, 'clinical case bq', sample_case_bq_barcodes, 'sample case bq', 'cases.submitter_id')
            self.compare_barcodes(program, clinical_case_bq_barcodes, 'clinical case bq', clinical_case_sql_barcodes, 'clinical case sql', 'cases.submitter_id')
            self.compare_barcodes(program, clinical_case_bq_barcodes, 'clinical case bq', sample_case_sql_barcodes, 'sample case sql', 'cases.submitter_id')
            if 'CCLE' != program:
                self.compare_barcodes(program, sample_case_bq_barcodes, 'sample case bq', sample_case_sql_barcodes, 'sample case sql', 'cases.submitter_id')
                self.compare_barcodes(program, sample_case_bq_barcodes, 'sample case bq', clinical_case_sql_barcodes, 'clinical case sql', 'cases.submitter_id')
            self.compare_barcodes(program, clinical_case_sql_barcodes, 'clincal case sql', sample_case_sql_barcodes, 'sample case sql', 'cases.submitter_id')

            self.compare_barcodes(program, case_endpoint_sample_barcodes, 'case endpoint sample', file_endpoint_sample_barcodes, 'file endpoint sample', 'cases.samples.submitter_id')
            if 'CCLE' != program:
                self.compare_barcodes(program, case_endpoint_sample_barcodes, 'case endpoint sample', sample_sample_bq_barcodes, 'sample sample bq', 'cases.samples.submitter_id')
            self.compare_barcodes(program, case_endpoint_sample_barcodes, 'case endpoint sample', sample_sample_sql_barcodes, 'sample sample sql', 'cases.samples.submitter_id')
            if 'CCLE' != program:
                self.compare_barcodes(program, file_endpoint_sample_barcodes, 'file endpoint sample', sample_sample_bq_barcodes, 'sample sample bq', 'cases.samples.submitter_id')
            self.compare_barcodes(program, file_endpoint_sample_barcodes, 'file endpoint sample', sample_sample_sql_barcodes, 'sample sample sql', 'cases.samples.submitter_id')
            if 'CCLE' != program:
                self.compare_barcodes(program, sample_sample_bq_barcodes, 'sample sample bq', sample_sample_sql_barcodes, 'sample sample sql', 'cases.samples.submitter_id')

    def testCloudSQLBQBarcodeMatchup(self):
        self.log.info('start testCloudSQLBQBarcodeMatchup()')
        self.diff_program_barcodes()
        self.log.info('finished testCloudSQLBQBarcodeMatchup()')

