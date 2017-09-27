'''
Created on Aug 31, 2017

# Copyright 2017, Google, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

@author: michael
'''
from datetime import date
import json
import logging
import sys

from bq_wrapper import fetch_paged_results, query_bq_table
import gcs_wrapper_gcloud as wrapper
from isbcgc_cloudsql_model import ISBCGC_database_helper
from util import create_log
from oauthlib.uri_validate import path

def read_path_file(path_file, log):
    with open(path_file) as paths:
        log.info('\treading bam/bai pairs from {}'.format(path_file))
        buckets = set()
        path2bam2bai = {}
        count = 0
        for line in paths:
            if 0 == count % 10000:
                log.info('\t\tread {} bam/bai pairs'.format(count))
            count += 1
            bam_bai = line.strip().split('\t')
            path = '/'.join(bam_bai[0].split('/')[:-1])
            path_bai = '/'.join(bam_bai[1].split('/')[:-1])
            assert path == path_bai, '{} and {} paths didn\'t match'.format(path, path_bai)
            buckets.add(bam_bai[0].split('/')[2])
            
            bam = bam_bai[0].split('/')[-1]
            bai = bam_bai[1].split('/')[-1]
            assert bam.split('.')[-1] in ('bam', 'bam_HOLD_QC_PENDING'), '{} had non-standard extension'.format(bam)
            assert bai.split('.')[-1] == 'bai', '{} had non-standard extension'.format(bai)
            bam2bai = path2bam2bai.setdefault(path, {})
            assert bam not in bam2bai, '{} already seen'.format(bam)
            bam2bai[bam] = bai
        log.info('\t\tbuckets: {}'.format(buckets))
        log.info('\n\tfinished reading bam/bai pairs from {}. total: {}'.format(path_file, count))
    return buckets, path2bam2bai

def call_bigquery(table, log):
    if table in ('TARGET_metadata_data_HG38', 'TCGA_metadata_data_HG38'):
        bq_table = '[isb-cgc:GDC_metadata.rel8_fileData_current]'
    else:
        bq_table = '[isb-cgc:GDC_metadata.rel8_fileData_legacy]'
    query_results = query_bq_table('select file_name from {} where cases__project__program__name = "{}"'.format(bq_table, table.split('_')[0]), True, 'isb-cgc', log)
    all_rows = set()
    page_token = None
    while True:
        _, rows, page_token = fetch_paged_results(query_results, 20000, None, page_token, log)
        all_rows.update(rows)
        if not page_token:
            log.info('\t\tfound %s rows ' % (len(all_rows)))
            break
    return all_rows

def get_bucket_content(project_name, buckets, log):
    path2contents = {}
    return path2contents
    for bucket_name in buckets:
        wrapper.open_connection({'cloud_projects':{'open':project_name}}, log)
        files = wrapper.get_bucket_contents(bucket_name, None, log)
        log.info('\tprocessing bucket {}'.format(bucket_name))
        tries = 0
        count = 0
        while True:
            try:
                for file_spec in files:
                    if file_spec.path.split('.')[-1] not in ('bam', 'bam_HOLD_QC_PENDING', 'bai'):
                        continue
                    count += 1
                    full_path = file_spec.path.split('/')[-1]
                    parts = full_path.replace('%2F', '/').split('/')
                    path = 'gs://' + bucket_name + '/' + '/'.join(parts[:-1])
                    if path in path2contents:
                        files = path2contents[path]
                        assert parts[-1] not in files, 'found file {} twice!'.format(file_spec)
                        files.add(parts[-1])
                    else:
                        path2contents.setdefault(path, set()).add(parts[-1])
            
                break
            except Exception as e:
                tries += 1
                if tries > 2:
                    raise
                log.info('retrying due to error: {}'.format(e))
        
        wrapper.close_connection()
        log.info('\tfinished processing bucket {}.  total files: {}'.format(bucket_name, count))
    
    return path2contents

def update_database(path2bam2bai, config, log):
    log.info('\tbegin update database to match bucket index files')
    
    table2dbpath2bam2bai = {}
    for table in ['CCLE_metadata_data_HG19', 'TARGET_metadata_data_HG19', 'TARGET_metadata_data_HG38', 'TCGA_metadata_data_HG19', 'TCGA_metadata_data_HG38']:
        program_filenames = call_bigquery(table, log)
        
        dbpath2bam2bai = table2dbpath2bam2bai.setdefault(table, {})
        rows = ISBCGC_database_helper.select(config, 'select file_name_key, index_file_name from {} where "BAM" = data_format order by 1'.
                                             format(table), log, [])
        log.info('\t\tfound {} rows for {}: {}...{}'.format(len(rows), table, rows[:5], rows[-5:]))
        count = 0
        
        print_at = len(rows) / 11
        missing_files = set()
        bucket2missing_files = {}
        mismatchs = 0
        bucket2mismatchs = {}
        params = []
        for row in rows:
            if not row[0]:
                continue
            if 0 == count % print_at:
                log.info('\t\t\tlooking at row {} for {}: {}'.format(count, table, row))
            count += 1
            parts = row[0].split('/')
            path = '/'.join(parts[:-1])
            bam = parts[-1]
            bai = row[1]
            bam2bai = path2bam2bai.get(path)
            if bam2bai: 
                assert bam in bam2bai, 'didn\'t find bam {} in tsv files'.format(bam)
                if bai != bam2bai[bam]:
                    mismatchs += 1
                    curcount = bucket2mismatchs.setdefault(parts[2], 0)
                    bucket2mismatchs[parts[2]] = curcount + 1
                    params += [[bam2bai[bam], row[0]]]
            else:
                missing_files.add(row[0])
                curcount = bucket2missing_files.setdefault(parts[2], 0)
                bucket2missing_files[parts[2]] = curcount + 1
            dbbam2bai = dbpath2bam2bai.setdefault(path, {})
            if bam in dbbam2bai:
                log.warning('\t\t\tfound {} already in the db map for {}'.format(bam, table))
            assert bai, 'didn\'t find a index file for {} in the db'.format(bam) 
            dbbam2bai[bam] = bai

        if 0 < len(params):
            ISBCGC_database_helper.update(config, 'update {} set index_file_name = %s where file_name_key = %s'.format(table), log, params)
            log.info('there were {} param sets for update'.format(len(params)))
        else:
            log.info('there were no param sets for update')
        
        if 0 < len(missing_files):
            missing_files = sorted(missing_files)
            log.warning('\n\t\t\tfound {} missing files in tsv that are in db out of {} total: {} ... {}\n'.format(len(missing_files), len(rows), missing_files[:6], missing_files[-5:]))
            for bucket, bmissing_files in bucket2missing_files.iteritems():
                if 0 < bmissing_files:
                    log.warning('\n\t\t\tfound {} missing files in tsv that is in bucket {}\n'.format(bmissing_files, bucket))
        if 0 < mismatchs:
            log.warning('\n\t\t\tfound {} index between tsv and db mismatches out of {} total\n'.format(mismatchs, len(rows)))
            for bucket, bmismatchs in bucket2mismatchs.iteritems():
                if 0 < bmismatchs:
                    log.warning('\n\t\t\tfound {} mismatches between tsv and db files in bucket {}\n'.format(bmismatchs, bucket))
        log.info('\t\tfinished table {}\n\n'.format(table))
    
    log.info('\t\tfind discrepencies in the db from tsv')
    for table, dbpath2bam2bai in table2dbpath2bam2bai.iteritems():
        bucket2bambai_diff = {}
        missing_paths = set()
        path2missing_dbfiles = {}
        for path, bam2bai in path2bam2bai.iteritems():
            if path not in dbpath2bam2bai:
                not_curpath = True
                for bam in bam2bai:
                    if bam in program_filenames:
                        not_curpath = False
                        break
                if not_curpath:
                    continue
                missing_paths.add(path)
                continue
            dbbam2bai = dbpath2bam2bai[path]
            for bam, bai in bam2bai.iteritems():
                if bam not in program_filenames:
                    continue
                if bam not in dbbam2bai:
                    path2missing_dbfiles.setdefault(path, set()).add(bam)
                    continue
                dbbai = dbbam2bai[bam]
                if dbbai != bai: 
                    if dbbai.endswith('bam.bai') and bai.endswith('bai'):
                        bambai_diff = bucket2bambai_diff.setdefault(path.split('/')[2], 0)
                        bucket2bambai_diff[path.split('/')[2]] = bambai_diff + 1
                    else:
                        log.warning('\t\t\tdidn\'t find matching bai in db for table {}: {} vs {}'.format(table, bai, dbbai))
        missing_paths = sorted(list(missing_paths))
        log.warning('\t\t\tfound {} paths missing from db for table {}: {}...{}\n'.format(len(missing_paths), table, missing_paths[:4], missing_paths[-3:]))
        for path, bams in path2missing_dbfiles.iteritems():
            bams = sorted(list(bams))
            log.warning('\t\t\tfound {} files missing from db for path {} for table {}: {}...{}'.format(len(bams), path, table, bams[:4], bams[-3:]))
        log.info('\n\t\t\ttotal of {} files missing from the db that are in the tsv for table {}\n'.format(sum(len(dbfiles) for dbfiles in path2missing_dbfiles.itervalues()), table))
        for bucket, bambai_diff in bucket2bambai_diff.iteritems():
            if bambai_diff > 0:
                log.warning('\t\t\tfound {} cases of \'bai\' in tsv and \'bam.bai\' in db for {} for table {}'.format(bambai_diff, bucket, table))
    log.info('\n\t\tfinished discrepancies in the db from tsv')
    log.info('\tfinished update database to match bucket index files')

def main(project_name, bucket_name, path_file, config_file):
    log_dir = str(date.today()).replace('-', '_') + '_bam_report_/'
    log_name = create_log(log_dir, 'top_processing')
    log = logging.getLogger(log_name)
    log.info('begin bam report')
    
    buckets, path2bam2bai = read_path_file(path_file, log)
    path2contents = get_bucket_content(project_name, buckets, log)
    with open(config_file) as con_file:
        config = json.load(con_file)
    update_database(path2bam2bai, config, log)
        
    count = 0
    no_index = set()
    ext_pairs2count = {}
    for contents in path2contents.itervalues():
        if 0 == count % 1024:
            log.info('\tprocessing contents {}: {}'.format(count, contents))
        count += 1
        file_prefix = None
        for file_name in contents:
            parts = file_name.split('.')
            if 'bam' in parts[-1]:
                bam_ext = parts[-1]
                file_prefix = '.'.join(parts[:-1])
                break
        
        if not file_prefix:
            log.warn('\tdid not find a bam file for {}'.format(contents))
            continue
        contents.remove(file_name)
        if 1 < len(contents):
            log.warn('\tfound  more than 2 files for bam file {}:  {}'.format(file_name, contents))
            continue
        if 0 == len(contents):
            log.warn('\tdidn\'t find an index file for bam file {}'.format(file_name))
            no_index.add(file_name)
            continue
        
        index_file = contents.pop()
        index_ext = index_file[len(file_prefix) + 1:]
        count = ext_pairs2count.setdefault((bam_ext, index_ext), 0)
        ext_pairs2count[(bam_ext, index_ext)] = count + 1
        
    if 0 < len(no_index):
        log.info('\n{}\n\tno-index file: \n\t\t{}\n\textension pairs:\n\t\t{}'.format(bucket_name, '\n\t\t'.join(no_index), '\n\t\t'.join('{}: {}'.format(pairs, count) for pairs, count in ext_pairs2count.iteritems())))
    else:
        log.info('\textension pairs:\n\t\t{}'.format('\n\t\t'.join('{}: {}'.format(pairs, count) for pairs, count in ext_pairs2count.iteritems())))
    log.info('bam report completed')

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])