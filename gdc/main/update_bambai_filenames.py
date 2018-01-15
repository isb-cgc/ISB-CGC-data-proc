'''
Created on Jan 12, 2018

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

@author: John Phan
'''
import argparse
from datetime import date
import json
import logging
import sys
from pprint import pprint


from bq_wrapper import fetch_paged_results, query_bq_table
import gcs_wrapper_gcloud as wrapper
from isbcgc_cloudsql_model import ISBCGC_database_helper
from util import create_log


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


def check_for_valid_index_files(bam_data, project_name, log):

    # open connection to gcs wrapper
    # wrapper.open_connection({'cloud_projects':{'open': project_name}}, log)

    bucket_contents = {}
    update_list = {}

    # iterate through bam data
    for table in bam_data:
        update_list[table] = []
        for record in bam_data[table]:
            # get bucket contents if not already retrieved
            # bucket = record['bucket']
            # if bucket not in bucket_contents:
            #    bucket_contents[bucket] = list(wrapper.get_bucket_contents(bucket, None, log))

            # check if index file exists in bucket
            if record['bai'].endswith('.bam.bai'):
                bai_file = record['bai'].replace('.bam.bai', '.bai')
                # check if bucket file is also .bam.bai
                # if bucket file ends with .bam.bai
                #    no need to update?
                # if bucket file ends with .bai
                update_list[table].append({
                    'id' : record['id'],
                    'bai': bai_file
                })
            elif record['bai'].endswith('.bai'):
                # check if bucket file is also .bai
                # if bucket file does not match
                #    update db?
                continue
            else:
                log.warning('invalid index file name: {}'.format(record['bai']))

 
    # wrapper.close_connection()

    return update_list


def process_gs_uri(path, log):

    # given a gs uri, e.g., gs://bucket/path/file.ext
    # return bucket, path, and filename in separate pieces

    pieces = path.split('/')
    if len(pieces) < 4:
        log.warning('invalid path: {}'.format(path))
        return (False, False, False)

    bucket = pieces[2]
    path = '/'+'/'.join(pieces[3:-2])
    filename = pieces[-1]

    return (bucket, path, filename)


def get_bambai_from_database(config, log):

    # query cloud sql metadata tables and extract list of bam and associated index files (*.bai)
    # return list for each table with: id, bucket, path, bam file name, index filename

    log.info('\tbegin get_bambai_from_database()')
    
    bam_data = {}

    for table in [
        'CCLE_metadata_data_HG19',
        'TARGET_metadata_data_HG19',
        'TARGET_metadata_data_HG38',
        'TCGA_metadata_data_HG19',
        'TCGA_metadata_data_HG38'
    ]:
	bam_data[table] = []

        rows = ISBCGC_database_helper.select(
            config,
            'select metadata_data_id, file_name_key, index_file_name from {} where data_format = "BAM"'.format(table),
            log,
            []
        )
        log.info('\t\tfound {} rows for {}'.format(len(rows), table))

        # keep track of how many are null
        num_null = 0

        # check rows for consistency
        for row in rows:
            if row[1] and row[2]:
                # both fields non-null

                # extract bucket name
                (bucket, path, filename) = process_gs_uri(row[1], log)
                if not bucket:
                    log.warning('\t\tskipping invalid path: {}'.format(row[1]))
                    continue

                bam_data[table].append({
                    'id'    : row[0],
                    'bucket': bucket,
                    'path'  : path,
                    'bam'   : filename,
                    'bai'   : row[2]
                })
                # log.info('\t\t{}, {}, {}, {}'.format(bucket, path, filename, row[1]))
            else: 
                # at least one of the two fields is null
                num_null += 1

        log.info('\t\t{} records are NULL in either file_name_key or index_file_name'.format(num_null))

    log.info('\tend get_bambai_from_database()')
    return bam_data


def parse_args():
    parser = argparse.ArgumentParser(
        description='update_bambai_filenames.py'
    )
    parser.add_argument(
        '-c', '--config',
        type=str,
        required=True,
        help='config file path'
    )
    args = parser.parse_args()

    return (
        args.config
    )


def main(config_file_path):
    log_dir = str(date.today()).replace('-', '_') + '_update_bambai_filenames/'
    log_name = create_log(log_dir, 'update_bambai_filenames')
    log = logging.getLogger(log_name)
    log.info('begin update_bambai_filenames.py')

    # open config file    
    with open(config_file_path) as config_file:
        config = json.load(config_file)

    # query database for bam/bai files
    bam_data = get_bambai_from_database(config, log)
    for table in bam_data:
        log.info('found {} non-NULL entries in table {}'.format(len(bam_data[table]), table))

    # check and update filenames for consistency
    update_list = check_for_valid_index_files(bam_data, 'ISB-CGC', log)
    for table in update_list:
        log.info('found {} entries (*.bam.bai) that need updating in table {}'.format(len(update_list[table]), table))

    log.info('end update_bambai_filenames.py')


if __name__ == '__main__':
    (
        config_file_path
    ) = parse_args()
    main(config_file_path)

