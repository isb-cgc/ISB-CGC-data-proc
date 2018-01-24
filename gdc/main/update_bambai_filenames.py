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


def check_for_valid_index_files(bam_data, bai_dict, log):

    # determine which records (in bam_data) need to be updated
    # based on actual file names in bucket (bai_dict)

    update_list = {}
    missing_list = {}

    # iterate through bam data
    for table in bam_data:
        update_list[table] = []
        missing_list[table] = []

        for record in bam_data[table]:
            # check if index file exists in bucket (i.e., bai_dict)
            if 'gs://{}{}/{}'.format(
                record['bucket'],
                record['path'],
                record['bai']
            ) in bai_dict:
                # file already exists in bucket, no update needed
                continue
            else:
                # if the record is a .bam.bai 
                if record['bai'].endswith('.bam.bai'):
                    bai_file = record['bai'].replace('.bam.bai', '.bai')
                    bai_path = 'gs://{}{}/{}'.format(
                        record['bucket'],
                        record['path'],
                        bai_file
                    )
                    log.info(bai_path)
                    # maybe the .bai file exists
                    if bai_path in bai_dict:
                        # update needed !
                        update_list[table].append({
                            'id': record['id'],
                            'index_file': bai_file # record should be updated to this value
                        })
                    else:
                        # index file is missing
                        missing_list[table].append({
                            'id': record['id']
                        })
                # check for opposite case, record is .bai, but real file is .bam.bai
                elif record['bai'].endswith('.bai'):
                    bambai_file = record['bai'].replace('.bai', '.bam.bai')
                    bambai_path = 'gs://{}{}/{}'.format(
                        record['bucket'],
                        record['path'],
                        bambai_file
                    )
                    if bambai_path in bai_dict:
                        # update needed
                        update_list[table].append({
                            'id': record['id'],
                            'index_file': bambai_file # record should be updated to this value
                        })
                    else:
                        # missing file
                        missing_list[table].append({
                            'id': record['id']
                        })
                else:
                    log.warning('invalid index file name: {}'.format(record['bai']))
 
    return (update_list, missing_list)


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


def load_bai_file_list(bai_file_path):

    # read file list into dict for fast lookup

    bai_dict = {}
    with open(bai_file_path, 'r') as f:
        bai_dict = {line.strip(): True for line in f}

    return bai_dict


def get_bambai_from_database(config, log):

    # query cloud sql metadata tables and extract list of bam and associated index files (*.bai)
    # return list for each table with: id, bucket, path, bam file name, index filename

    log.info('\tbegin get_bambai_from_database()')
    
    bam_data = {}
    used_buckets = {}

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
            (
                'select metadata_data_id, file_name_key, index_file_name '
                'from {} where data_format = "BAM"'
            ).format(table),
            log,
            []
        )
        log.info('\t\tfound {} rows for {}'.format(len(rows), table))

        # keep track of how many are null
        num_null = 0

        # check rows for consistency
        for row in rows:
            if row[1]:
                # bam field non-null

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

                # keep track of all buckets where bam/bai files reside
                used_buckets[bucket] = True

            else: 
                # at least one of the two fields is null
                num_null += 1

        log.info('\t\t{} records are NULL in either file_name_key or index_file_name'.format(num_null))

    log.info('\tend get_bambai_from_database()')
    return (bam_data, used_buckets)


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
    parser.add_argument(
        '-b', '--bai-file',
        type=str,
        required=True,
        help='file with list of bam/bai files'
    )
    args = parser.parse_args()

    return (
        args.config,
        args.bai_file
    )


def main(config_file_path, bai_file_path):
    log_dir = str(date.today()).replace('-', '_') + '_update_bambai_filenames/'
    log_name = create_log(log_dir, 'update_bambai_filenames')
    log = logging.getLogger(log_name)
    log.info('begin update_bambai_filenames.py')

    # open config file    
    with open(config_file_path) as config_file:
        config = json.load(config_file)

    # load list of bam/bai files into dict
    bai_dict = load_bai_file_list(bai_file_path)

    # query database for bam/bai files
    (bam_data, used_buckets) = get_bambai_from_database(config, log)
    for table in bam_data:
        log.info('found {} non-NULL entries in table {}'.format(
            len(bam_data[table]), table
        ))

    log.info('used buckets: ')
    for bucket in used_buckets:
        log.info(' - {}'.format(bucket))

    # check and update filenames for consistency
    (update_list, missing_list) = check_for_valid_index_files(bam_data, bai_dict, log)
    for table in update_list:
        log.info('found {} index filenames that need updating in table {}'.format(
            len(update_list[table]), table
        ))
        log.info('{} index files missing (in cloud SQL table {}, but not in bucket)'.format(
            len(missing_list[table]), table
        )) 

    log.info('end update_bambai_filenames.py')


if __name__ == '__main__':
    (
        config_file_path,
        bai_file_path
    ) = parse_args()

    main(config_file_path, bai_file_path)

