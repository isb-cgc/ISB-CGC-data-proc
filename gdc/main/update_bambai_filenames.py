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
import argparse
from datetime import date
import json
import logging
import sys


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


def query_database(config, log):
    log.info('\tbegin query database to check index files')
    
    for table in [
        'CCLE_metadata_data_HG19',
        'TARGET_metadata_data_HG19',
        'TARGET_metadata_data_HG38',
        'TCGA_metadata_data_HG19',
        'TCGA_metadata_data_HG38'
    ]:
        rows = ISBCGC_database_helper.select(
            config,
            'select file_name_key, index_file_name from {} where data_format = "BAM"'.format(table),
            log,
            []
        )
        log.info('\t\tfound {} rows for {}'.format(len(rows), table))

        # check rows for consistency
        for row in rows:
            if row[0] and row[1]:
                if row[1].endswith('bam.bai'):
                # check if .bam.bai or .bai file exists
                # if .bai file in bucket, update database


    log.info('\tend query database')
    return True      


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
    query_database(config, log)
        
    log.info('update_bambai_filenames.py completed')

if __name__ == '__main__':
    (
        config_file_path
    ) = parse_args()
    main(config_file_path)
