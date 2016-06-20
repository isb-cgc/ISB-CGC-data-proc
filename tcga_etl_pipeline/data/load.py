#!/usr/bin/env python

# Copyright 2015, Institute for Systems Biology.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Script to load Data data
To run: python load.py config_file
"""
import sys
from bigquery_etl.load import load_data_from_file
import json
import os

from bigquery_etl.utils.logging_manager import configure_logging

def load(config):
    """
    Load the bigquery table
    load_data_from_file accepts following params:
    project_id, dataset_id, table_name, schema_file, data_path,
          source_format, write_disposition, poll_interval, num_retries
    """
    log = configure_logging('data_load', 'logs/data_load.log')
    log.info('begin load of data into bigquery')
    
    schemas_dir = os.environ.get('SCHEMA_DIR', 'schemas/')

    log.info("\tLoading Data data into BigQuery...")
    load_data_from_file.run(
        config['project_id'],
        config['bq_dataset'],
        config['data']['bq_table'],
        schemas_dir + config['data']['schema_file'],
        'gs://' + config['buckets']['open'] + '/' +\
            config['data']['output_dir'] + '*',
        'NEWLINE_DELIMITED_JSON',
        'WRITE_EMPTY'
    )
    
    log.info('finished load of data into bigquery')


if __name__ == '__main__':
    load(json.load(open(sys.argv[1])))
