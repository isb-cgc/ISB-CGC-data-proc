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

"""Script to load methylation data
To run: python load.py config_file
"""
import sys
from bigquery_etl.load import load_data_from_file
import json
import os

from bigquery_etl.utils.logging_manager import configure_logging
from methylation.split_table import main

def load(config):
    """
    Load the bigquery table
    load_data_from_file accepts following params:
    project_id, dataset_id, table_name, schema_file, data_path,
          source_format, write_disposition, poll_interval, num_retries
    """
    log = configure_logging('methylation_split', 'logs/methylation_load.log')
    log.info('begin load of methylation into bigquery')
    
    schemas_dir = os.environ.get('SCHEMA_DIR', 'schemas/')

    #print "Loading Methylation 450K data into BigQuery.."
    #load_data_from_file.run(
    #    config['project_id'],
    #    config['bq_dataset'],
    #    config['methylation']['bq_table'],
    #    schemas_dir + config['methylation']['schema_file'],
    #    'gs://' + config['buckets']['open'] + '/' +\
    #        config['methylation']['output_dir'] + 'HumanMethylation450/*',
    #    'CSV',
    #    'WRITE_EMPTY'
    #)
    log.info("\tLoading Methylation data into BigQuery...")
    load_data_from_file.run(
        config['project_id'],
        config['bq_dataset'],
        config['methylation']['bq_table'],
        schemas_dir + config['methylation']['schema_file'],
        'gs://' + config['buckets']['open'] + '/' +\
            config['methylation']['output_dir'] + '*',
        'CSV',
        'WRITE_APPEND'
    )
    
    main(config, log)
    
    log.info('finished load of methylation into bigquery')


if __name__ == '__main__':
    load(json.load(open(sys.argv[1])))
