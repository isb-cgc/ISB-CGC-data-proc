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

"""Script to parse MAF file
To run: python load.py config_file
"""
import sys
from bigquery_etl.load import load_data_from_file
import json
import os

def load(config):
    """
    Load the bigquery table
    load_data_from_file accepts following params:
    project_id, dataset_id, table_name, schema_file, data_path,
          source_format, write_disposition, poll_interval, num_retries
    """

    schemas_dir = os.environ.get('SCHEMA_DIR', 'schemas/')

    print "Loading Isoform HiSeq data into BigQuery.."
    load_data_from_file.run(
        config['project_id'],
        config['bq_dataset'],
        config['mirna']['isoform']['bq_table_hiseq'],
        schemas_dir + config['mirna']['isoform']['schema_file'],
        'gs://' + config['buckets']['open'] + '/' +\
            config['mirna']['isoform']['output_dir'] + 'IlluminaHiSeq/*',
        'NEWLINE_DELIMITED_JSON',
        'WRITE_EMPTY'
    )
    print "*"*30
    print "Loading Isoform GA data into BigQuery.."
    load_data_from_file.run(
        config['project_id'],
        config['bq_dataset'],
        config['mirna']['isoform']['bq_table_ga'],
        schemas_dir + config['mirna']['isoform']['schema_file'],
        'gs://' + config['buckets']['open'] + '/' +\
            config['mirna']['isoform']['output_dir'] + 'IlluminaGA/*',
        'NEWLINE_DELIMITED_JSON',
        'WRITE_EMPTY'
    )


if __name__ == '__main__':
    load(json.load(open(sys.argv[1])))
