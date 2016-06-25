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

"""Script to parse CCLE GCT file
To run: python load.py (may have to manually edit the params)
"""
import sys
from bigquery_etl.load import load_data_from_file
import json
import os

def load():
    """
    Load the bigquery table
    load_data_from_file accepts following params:
    project_id, dataset_id, table_name, schema_file, data_path,
          source_format, write_disposition, poll_interval, num_retries
    """

    print "*"*30
    print "Loading CCLE GCT data into BigQuery.."
    load_data_from_file.run(
        '',
        '',
        '',
        'schema.json',
        'intermediary/CCLE_mrna_expr/bq_data_files/ccle_mrna_expr.csv',
        'CSV',
        'WRITE_EMPTY'
    )


if __name__ == '__main__':
    load()
