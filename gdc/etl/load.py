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

"""
Load the set of files in each directory specified in gcs_file_paths into the matching BQ table in bq_tables
"""
import sys
from bigquery_etl.load import load_data_from_file
import json

def load(project_id, bq_datasets, bq_tables, schema_files, gcs_file_paths, write_dispositions, log):
    """
    Load the bigquery table
    load_data_from_file accepts following params:
    project_id, dataset_id, table_name, schema_file, data_path,
          source_format, write_disposition, poll_interval, num_retries
    """
    log.info('\tbegin load of %s data into bigquery' % (gcs_file_paths))
    sep = ''
    for index in range(len(bq_datasets)):
        log.info("%s\t\tLoading %s table into BigQuery.." % (sep, bq_datasets[index]))
        load_data_from_file.run(project_id, bq_datasets[index], bq_tables[index], schema_files[index], 
                            gcs_file_paths[index] + '/*', 'NEWLINE_DELIMITED_JSON', write_dispositions[index])
        sep = '\n\t\t"*"*30\n'

    log.info('done load %s of data into bigquery' % (gcs_file_paths))


if __name__ == '__main__':
    load(json.load(open(sys.argv[1])))
