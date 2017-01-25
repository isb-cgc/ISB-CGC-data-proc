'''
Created on Jan 22, 2017

Copyright 2017, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author: michael
'''
from google.cloud import bigquery

def query_bq_table(query, use_legacy, project, log):
    log.info('\t\tquerying bq: %s' % query)
    client = bigquery.Client(project=project)
    query_results = client.run_sync_query(query)
# Use standard SQL syntax for queries.
# See: https://cloud.google.com/bigquery/sql-reference/
    query_results.use_legacy_sql = use_legacy
    query_results.run()
    log.info('\t\tdone querying bq: %s' % query)
    return query_results

def fetch_paged_results(query_results, fetch_count, project_name, page_token, log):
    log.info('\t\tfetching %d rows for %s' % (fetch_count, project_name))
    rows, total_rows, page_token = query_results.fetch_data(
        max_results=fetch_count, 
        page_token=page_token)
    log.info('\t\tdone fetching %d rows for %s' % (fetch_count, project_name))
    return total_rows, rows, page_token

