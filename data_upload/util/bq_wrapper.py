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
    log.info('\t\tquerying bq for %s: %s' % (project, query))
    client = bigquery.Client(project=project)
    query_results = client.run_sync_query(query)
# Use standard SQL syntax for queries.
# See: https://cloud.google.com/bigquery/sql-reference/
    query_results.use_legacy_sql = use_legacy
    try:
        query_results.run()
    except:
        log.exception('problem with query:\n{}'.format(query))
        raise
    log.info('\t\tdone querying bq: %s' % query)
    return query_results

def fetch_paged_results(query_results, fetch_count, project_name, page_token, log):
    log.info('\t\trequesting %d rows %s' % (fetch_count, (' for ' + project_name) if project_name else ''))

    #
    # Encountered this, which was the only error in a full load. Per the error response,
    # a retry seems to be in order:
    #
    # ServiceUnavailable: 503 GET https://www.googleapis.com/bigquery/v2/projects/isb-cgc/queries/job_blah-?pageToken=blah%3D&maxResults=50:
    # Error encountered during execution. Retrying may solve the problem.
    #
    rows = list(query_results.fetch_data(
        max_results=fetch_count, 
        page_token=page_token))
    return query_results.total_rows, rows, query_results.page_token

