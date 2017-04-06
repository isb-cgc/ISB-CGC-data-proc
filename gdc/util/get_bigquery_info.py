'''
Created on Dec 19, 2016

Copyright 2016, Institute for Systems Biology.

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
from collections import OrderedDict
from google.cloud import bigquery

def delete(client, query, suffix):
    results = client.run_sync_query(query % (suffix))
    results.useLegacySql = False
    results.timeout_ms = 60000
    results.run()

def map_results(client, query, suffix, project2counts):
    # Drain the query results by requesting a page at a time.
    results = client.run_sync_query(query % (suffix))
    results.timeout_ms = 60000
    results.run()
    page_token = None
    while True:
        rows, total_rows, page_token = results.fetch_data(
            max_results=35, 
            page_token=page_token)
        print '%s total rows: %s' % (query % (suffix), total_rows)
        for row in rows:
            curcounts = project2counts.setdefault(row[0], [0] * (len(row) - 1))
            for i in range(1, len(row)):
                curcounts[i - 1] += row[i] 
#             print '\t%s' % str(row)
        
        if not page_token:
            break
    
    return project2counts

def get_methylation_counts(dataset_name = 'test', project='isb-cgc'):
    """
    Lists all of the tables in a given dataset.

    If no project is specified, then the isb-cgc default project is used.
    """
    client = bigquery.Client(project=project)
#     query = '''
#         delete from test.TCGA_Methylation%s
#         where project_short_name = 'TCGA-LAML'
#     '''
#     for i in range(1,23):
#         delete(client, query, '_chr%s' % str(i))
#     for i in ('X', 'Y'):
#         delete(client, query, '_chr%s' % str(i))
    
    query = '''
        SELECT 
            project_short_name, 
            count(distinct case_gdc_id, 10000) cases, 
            count(distinct sample_gdc_id, 10000) samples, 
            count(distinct file_gdc_id, 10000) files, 
            count(distinct probe_id, 10000) probes,
            count(*) rows 
        FROM [isb-cgc:test.TCGA_Methylation%s] 
        group by project_short_name
        order by project_short_name 
    '''

    project2total_counts = {}
    map_results(client, query, '', project2total_counts)
    print '\n' + str(project2total_counts)
    
    chr2project2chr_counts = {}
    for i in range(1,23):
        project2chr_counts = chr2project2chr_counts.setdefault(i, {})
        map_results(client, query, '_chr%s' % str(i), project2chr_counts)
    for i in ('X', 'Y'):
        project2chr_counts = chr2project2chr_counts.setdefault(i, {})
        map_results(client, query, '_chr%s' % str(i), project2chr_counts)
    
    print '\ncomparing results:'
    total_probe_sum = 0
    total_row_sum = 0
    for project_counts in project2total_counts.itervalues():
        total_probe_sum += project_counts[-2]
        total_row_sum += project_counts[-1]
    
    project2chr_probe_sum = {}
    chr_probe_sum = 0
    project2chr_row_sum = {}
    chr_row_sum = 0
    for chrom, project2chr_counts in chr2project2chr_counts.iteritems():
        for project, chr_counts in project2chr_counts.iteritems():
            total_chr_counts = project2total_counts[project]
            if total_chr_counts[:-2] != chr_counts[:-2]:
                print '\tfor %s:%s, counts are different: %s vs %s' % (chrom, project, total_chr_counts[:-2], chr_counts[:-2])
            project_chr_probe_sum = project2chr_probe_sum.setdefault(project, 0)
            project2chr_probe_sum[project] = project_chr_probe_sum + chr_counts[-2]
            chr_probe_sum += chr_counts[-2]
            project_chr_row_sum = project2chr_row_sum.setdefault(project, 0)
            project2chr_row_sum[project] = project_chr_row_sum + chr_counts[-1]
            chr_row_sum += chr_counts[-1]

    print '\ttotal probe count vs. sum of chr counts: %d/%d (%.4f)' % (total_probe_sum, chr_probe_sum, float(total_probe_sum)/float(chr_probe_sum))
    print '\ttotal row count vs. sum of chr counts: %d/%d (%.4f)' % (total_row_sum, chr_row_sum, float(total_row_sum)/float(chr_row_sum))
    for project in project2total_counts:
        print '\t%s' % (project)
        print '\t\tproject probe count vs. sum of chr counts: %d/%d (%.4f)' % (project2total_counts[project][-2], project2chr_probe_sum[project], float(project2total_counts[project][-2])/float(project2chr_probe_sum[project]))
        print '\t\tproject row count vs. sum of chr counts: %d/%d (%.4f)' % (project2total_counts[project][-1], project2chr_row_sum[project], float(project2total_counts[project][-1])/float(project2chr_row_sum[project]))

def list_table_info(dataset_name, project='isb-cgc'):
    """
    Lists all of the tables in a given dataset.

    If no project is specified, then the isb-cgc default project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    dataset = bigquery_client.dataset(dataset_name)

    if not dataset.exists():
        print('Dataset {} does not exist.'.format(dataset_name))
        return

    print('%s' % (dataset_name))
    for table in dataset.list_tables():
        print('\t%s' % (table.name))
        table.reload()
        for field in table.schema:
            print('\t\t%s\t%s' % (field.name, field.field_type))
            if field.field_type == 'RECORD':
                for subfield in field.fields:
                    print('\t\t\t%s\t%s' % (subfield.name, subfield.field_type))
        print('\n')

if __name__ == '__main__':
    datasets = ['metadata', 'GDC_metadata', 'CCLE_bioclin_v0', 'CCLE_hg19_data_v0', 'CCLE_hg38_data_v0', 'TARGET_bioclin_v0', 'TARGET_hg19_data_v0', 'TARGET_hg38_data_v0', 'TCGA_bioclin_v0', 'TCGA_hg19_data_v0', 'TCGA_hg38_data_v0']
    for dataset in datasets:
        list_table_info(dataset)
        
    # get methylation counts
#     get_methylation_counts()
