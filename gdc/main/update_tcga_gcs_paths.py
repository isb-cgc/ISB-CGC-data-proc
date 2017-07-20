'''
Created on Mar 29, 2017
a one-off script to update the CCLE GCS paths in the production database

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
from datetime import date
import json
import logging
import sys

from bq_wrapper import fetch_paged_results, query_bq_table
from isbcgc_cloudsql_model import ISBCGC_database_helper as helper
from util import close_log, create_log

def main(config_file_name):
    log = None
    try:
        with open(config_file_name) as configFile:
            config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_' + 'tcga/'
        log_name = create_log(log_dir, 'update_tcga_gcs_paths')
        log = logging.getLogger(log_name)
        
        log.info('begin updating TCGA paths in production')

        # get the db rows from production cloudsql
        log.info('\tselect tcga filenames from cloudsql')
        query = 'SELECT datafilename ' \
            'FROM metadata_data ' \
            'where 0 < instr(datafilename, \'bam\') and project = \'TCGA\''
        
        cloudsql_rows = set(row[0] for row in helper.select(config, query, log, []))
        log.info('\tselected %s tcga filenames from cloudsql' % (len(cloudsql_rows)))
        
        # read in the file paths from BigQuery
        query = 'SELECT file_gdc_id, file_gcs_url ' \
            'FROM [isb-cgc:GDC_metadata.GDCfileID_to_GCSurl] ' \
            'where 0 < instr(file_gcs_url, \'TCGA\') and 0 < instr(file_gcs_url, \'legacy\') and 0 < instr(file_gcs_url, \'bam\') ' \
            'order by file_gcs_url'

        query_results = query_bq_table(query, True, 'isb-cgc', log)
        total_not_matched = 0
        total_distinct = set()
        page_token = None
        while True:
            total_rows, rows, page_token = fetch_paged_results(query_results, 2000, None, page_token, log)
        
            log.info('\t\tcreate map of filename to path')
            name2path = {}
            for row in rows:
                fields = row[1].split('/')
                name2path[fields[-1]] = '/'.join(fields[3:])
            log.info('\t\tfinished map of filename to path')
            
            # now setup and do the update of paths in cloud sql
            log.info('\t\tstart updating paths in cloudsql')
            params = []
            select_params = []
            not_matched = []
            for name, path in name2path.iteritems():
                if name in cloudsql_rows:
                    total_distinct.add(name)
                    params += [[path, name]]
                    select_params += [name]
                else:
                    not_matched += [path]
            update = 'update metadata_data set datafilenamekey = %s, datafileuploaded = \'true\' where datafilename = %s'
            helper.update(config, update, log, params)
            select_in = '%s,' * len(select_params)
            select_in = select_in[:-1]
            select_query = 'select count(*) from metadata_data where datafilename in (%s)' % (select_in)
            count = helper.select(config, select_query, log, select_params)
            log.info('select %s file name matches for %s file names.' % (count[0][0], len(select_params))) 
            
            total_not_matched += len(not_matched)
            if not page_token:
                log.info('\t\tupdated total of %s rows for TCGA with %d distinct file names' % (total_rows - total_not_matched, len(total_distinct)))
                break
            else:
                log.info('\t\tupdated %d rows, did not find matches from BQ in cloudsql for %d:\n\t%s' % (len(params), len(not_matched), '\n\t'.join(not_matched)))

        log.info('\tcompleted update of paths in cloudsql')
        
        log.info('finished updating TCGA paths in production')
    except:
        if log:
            log.exception('failed to update tcga GCS filepaths')
    finally:
        if log:
            close_log(log)

if __name__ == '__main__':
    main(sys.argv[1])