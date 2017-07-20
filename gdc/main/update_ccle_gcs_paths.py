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
        
        log_dir = str(date.today()).replace('-', '_') + '_' + 'ccle/'
        log_name = create_log(log_dir, 'update_ccle_gcs_paths')
        log = logging.getLogger(log_name)
        
        log.info('begin updating CCLE paths in production')
        # first thing to do is to read in the file paths from BigQuery
        query = 'SELECT file_gdc_id, file_gcs_url ' \
            'FROM [isb-cgc:GDC_metadata.GDCfileID_to_GCSurl] ' \
            'where 0 < instr(file_gcs_url, \'CCLE\')'

        query_results = query_bq_table(query, True, 'isb-cgc', log)
        _, rows, _ = fetch_paged_results(query_results, 2000, None, None, log)
        
        log.info('\tcreate map of filename to path')
        name2path = {}
        for row in rows:
            fields = row[1].split('/')
            name2path[fields[-1]] = '/'.join(fields[3:])
        log.info('\tfinished map of filename to path')
        
        # get the db rows from production cloudsql
        log.info('\tselect ccle filenames from cloudsql')
        query = 'SELECT datafilename ' \
            'FROM main.metadata_data ' \
            'where 0 < instr(datafilename, \'bam\') and project = \'CCLE\''
        
        rows = helper.select(config, query, log, [])
        log.info('\tselected %s ccle filenames from cloudsql' % (len(rows)))
        
        # now setup and do the update of paths in cloud sql
        log.info('\tstart updating paths in cloudsql')
        params = []
        not_matched = []
        for row in rows:
            if row[0] in name2path:
                params += [[name2path[row[0]], row[0]]]
            else:
                not_matched += [row[0]]
        update = 'update main.metadata_data set datafilenamekey = %s where datafilename = %s'
        helper.update(config, update, log, params)
        log.info('\tcompleted update of paths in cloudsql. updated %d, did not find matches from BQ in cloudsql for %s' % (len(params), ', '.join(not_matched)))

        log.info('finished updating CCLE paths in production')
    except:
        if log:
            log.exception('failed to update ccle GCS filepaths')
    finally:
        if log:
            close_log(log)

if __name__ == '__main__':
    main(sys.argv[1])