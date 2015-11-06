'''
Created on Oct 8, 2015

one off utility script to updata the metadata_samples.has_Illumia_DNASeq column

Copyright 2015, Institute for Systems Biology.

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

import isbcgc_cloudsql_model
from util import create_log

def main(configFileName):
    try:
        with open(configFileName) as configFile:
            config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'create_datadictionary')
        log = logging.getLogger(log_name)
        log.info('begin update of has_Illumia_DNASeq')
        select_stmt = "select samplebarcode from metadata_data \
            where 0 < instr(Platform, 'DNASeq') and 0 = instr(Platform, 'Roche') and \
                0 = instr(Platform, 'ABSOLiD') and 0 = instr(Platform, 'PacBio') and \
                project <> 'CCLE' \
            group by samplebarcode"
        cursor = isbcgc_cloudsql_model.ISBCGC_database_helper.select(config, select_stmt, log)
        barcodes = ''
        for blist in cursor[:20]:
            barcodes += '\n\t\t' + blist[0]
        log.info('\tcompleted select, %s rows:\n\t\t%s\n\t\t...' % (len(cursor), barcodes))
        # there's a problem that the rows that come back for this are [(barcode,), (barcode,), ...] so update fails with too many arguments
        # so put back together
        fixedcursor = []
        for blist in cursor:
            fixedcursor += [blist[0]]
    except Exception as e:
        log.exception('\tproblem selecting')
        raise e
        
    try:
        update_stmt = "update metadata_samples set has_Illumina_DNASeq = 1 where samplebarcode = %s"
        cursor = isbcgc_cloudsql_model.ISBCGC_database_helper.update(config, update_stmt, log, fixedcursor)
        log.info('finished update of has_Illumia_DNASeq')
    except Exception as e:
        log.exception('\tproblem updating')
        raise e

if __name__ == '__main__':
    main(sys.argv[1])
