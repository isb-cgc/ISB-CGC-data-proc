'''
Created on Jun 30, 2016

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
from datetime import date, datetime
import json
import logging
import sys

from isbcgc_cloudsql_model import ISBCGC_database_helper
from util import create_log

def main(configfilename, baminfo_filename):
    log_dir = str(date.today()).replace('-', '_') + '_gg_update' + '/'
    log_name = create_log(log_dir, 'update_gg_metadata')
    log = logging.getLogger(log_name)
    log.info('begin update gg metadata')

    idcol = 0
    ggdatasetcol = 7
    ggreadgroupset = 6
    nexist = 'NA'
    updates = []
    count = 0
    with open(baminfo_filename) as baminfo:
        baminfo.readline()
        for line in baminfo:
            fields = line.strip().split('\t')
            if fields[ggreadgroupset] == nexist:
                continue
            values = [fields[ggdatasetcol], fields[ggreadgroupset], fields[idcol]]
            if 0 == count % 1000:
                log.info('%s processing row %s--%s' % (datetime.now(), count, ':'.join(values)))
            count += 1
            updates += [values]

    stmt = 'update metadata_data set GG_dataset_id = %s, GG_readgroupset_id = %s where analysis_id = %s'
    with open(configfilename) as configFile:
        config = json.load(configFile)
    ISBCGC_database_helper.update(config, stmt, log, updates, False)

    log.info('finished update gg metadata')
    
if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])