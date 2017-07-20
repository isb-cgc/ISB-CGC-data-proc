'''
Created on Jul 28, 2015

Script to load the metadata data dictionary into CloudSQL

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

from isbcgc_cloudsql_metadata_model import ISBCGC_database_helper
from util import create_log

def main(configFileName):
    try:
        with open(configFileName) as configFile:
            config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'create_datadictionary')
        log = logging.getLogger(log_name)
        log.info('begin create metadata data dictionary')
        
        listlist = []
        with open('MetadataDataDictionary_v5.txt', 'r') as datadict:
            #read the header line and discard
            datadict.readline()
            for line in datadict:
                fields = [field.strip() for field in line.split('\t')]
                if 10 != len(fields):
                    raise ValueError('line wrong length: %s' % fields)
                listlist += [fields]
        ISBCGC_database_helper.initialize(config, log)
        ISBCGC_database_helper.insert(config, listlist, 'metadata_datadictionary', log)

        log.info('end create metadata data dictionary')
    except Exception as e:
        raise e

if __name__ == '__main__':
    main(sys.argv[1])
