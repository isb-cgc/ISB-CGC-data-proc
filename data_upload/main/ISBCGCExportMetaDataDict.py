'''
Created on Jul 28, 2015

Script to export the metadata data dictionary as JSON.  Joins with allowed values
where there column is a controlled vocabulary

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

from util import create_log

def main(configFileName):
    try:
        with open(configFileName) as configFile:
            config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'export_datadictionary')
        log = logging.getLogger(log_name)
        log.info('begin export metadata data dictionary')
    except Exception as e:
        log.exception('problem with opening log for json export')
        raise e
        
    try:
        with open(config['controlled_vocab'], 'r') as vocab:
            # create map of term to values
            term2values = {}
            name_index = 0
            table_index = 1
            values_index = 4
            vocab.readline()
            for line in vocab:
                fields = line.strip().split('\t')
                if 4 < len(fields) and fields[table_index] in ['metadata_clinical', 'metadata_biospecimen'] and 'distinct values' not in fields[values_index]:
                    values = [value[:value.find(":")] if -1 < value.find(':') else value for value in fields[values_index].split(',')]
                    term2values[fields[name_index]] = values
    except Exception as e:
        log.exception('problem with parsing vocab for json export')
        raise e

    try:
        with open(config['metadatadatadictionary'], 'rb') as datadict, open(config['outputfile'], 'w') as out:
            term2attrs = {}
            table_index = 2
            output_fields = config['output_fields']
            # create map of index to column header
            headers = datadict.readline().strip().split('\t')
            while True:
                line = datadict.readline()
                if not line:
                    break
                fields = [field.strip() for field in line.split('\t')]
                if 10 != len(fields):
                    raise ValueError('line wrong length: %s' % fields)
                if fields[name_index] in term2attrs:
                    continue
                attr2value = {}
                if fields[table_index] in ['metadata_clinical', 'metadata_biospecimen']:
                    for attr, value in zip(headers, fields):
                        if attr in output_fields:
                            attr2value[attr] = value
                    attr2value['Controlled Vocabulary'] = term2values.get(fields[name_index], '')
                    term2attrs[fields[name_index]] = attr2value
            json.dump(term2attrs, out)
            log.info('end export metadata data dictionary')
    except Exception as e:
        log.exception('problem with writing json export')
        raise e

if __name__ == '__main__':
    main(sys.argv[1])