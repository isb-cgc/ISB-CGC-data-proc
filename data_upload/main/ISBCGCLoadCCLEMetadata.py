'''
Created on Jul 28, 2015

upload the CCLE metadata into CloudSQL

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
import traceback

# from isbcgc_metadata_cloudsql_model import ISBCGC_metadata_database_helper
from util import create_log
from util import import_module

def getlog(configFileName):
    try:
        with open(configFileName) as configFile:
            config = json.load(configFile)
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'create_ccle_metadata')
        log = logging.getLogger(log_name)
    except Exception as e:
        traceback.print_exc(5)
        raise e
    return config, log

def checkliteral(fields, value, name2index):
    if "'" == value[0]:
        return value[1:-1]
    else:
        return fields[name2index[value]]

def choice(fields, choices, name2index):
    '''
        choices = [fieldname1, value, fieldname2]
        choice1 = fields[name2index[fieldname1]]
        choice2 = fields[name2index[fieldname2]]
        if choice1 == value:
            return choice2
        else:
            return choice1
    '''
    value = fields[name2index[choices[0]]]
    if value == choices[1]:
        return fields[name2index[choices[2]]]
    else:
        return value

def mapit(config, fields, mappings, name2index):
    '''
        mappings = [fieldname, mapname]:
        key = fields[name2index[fieldname]]
        return config[mapname][key]
            or
        mappings = [fieldname1!fieldname2, mapname]:
        key1 = fields[name2index[fieldname1]]
        key2 = fields[name2index[fieldname2]]
        return config[mapname][key1][key2]
    '''
    mapkeys = mappings[0].split('!')
    if 1 == len(mapkeys):
        return config[mappings[1]][fields[name2index[mapkeys[0]]]]
    else:
        return config[mappings[1]][fields[name2index[mapkeys[0]]]][fields[name2index[mapkeys[1]]]]

def concat(config, fields, pieces, name2index):
    retVal = ''
    for piece in pieces:
        if 1 < len(piece.split('%')):
            # nested map
            retVal += mapit(config, fields, piece.split('%')[1:], name2index)
        else:
            retVal += checkliteral(fields, piece, name2index)
    return retVal

def substr(config, fields, pieces, name2index):
    return checkliteral(fields, pieces[0], name2index)[int(pieces[1]):int(pieces[2])]

def create_row(config, mapname, fields, name2index):
    retVal = []
    for key, _ in config[mapname]:
        mapping = key.split(':')
        if 1 == len(mapping):
            retVal += [checkliteral(fields, mapping[0], name2index)]
        elif 'choice' == mapping[0]:
            retVal += [choice(fields, mapping[1:], name2index)]
        elif 'map' == mapping[0]:
            retVal += [mapit(config, fields, mapping[1:], name2index)]
        elif 'concat' == mapping[0]:
            retVal += [concat(config, fields, mapping[1:], name2index)]
        elif 'substr' == mapping[0]:
            retVal += [substr(config, fields, mapping[1:], name2index)]
    return tuple(retVal)

def create_insert_columns(mapping):
    retVal = []
    for (_, value) in mapping:
        retVal += [value]
    return retVal

def parse_metadata(config, log):
    clinical_index = 0
    biospeciman_index = 1
    data_index = 2
    samples_index = 3
    try:
        log.info('\tbegin parse %s' % (config['input_file']))
        count = 0
        with open(config['input_file'], 'r') as input_file:
            #red the header line and map the indices
            header = input_file.readline()
            count += 1
            name2index = {}
            for index, name in enumerate(header.split('\t')):
                name2index[name.strip()] = index
            
            table_columns = [
                create_insert_columns(config['clinical_column_mapping']), 
                create_insert_columns(config['biospecimen_column_mapping']), 
                create_insert_columns(config['data_column_mapping']),
                create_insert_columns(config['samples_column_mapping'])
            ]
            table_rows = [set(), set(), set(), set()]
            for line in input_file:
                if 0 == count % 128:
                    log.info('\t\tprocessed %s rows' % (count))
                count += 1
                fields = [field.strip() for field in line.split('\t')]
                table_rows[clinical_index].add(create_row(config, 'clinical_column_mapping', fields, name2index))
                table_rows[biospeciman_index].add(create_row(config, 'biospecimen_column_mapping', fields, name2index))
                table_rows[data_index].add(create_row(config, 'data_column_mapping', fields, name2index))
                table_rows[samples_index].add(create_row(config, 'samples_column_mapping', fields, name2index))
        log.info('\tend parse %s.  processed %s total rows' % (config['input_file'], count))
    except Exception as e:
        log.exception('problem parsing on line %s' % (count))
        raise e
    return table_columns, table_rows

def insert_metadata(config, table_columns, table_rows, log):
    try:
        datastore = import_module(config['database_module'])
        tables = ['metadata_clinical', 'metadata_biospecimen', 'metadata_data', 'metadata_samples']
        for index in range(len(tables)):
            datastore.ISBCGC_database_helper.column_insert(config, list(table_rows[index]), tables[index], table_columns[index], log)
    
    except Exception as e:
        log.exception('problem saving metadata to the database')
        raise e

def main(configFileName):
    config, log = getlog(configFileName)
    log.info('begin create ccle metadata')
    table_columns, table_rows = parse_metadata(config, log)
    insert_metadata(config, table_columns, table_rows, log)
    log.info('end create metadata data dictionary')

if __name__ == '__main__':
    main(sys.argv[1])
