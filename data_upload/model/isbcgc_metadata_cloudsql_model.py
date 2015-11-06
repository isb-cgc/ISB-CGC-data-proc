'''
a wrapper to google cloud sql.

TODO: refactor this with isbcgc_cloudsql_model.py

the MySQLdb module is thread safe but the connections to the database are not.  so the
recommendation is that each thread have an independent connection.

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
'''
import MySQLdb

class ISBCGC_metadata_database_helper():
    """
    this class manages the cloud sql metadata datadictionary upload
    """
    metadata_datadictionary = {
        'table_name': 'metadata_datadictionary',
        'primary_key_name': 'metadata_datadictionary_id',
        'columns': [
            ['MetadataFieldName', 'VARCHAR(70)', 'NULL'],
            ['ISBCGCTerm', 'VARCHAR(90)', 'NULL'],
            ['MetadataTable', 'VARCHAR(22)', 'NULL'],
            ['Source', 'VARCHAR(120)', 'NULL'],
            ['SourceComment', 'VARCHAR(200)', 'NULL'],
            ['ValueType', 'VARCHAR(30)', 'NULL'],
            ['Definition', 'VARCHAR(1000)', 'NULL'],
            ['ExampleValue', 'VARCHAR(500)', 'NULL'],
            ['ControlledVocabulary', 'VARCHAR(63)', 'NULL'],
            ['CDEPublicID', 'VARCHAR(10)', 'NULL']
        ],
#         'natural_key_cols': [
#             'MetadataFieldName',
#             'MetadataTable'
#         ],
        'indices_defs': [
            ['MetadataFieldName'],
            ['ISBCGCTerm'],
            ['MetadataTable'],
            ['Source'],
            ['SourceComment'],
            ['ValueType'],
            ['ExampleValue'],
            ['ControlledVocabulary'],
            ['CDEPublicID']
        ]
    }

    metadata_tables = {'metadata_datadictionary': metadata_datadictionary}

    self = None
    def __init__(self, config, log):
        db = None
        cursor = None
        try:
            log.info('\tconnecting to database %s' % (config['cloudsql']['db']))
            db = MySQLdb.connect(host = config['cloudsql']['host'], db = config['cloudsql']['db'], user = config['cloudsql']['user'], passwd = config['cloudsql']['passwd'])
            cursor = db.cursor()
            cursor.execute('select table_name from information_schema.tables where table_schema = "%s"' % (config['cloudsql']['db']))
            not_found = dict(self.metadata_tables)
            found = {}
            for next_row in cursor:
                print next_row[0]
                if next_row[0] in self.metadata_tables:
                    found[next_row[0]] = self.metadata_tables[next_row[0]]
                    not_found.pop(next_row[0])
            if 0 != len(found) and config['cloudsql']['update_schema']:
                # need to delete in foreign key dependency order
                found_list = []
                for table_name in ['metadata_datadictionary']:
                    if table_name in found:
                        found_list += [found[table_name]]
                self._drop_schema(cursor, config, found_list, log)
                self._create_schema(cursor, config, self.metadata_tables.values(), log)
            elif 0 != len(not_found):
                log.info('\tcreating table(s) %s' % (', '.join(not_found)))
                self._create_schema(cursor, config, not_found.values(), log)
            else:
                log.info('\tpreserving tables')
            log.info('\tconnection successful')
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    @classmethod
    def initialize(cls, config, log):
        if cls.self:
            log.warning('class has already been initialized')
        else:
            cls.self = ISBCGC_metadata_database_helper(config, log)
    
    def _drop_schema(self, cursor, config, tables, log):
        drop_schema_template = 'DROP TABLE %s.%s'
        
        for table in tables:
            drop_statement = drop_schema_template % (config['cloudsql']['db'], table['table_name'])
            log.info('\tdropping table %s:\n%s' % (config['cloudsql']['db'], drop_statement))
            try:
                cursor.execute(drop_statement)
            except Exception as e:
                log.exception('\tproblem dropping %s' % (table['table_name']))
                raise e

    def _create_schema(self, cursor, config, tables, log):
        create_table_template = "CREATE TABLE IF NOT EXISTS %s.%s (\n\t%s\n)" 
        primary_key_template = '%s INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n\t'
        create_col_template = '%s %s %s,\n\t'
        index_template = 'INDEX %s (%s),\n\t'
        foreign_key_template = 'CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s),\n\t'

        for table in tables:
            columnDefinitions = primary_key_template % (table['primary_key_name'])
            columnDefinitions += ''.join([create_col_template % (column[0], column[1], column[2]) for column in table['columns']])
            if 'natural_key_cols' in table and 0 < table['natural_key_cols']:
                index_cols = ','.join(table['natural_key_cols'])
                columnDefinitions += 'UNIQUE ' + index_template % (table['table_name'] + '_nkey', index_cols)
            count = 1
            if 'indices_defs' in table and 0 < table['indices_defs']:
                for index_def in table['indices_defs']:
                    index_cols = ','.join(index_def)
                    columnDefinitions += index_template % (table['table_name'] + str(count), index_cols)
                    count += 1
            if 'foreign_key' in table:
                columnDefinitions += foreign_key_template % ('fk_' + table['table_name'] + '_' + table['foreign_key'][1], 
                                            table['foreign_key'][0], table['foreign_key'][1], table['foreign_key'][2])
            columnDefinitions = columnDefinitions[:-3]
            table_statement = create_table_template % (config['cloudsql']['db'], table['table_name'], columnDefinitions)
            log.info('\tcreating table %s:\n%s' % (config['cloudsql']['db'], table_statement))
            try:
                cursor.execute(table_statement)
            except Exception as e:
                log.exception('problem creating %s' % (table['table_name']))
                raise e
        
    @classmethod
    def insert(cls, config, rows, table, log):
        db = None
        cursor = None
        try:
            log.info('\t\tstarting insert for %s' % (table))
            field_names = cls.field_names(table)
            insert_stmt = 'insert into %s.%s\n\t(%s)\nvalues\n\t(%s)' % (config['cloudsql']['db'], table, ', '.join(field_names), ', '.join(['%s']*len(field_names)))
            db = MySQLdb.connect(host = config['cloudsql']['host'], db = config['cloudsql']['db'], user = config['cloudsql']['user'], passwd = config['cloudsql']['passwd'])
            cursor = db.cursor()
            cursor.execute("START TRANSACTION")
            # now save in batches
            batch = 1028
            inserts = []
            for start in range(0, len(rows), batch):
                for index in range(batch):
                    if start + index == len(rows):
                        break
                    inserts += [rows[start + index]]
                log.info('\t\t\tinsert rows %s to %s' % (start, index))
                cursor.executemany(insert_stmt, inserts)
                inserts = []
            
            cursor.execute("COMMIT")
            log.info('\t\tcompleted insert')
            log.info('\t\tchecking counts for insert')
            cursor.execute('select count(*) from %s.%s' % (config['cloudsql']['db'], table))
            log.info('\t\tcounts for insert: submitted--%s saved--%s' % (len(rows), cursor.fetchone()[0]))
        except Exception as e:
            log.exception('\t\tinsert failed')
            if cursor:
                cursor.execute("ROLLBACK")
            raise e
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    @classmethod
    def field_names(cls, table):
        return [field_parts[0] for field_parts in cls.metadata_tables[table]['columns']]
