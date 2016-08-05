'''
a wrapper to google cloud sql.

the MySQLdb module is thread safe but the connections to the database are not.  so the
recommendation is that each thread have an independent connection.  currently, each
database access will use its own connection and at the end of the method, close it.
if this becomes expensive, timewise, a mapping of thread to connection can be utilized.

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
import time

class ISBCGC_database_helper(object):
    """
    this class is the base class to manage subclass the CloudSQL  uploads
    """

    @classmethod
    def getDBConnection(cls, config, log):
        try:
            ssl_dir = config['cloudsql']['ssl_dir']
            ssl = {
        #             'ca': ssl_dir + 'server-ca.pem',
                'cert': ssl_dir + 'client-cert.pem',
                'key': ssl_dir + 'client-key.pem' 
            }
            db = MySQLdb.connect(host=config['cloudsql']['host'], db=config['cloudsql']['db'], user=config['cloudsql']['user'], passwd=config['cloudsql']['passwd'], ssl = ssl)
        except Exception as e:
            # if connection requests are made too close together over a period of time, the connection attempt might fail
            count = 7
            while count > 0:
                count -= 1
                time.sleep(count)
                log.warning('\n\n!!!!!!sleeping on error to reattempt db connection!!!!!!\n')
                try:
                    db = MySQLdb.connect(host=config['cloudsql']['host'], db=config['cloudsql']['db'], user=config['cloudsql']['user'], passwd=config['cloudsql']['passwd'], ssl = ssl)
                    break
                except Exception as e:
                    if 1 == count:
                        log.exception("failed to reconnect to database")
                        raise e
            
        return db

    @classmethod
    def process_tables(cls, config, process_function, log):
        db = None
        cursor = None
        try:
            if not config['cloudsql']['update_schema']:
                return
            if not config['process_bio']:
                log.warning('process_bio must be true for initialization to proceed')
                return
            db = cls.getDBConnection(config, log)
            cursor = db.cursor()
            process_function(cursor, config, cls.metadata_tables, log)
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()

    @classmethod
    def drop_tables(cls, config, log):
        cls.process_tables(config, cls._drop_schema, log)
    
    @classmethod
    def setup_tables(cls, config, log):
        cls.process_tables(config, cls._create_schema, log)
    
    @classmethod
    def _drop_schema(cls, cursor, config, tables, log):
        drop_schema_template = 'DROP TABLE IF EXISTS %s.%s'
        
        for table in tables.keys():
            drop_statement = drop_schema_template % (config['cloudsql']['db'], table)
            log.info('\tdropping table %s:\n%s' % (table, drop_statement))
            try:
                cursor.execute(drop_statement)
            except Exception as e:
                log.exception('\tproblem dropping %s' % (table))
                raise e

    @classmethod
    def _create_schema(cls, cursor, config, tables, log):
        create_table_template = "CREATE TABLE IF NOT EXISTS %s.%s (\n\t%s\n)" 
        primary_key_template = '%s INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n\t'
        create_col_template = '%s %s %s,\n\t'
        index_template = 'INDEX %s (%s),\n\t'
        foreign_key_template = 'CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s),\n\t'

        for table in tables.itervalues():
            columnDefinitions = ''
            if 'primary_key_name' in table:
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
            if 'foreign_keys' in table:
                for index in range(len(table['foreign_keys'])):
                    columnDefinitions += foreign_key_template % ('fk_' + table['table_name'] + '_' + table['foreign_keys'][index][1], 
                                                table['foreign_keys'][index][0], table['foreign_keys'][index][1], table['foreign_keys'][index][2])
            columnDefinitions = columnDefinitions[:-3]
            table_statement = create_table_template % (config['cloudsql']['db'], table['table_name'], columnDefinitions)
            log.info('\tcreating table %s:\n%s' % (table['table_name'], table_statement))
            try:
                cursor.execute(table_statement)
            except Exception as e:
                log.exception('problem creating %s' % (table['table_name']))
                raise e

    @classmethod
    def select(cls, config, stmt, log, params = [], verbose = True):
        db = None
        cursor = None
        try:
            if verbose:
                log.info('\t\tstarting \'%s:%s\'' % (stmt, params))
            db = cls.getDBConnection(config, log)
            cursor = db.cursor()
            # now execute the select
            cursor.execute(stmt, params)
            if verbose:
                log.info('\t\tcompleted select.  fetched %s rows', cursor.rowcount)
            retval = [row for row in cursor]
            return retval
        except Exception as e:
            log.exception('\t\tselect failed: %s(%s)' % (stmt, params))
            if cursor:
                cursor.execute("ROLLBACK")
            raise e
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()
        
    @classmethod
    def select_paged(cls, config, stmt, log, countper = 1000, verbose = True):
        db = None
        cursor = None
        try:
            if verbose:
                log.info('\t\tstarting \'%s\'' % (stmt))
            db = cls.getDBConnection(config, log)
            cursor = db.cursor()
            retval = []
            # now execute the select
            curcount = countper
            while 0 < cursor.rowcount:
                curstmt = stmt % (curcount)
                cursor.execute(curstmt, [curcount])
                retval += [row for row in cursor]
                log.info('\t\tcompleted select.  fetched %s rows for %s', cursor.rowcount, curstmt)
                curcount += countper
            return retval
        except Exception as e:
            log.exception('\t\tselect failed')
            if cursor:
                cursor.execute("ROLLBACK")
            raise e
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()
        
    @classmethod
    def update(cls, config, stmt, log, params = [], verbose = True):
        db = None
        cursor = None
        try:
            if verbose:
                log.info('\t\tstarting \'%s\'' % (stmt))
            db = cls.getDBConnection(config, log)
            cursor = db.cursor()
            # now execute the updates
            cursor.execute("START TRANSACTION")
            count = 0
            report = len(params) / 20
            for paramset in params:
                if 0 == report or 0 == count % report:
                    log.info('\t\t\tupdated %s records' % (count))
                count += 1
                try:
                    cursor.execute(stmt, paramset)
                except MySQLdb.OperationalError as oe:
                    log.warning('checking operation error: %s' % (oe))
                    if oe.errno == 1205:
                        log.warning('\t\t\tupdate had operation error (%s:%s) on %s, sleeping' % (stmt, count, paramset))
                        time.sleep(1)
                        cursor.execute(stmt, paramset)
                    else:
                        log.exception('\t\t\tupdate had operation error (%s:%s) on %s' % (stmt, count, paramset))
                except Exception as e:
                    log.exception('problem with update(%s): \n\t\t\t%s\n\t\t\t%s' % (count, stmt, paramset))
                    raise e
            if verbose:
                log.info('\t\tcompleted update.  updated %s:%s record', count, cursor.rowcount)
            cursor.execute("COMMIT")
        except Exception as e:
            log.exception('\t\tupdate failed')
            if cursor:
                cursor.execute("ROLLBACK")
            raise e
        finally:
            if cursor:
                cursor.close()
            if db:
                db.close()
        
    @classmethod
    def insert(cls, config, rows, table, log):

        field_names = cls.field_names(table)
        cls.column_insert(config, rows, table, field_names, log)


    @classmethod
    def column_insert(cls, config, rows, table, field_names, log):
        db = None
        cursor = None
        try:
            log.info('\t\tstarting insert for %s' % (table))
            insert_stmt = 'insert into %s.%s\n\t(%s)\nvalues\n\t(%s)' % (config['cloudsql']['db'], table, ', '.join(field_names), ', '.join(['%s']*len(field_names)))
            db = cls.getDBConnection(config, log)
            cursor = db.cursor()
            cursor.execute("START TRANSACTION")
            # now save in batches
            batch = 1028
            inserts = []
            for tries in range(3):
                retrying = False
                for start in range(0, len(rows), batch):
                    if retrying:
                        retrying = False
                        continue
                    for index in range(batch):
                        if start + index == len(rows):
                            break
                        inserts += [rows[start + index]]
                    log.info('\t\t\tinsert rows %s to %s' % (start, index))
                    try:
                        cursor.executemany(insert_stmt, inserts)
                    except MySQLdb.OperationalError as oe:
                        if oe.errno == 2006 and 2 > tries:
                            log.warning('\t\t\tupdate had operation error 2006, lost connection for %s, sleeping' % (insert_stmt))
                            time.sleep(1)
                            # rollback any previous inserts
                            cursor.execute("ROLLBACK")

                            # and setup to retry
                            retrying = True
                            try:
                                # make sure connection is closed
                                db.close()
                            except:
                                pass
                            db = cls.getDBConnection(config, log)
                            cursor = db.cursor()
                            cursor.execute("START TRANSACTION")
                        else:
                            log.exception('\t\t\tupdate had multiple operation errors 2006 for %s' % (insert_stmt))
                            raise oe
                    except Exception as e:
                        log.exception('problem with update for %s: %s' % (insert_stmt, e))
                        raise e
                    inserts = []
                # successfully looped through so stop trying
                break
            
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

