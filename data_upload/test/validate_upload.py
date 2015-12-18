'''
Created on Aug 6, 2015

validates the DCC upload.
    compares the metadata to the files uploaded
    generates a report on the metadata tables

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
from util import import_module


def validate_files(config, log, log_dir):
    log.info('start validating files')
# TODO: look at the set of file extensions that have been uploaded
#     ext2file = {}
    gcs_wrapper = None
    try:
        # get the metadata contents
        datastore = import_module(config['database_module'])
        helper = datastore.ISBCGC_database_helper
        select = 'select datafilename, datafileuploaded, datafilenamekey from metadata_data group by datafilename, datafileuploaded, datafilenamekey'
        cursor = helper.select(config, select, log)
        name_index = 0
        upload_index = 1
        keypath_index = 2
        datafilename2datafilenameinfo = {}
        count = 0
        inconsistent_update_path = set()
        inconsistent_update_not_path = set()
        inconsistent_not_update_path = set()
        for datafileinfo in cursor:
            if datafilename2datafilenameinfo.get(datafileinfo[name_index]):
                prev_datafileinfo = datafilename2datafilenameinfo[datafileinfo[name_index]]
                if prev_datafileinfo[upload_index] != datafileinfo[upload_index]:
                    if prev_datafileinfo[keypath_index] != datafileinfo[keypath_index]:
                        inconsistent_update_path.add(datafileinfo[name_index])
                    else:
                        inconsistent_update_not_path.add(datafileinfo[name_index])
                else:
                    if prev_datafileinfo[keypath_index] != datafileinfo[keypath_index]:
                        inconsistent_not_update_path.add(datafileinfo[name_index])
                continue
            if 0 == count % 2056:
                log.info('\t\tfound %s files. current file %s' % (count, datafileinfo[name_index]))
            count += 1
            datafilename2datafilenameinfo[datafileinfo[name_index]] = datafileinfo
        log.info('\tfinished getting files from metadata.  total count: %s' % (count))
        log.info('\tfound %s mismatched datafileupdated and keypath files:\n\t%s' % (len(inconsistent_update_path), '\n\t'.join(inconsistent_update_path[:10])))
        log.info('\tfound %s mismatched datafileuploaded:\n\t%s' % (len(inconsistent_update_not_path), '\n\t'.join(inconsistent_update_not_path[:10])))
        log.info('\tfound %s mismatched keypath:\n\t%s' % (len(inconsistent_not_update_path), '\n\t'.join(inconsistent_not_update_path[:10])))
        cursor = None
        
        # make sure the gcs  wrapper is initialized
        gcs_wrapper = import_module(config['gcs_wrapper'])
        gcs_wrapper.open_connection(config, log)
        # get the bucket contents
        for bucket_name in config['buckets']['update_uploaded']:
            log.info('\tgetting files in bucket %s' % (bucket_name))
            fileiter = gcs_wrapper.get_bucket_contents(bucket_name,log)
            matched_uploaded = 0
            mismatched_keypath = set()
            metapath_not_set = set()
            upload_not_set_metapath_set_matches = set()
            upload_not_set_metapath_set_not_matches = set()
            not_meta_marked_uploaded = set()
            not_in_metadata = set()
            count = 0
            for uploaded_file_info in fileiter:
                keypath = uploaded_file_info.name
                if not keypath.startswith('tcga'):
                    continue
                if 0 == count % 2056:
                    log.info('\t\tfound %s files. current file %s' % (count, keypath))
                count += 1
                filename = keypath[keypath.rindex['/']+1:]
                if datafilename2datafilenameinfo.get(filename):
                    datafileinfo = datafilename2datafilenameinfo[filename]
                    if datafileinfo[upload_index] == 'true':
                        if keypath == datafileinfo[keypath_index]:
                            matched_uploaded += 1
                        elif datafileinfo[keypath_index]:
                            mismatched_keypath.add(keypath + ' ' + datafileinfo[keypath_index])
                        else:
                            metapath_not_set.add(keypath)
                    elif datafileinfo[keypath_index]:
                        if keypath == datafileinfo[keypath_index]:
                            upload_not_set_metapath_set_matches.add(keypath)
                        elif datafileinfo[keypath_index]:
                            upload_not_set_metapath_set_not_matches.add(keypath + ' ' + datafileinfo[keypath_index])
                    else:
                        not_meta_marked_uploaded.add(keypath)
                    datafilename2datafilenameinfo.pop(filename)
                else:
                    not_in_metadata.add(keypath)
            log.info('\tfinished getting files in bucket %s.  total count: %s' % (bucket_name, count))
            log.info('\tfound %s matching between the bucket and the metadata' % (matched_uploaded))
            log.info('\tfound %s mismatched paths:\n\t%s' % (len(mismatched_keypath), '\n\t'.join(mismatched_keypath[:10])))
            log.info('\tfound %s where file in metadata as uploaded but keypath was not set:\n\t%s' % (len(metapath_not_set), '\n\t'.join(metapath_not_set[:10])))
            log.info('\tfound %s where uploaded not set but the keypath matched:\n\t%s' % (len(upload_not_set_metapath_set_matches), '\n\t'.join(upload_not_set_metapath_set_matches[:10])))
            log.info('\tfound %s where uploaded is not set there is a keypath in the metadata but it doesn\'t match actual key path:\n\t%s' % (len(upload_not_set_metapath_set_not_matches), '\n\t'.join(upload_not_set_metapath_set_not_matches[:10])))
            log.info('\tfound %s where uploaded and keypath not set in metadata:\n\t%s' % (len(not_meta_marked_uploaded), '\n\t'.join(not_meta_marked_uploaded[:10])))
            log.info('\tfound %s where file not in the metadata at all:\n\t%s' % (len(not_in_metadata), '\n\t'.join(not_in_metadata[:10])))
            
        # now find the remaining files marked in the metadata as uploaded that aren't actually
        not_marked_count = 0
        marked_uploaded_with_path = set()
        marked_not_uploaded_with_path = set()
        marked_uploaded_without_path = set()
        for datafileinfo in datafilename2datafilenameinfo.itervalues():
            if datafileinfo[upload_index] == 'false':
                if not datafileinfo[keypath_index]:
                    not_marked_count += 1
                else:
                    marked_not_uploaded_with_path.add(datafileinfo[keypath_index])
            else:
                if not datafileinfo[keypath_index]:
                    marked_uploaded_without_path.add(datafileinfo[name_index])
                else:
                    marked_uploaded_with_path.add(datafileinfo[keypath_index])
        log.info('found %s properly marked non-uploaded in the metadata' % (not_marked_count))
        log.info('found %s falsely marked uploaded with a path:\n\t%s' % (len(marked_uploaded_with_path), '\n\t'.join(marked_uploaded_with_path[:10])))
        log.info('found %s falsely marked not uploaded with a path:\n\t%s' % (len(marked_not_uploaded_with_path), '\n\t'.join(marked_not_uploaded_with_path[:10])))
        log.info('found %s falsely marked uploaded without a path:\n\t%s' % (len(marked_uploaded_without_path), '\n\t'.join(marked_uploaded_without_path[:10])))
    
        log.info('finished validating files')
    finally:
        if gcs_wrapper:
            gcs_wrapper.close_connection()
    
def validate_database(config, log, log_dir):
    log.info('start validating database')
# get the column names for the metadata tables from the data dictionary
    datastore = import_module(config['database_module'])
    helper = datastore.ISBCGC_database_helper
    select = "select MetadataFieldName, MetadataTable, ValueType from dev.metadata_datadictionary"
    cursor = helper.select(config, select, log)
    columns = [{}, {}, {}]
    table2column = {'metadata_clinical':0, 'metadata_biospecimen':1, 'metadata_data':2}
    for row in cursor:
        columns[table2column[row[1]]][row[0]] = row[2]
    
# create a summary table of statistics
    summary = 'column name\ttable name\tstudies\tvalid values\tdistinct values\tsstats\n'
    for table_name in table2column:
        log.info('validating table %s\n' % (table_name))
        dd_columns2type = columns[table2column[table_name]]
        cursor = helper.select(config, "desc %s" % (table_name), log)
        colname2colrow = {}
        for row in cursor:
            colname2colrow[row[0]] = row
        
        # match up the data dictionary with the table column names
        not_dd = set()
        for colname in colname2colrow:
            if colname not in dd_columns2type:
                not_dd.add(colname)
        
        if 0 < len(not_dd):
            log.warning('these columns in table %s are not in the data dictionary:\n\t%s\n' % (table_name, '\n\t'.join(not_dd)))
        not_table = set()
        for colname in dd_columns2type:
            if colname not in colname2colrow:
                not_table.add(colname)
        
        if 0 < len(not_table):
            log.warning('these columns in the data dictionary are not in table %s:\n\t%s\n' % (table_name, '\n\t'.join(not_table)))
        # validate data type in data dictionary
        mismatch_stmt = set()
        mismatches = set()
        for colname, dd_type in dd_columns2type.iteritems():
            if colname not in colname2colrow:
                continue

            datatype = colname2colrow[colname][1]
            if datatype.startswith('varchar'):
                if dd_type in ('int', 'number', 'DATE'):
                    mismatch_stmt.add('%s is specifed as a %s type in the data dictionary but is a string in the database' % (colname, dd_type))
                    mismatches.add(colname)
            elif datatype.startswith('date'):
                if dd_type != 'DATE':
                    mismatch_stmt.add('%s is specifed as a date type in the database but is specified as a %s type in the data dictionary' % (colname, dd_type))
                    mismatches.add(colname)
            elif datatype.startswith('int'):
                if dd_type not in ('int', 'number'):
                    mismatch_stmt.add('%s is specifed as an integer type in the database but is specified as a %s type in the data dictionary' % (colname, dd_type))
                    mismatches.add(colname)
            elif datatype.startswith('float'):
                if dd_type != 'number':
                    mismatch_stmt.add('%s is specifed as a float type in the database but is specified as a %s type in the data dictionary' % (colname, dd_type))
                    mismatches.add(colname)
            else:
                mismatch_stmt.add('unrecognized datatype, %s, for database column %s' % (datatype, colname))
                mismatches.add(colname)
        
        if 0 < len(mismatch_stmt):
            log.warning('these columns have different datatypes between the data dictionary and the table %s:\n\t%s\n' % (table_name, '\n\t'.join(mismatch_stmt)))
        # validate rexex's
        regex_stmt = 'select {0} from {1} where {0} not regexp {2} and {0} is not null and {0} <> "" limit 10'
        regex_print = '\t\t\tvalues don\'t conform to expected \'%s\' format:\n\t\t\t\t%s\n'
        hex32 = '\'^[0-9a-fA-F]{32}$\''
        uuid = '\'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$\''
        single = '\'[DWXHRT]\''
        # get basic stats for the columns
        stats = '\tcolumn statistics:\n'
        column_print = '\t\t%s(%s):\n'
        study_stmt = 'select study, count(*) from {1} where {0} is not null and {0} <> "" group by study'
        study_print = '\t\t\tstudies:\n\t\t\t\t%s\n'
        basic_stmt = 'select "VALUE", count(*) from {0} where {1} is not null and {1} <> "" union select "NULL", count(*) from {0} where {1} is null or {1} = ""'
        basic_print = '\t\t\tvalid values %s out of %s\n'
        distinct_stmt = 'select {0}, count(*) from {1} where {0} is not null and {0} <> "" group by {0} limit 1000'
        distinct_print = '\t\t\tdistinct values:\n\t\t\t\t%s\n'
        numeric_stmt = 'select min({0}), max({0}), avg({0}), std({0}), variance({0}) from {1}'
        numeric_print = '\t\t\tmin: %s max: %s avg: %s stddev: %s variance: %s\n'
        len_stmt = 'select distinct({0}) from {1} where length({0}) = {2} limit 10'
        len_print = '\t\t\tpossible truncation(%s):\n\t\t\t\t%s\n'
        for colname in colname2colrow:
            if colname not in dd_columns2type or colname in mismatches:
                datatype = 'text' if colname2colrow[colname][1].startswith('varchar') else 'int'
            else:
                datatype = dd_columns2type[colname]
            stats += column_print % (colname, datatype)
            if datatype.startswith('32'):
                cursor = helper.select(config, regex_stmt.format(colname, table_name, hex32), log)
                if len(cursor) > 0:
                    stats += regex_print % ('hex 32', '\n\t\t\t\t'.join(row[0] for row in cursor))
            elif datatype.startswith('UUID'):
                cursor = helper.select(config, regex_stmt.format(colname, table_name, uuid), log)
                if len(cursor) > 0:
                    stats += regex_print % ('UUID', '\n\t\t\t\t'.join(row[0] for row in cursor))
            elif datatype.startswith('single'):
                cursor = helper.select(config, regex_stmt.format(colname, table_name, single), log)
                if len(cursor) > 0:
                    stats += regex_print % ('single letter code', '\n\t\t\t\t'.join(row[0] for row in cursor))
            cursor = helper.select(config, study_stmt.format(colname, table_name), log)
            stats += study_print % ('\n\t\t\t\t'.join(str(row[0]) + ': ' + str(row[1]) for row in cursor))
            summary += '%s\t%s\t%s\t' % (colname, table_name, ','.join(str(row[0]) + ': ' + str(row[1]) for row in cursor))
            cursor = helper.select(config, basic_stmt.format(table_name, colname), log)
            results = []
            for row in cursor:
                results += [row]
            
            total = results[0][1] + results[1][1]
            if 'VALUE' == results[0][0]:
                stats += basic_print % (results[0][1], total)
                summary += '%s/%s\t' % (results[0][1], total)
            else:
                stats += basic_print % (results[1][1], total)
                summary += '%s/%s\t' % (results[1][1], total)
            cursor = helper.select(config, distinct_stmt.format(colname, table_name), log)
            if 50 < len(cursor):
                if 1000 == len(cursor):
                    stats += distinct_print % ('1000+ distinct values')
                    summary += '%s\t' % ('1000+ distinct values')
                else:
                    stats += distinct_print % (str(len(cursor)) + ' distinct values')
                    summary += '%s\t' % (str(len(cursor)) + ' distinct values')
            else:
                cursor.sort()
                stats += distinct_print % ('\n\t\t\t\t'.join(str(row[0]) + ': ' + str(row[1]) for row in cursor))
                summary += '%s\t' % (','.join(str(row[0]) + ': ' + str(row[1]) for row in cursor))
            if datatype in ('int', 'number', 'DATE'):
                cursor = helper.select(config, numeric_stmt.format(colname, table_name), log)
                for row in cursor:
                    stats += numeric_print % (row[0], row[1], row[2], row[3], row[4])
                    summary += 'min: %s max: %s avg: %s stddev: %s variance: %s' % (row[0], row[1], row[2], row[3], row[4])
            
            else:
                coltype = colname2colrow[colname][1]
                length = coltype[coltype.index('(') + 1:coltype.index(')')]
                cursor = helper.select(config, len_stmt.format(colname, table_name, length), log)
                if 0 < len(cursor):
                    stats += len_print % (length, '\n\t\t\t\t'.join(row[0] for row in cursor))
            summary += '\n'
        
        log.info(stats)
    
    with open(log_dir + 'summary_stats.tsv', 'w') as out:
        out.write(summary)
    log.info('finished validating database')

def main(configFileName):
    try:
        with open(configFileName) as configFile:
            config = json.load(configFile)
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'validating')
        log = logging.getLogger(log_name)
    except Exception as e:
        log.exception("problem in creating the log for validation")
        raise e

    try:
        validate_database(config, log, log_dir)
    except:
        log.exception('problem with validating the database')
    try:
        validate_files(config, log, log_dir)
    except:
        log.exception('problem with validating the files')
    
if __name__ == '__main__':
    main(sys.argv[1])
