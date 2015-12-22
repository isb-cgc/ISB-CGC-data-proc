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
import traceback

from util import create_log
from util import import_module


def validate_files_one_by_one(config, log, log_dir):
    log.info('start validating files one by one')
    gcs_wrapper = None
    try:
        # get the metadata contents
        datastore = import_module(config['database_module'])
        helper = datastore.ISBCGC_database_helper
        select = 'select datafilename, datafileuploaded, datafilenamekey from metadata_data where datafilename = %s group by datafilename, datafileuploaded, datafilenamekey'
        # make sure the gcs  wrapper is initialized
        gcs_wrapper = import_module(config['gcs_wrapper'])
        gcs_wrapper.open_connection(config, log)
        # get the bucket contents
        name_index = 0
        upload_index = 1
        keypath_index = 2
        inconsistent_update_path = set()
        inconsistent_update_not_path = set()
        inconsistent_not_update_path = set()

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
                if not keypath.startswith('tcga/') or keypath.startswith('tcga/intermediary') or keypath.endswith('bai') or 'hg18' in keypath:
                    continue
                if 0 == count % 2056:
                    log.info('\t\tfound %s files. current file %s' % (count, keypath))
                count += 1
                filename = keypath[keypath.rindex('/')+1:]
                cursor = helper.select(config, select, log, [filename], False)
                if 0 < len(cursor):
                    first_datafileinfo = cursor[0]
                    for datafileinfo in cursor[1:]:
                        if first_datafileinfo[upload_index] != datafileinfo[upload_index]:
                            if first_datafileinfo[keypath_index] != datafileinfo[keypath_index]:
                                inconsistent_update_path.add(datafileinfo[name_index])
                            else:
                                inconsistent_update_not_path.add(datafileinfo[name_index])
                        else:
                            if first_datafileinfo[keypath_index] != datafileinfo[keypath_index]:
                                inconsistent_not_update_path.add(datafileinfo[name_index])
                        continue
                
                if 0 < len(cursor):
                    datafileinfo = cursor[0]
                    if datafileinfo[upload_index] == 'true':
                        if keypath == datafileinfo[keypath_index][1:]:
                            matched_uploaded += 1
                        elif datafileinfo[keypath_index]:
                            mismatched_keypath.add(keypath + ' ' + datafileinfo[keypath_index])
                        else:
                            metapath_not_set.add(keypath)
                    elif datafileinfo[keypath_index]:
                        if keypath == datafileinfo[keypath_index][1:]:
                            upload_not_set_metapath_set_matches.add(keypath)
                        elif datafileinfo[keypath_index]:
                            upload_not_set_metapath_set_not_matches.add(keypath + ' ' + datafileinfo[keypath_index])
                    else:
                        not_meta_marked_uploaded.add(keypath)
                else:
                    not_in_metadata.add(keypath)
            log.info('\tfinished getting files in bucket %s.  total count: %s' % (bucket_name, count))
            log.info('\tfound %s matching between the bucket and the metadata' % (matched_uploaded))
            log.info('\tfound %s mismatched paths:\n\t%s' % (len(mismatched_keypath), '\n\t'.join(list(mismatched_keypath)[:50])))
            log.info('\tfound %s where file in metadata as uploaded but keypath was not set:\n\t%s' % (len(metapath_not_set), '\n\t'.join(list(metapath_not_set)[:50])))
            log.info('\tfound %s where uploaded not set but the keypath matched:\n\t%s' % (len(upload_not_set_metapath_set_matches), '\n\t'.join(list(upload_not_set_metapath_set_matches)[:50])))
            log.info('\tfound %s where uploaded is not set there is a keypath in the metadata but it doesn\'t match actual key path:\n\t%s' % (len(upload_not_set_metapath_set_not_matches), '\n\t'.join(list(upload_not_set_metapath_set_not_matches)[:50])))
            log.info('\tfound %s where uploaded and keypath not set in metadata:\n\t%s' % (len(not_meta_marked_uploaded), '\n\t'.join(list(not_meta_marked_uploaded)[:50])))
            log.info('\tfound %s where file not in the metadata at all:\n\t%s' % (len(not_in_metadata), '\n\t'.join(list(not_in_metadata)[:50])))
        log.info('\tfound %s mismatched datafileupdated and keypath files:\n\t%s' % (len(inconsistent_update_path), '\n\t'.join(list(inconsistent_update_path)[:50])))
        log.info('\tfound %s mismatched datafileuploaded:\n\t%s' % (len(inconsistent_update_not_path), '\n\t'.join(list(inconsistent_update_not_path)[:50])))
        log.info('\tfound %s mismatched keypath:\n\t%s' % (len(inconsistent_not_update_path), '\n\t'.join(list(inconsistent_not_update_path)[:50])))
    except Exception as e:
        log.info('\tfinished getting files in bucket %s.  total count: %s' % (bucket_name, count))
        log.info('\tfound %s matching between the bucket and the metadata' % (matched_uploaded))
        log.info('\tfound %s mismatched paths:\n\t%s' % (len(mismatched_keypath), '\n\t'.join(list(mismatched_keypath)[:50])))
        log.info('\tfound %s where file in metadata as uploaded but keypath was not set:\n\t%s' % (len(metapath_not_set), '\n\t'.join(list(metapath_not_set)[:50])))
        log.info('\tfound %s where uploaded not set but the keypath matched:\n\t%s' % (len(upload_not_set_metapath_set_matches), '\n\t'.join(list(upload_not_set_metapath_set_matches)[:50])))
        log.info('\tfound %s where uploaded is not set there is a keypath in the metadata but it doesn\'t match actual key path:\n\t%s' % (len(upload_not_set_metapath_set_not_matches), '\n\t'.join(list(upload_not_set_metapath_set_not_matches)[:50])))
        log.info('\tfound %s where uploaded and keypath not set in metadata:\n\t%s' % (len(not_meta_marked_uploaded), '\n\t'.join(list(not_meta_marked_uploaded)[:50])))
        log.info('\tfound %s where file not in the metadata at all:\n\t%s' % (len(not_in_metadata), '\n\t'.join(list(not_in_metadata)[:50])))
        log.info('\tfound %s mismatched datafileupdated and keypath files:\n\t%s' % (len(inconsistent_update_path), '\n\t'.join(list(inconsistent_update_path)[:50])))
        log.info('\tfound %s mismatched datafileuploaded:\n\t%s' % (len(inconsistent_update_not_path), '\n\t'.join(list(inconsistent_update_not_path)[:50])))
        log.info('\tfound %s mismatched keypath:\n\t%s' % (len(inconsistent_not_update_path), '\n\t'.join(list(inconsistent_not_update_path)[:50])))
        raise e
    finally:
        if gcs_wrapper:
            gcs_wrapper.close_connection()
    log.info('finished validating files one by one')

def validate_files(config, log, log_dir):
    log.info('start validating files')
# TODO: look at the set of file extensions that have been uploaded
#     ext2file = {}
    gcs_wrapper = None
    try:
        # paths to get level, center, and platform information
        # dcc open:         tcga/acc/Genome_Wide_SNP_6/broad.mit.edu__snp_cnv/Level_3/<file name>
        #                       or
        #                   tcga/acc/bio/Level_1/<file name>
        # cghub controlled: tcga/CHOL/DNA/WXS/WUGSC/ILLUMINA/109e9868e439717a65c48c775552212a.bam (use shortname2centername)
        # dcc controlled:   tcga/acc/Genome_Wide_SNP_6/broad.mit.edu__snp_cnv/Level_1/<file name>
        
        # make sure the gcs  wrapper is initialized
        gcs_wrapper = import_module(config['gcs_wrapper'])
        gcs_wrapper.open_connection(config, log)
        # get the bucket contents and organize in the master map
        shortname2centername = config['metadata_locations']['cghub']['shortname2centername']
        study2level2center2platform2fileinfo = {}
        for bucket_name in config['buckets']['update_uploaded']:
            log.info('\tgetting files in bucket %s' % (bucket_name))
            if bucket_name.startswith('360'):
                fieldlen = 7
            else:
                fieldlen = 6
            fileiter = gcs_wrapper.get_bucket_contents(bucket_name,log)
            count = 0
            for uploaded_file_info in fileiter:
                keypath = uploaded_file_info.name
                if not keypath.startswith('tcga/') or keypath.startswith('tcga/intermediary') or keypath.endswith('bai') or keypath.endswith('xml') \
                    or 'antibody_annotation' in keypath or 'sdrf' in keypath:
                    continue
                filename = keypath[keypath.rindex('/')+1:]
                # add file info to master map
                fields = keypath.split('/')
                if fieldlen != len(fields):
                    log.warning('\t\todd path: %s' % (keypath))
                    continue
                level2center2platform2fileinfo = study2level2center2platform2fileinfo.get(fields[1].lower(), {})
                pipeline_fields = fields[3].split('__')
                if 1 == len(pipeline_fields):
                    # CGHub style path
                    level = 'Level_1'
                    center = shortname2centername[fields[4]]
                else:
                    # DCC style path
                    level = fields[4]
                    center = pipeline_fields[0]
                center2platform2fileinfo = level2center2platform2fileinfo.get(level, {})
                platform2fileinfo = center2platform2fileinfo.get(center, {})
                fileinfo = platform2fileinfo.get(fields[2], set())
                if tuple([filename, keypath]) in fileinfo:
                    raise ValueError('already saw %s%s' % (filename, keypath))
                fileinfo.add(tuple([filename, keypath]))
                if 0 == count % 8192:
                    log.info('\t\tfound %s files. current file %s\n\tfilename: %s\n\tstudy: %s\n\tlevel: %s\n\tcenter: %s\n\tplatform: %s' % 
                             (count, keypath, filename, fields[1].lower(), level, center, fields[2]))
                count += 1
        # print out the first few entries from the dictionary
        level2center2platform2fileinfo = study2level2center2platform2fileinfo['acc']
        for level, center2platform2fileinfo in level2center2platform2fileinfo.iteritems():
            log.info('\t\t%s' % (level))
            for center, platform2fileinfo in center2platform2fileinfo.iteritems():
                log.info('\t\t\t%s' % (center))
                for platform, fileinfo in platform2fileinfo.iteritems():
                    log.info('\t\t\t\t%s(%s): %s' % (platform, len(fileinfo), ', '.join(list(fileinfo)[:10])))
        
        datastore = import_module(config['database_module'])
        helper = datastore.ISBCGC_database_helper
        # get the metadata contents by level, center and platform combinations.
        select = 'select study, datalevel, datacentername, platform, count(*) \
                from metadata_data \
                group by study, datalevel, datacentername, platform \
                order by study, replace(datalevel, \' \', \'_\')'
        combinations = helper.select(config, select, log)

        # now loop over the study, level, center, platform combinations in the database
        name_index = 0
        upload_index = 1
        keypath_index = 2
        count = 0
        inconsistent_update_path = set()
        inconsistent_update_not_path = set()
        inconsistent_not_update_path = set()
        upload_archives = config['upload_archives']
        for combo in combinations:
            combo_name = ':'.join([str(piece) if piece else 'None' for piece in combo])
            log.info('\tlooking at %s' % (combo_name))
            
            datafilename2datafilenameinfo = {}
            uploadable = False
            if 'Level_1' == combo[1]:
                # adjust for CGHub lack of full paltform name information
                if 'DNA'  in combo[3]:
                    fileinfo = study2level2center2platform2fileinfo.get(combo[0], {}).get(combo[1], {}).get(combo[2], {}).get('DNA', set())
                elif 'RNA' in combo[3]:
                    fileinfo = study2level2center2platform2fileinfo.get(combo[0], {}).get(combo[1], {}).get(combo[2], {}).get('RNA', set())
                platforms = upload_archives.get(combo[1], {}).get(combo[2], [])
                for platform in platforms:
                    if combo[3] in platform:
                        # not a perfect check but no way to know from CGHub the exact DNA or RNA platform
                        uploadable = True
                        break
            else:
                uploadable = True if upload_archives.get(combo[1].replace(' ', '_'), {}).get(combo[2], []).count(combo[3]) else False
                fileinfo = study2level2center2platform2fileinfo.get(combo[0], {}).get(combo[1].replace(' ', '_'), {}).get(combo[2], {}).get(combo[3], set())
            log.info('\t\tuploadable: %s file count: %s' % (uploadable, len(fileinfo)))
            if 0 == len(fileinfo):
                if not uploadable:
                    log.info('\t\tno files properly found in bucket for not loadable combo')
                else:
                    log.info('\t\tno files found in bucket for what is loadable combo')
            else:
                if not uploadable:
                    log.info('\t\tfiles found in bucket for combo that should not have been uploaded')
                else:
                    log.info('\t\tfiles properly found in bucket for loadable combo')

            select = 'select datafilename, datafileuploaded, datafilenamekey \
                    from metadata_data \
                    where study = %s and datalevel = %s and datacentername = %s and platform = %s \
                    group by datafilename, datafileuploaded, datafilenamekey'
            cursor = helper.select(config, select, log, [combo[0], combo[1], combo[2], combo[3]])
            log.info('\t\tselect %s rows in the database for %s' % (len(cursor), combo_name))
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
            
            matched_uploaded = 0
            mismatched_keypath = set()
            metapath_not_set = set()
            upload_not_set_metapath_set_matches = set()
            upload_not_set_metapath_set_not_matches = set()
            not_meta_marked_uploaded = set()
            not_in_metadata = set()
            metadata_marked_uploaded_not_in_bucket = set()
            count = 0
            if 0 < len(fileinfo):
                for filename, keypath in fileinfo:
                    count += 1
                    if datafilename2datafilenameinfo.get(filename):
                        datafileinfo = datafilename2datafilenameinfo[filename]
                        if datafileinfo[upload_index] == 'true':
                            if keypath == datafileinfo[keypath_index][1:]:
                                matched_uploaded += 1
                            elif datafileinfo[keypath_index]:
                                mismatched_keypath.add(keypath + ' ' + datafileinfo[keypath_index])
                            else:
                                metapath_not_set.add(keypath)
                        elif datafileinfo[keypath_index]:
                            if keypath == datafileinfo[keypath_index][1:]:
                                upload_not_set_metapath_set_matches.add(keypath)
                            elif datafileinfo[keypath_index]:
                                upload_not_set_metapath_set_not_matches.add(keypath + ' ' + datafileinfo[keypath_index])
                        else:
                            not_meta_marked_uploaded.add(keypath)
                        datafilename2datafilenameinfo.pop(filename)
                    else:
                        not_in_metadata.add(keypath)
            else:
                # none of these should be marked as uploaded!
                for datafileinfo in datafilename2datafilenameinfo:
                    if datafileinfo[upload_index] == 'true':
                        metadata_marked_uploaded_not_in_bucket.add(datafileinfo[name_index] + ' ' + datafileinfo[keypath_index])
                
            log.info('\t\tfinished getting files in bucket %s for %s.  total count: %s' % (bucket_name, combo_name, count))
            log.info('\t\tfound %s matching between the bucket and the metadata' % (matched_uploaded))
            log.info('\t\tfound %s mismatched paths:\n\t%s' % (len(mismatched_keypath), '\n\t'.join(list(mismatched_keypath)[:50])))
            log.info('\t\tfound %s where file in metadata as uploaded but keypath was not set:\n\t%s' % (len(metapath_not_set), '\n\t'.join(list(metapath_not_set)[:50])))
            log.info('\t\tfound %s where uploaded not set but the keypath matched:\n\t%s' % (len(upload_not_set_metapath_set_matches), '\n\t'.join(list(upload_not_set_metapath_set_matches)[:50])))
            log.info('\t\tfound %s where uploaded is not set there is a keypath in the metadata but it doesn\'t match actual key path:\n\t%s' % (len(upload_not_set_metapath_set_not_matches), '\n\t'.join(list(upload_not_set_metapath_set_not_matches)[:50])))
            log.info('\t\tfound %s where uploaded and keypath not set in metadata:\n\t%s' % (len(not_meta_marked_uploaded), '\n\t'.join(list(not_meta_marked_uploaded)[:50])))
            log.info('\t\tfound %s where file not in the metadata at all:\n\t%s' % (len(not_in_metadata), '\n\t'.join(list(not_in_metadata)[:50])))
            log.info('\t\tfound %s where file in the metadata marked as uploaded but not in the bucket\n\t%s' % (len(metadata_marked_uploaded_not_in_bucket), '\n\t'.join(list(metadata_marked_uploaded_not_in_bucket)[:50])))
        
        log.info('\tfound %s mismatched datafileupdated and keypath files:\n\t%s' % (len(inconsistent_update_path), '\n\t'.join(list(inconsistent_update_path)[:50])))
        log.info('\tfound %s mismatched datafileuploaded:\n\t%s' % (len(inconsistent_update_not_path), '\n\t'.join(list(inconsistent_update_not_path)[:50])))
        log.info('\tfound %s mismatched keypath:\n\t%s' % (len(inconsistent_not_update_path), '\n\t'.join(list(inconsistent_not_update_path)[:50])))
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
        print 'problem in creating the log for validation'
        traceback.print_exc(limit = 5)
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
