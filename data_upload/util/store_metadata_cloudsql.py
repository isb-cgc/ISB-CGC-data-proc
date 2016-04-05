'''
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
from threading import Lock

from util import import_module

lock = Lock()

fields2value = {}

def store_metadata(config, log, table, key_metadata):
    if not config['process_bio']:
        return
    if 0 == len(fields2value):
        # set up the map to get statistics across all studies for desired bio columns
        clinical_auxiliary_filters = config['metadata_locations']['clinical']
        clinical_auxiliary_filters.update(config['metadata_locations']['auxiliary'])
        for value in clinical_auxiliary_filters.itervalues():
            try:
                fields2value[value] = 0
            except:
                # just ignore the derived fields that are specified as unhashable dicts
                pass
        biospecimen_filters = config['metadata_locations']['biospecimen']
        for value in biospecimen_filters.itervalues():
            fields2value[value] = 0
    
    count = 0
    count_upload = 0
    not_data_fields = set()
    upload_exts = set()
    field2stats = {}
    nospecies = []
    nosdrf = set()
    insert_str_listlist = []
    datastore = import_module(config['database_module'])
    field_names = datastore.ISBCGC_database_helper.field_names(table)
    field_name2column = dict([(column_name, index) for index, column_name in enumerate(field_names)])
    list_fields = config['list_fields']
    log.info('\tstarting store metadata')
    for metadata in key_metadata.itervalues():
        try:
            # skip cellline/control samples:
            if 'SampleBarcode' in metadata and '20' == metadata['SampleBarcode'][13:15]:
                continue

            list_values = [None for _ in range(len(field_name2column))]
            for field, value in metadata.iteritems():
                # make sure every string field is stored as a string rather than unicode
                # unicode is stored as a blob in the datastore
                try:
                    if value in (None, '->'):
                        metadata[field] = None
                    else:
                        if field in fields2value:
                            fields2value[field] = fields2value[field] + 1
                        if field in list_fields:
                            metadata[field] = [str(v.encode('ascii', 'ignore').strip()) for v in value]
                            try:
                                value = '(' + ','.join(value) + ')'
                            except:
                                log.exception('problem setting list %s: %s' % (field, value))
                        else:
                            metadata[field] = str(value.encode('ascii', 'ignore').strip())
                        # and add to the statistics for this field
                        stats = field2stats.setdefault(field, [0, set()])
                        stats[0] += 1
                        try:
                            stats[1].add(value)
                        except:
                            log.exception('problem adding value %s' % (value))
                        
                    if field in field_name2column:
                        list_values[field_name2column[field]] = value
                    else:
                        not_data_fields.add(field)
                    
                    if 'DatafileUploaded' == field and 'true' == value:
                        count_upload += 1
                        upload_exts.add(metadata['DatafileName'][metadata['DatafileName'].rfind('.') + 1:])
                except Exception as e:
                    log.exception("problem with converting to string and recording stats for %s: %s" % (field, value))
                    raise e
                    
            insert_str_listlist += [list_values]
            archive = metadata['DataArchiveName'] if 'DataArchiveName' in metadata else 'NO_ARCHIVE'
            if 'metadata_data' == table and 'Species' not in metadata:
                nospecies.append(metadata['DatafileName'] + ' ' + metadata['DataLevel'] + ' ' + metadata['DatafileUploaded'] + ' ' + archive)
            if 'metadata_data' == table and 'SDRFFileName' not in metadata and 'project_accession' not in metadata:
                nosdrf.add(metadata['DatafileName'] + ' ' + metadata['DataLevel'] + ' ' + metadata['DatafileUploaded'] + ' ' + archive)

            if 0 == count % 1024:
                log.info('\tinsert statement %s file metadata.  latest: %s' % (count, metadata))
            count += 1

        
        except Exception as e:
            log.exception('problem in store_metadata()')
            raise e
    log.info('\tsetup %s total records, %s to upload with extensions %s' % (count, count_upload, ','.join(upload_exts)))

    # now save to cloudsql
    datastore.ISBCGC_database_helper.insert(config, insert_str_listlist, table, log)
    
    log.info('\tstatistics:')
    fields = field2stats.keys()
    fields.sort()
    for field in fields:
        stats = field2stats[field]
        try:
            log.info('\t\tstats for %s(%s:%s): %s' % (field, len(stats[1]), stats[0], ','.join(stats[1]) if 21 > len(stats[1]) else stats[1].pop()))
        except:
            log.exception('problem with %s(total: %s distinct: %s)' % (field, stats[0], len(stats[1])))
    if 0 < len(nospecies) or 0 < len(nosdrf):
        if nospecies == nosdrf:
            log.warning( '\tfiles with no species match files with no sdrf file(%s):\n\t\t%s' % (len(nospecies), '\n\t\t'.join(list(nospecies)[:150])))
        else:
            log.warning( '\tfiles with no species(%s):\n\t\t%s' % (len(nospecies), '\n\t\t'.join(list(nospecies)[:150])))
            log.warning( '\tfiles with no sdrf file(%s):\n\t\t%s' % (len(nosdrf), '\n\t\t'.join(list(nosdrf)[:150])))
    
    log.info('\tfinished store metadata.  stored %s total records' % (count))

def print_combined_stats(log):
    fields = fields2value.keys()
    fields.sort()
    log.info('metadata stats over all studies:')
    for field in fields:
        log.info('\t%s: %s' % (field, fields2value[field]))
