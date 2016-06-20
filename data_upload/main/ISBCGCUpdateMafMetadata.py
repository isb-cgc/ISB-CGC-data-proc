'''
Created on May 6, 2016

parses the data ETL files from both the open and protected uploads to find
protected entries not in the open.  the metadata for these are then saved to 
the metadata_data table

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
from datetime import date, datetime
import json
import logging
import sys

from util import create_log, import_module

def processETLFile(etlFilename, filename2aliquot2metadata, log):
    '''
    process the file into the nested map
    
    parameters:
        etlFilename: the file to process
        filename2aliquot2metadata: the map to setup
    '''
    log.info('%s: start processing etl file %s' % (str(datetime.now()), etlFilename))
    no_archive = 0
    with open(etlFilename) as infile:
        for line in infile:
            metadata = json.loads(line)
            if 'DataArchiveName' not in metadata:
                no_archive += 1
            aliquot2metadata = filename2aliquot2metadata.setdefault(metadata['DatafileName'], {})
            if metadata['AliquotBarcode'] in aliquot2metadata:
                raise ValueError('unexpected repeat of barcode %s for file %s' % (metadata['AliquotBarcode'], metadata['DatafileName']))
            aliquot2metadata[metadata['AliquotBarcode']] = line
    log.info('\n\tmetadata with no archive: %s\n' % (no_archive))
    log.info('%s: finished processing etl file %s' % (str(datetime.now()), etlFilename))

def processBucketContents(bucketContentFilename, bucketFiles, log):
    '''
    process the file names in the bucket file into set
    
    parameters:
        bucketContentFilename: the file to get file names from
        bucketFiles:  the set to fill with the filenames
    '''
    log.info('%s: start processing bucket file %s' % (str(datetime.now()), bucketContentFilename))
    exts = {}
    with open(bucketContentFilename) as infile:
        for path in infile:
            path = path.strip()
            filename = path[path.rindex('/') + 1:]
            bucketFiles.add(filename)
            if filename.endswith('.gz'):
                ext = filename[-6:]
            else:
                ext = filename[-3:]
            count = exts.setdefault(ext, 0) + 1
            exts[ext] = count
    log.info('\n\texts: %s\n' % (', '.join('%s: %s' % (ext, count) for ext, count in exts.iteritems())))
    log.info('%s: finished processing bucket file %s' % (str(datetime.now()), bucketContentFilename))

def main(configFilename, openETLFilename, contETLFilename, outputFilename, contBucketContentFilename):
    '''
    process the two ETL files and obtain the metadata for the filenames in contETLFilename
    that aren't in openETLFilename and save to the output file
    
    parameters:
        openETLFilename: the data metadata file produced by the open access upload run (note: this will have controlled access
        metadata for those controlled access files who appear in the metadata)
        contETLFilename: the data metadata file produced by the controlled access upload run
        outputFilename: he file to write the results to
    '''
    with open(configFilename) as configFile:
        config = json.load(configFile)
    run_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
    log_name = create_log(run_dir, 'top_processing')
    log = logging.getLogger(log_name)
    log.info('%s: start processing controlled vs open upload metadata' % (str(datetime.now())))

    openFilename2aliquot2metadata = {}
    processETLFile(openETLFilename, openFilename2aliquot2metadata, log)
    contFilename2aliquot2metadata = {}
    processETLFile(contETLFilename, contFilename2aliquot2metadata, log)
    
    openkeys = set(openFilename2aliquot2metadata.keys())
    contkeys = set(contFilename2aliquot2metadata.keys())
    onlycontkeys = contkeys - openkeys

    bucketFiles = set()
    processBucketContents(contBucketContentFilename, bucketFiles, log)
    intersect = bucketFiles & onlycontkeys
    log.info('\n\tthe count of files only in the controlled access ETL: %s\n\tthe intersection count with the bucket: %s\n\tthe non intersect left in the ETL: %s' % \
        (len(onlycontkeys), len(intersect), len(onlycontkeys - bucketFiles)))
    
    headerCols = [
        'Pipeline',
        'DataArchiveURL',
        'DatafileUploaded',
        'Datatype',
        'Study',
        'DataArchiveVersion',
        'DataCenterType',
        'Project',
        'Platform',
        'DataLevel',
        'ParticpantBarcode',
        'SecurityProtocol',
        'SampleBarcode',
        'AliquotBarcode',
        'IncludeForAnalysis',
        'DataCenterName',
        'DatafileName',
        'DataCenterCode',
        'DataArchiveName',
        'Species',
        'AliquotUUID'
    ]
    inserts = []
    missing_cols = {}
    missing_filenames = set()
    missing_aliquots = set()
    with open(outputFilename, 'w') as outfile:
        for key in onlycontkeys:
            for metadata in contFilename2aliquot2metadata[key].itervalues():
                outfile.write(metadata)
                metadata_json = json.loads(metadata)
                if len(metadata_json) > len(headerCols):
                    raise ValueError('found unknown column(s): %s' % (','.join(set(metadata_json.keys()) - set(headerCols))))
                newinsert = []
                for header in headerCols:
                    if header in metadata_json:
                        newinsert += [metadata_json[header]]
                    else:
                        newinsert += [None]
                        count = missing_cols.setdefault(header, 0) + 1
                        missing_filenames.add(key)
                        missing_aliquots.add(metadata_json['AliquotBarcode'])
                        missing_cols[header] = count
            inserts += [newinsert]
        log.info('\n\tmissing cols: %s from files %s and aliquots %s\n' % (', '.join('%s: %s' % (column, count) for column, count in missing_cols.iteritems()), len(missing_filenames), len(missing_aliquots)))
    
    db_module = import_module(config['database_module'])
    db_module.ISBCGC_database_helper.column_insert(config, inserts, 'metadata_data', headerCols, log)
        
    log.info('%s: finished processing controlled vs open upload metadata' % (str(datetime.now())))
    
if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    