'''
Created on Jun 29, 2016

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
from datetime import date
import json
import logging
import os
import sys

import gcs_wrapper_gcloud
from util import create_log, upload_file

def main(configfilename):
    try:
        with open(configfilename) as configFile:
            config = json.load(configFile)
    
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'create_snapshot')
        log = logging.getLogger(log_name)
        log.info('begin create snapshot')
        gcs_wrapper_gcloud.open_connection(config, log)
    
        latestarchive07jun16path = '/titan/cancerregulome11/TCGA/repositories/dcc-mirror/datareports/resources/latestarchive_07jun16'
        latestarchivepath = 'latestarchive'
        snapshotprefix = '/titan/cancerregulome11/TCGA/repositories/dcc-mirror/public'
        dccprefixlen = len('https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous')
        googlebucket = 'dcc_repository'
        googlefolderprefix = '2016_06_07/public'
        googlelinkprefix = 'https://console.cloud.google.com/m/cloudstorage/b/dcc_repository/o/2016_06_07/public'
        
        count = 1
        with open(latestarchive07jun16path) as latestarchive07jun16, open(latestarchivepath, 'w') as latestarchive:
            # copy the header
            latestarchive.write(latestarchive07jun16.readline())
            for line in latestarchive07jun16:
                try:
                    fields = line.strip().split('\t')
                    if 'tcga4yeo' in fields[2]:
                        # skip controlled access archives
                        continue
                    # translate the location in the dcc to the location in our mirror
                    pathsuffix = fields[2][dccprefixlen:]
                    fields[2] = googlelinkprefix + pathsuffix
                    latestarchive.write('\t'.join(fields) + '\n')
                    # upload to GCS
                    snapshotloc = snapshotprefix + pathsuffix
                    uploadpath = 'gs://' + googlebucket + '/' + googlefolderprefix + pathsuffix
                    try:
                        if os.path.exists(snapshotloc):
                            upload_file(config, snapshotloc, googlebucket, uploadpath, log)
                    except ValueError as ve:
                        if ('%s' % ve) != ('found %s in %s' % (uploadpath, googlebucket)):
                            raise ve
                    except:
                        raise
                    if 1 == count % 250:
                        log.info('\t==================================\n\tgoogle path: %s\n\tgoogle link: %s\n\tsnapshot location: %s\n' % (uploadpath, fields[2], snapshotloc))
                    count += 1
                except:
                    log.exception('problem on row %d: %s(%s)' % (count, line, fields))
                    raise
                
        log.info('finished create snapshot, found %s archives' % (count))
    finally:
        gcs_wrapper_gcloud.close_connection()
    
if __name__ == '__main__':
    main(sys.argv[1])