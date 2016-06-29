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

import gcs_wrapper
from util import create_log

from oauth2client.client import OAuth2WebServerFlow 
from oauth2client import tools 
from oauth2client.file import Storage 

def get_credentials(): 
    EMAIL_SCOPE = 'https://www.googleapis.com/auth/userinfo.email' 
    CLIENT_ID = "907668440978-0ol0griu70qkeb6k3gnn2vipfa5mgl60.apps.googleusercontent.com",
    CLIENT_SECRET = "To_WJH7-1V-TofhNGcEqmEYi",
    STORAGE_FILE = ".isb_credentials_dev" 
    
    oauth_flow_args = ['--noauth_local_webserver'] 
    # where a default token file (based on the google project) will be stored for use by the endpoints 
    DEFAULT_STORAGE_FILE = os.path.join(os.path.expanduser("~"), STORAGE_FILE) 
    storage = Storage(DEFAULT_STORAGE_FILE) 
    credentials = storage.get() 
    if not credentials or credentials.invalid: 
        # this will bring up a verification URL to paste in a browser
        flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, EMAIL_SCOPE) 
        flow.auth_uri = flow.auth_uri.rstrip('/') + '?approval_prompt=force' 
        credentials = tools.run_flow(flow, storage, tools.argparser.parse_args(oauth_flow_args)) 
    return credentials 

def main(configfilename):
    try:
        os.environ['BOTO_CONFIG'] = 'C:\\Users\\michael'
        get_credentials()
        with open(configfilename) as configFile:
            config = json.load(configFile)
    
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'create_snapshot')
        log = logging.getLogger(log_name)
        log.info('begin create snapshot')
        gcs_wrapper.open_connection()
    
        latestarchive07jun16path = '/titan/cancerregulome11/TCGA/repositories/dcc-mirror/datareports/resources/latestarchive_07jun16'
        latestarchivepath = 'latestarchive'
        snapshotprefix = '/titan/cancerregulome11/TCGA/repositories/dcc-mirror/public'
        dccprefixlen = len('https://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous')
        googlebucket = 'dcc_repository'
        googlefolderprefix = '2016_06_07/public'
        googlelinkprefix = 'https://console.cloud.google.com/m/cloudstorage/b/dcc_repository/o/2016_06_07/public'
        
        count = 0
        with open(latestarchive07jun16path) as latestarchive07jun16, open(latestarchivepath, 'w') as latestarchive:
            # copy the header
            latestarchive.write(latestarchive07jun16.readline())
            for line in latestarchive07jun16.readline():
                fields = line.strip().split('\t')
                # translate the location in the dcc to the location in our mirror
                pathsuffix = fields[2][dccprefixlen:]
                fields[2] = googlelinkprefix + pathsuffix
                latestarchive.write('\t'.join(fields) + '\n')
                snapshotloc = snapshotprefix + pathsuffix
                uploadpath = 'gs://' + googlebucket + '/' + googlefolderprefix + pathsuffix
                if 0 == count % 100:
                    log.info('\t==================================\n\tgoogle path: %s\n\tgoogle link: %s\n\tsnapshot location: %s\n' % (uploadpath, fields[2], snapshotloc))
                count += 1
                
        log.info('finished create snapshot')
    finally:
        gcs_wrapper.close_connection()
    
if __name__ == '__main__':
    main(sys.argv[1])