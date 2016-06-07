#!/usr/bin/env python

# Copyright 2015, Institute for Systems Biology.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
import re
import json
from gcloud import storage
from collections import defaultdict
import pandas as pd
import numpy as np
from StringIO import StringIO
from os.path import basename
import sys
import time
import logging
import tempfile
import chardet
import traceback

log = logging.getLogger(__name__)

class GcsConnector(object):
    """Google Cloud Storage Connector
    """
    def __init__(self, project, bucket_name, tempdir='/tmp'):
        # connect to the cloud bucket
        self.client = storage.Client(project)
        retry = 0
        while True:
            try:
                self.bucket = self.client.get_bucket(bucket_name)
                break
            except Exception as e:
                if 3 == retry:
                    print 'could not get bucket %s' % (bucket_name)
                    raise e
                retry += 1
                time.sleep(1)
        self.tempdir = tempdir

    #--------------------------------------
    # uploads a file to the bucket
    # this is just a wrapper around the main gcloud package
    # deprecated : gcloud 7 now has a direct function to create the blob
    # Note this is different from upload_from_file, which accepts a file-like object
    # Check if the blob exits
    #--------------------------------------
    def upload_from_filename(self, local_filename, dest_filename):
        log.info("Uploading file :{0} to bucket".format(local_filename))
        try:
            self.bucket.upload_file(local_filename, dest_filename)
        except Exception as e:
           log.error(e)
           raise
        log.info("Uploaded file :{0} to bucket".format(dest_filename))
        return True

    #-------------------------------------------
    # Download the file to disk or file-like object
    # if rollover is true , it writes to the disk
    #-------------------------------------------
    def download_blob_to_file(self, blobname, rollover=False):
        
        blob = self.bucket.get_blob(blobname)

        if not blob  :
           raise Exception ('No blob found for the key:' + str(blobname))

        # if rollover is true, write to temp file on disk 
        # if false, write to StringIO buffer
        if rollover:
           temp_data_file = self.create_tempfile()
           blob.download_to_filename(temp_data_file.name)
        else:
           log.info("StringIO")
           temp_data_file = StringIO() # string buffer
           blob.download_to_file(temp_data_file)

        temp_data_file.seek(0)

        return temp_data_file 


    #-------------------------------------------
    # create a temp file.
    # this create a Named temp file
    #-------------------------------------------      
    def create_tempfile(self):
         temp_file = tempfile.NamedTemporaryFile(bufsize=0, suffix=str(time.time()), dir = self.tempdir)
         return temp_file

    #------------------------------------------
    # search files in the bucket by pattern
    #------------------------------------------
    def search_files(self, search_patterns, regex_search_pattern, prefixes=["tcga/"]):
        files_info = []
        for prefix in prefixes:
            log.info("Searching for files with search patters - {0}, {1}, and prefix - {2}".format(search_patterns, regex_search_pattern.pattern, prefix))
            for blob in self.bucket.list_blobs(prefix=prefix):
                filename = blob.name
                size = blob.size
                timestamp = blob.updated

                # search pattern
                if not all(x in filename for x in search_patterns):
                   continue
        
                if not regex_search_pattern.match(filename):
                   continue
        
                files_info.append({
                    'filename': filename, 
                    'size': size,
                    'timestamp': timestamp
                    
                })

        df = pd.DataFrame(files_info)
        log.info('Found {0} files in the bucket matching the pattern'.format(len(df.index)))
        return df

    def upload_blob_from_string(self, blobname, df_stringIO, metadata={}):
        # upload the file
        upload_blob = storage.blob.Blob(blobname, bucket=self.bucket)
        tries = 0
        while True:
            try:
                upload_blob.upload_from_string(df_stringIO)
                break
            except Exception as e:
                if 3 == tries:
                    traceback.print_exc(10)
                    raise e
                time.sleep(1)
                tries += 1
                print '\tretry %s for upload of %s' % (tries, blobname)

        # set blob metadata
        if metadata:
            log.info("Setting object metadata")
            upload_blob.metadata = metadata
            upload_blob.patch()

        # check if the uploaded blob exists. Just a sanity check
        if upload_blob.exists():
            log.info("The uploaded file {0} has size {1} bytes.".format(blobname, upload_blob.size))
            return True
        else:
            raise Exception('File upload failed - {0}.'.format(blobname))

  
    #----------------------------------------
    # Convert a dataframe into newline-delimited JSON string
    #  -- should work for a small to medium files
    # if rollover_file is true
    # works only in a single bucket
    # set the object metadata
    #----------------------------------------
    def convert_df_to_njson_and_upload(self, df, destination_blobname, metadata={}):

        log.info("Converting dataframe into a new-line delimited JSON file to save as %s" % (destination_blobname))

        log.info('\tstart conversion of %s' % (destination_blobname))

        dfjson = json.loads(df.to_json(orient='records'))
        file_to_upload = StringIO()
        modcount = len(df) / 20
        count = 0

        for rec in dfjson:
            if 0 == count % modcount:
                log.info('\t\tconverted %s rows' % (count))
            count += 1
            file_to_upload.write(rec + "\n")
        
#         for _, rec in df.iterrows():
#             if 0 == count % modcount:
#                 log.info('\t\tconverted %s rows' % (count))
#             count += 1
#             file_to_upload.write(rec.convert_objects(convert_numeric=False).to_json() + "\n")
        file_to_upload.seek(0)
        log.info('\tcompleted conversion.  converted %s total rows for %s' % (count, destination_blobname))

        upload_blob = storage.blob.Blob(destination_blobname, bucket=self.bucket)
        retry = 0
        log.info('\tstart upload of %s' % (destination_blobname))
        while True:
            try:
                upload_blob.upload_from_string(file_to_upload.getvalue())
                break
            except Exception as e:
                if 3 == retry:
                    log.exception('problem with upload to %s, no more retries' % (destination_blobname))
                    raise e
                retry += 1
                log.exception('problem with upload to %s, retry %s' % (destination_blobname, retry))
        log.info('\tfinished upload of %s' % (destination_blobname))

        # set blob metadata
        if metadata:
            log.info("Setting object metadata")
            upload_blob.metadata = metadata
            upload_blob.patch()
        file_to_upload.close()

        # check if the uploaded blob exists. Just a sanity check
        if upload_blob.exists():
            log.info("The uploaded file {0} has size {1} bytes.".format(destination_blobname, upload_blob.size))
            return True
        else:
            raise Exception('File upload failed - {0}.'.format(destination_blobname)) 
     
    def check_blob_exists(self, blob):
        """
        Checks if a blob exists
        Returns True or False
        """
        return self.bucket.get_blob(blob).exists()

    def delete_blob(self, blob):
        return self.bucket.get_blob(blob).delete()
