'''
Created on May 27, 2015
a wrapper to google cloud storage using boto.

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
import boto.gs.connection
import boto.gs.key
from multiprocessing import Lock
import time

import requests

# value to delay resubmitting
backoff = 0
connection = None
name2bucket = {}
lock = Lock()

def open_connection(config, log):
    global connection
    if connection:
        raise ValueError('the connection to GCS is already open')
    connection = boto.gs.connection.GSConnection()

def close_connection():
    global connection
    if connection:
        connection.close()
        connection = None
    
def __get_bucket(bucket_name):
    if bucket_name in name2bucket:
        bucket = name2bucket[bucket_name]
    else:
        with lock:
            if bucket_name in name2bucket:
                bucket = name2bucket[bucket_name]
            else:
                bucket = connection.get_bucket(bucket_name)
                name2bucket[bucket_name] = bucket
    return bucket

def upload_file(file_path, bucket_name, key_name, log):
    global backoff
    for attempt in range(1, 4):
        try:
            __attempt_upload(file_path, bucket_name, key_name, log)
            backoff *= .9
            if backoff < .006:
                backoff = 0
#             log.info('\tcompleted upload %s' % (key_name))
            return
        except requests.exceptions.ConnectionError:
            with lock:
                # request failed, trigger backoff
                if backoff == 0:
                    backoff = .005
                else:
                    backoff = min(1, backoff * 1.15)
            log.warning('\tattempt %s had connection error.  backoff at: %s' % (attempt, backoff))
        except Exception as e:
            log.warning('\tproblem uploading %s due to %s' % (key_name, e))
            time.sleep(1)
    log.error('\tfailed to upload %s due to multiple errors' % (key_name))
        
def __attempt_upload(file_path, bucket_name, key_name, log):
    time.sleep(backoff)
    bucket = __get_bucket(bucket_name)
    if key_name in bucket:
        raise ValueError('found %s already uploaded in %s' % (key_name, bucket_name))
    key = boto.gs.key.Key(bucket, key_name)
    try:
        if (file_path.endswith('txt')):
            # make sure can be loaded as ascii!!!
            with open(file_path) as infile:
                contents = infile.read()
                key.set_contents_from_string(str(contents.encode('ascii','replace')))
        else:
            key.set_contents_from_filename(file_path)
    finally:
        key.close()

def download_file(file_path, bucket_name, key_name, log):
    global backoff
    if key_name.startswith('/'):
        key_name = key_name[1:]
    for attempt in range(1, 4):
        try:
            __attempt_download(file_path, bucket_name, key_name, log)
            backoff *= .9
            if backoff < .006:
                backoff = 0
#             log.info('\tcompleted upload %s' % (key_name))
            return
        except requests.exceptions.ConnectionError:
            with lock:
                # request failed, trigger backoff
                if backoff == 0:
                    backoff = .005
                else:
                    backoff = min(1, backoff * 1.15)
            log.warning('\tattempt %s had connection error.  backoff at: %s' % (attempt, backoff))
        except Exception as e:
            log.warning('\tproblem downloading %s due to %s' % (key_name, e))
    log.error('\tfailed to upload %s due to multiple connection errors' % (key_name))

def __attempt_download(file_path, bucket_name, key_name, log):
    time.sleep(backoff)
    bucket = __get_bucket(bucket_name)
    if key_name not in bucket:
        raise ValueError('did not find %s in %s' % (key_name, bucket_name))
    key = boto.gs.key.Key(bucket, key_name)
    try:
        key.get_contents_to_filename(file_path)
    finally:
        key.close()

    log.info('successfully downloaded %s' % key_name)

def delete_object(bucket_name, key_name, log):
    bucket = __get_bucket(bucket_name)
    if key_name not in bucket:
        raise ValueError('did not find %s in %s' % (key_name, bucket_name))
    key = boto.gs.key.Key(bucket, key_name)
    try:
        key.delete()
    finally:
        key.close()

    log.info('successfully deleted %s' % key_name)

def get_bucket_contents(bucket_name, prefix):
    bucket = __get_bucket(bucket_name)
    return bucket.list(prefix)
