'''
Created on May 27, 2015
a wrapper to google cloud storage.

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
from datatime
from multiprocessing import Lock
import time
import requests

from gcloud import storage
# value to delay resubmitting
backoff = 0
name2bucket = {}
lock = Lock()
storage_service = None
def open_connection(config = None, log = None):
    global storage_service
    if storage_service:
        raise ValueError('storage has already been initialized')
    
    storage_service = storage.Client(project = config['cloud_projects']['open'])
    
def close_connection():
    pass
    
def __get_bucket(bucket_name):
    if bucket_name in name2bucket:
        bucket = name2bucket[bucket_name]
    else:
        with lock:
            if bucket_name in name2bucket:
                bucket = name2bucket[bucket_name]
            else:
                bucket = storage_service.get_bucket(bucket_name)
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
            log.exception('\tproblem uploading %s' % (key_name))
            raise e
    log.error('\tfailed to upload %s' % (key_name))
    raise ValueError('\tcould not load %s' % (key_name))
        

def __attempt_upload(file_path, bucket_name, key_name, log):
    time.sleep(backoff)
    bucket = __get_bucket(bucket_name)
    if bucket.get_blob(key_name):
        raise ValueError('found %s in %s' % (key_name, bucket_name))
    blob = bucket.blob(key_name)
    blob.upload_from_filename(file_path)
    log.info('successfully uploaded %s' % key_name)

def get_bucket_contents(bucket_name, log):
    bucket = __get_bucket(bucket_name)
    iter = bucket.list_blobs()
    count = 0
    for fileinfo in iter:
        if 0 == count % 1028:
            log.info(count, fileinfo.name)
        count += 1
        yield fileinfo
