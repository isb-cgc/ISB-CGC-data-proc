'''
Created on Mar 26, 2015

helper module for common routines

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
# import boto
import base64
from copy import deepcopy
from cStringIO import StringIO
import json
import logging
from multiprocessing import Lock
import os
import requests
import tarfile
import threading
import time
import traceback
import urllib2

gcs_wrapper = None 

backoff = 0
lock = Lock()
# used to limit the amount of simultaneous downloads from the DCC to their limit for a site
semaphore = threading.Semaphore(20)

def log_info(log, msg):
    if log:
        log.info(msg)
    else:
        print msg

def log_exception(log, msg):
    if log:
        log.exception(msg)
    else:
        print msg
        traceback.print_exc()

def getURLData(url, name, log):
    # see preston's archive_parse script for alternative way to fetch
    try:
        log_info(log, '\tstart get %s' % (name))
        response = requests.get(url)
        log_info(log, '\tfinish get %s' % (name))
    
        log_info(log, '\tstart read %s' % (name))
        data = response.text
        log_info(log, '\tfinish read %s' % (name))
        if 200 > len(data):
            # unusually small so log more info
            log_info(log, '\tdata is smaller than expected: \'%s\'\n\t\tstatus code: %s\n\t\theaders: {\n\t\t\t%s\n\t\t}' % (data, response.status_code, '\n\t\t\t'.join([key + ': ' + value for key, value in response.headers.iteritems()])))
    except Exception as e:
        log_exception(log, 'problem fetching %s' % (url))
        raise e
    
    return data

def post_run_file(path, file_name, contents):
    if not os.path.isdir(path):
        os.makedirs(path)
    with open(path + file_name, 'w') as outfile:
        outfile.write(contents)

def upload_run_files(config, path, log):
    if path.endswith('/'):
        path = path[:-1]
    log.info('start upload of run files')
    bucket_name = config['buckets']['controlled']
    if config['upload_open']:
        bucket_name = config['buckets']['open']
    for (dirpath, _, filenames) in os.walk(path):
        for filename in filenames:
            if config['upload_etl_files'] or config['upload_files']:
                filepath = '%s/%s' % (dirpath, filename)
                upload_file(config, filepath, bucket_name, config['base_run_upload_folder'] + filepath, log)
            else:
                log.info('\t\tfolder: %s path: %s/%s' % (config['base_run_upload_folder'], dirpath, filename))
    log.info('finished upload of run files')


def upload_file(config, file_path, bucket_name, key_name, log):
    global gcs_wrapper
    if None == gcs_wrapper:
        gcs_wrapper = import_module(config['gcs_wrapper'])
    gcs_wrapper.upload_file(file_path, bucket_name, key_name, log)

def upload_etl_file(config, key_name, barcode2field2value, log, type_bio, remove_keys=[]):
    log.info('\tstart upload_etl_file(%s)' % (key_name))
    output_file = StringIO()
    
    for field2value in barcode2field2value.itervalues():
        for remove_key in remove_keys:
            if remove_key[0] in field2value and field2value[remove_key[0]] in remove_key[1]:
                log.warning("\t\tWARNING: %s samples should be excluded. Skipped sample: %s" % (remove_key[0], field2value['SampleBarcode']))
                continue
        output_file.write(json.dumps(field2value) + "\n")
    tmp_dir_parent = os.environ.get('ISB_TMP', '/tmp/')
    path = os.path.join(tmp_dir_parent, type_bio + '/')
    if not os.path.isdir(path):
        os.makedirs(path)
    file_path = path + type_bio + '.json'
    with open(file_path, 'w') as bio_file:
        bio_file.write(output_file.getvalue())
        output_file.close()
    bucket_name = config['buckets']['open']
    if config['upload_etl_files']:
        upload_file(config, file_path, bucket_name, key_name, log)
        log.info('\tuploaded etl file')
    else:
        log.info('\tnot uploading etl file')
    log.info('\tfinish upload_etl_file')
    
def is_upload_archive(archive_name, upload_archives, archive2metadata):
    center = archive2metadata[archive_name]['DataCenterName']
    platform = archive2metadata[archive_name]['Platform']
    upload = False
    for level in ['Level_1', 'Level_2', 'Level_3']:
        if level in archive_name and center in upload_archives[level] and platform in upload_archives[level][center]:
            upload = True
            break
    return upload

def setup_archive(archive_fields, log, user = None, password = None):
    tmp_dir_parent = os.environ.get('ISB_TMP', '/tmp/')
    archive_path = os.path.join(tmp_dir_parent, archive_fields[0] + '/')
    if not os.path.isdir(archive_path):
        os.makedirs(archive_path)
    tries = 1
    with semaphore:
        while True:
            try:
                log.info('\t\tstart download of %s' % (archive_fields[0]))
                _download_file(archive_fields[2], archive_path + archive_fields[0] + '.tar.gz', log, user, password)
                log.info('\t\tfinished download of %s' % (archive_fields[0]))
                break
            except (requests.exceptions.ConnectionError, urllib2.URLError) as ce:
                if 4 > tries:
                    log.warning('WARNING: retrying %s.  backoff at %s' % (archive_fields[0], backoff))
                    tries += 1
                else:
                    log.exception('ERROR: failed downloading %s.  backoff at %s' % (archive_fields[0], backoff))
                    raise ce
    try:
        with tarfile.open(archive_path + archive_fields[0] + '.tar.gz') as tf:
            log.info('\t\textract files from %s' % (archive_fields[0]))
            tf.extractall(path = archive_path[:archive_path.rindex('/', 0, -1)])
            log.info('\t\tdone extract files from %s' % (archive_fields[0]))
    except Exception as e:
        log.exception('problem extracting from %s' % archive_path + archive_fields[0] + '.tar.gz')
        raise e
    return archive_path

def increase_backoff():
    with lock:
        global backoff
        if backoff == 0:
            backoff = .005
        else:
            backoff = min(1, backoff * 1.15)

def decrease_backoff():
    with lock:
        global backoff
        backoff *= .9
        if backoff < .006:
            backoff = 0

def _download_file(url, file_path, log, user, password):
    try:
        time.sleep(backoff)

        request = urllib2.Request(url)
        if password:
            base64string = base64.encodestring('%s:%s' % (user, password)).replace('\n', '')
            request.add_header ("Authorization", "Basic %s" % base64string)
        f = urllib2.urlopen(request)
        chunk_size = 512 * 1024
        with open(file_path, 'wb') as out:
            while True:
                chunk = f.read(chunk_size)
                if not chunk: 
                    break
                out.write(chunk)
                out.flush()

#         if password:
#             r = requests.get(url, stream=True, auth=requests.auth.HTTPBasicAuth(user, password))
#         else:
#             r = requests.get(url, stream=True)
#         chunk_size = 512 * 1024
#         with open(file_path, 'wb') as out:
#             for chunk in r.iter_content(chunk_size):
#                 if chunk: # filter out keep-alive new chunks
#                     out.write(chunk)
#                     out.flush()
    except (requests.exceptions.ConnectionError, urllib2.URLError) as ce:
        increase_backoff()
        raise ce
    except Exception as e:
        log.exception('problem downloading %s', url)
        raise e
    decrease_backoff()

# def upload_files(archive_path, key_path):
#     
def create_log(log_dir, log_name):
    try:
        # creates the logs for the run
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)
        logger = logging.getLogger(log_name)
        logger.setLevel(logging.DEBUG)
        log = logging.FileHandler(log_dir + log_name + '.txt', 'w')
        log.setLevel(logging.DEBUG)
        slog = logging.StreamHandler()
        slog.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
        log.setFormatter(formatter)
        slog.setFormatter(formatter)
        logger.addHandler(log)
        logger.addHandler(slog)
        return log_name
    except:
        print 'ERROR: couldn\'t create log %s in %s' % (log_name, log_dir)

def merge_metadata(master_metadata, current_metadata, platform, log):
    count_exist = 0
    count_new = 0
    common = set()
    for aliquot in current_metadata:
        if aliquot in master_metadata:
            count_exist += 1
            # check whether filenames overlap and, if so, do values match
            master_files = set(master_metadata[aliquot].keys())
            cur_files = set(current_metadata[aliquot].keys())
            cur_common = (master_files & cur_files)
            for filename in cur_common:
                if master_metadata[aliquot][filename] != current_metadata[aliquot][filename]:
                    if ('bam' in filename or 'tar.gz' in filename) and master_metadata[aliquot][filename].get('DataCGHubID', 1) == current_metadata[aliquot][filename].get('DataCGHubID', 2):
                        continue
                    # there can be two SRDF files associated with DNASeq data, one for controlled access and one for open access files.  unfortunately, the
                    # controlled access SDRF shows all the files in the controlled access archive and the open access SDRF shows all the same files in the open
                    # access file.  based on the file extension, either the controlled access or open access related metadata is assigned
                    if filename.endswith('somatic.maf'):
                        if '_Cont_' not in current_metadata[aliquot][filename]['DataArchiveName']:
                            master_metadata[aliquot][filename] = current_metadata[aliquot][filename]
                        continue
                    elif (filename.endswith('vcf') or filename.endswith('vcf.gz') or filename.endswith('protected.maf')):
                        if '_Cont_' in current_metadata[aliquot][filename]['DataArchiveName']:
                            master_metadata[aliquot][filename] = current_metadata[aliquot][filename]
                        continue
                    common.add(filename)
            master_metadata[aliquot].update(current_metadata[aliquot])
        else:
            count_new += 1
            master_metadata[aliquot] = current_metadata[aliquot]
    if 0 < len(common):
        log.warning('\tin merge_metadata, for %s, these files are already present:\n\t\t%s' % (platform, ','.join(common)) )
    total_files = set()
    try:
        for files in master_metadata.itervalues():
            total_files |= set(files.keys())
    except Exception as e:
        log.exception('problem merging SDRF info')
        raise e
    log.info('\t\tmerge_metadata(%s): found %s matching aliquots and %s additional aliquots.  total of %s files' % (platform, count_exist, count_new, len(total_files)))
    
def import_module(metadata_module):
    mod = __import__(metadata_module)
    return mod

def getTumorTypes(config, log):
    processAll = True
    process = None
    if config:
        if 'tumor_types' in config:
            process = config['tumor_types']
            if 0 < len(process) and not (1 == len(process) and 'all' == process[0]):
                processAll = False
    if processAll:
        log.info('\tprocessing all tumor types')
    else:
        log.info('\tprocessing %s tumor types' % (', '.join(process)))
    return processAll, process

def print_list_synopsis(fulllist, label, log, count = 2):
    if (count * 2) + 1 > len(fulllist):
        log.info("%s\n%s" % (label, json.dumps (fulllist, indent=4)))
    else:
        log.info("%s\n%s\n\t...\n%s\n\n" % (label, json.dumps (fulllist[:count], indent=4), json.dumps (fulllist[-count:], indent=4)))

def __recurse_filter_map(retmap, origmap, thefilter):
    if 'value' in thefilter:
        for value in thefilter['value']:
            newlabel = thefilter['value'][value]
            retmap[newlabel] = origmap[value]
    
    if 'list' in thefilter:
        for value in thefilter['list']:
            newlabel = thefilter['list'][value]
            retmap[newlabel] = deepcopy(origmap[value])
    
    if 'map' in thefilter:
        for value in thefilter['map']:
            newlabel = thefilter['map'][value][value]
            retmap[newlabel] = __recurse_filter_map({}, origmap[value], thefilter['map'][value])
    
    if 'map_list' in thefilter:
        for value in thefilter['map_list']:
            newfilter = thefilter['map_list'][value]
            newlabel = newfilter[value]
            retmap[newlabel] = []
            for nextmap in origmap[value]:
                retmap[newlabel] += [__recurse_filter_map({}, nextmap, newfilter)]

    return retmap

def filter_map(origmap, thefilter):
    return __recurse_filter_map({}, origmap, thefilter)
