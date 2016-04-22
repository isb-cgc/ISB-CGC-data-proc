'''
Created on Mar 28, 2015

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
import os
import shutil

import util

def get_bucket_key_prefix(config, metadata):
    # note that in an archive, all the files will share the same values for these fields
    open_access = config['access_tags']['open']
    if open_access == metadata['SecurityProtocol']:
        bucket_name = config['buckets']['open']
    else:
        bucket_name = config['buckets']['controlled']
    key = '/%s/%s/%s/%s/' % (metadata['Project'].lower(), metadata['Study'].lower(), metadata['Platform'], metadata['Pipeline'])
    return bucket_name, key

def upload_files(config, archive_path, file2metadata, log):
    # TODO: for the DatafileNameKey, use the value already in the metadata
    files = os.listdir(archive_path)
    if 0 < len(files):
        bucket_name, key_prefix = get_bucket_key_prefix(config, file2metadata[files[0]])
        for file_name in files:
            metadata = file2metadata[file_name]
            key_name = key_prefix + metadata['DataLevel'].replace(' ', '_') + '/'+ file_name
            metadata['DatafileNameKey'] = key_name
            if config['upload_files']:
                util.upload_file(config, archive_path + file_name, bucket_name, key_name, log)
    else:
        log.warning('\tno files for %s' % (archive_path))
 
def upload_file(filename, metadata, nonupload_files, ffpe_samples, level, log):
    upload = 'true'
    if level != metadata['DataLevel'] or '20' == metadata['AliquotBarcode'][13:15]:
        upload = 'false'
    elif 'AnnotationCategory' in metadata and metadata['AnnotationCategory']:
        log.info('\t\tskipping file %s: \'%s\'' % (filename, metadata['AnnotationCategory']))
        upload = 'false'
    elif metadata['SampleBarcode'] in ffpe_samples:
        log.info('\t\tskipping ffpe file %s' % (filename))
        upload = 'false'
    elif 'yes' != metadata['IncludeForAnalysis']:
        log.info('\t\tskipping not included file %s' % (filename))
        upload = 'false'
    else:
        for nonupload_file in nonupload_files:
            if ('*' == nonupload_file[0] and nonupload_file[1:-1] in filename) or filename.endswith(nonupload_file):
                log.info('\t\tskipping \'%s\' file %s' % (nonupload_file, filename))
                upload = 'false'
                break
    metadata['DatafileUploaded'] = upload
    return True if 'true' == upload else False

def process_files(config, archive_path, sdrf_metadata, seen_files, nonupload_files, ffpe_samples, level, log):
    # TODO: set DatafileNameKey and DatafileUploaded here
    files = os.listdir(archive_path)
    metadatafiles = set(sdrf_metadata.values().keys())
    archiveonly = files - metadatafiles
    metaonly = metadatafiles - files
    if 0 < len(archiveonly):
        log.warning('files only in the archive, not sdrf: %s' % (','.join(archiveonly)))
    if 0 < len(metaonly):
        log.warning('files only in the sdrf, not archive: %s' % (','.join(metaonly)))
    
    file2metadata = {}
    filenames = set()
    for filename2metadata in sdrf_metadata.itervalues():
        try:
            for filename, metadata in filename2metadata.iteritems():
                file2metadata[filename] = metadata
                if upload_file(filename, metadata, nonupload_files, ffpe_samples, level, log):
                    filenames.add(filename)
        except:
            log.exception('problem looking up %s in metadata' % filename)
    
    # setup the directory so only files to be uploaded remain in the directory
    for file_name in files:
        if file_name not in filenames or file_name in seen_files:
            if file_name in seen_files:
                log.warning('\t\tfound repeated file %s' % (file_name))
            os.remove(archive_path + file_name)
        else:
            seen_files.add(file_name)
            log.info('\t\tuploading %s' % (file_name))
    return file2metadata

def upload_archive(config, sdrf_metadata, archive2metadata, ffpe_samples, archive_fields, upload_archives, seen_files, nonupload_files, access, log):
    archive_path = None
    if config['download_archives'] and util.is_upload_archive(archive_fields[0], upload_archives, archive2metadata):
        log.info('\tuploading %s-access archive %s.' % (access, archive_fields[0]))
        try:
            level = archive_fields[0].split('.')[-4].replace('_', ' ')
            user_info = config['user_info']
            archive_path = util.setup_archive(archive_fields, log, user_info['user'], user_info['password'])
            file2metadata = process_files(config, archive_path, sdrf_metadata, seen_files, nonupload_files, ffpe_samples, level, log)
            if 0 < len(file2metadata):
                upload_files(config, archive_path, file2metadata, log)
            else:
                log.warning('did not find files to load for %s' % (archive_fields[0]))
        finally:
            if archive_path:
                shutil.rmtree(archive_path)
        log.info('\tfinished uploading %s-access archive %s' % (access, archive_fields[0]))
    else:
        log.info('\tskipping %s-access archive %s' % (access, archive_fields[0]))

def upload_archives(config, log, archives, sdrf_metadata, archive2metadata, ffpe_samples):
    log.info('start upload archives')
    upload_archives = config['upload_archives']
    nonupload_files = config['nonupload_files']
    archives.sort(key=lambda archive_fields: archive2metadata[archive_fields[0]]['DataArchiveVersion'], reverse=True)
    seen_files = set()
    for archive_fields in archives:
        if 'tcga4yeo' in archive_fields[2] and config['upload_controlled']:
            upload_archive(config, sdrf_metadata, archive2metadata, ffpe_samples, archive_fields, upload_archives, seen_files, nonupload_files, 'controlled', log)
        if 'tcga4yeo' not in archive_fields[2] and config['upload_open']:
            upload_archive(config, sdrf_metadata, archive2metadata, ffpe_samples, archive_fields, upload_archives, seen_files, nonupload_files, 'open', log)

    log.info('finished upload archives')
