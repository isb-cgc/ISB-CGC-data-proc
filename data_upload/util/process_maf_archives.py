'''
Created on Jul 4, 2015

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

import upload_archives
import util

def parse_maf_file(file_name, archive_path, log, archive_fields, archive2metadata, sdrf_metadata):
    pieces = archive_fields[0].split('_')
    study = pieces[1][:pieces[1].index('.')]
    version = pieces[-1][pieces[-1].index('.') + 1:]
    field2value = dict(archive2metadata[archive_fields[0]], 
        **{'DatafileName': file_name, 
        'DataArchiveName': archive_fields[0],
        'DataArchiveVersion': version,
        'Study': study,
        'DataLevel': 'Level 2',
        'DatafileUploaded': 'true',
        'Datatype': 'Mutations',
        'IncludeForAnalysis': 'yes',
        'Species': 'Homo sapiens'})
    with open(archive_path + file_name, 'rb') as maf_file:
        aliquot_column = None
        count = 0
        for line in maf_file:
            count += 1
            if line.startswith('#'):
                continue
            fields = line.split('\t')
            if not aliquot_column:
                count = 0
                for field in fields:
                    if 'Tumor_Sample_Barcode' == field:
                        aliquot_column = count
                        break
                    count += 1
                continue
            aliquot = fields[aliquot_column]
            copyfield2value = dict(field2value)
            copyfield2value['AliquotBarcode'] = aliquot
            copyfield2value['SampleBarcode'] = aliquot[:16]
            copyfield2value['ParticpantBarcode'] = aliquot[:12]
            files = sdrf_metadata.setdefault(aliquot, {})
            if file_name not in files:
                files[file_name] = copyfield2value
    log.info('\tfound %s lines for %s' % (count, file_name))
    return field2value
            
def process_files(archive_path, log):
    files = os.listdir(archive_path)
    filenames = set()
    for nextfile in files:
        try:
            # TODO: put this in config file
            if nextfile.endswith('maf') or nextfile.endswith('vcf') or nextfile.endswith('vcf.gz'):
                filenames.add(nextfile)
        except Exception as e:
            log.exception('problem processing %s for maf files' % archive_path)
            raise e
    
    # setup the directory so only files to be uploaded remain in the directory
    for file_name in files:
        if file_name not in filenames:
            os.remove(archive_path + file_name)
            log.info('\t\tskipping %s' % (file_name))
        else:
            log.info('\t\tuploading maf file %s' % (file_name))
    return filenames

def upload_archive(config, log, archive_fields, archive2metadata, sdrf_metadata, access):
    user_info = config['user_info']
    log.info('\tchecking %s-access maf archive %s.' % (access, archive_fields[0]))

    try:
        if config['download_archives']:
            archive_path = util.setup_archive(archive_fields, log, user_info['user'], user_info['password'])
            filenames = process_files(archive_path, log)
            if 0 < len(filenames):
                file2metadata = {}
                for file_name in filenames:
                    if file_name.endswith('maf'):
                        file2metadata[file_name] = parse_maf_file(file_name, archive_path, log, archive_fields, archive2metadata, sdrf_metadata)
                upload_archives.upload_files(config, archive_path, file2metadata, log)
            else:
                log.warning('\tdid not find files to load for %s' % (archive_fields[0]))
        else:
            log.info('\tskipping %s-access archive %s' % (access, archive_fields[0]))
    finally:
        shutil.rmtree(archive_path)


def process_maf_archives(config, maf_archives, sdrf_metadata, data_archive_info, archive2metadata, log):
    log.info('start process potential maf archives')
    archives = [archive_fields[0] for archive_fields in data_archive_info]
    for archive_fields in maf_archives:
        if archive_fields[0] not in archives:
            if 'tcga4yeo' in archive_fields[2] and config['upload_controlled']:
                if config['download_archives']:
                    upload_archive(config, log, archive_fields, archive2metadata, sdrf_metadata, 'controlled')
                else:
                    log.info('\tskipping maf controlled-archive %s download' % (archive_fields[0]))
            if 'tcga4yeo' not in archive_fields[2] and config['upload_open']:
                if config['download_archives']:
                    upload_archive(config, log, archive_fields, archive2metadata, sdrf_metadata, 'open')
                else:
                    log.info('\tskipping maf open-archive %s download' % (archive_fields[0]))
        else:
            log.info('\tskipping maf archive %s, already processed' % (archive_fields[0]))
    
    log.info('finished process potential maf archives')
    return sdrf_metadata
