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
import gzip
import os
import re
import shutil

import upload_archives
import util

tumor_barcode_pattern = re.compile("^TCGA-[A-Z0-9]{2}-[A-Z0-9]{4}-0[1-9][A-Z]-[0-9]{2}[A-Z]-[A-Z0-9]{4}-[A-Z0-9]{2}")
normal_barcode_pattern = re.compile("^TCGA-[A-Z0-9]{2}-[A-Z0-9]{4}-1[0-9][A-Z]-[0-9]{2}[A-Z]-[A-Z0-9]{4}-[A-Z0-9]{2}")

def init_metadata(file_name, archive2metadata, archive_fields, study, version):
    '''
    
    
    parameters:
        
    
    returns:
        
    '''
    field2value = dict(archive2metadata[archive_fields[0]], **
        {'DatafileName':file_name, 
        'DataArchiveName':archive_fields[0], 
        'DataArchiveVersion':version, 
        'Study':study, 
        'DataLevel':'Level 2', 
        'DatafileUploaded':'true', 
        'Datatype':'Mutations', 
        'IncludeForAnalysis':'yes', 
        'Species':'Homo sapiens'})
    return field2value

def parse_maf_file(file_name, archive_path, log, archive_fields, archive2metadata, sdrf_metadata):
    pieces = archive_fields[0].split('_')
    study = pieces[1][:pieces[1].index('.')]
    version = pieces[-1][pieces[-1].index('.') + 1:]
    field2value = init_metadata(file_name, archive2metadata, archive_fields, study, version)
    with open(archive_path + file_name, 'rb') as maf_file:
        aliquot_column = None
        aliquots = set()
        for line in maf_file:
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
            aliquots.add(aliquot)
            copyfield2value = dict(field2value)
            copyfield2value['AliquotBarcode'] = aliquot
            copyfield2value['SampleBarcode'] = aliquot[:16]
            copyfield2value['ParticpantBarcode'] = aliquot[:12]
            files = sdrf_metadata.setdefault(aliquot, {})
            if file_name not in files:
                files[file_name] = copyfield2value
    log.info('\tset metadata for %s.  found %s aliquots' % (file_name, len(aliquots)))
    return field2value

def parse_vcf_contents(vcf_file, file_name, field2value, sdrf_metadata, log):
    tumor_barcode = None
    normal_barcode = None
    for line in vcf_file:
        if not line.startswith('##'):
            raise ValueError('didn\'t find barcodes for %s' % file_name)

        if line.startswith('##SAMPLE'):
            try:
                # the sample line can have odd entries with nested key=<prefix,suffix> that, between the brackets can represent a list
                # or even a nested map.  since those keys aren't needed, special case to set them as the blank key value
                sample_info = dict([(pair.split('=') if '=' in pair and not pair.startswith('softwareParam') else ['blank', 'blank']) for 
                        pair in line[10:].split(',')]) 
            except Exception as e:
                log.exception("bad vcf sample line(%s): %s" % (e, line))
                raise e
            # different centers set the ID value different
            if sample_info['ID'] in ('NORMAL', 'DNA_NORMAL', 'RNA_NORMAL') or normal_barcode_pattern.match(sample_info['ID']):
                normal_barcode = sample_info['SampleTCGABarcode']
            elif sample_info['ID'] in ('PRIMARY', 'TUMOR', 'DNA_TUMOR') or tumor_barcode_pattern.match(sample_info['ID']):
                tumor_barcode = sample_info['SampleTCGABarcode']
            else:
                raise ValueError('unknown sample type, %s, for %s' % (sample_info['ID'], file_name))
        
        if tumor_barcode and normal_barcode:
            break
    
    for aliquot in tumor_barcode, normal_barcode:
        copyfield2value = dict(field2value)
        copyfield2value['AliquotBarcode'] = aliquot
        copyfield2value['SampleBarcode'] = aliquot[:16]
        copyfield2value['ParticpantBarcode'] = aliquot[:12]
        files = sdrf_metadata.setdefault(aliquot, {})
        if file_name not in files:
            files[file_name] = copyfield2value
        log.info('\tset metadata for %s' % (file_name))

def parse_vcf_file(file_name, archive_path, log, archive_fields, archive2metadata, sdrf_metadata):
    pieces = archive_fields[0].split('_')
    study = pieces[1][:pieces[1].index('.')]
    version = pieces[-1][pieces[-1].index('.') + 1:]
    field2value = init_metadata(file_name, archive2metadata, archive_fields, study, version)
    if file_name.endswith('vcf'):
        with open(archive_path + file_name, 'rb') as vcf_file:
            parse_vcf_contents(vcf_file, file_name, field2value, sdrf_metadata, log)
    elif file_name.endswith('vcf.gz'):
        with gzip.open(archive_path + file_name, 'rb') as vcf_file:
            parse_vcf_contents(vcf_file, file_name, field2value, sdrf_metadata, log)
    else:
        log.warning('unexpected file exte4nsion in parse vcf: %s' % (file_name))
    return field2value
            
def process_files(archive_path, maf_upload_files, log):
    files = os.listdir(archive_path)
    filenames = set()
    for nextfile in files:
        try:
            for maf_ext in maf_upload_files:
                if nextfile.endswith(maf_ext):
                    filenames.add(nextfile)
                    break
        except Exception as e:
            log.exception('problem processing %s for maf files' % archive_path)
            raise e
    
    # setup the directory so only files to be uploaded remain in the directory
    for file_name in files:
        if file_name not in filenames:
            os.remove(archive_path + file_name)
            log.info('\t\tskipping %s' % (file_name))
        else:
            log.info('\t\tuploading maf-related file %s' % (file_name))
    return filenames

def upload_archive(config, log, archive_fields, archive2metadata, sdrf_metadata, access):
    user_info = config['user_info']
    log.info('\tchecking %s-access maf archive %s.' % (access, archive_fields[0]))

    archive_path = None
    try:
        if config['download_archives']:
            archive_path = util.setup_archive(archive_fields, log, user_info['user'], user_info['password'])
            maf_upload_files = config['maf_upload_files']
            filenames = process_files(archive_path, maf_upload_files, log)
            if 0 < len(filenames):
                file2metadata = {}
                for file_name in filenames:
                    if file_name.endswith('maf'):
                        file2metadata[file_name] = parse_maf_file(file_name, archive_path, log, archive_fields, archive2metadata, sdrf_metadata)
                    elif file_name.endswith('vcf') or file_name.endswith('vcf.gz'):
                        file2metadata[file_name] = parse_vcf_file(file_name, archive_path, log, archive_fields, archive2metadata, sdrf_metadata)
                upload_archives.upload_files(config, archive_path, file2metadata, log)
            else:
                log.warning('\tdid not find files to load for %s' % (archive_fields[0]))
        else:
            log.info('\tskipping %s-access archive %s' % (access, archive_fields[0]))
    finally:
        if archive_path:
            shutil.rmtree(archive_path)

def process_maf_files(config, maf_archives, sdrf_metadata, archive2metadata, log):
    log.info('start process potential maf archives')
    
    for archive_fields in maf_archives:
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
    
    log.info('finished process potential maf archives')
    return sdrf_metadata
