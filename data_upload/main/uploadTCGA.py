'''
Created on Mar 25, 2015

Uploads the DCC files specified by the config file into Google Cloud Storage and
creates the metadata to be saved in mysql and bigquery

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
from concurrent import futures
import copy
from datetime import date
from datetime import datetime
import json
import logging
import sys

from parse_bio import parse_bio
from parse_bio import omf_element2count
from parse_bio import omf_study2element2values2count
from parse_bio import ssf_element2count
from parse_bio import ssf_study2element2values2count
from prepare_upload import prepare_upload
from process_annotations import process_annotations
from process_latestarchive import process_latestarchive
from process_metadata_current import process_metadata_current
from process_sdrf import process_sdrf
from process_cghub import process_cghub
from process_maf_files import process_maf_files
from upload_archives import upload_archives
from util import create_log, import_module, merge_metadata, upload_etl_file, upload_run_files

# using thread pool over process pool because can't nest process pools (it hangs, as documented)
# questions: do thread pools spread across processors?  also, because CPython has a global 
# interpreter lock ("the GIL") that only allows one thread to run at a time, CPU bound will run slower
# although IO bound (which is the case here) does improve.  also, the objects are copied to the new
# thread/process so any changes to that copy won't be in the original object when the future returns,
# so changes of state need to be passed back as results from the future and dealt with that way.
executor = None

def merge_cghup(config, master_metadata, cghub_records, log):
    '''
    merges the cghub metadata from the manfest with the metadata obtained from dcc file
    paths and SDRF values
    
    parameters:
        config: the configuration map
        master_metadata: the dcc based metadata
        cghub_records: the cghub metadata
        log: logger to log any messages
    '''
    log.info('start merge_cghup().  merge %s cghub records' % (len(cghub_records)))
    count_exist = 0
    count_match = 0
    count_new = 0
    dups = set()
    platforms = set()
    for cghub_record in cghub_records:
        aliquot = cghub_record['AliquotBarcode']
        filename = cghub_record['DatafileName']
        cghubfilename2metadata = {filename: cghub_record}
        if aliquot in master_metadata:
            count_exist += 1
            if filename in master_metadata[aliquot]:
                count_match += 1
                # do a sanity check for shared keys
                dcc_metadata = master_metadata[aliquot][filename]
                platforms.add(tuple([dcc_metadata['Platform'], dcc_metadata['Pipeline'], cghub_record['DataCenterCode'], cghub_record['platform_full_name'], cghub_record['analyte_code']]))
                for key in cghub_record.iterkeys():
                    if key in dcc_metadata:
                        if 'Repository' == key:
                            continue
                        elif 'DatafileUploaded' == key:
                            dcc_metadata[key] = cghub_record[key]
                        elif not cghub_record[key]:
                            cghub_record[key] = dcc_metadata[key]
                        elif dcc_metadata[key] and cghub_record[key] != dcc_metadata[key]:
                            if key in ['last_modified', 'analysis_data_uri', 'md5']:
                                dups.add(filename)
                            if 'GenomeReference' == key and cghub_record[key][:6].lower() == dcc_metadata[key][:6].lower() or \
                                ('hg19' == cghub_record[key][:4].lower() and 'hg19' == dcc_metadata[key][:4].lower()):
                                continue
                            if 'Platform' == key:
                                if cghub_record[key] not in dcc_metadata[key]:
                                    log.warning('\tPlatform mismatch for %s(%s): %s (CGHub) vs. %s (DCC)' % (aliquot, filename, cghub_record[key], dcc_metadata[key]))
                                cghub_record[key] = dcc_metadata[key]
                            elif 'Pipeline' == key:
                                cghub_fields = cghub_record[key].split('__')
                                dcc_fields = dcc_metadata[key].split('__')
                                cghub_record[key] = cghub_fields[0] + '__' + dcc_fields[1]
                            elif 'DataCenterName' == key:
                                cghub_record[key] = dcc_metadata[key]
                            elif 'IncludeForAnalysis' == key:
                                log.warning('\tIncludeForAnalysis mismatched values for %s:%s:%s\tcghub: %s\tdcc: %s' % (aliquot, filename, key, cghub_record[key], dcc_metadata[key]))
                            else:
                                log.warning('\tmismatched values for %s:%s:%s\n\t\tcghub:%s\n\t\tdcc:  %s' % (aliquot, filename, key, cghub_record[key], dcc_metadata[key]))
                #now to merge the info
                dcc_metadata.update(cghub_record)
            else:
                # update the cghub metadata with fields from the level 3 equivalent
                # add the file information
                platforms.add(tuple([cghub_record['DataCenterCode'], cghub_record['platform_full_name'], cghub_record['analyte_code']]))
                master_metadata[aliquot].update(cghubfilename2metadata)
        else:
            platforms.add(tuple([cghub_record['DataCenterCode'], cghub_record['platform_full_name'], cghub_record['analyte_code']]))
            count_new += 1
            master_metadata[aliquot] = cghubfilename2metadata
    log.info('\tplatform/pipeline combinations(%s):\n\t\t%s' % (len(platforms), '\n\t\t'.join([','.join(platform) for platform in platforms])))
    log.info("\tfound %s existing aliquots in metadata (of which %s matched sdrf file names) and added %s new for %s CGHub bam files" % (count_exist, count_match, count_new, len(cghub_records)))
    log.info('\t%s cghub files appeared twice w/ different values' % (len(dups)))
    log.info('finished merge_cghup()')

def process_platform(config, log_dir, log_name, tumor_type, platform, archive2metadata, archive_types2archives, barcode2annotations, exclude_samples):
    '''
    
    
    parameters:
        config: the configuration map
        log_dir: the base directory for the logs
        log_name: the name of the log to use to log any messages
        tumor_type: the TCGA study being processed
        platform: the TCGA platform being processsed
        archive2metadata: map of archive name to its metadata
        archive_types2archives: map of archive types ('maf', 'mage-tab', and 'data') for this study to its archives
        barcode2annotations: map of barcodes to TCGA annotations
        exclude_samples: a list of barcodes of ffpe samples
    
    returns:
        sdrf_metadata: the netadata obtained from parsing the SDRF files
    '''
    try:
        create_log(log_dir + tumor_type + '/', log_name)
        log = logging.getLogger(log_name)
        if 'mage-tab' not in archive_types2archives:
            orphan_data_archives = (set([archive_info[0] for archive_info in archive_types2archives['data']]) if 'data' in archive_types2archives else set()) - \
                set((archive_info[0] for archive_info in archive_types2archives['maf']) if 'maf' in archive_types2archives else [])
            if 0 < len(orphan_data_archives):
                log.warning('\tno mage-tab archives for %s and but there are data archives that are not maf: %s' % (platform, orphan_data_archives))
            else:
                log.warning('\tno mage-tab archives for %s' % (platform))

            maf_metadata = {}
            if 'maf' in archive_types2archives:
                return process_maf_files(config, archive_types2archives['maf'], maf_metadata, archive2metadata, log)
            return maf_metadata
        sdrf_metadata = process_sdrf(config, log, archive_types2archives['mage-tab'], archive2metadata, barcode2annotations)
        if 'data' in archive_types2archives:
            upload_archives(config, log, archive_types2archives['data'], sdrf_metadata, archive2metadata, exclude_samples)
        else:
            log.warning('\tno data archives found for %s' % (tumor_type + ':' + platform))
        
        if 'maf' in archive_types2archives:
            process_maf_files(config, archive_types2archives['maf'], sdrf_metadata, archive2metadata, log)
        return sdrf_metadata
    except Exception as e:
        log.exception('%s generated an exception' % (platform))
        raise e

def merge_metadata_current_metadata(sdrf_metadata, barcode2metadata, log):
    '''
    merges the metadata per barcode obtained from metadata.current.txt into the metadata
    obtained from the SDRF file
    
    parameters:
        sdrf_metadata: metadata from the SDRF files
        barcode2metadata: metadata from metadata.current.txt
        log: logger to log any messages
    '''
    for aliquot, filename2fields2values in sdrf_metadata.iteritems():
        try:
            for field2values in filename2fields2values.itervalues():
                field2values.update(barcode2metadata[aliquot])
        except Exception as e:
            if '20' == aliquot[13:15]:
                continue
            log.warning('problem with annotations for %s(%s) for type %s' % (aliquot, e, field2values['Datatype']))
    
def store_metadata(config, log, table, key2metadata):   
    '''
    calls the store_metadata method in the module specified by the configuration file
    
    parameters:
        config: the configuration map
        log: logger to log any messages
        table: the mysql table to save the metadata to
        key2metadata: the metadata to save
    '''
    metadata_modules = config['metadata_modules']
    for metadata_module in metadata_modules:
        module = import_module(metadata_module)
        module.store_metadata(config, log, table, key2metadata)
    

def process_metadata_samples(clinical_metadata, biospecimen_metadata, aliquot2filename2metadata, log):
    '''
    creates the denormalized samples metadata from the other metadata passed in
    
    parameters:
        clinical_metadata:  metadata from the clinical bio files
        biospecimen_metadata: metadata from the biospecimen bio files
        aliquot2filename2metadata:metadata from the file paths and SDRF files
        log: logger to log any messages
    
    returns:
        samples_metadata: the denormalized sample-based metadata
    '''
    samples_metadata = {}
    for samplebarcode, key2value in biospecimen_metadata.items():
        # merge biospecimen_metadata and clinical_metadata
        samples_metadata[samplebarcode] = copy.copy(key2value)

        ParticipantBarcode = key2value['ParticipantBarcode']
        if ParticipantBarcode in clinical_metadata:
            samples_metadata[samplebarcode].update(clinical_metadata[ParticipantBarcode])
    
    for filecollection in aliquot2filename2metadata.itervalues():
        for aliquot_data_dict in filecollection.itervalues():
            if 'Platform' in aliquot_data_dict:
                if 'SampleBarcode' in aliquot_data_dict:
                    sample_barcode = aliquot_data_dict['SampleBarcode']
                else:
                    log.error("didn't find sample barcode for %s" % (aliquot_data_dict['aliquotbarcode']))
                sample_barcode_dict = samples_metadata[sample_barcode] if sample_barcode in samples_metadata else {}
                # DNA sequencing
                if 'DNA' in aliquot_data_dict['Platform'] and ('Illumina' in aliquot_data_dict['Platform'] or 'Mixed' in aliquot_data_dict['Platform']):
                    sample_barcode_dict['has_Illumina_DNASeq'] = unicode(1)
                # RNA sequencing
                if aliquot_data_dict['Pipeline'] == 'bcgsc.ca__RNASeq':
                    if aliquot_data_dict['Platform'] == 'IlluminaHiSeq_RNASeq':
                        sample_barcode_dict['has_BCGSC_HiSeq_RNASeq'] = unicode(1)
                    if aliquot_data_dict['Platform'] == 'IlluminaGA_RNASeq':
                        sample_barcode_dict['has_BCGSC_GA_RNASeq'] = unicode(1)
                if aliquot_data_dict['Pipeline'] == 'unc.edu__RNASeqV2':
                    if aliquot_data_dict['Platform'] == 'IlluminaHiSeq_RNASeqV2':
                        sample_barcode_dict['has_UNC_HiSeq_RNASeq'] = unicode(1)
                    if aliquot_data_dict['Platform'] == 'IlluminaGA_RNASeqV2':
                        sample_barcode_dict['has_UNC_GA_RNASeq'] = unicode(1)
                # microRNA sequencing
                if aliquot_data_dict['Pipeline'] == 'bcgsc.ca__miRNASeq':
                    if aliquot_data_dict['Platform'] == 'IlluminaGA_miRNASeq':
                        sample_barcode_dict['has_GA_miRNASeq'] = unicode(1)
                    if aliquot_data_dict['Platform'] == 'IlluminaHiSeq_miRNASeq':
                        sample_barcode_dict['has_HiSeq_miRnaSeq'] = unicode(1)
                # protein data
                if aliquot_data_dict['Platform'] == 'MDA_RPPA_Core':
                    sample_barcode_dict['has_RPPA'] = unicode(1)
                # CNV data
                if aliquot_data_dict['Platform'] == 'Genome_Wide_SNP_6':
                    sample_barcode_dict['has_SNP6'] = unicode(1)
                # methylation data
                if aliquot_data_dict['Pipeline'] == 'jhu-usc.edu__methylation':
                    if aliquot_data_dict['Platform'] == 'HumanMethylation27':
                        sample_barcode_dict['has_27k'] = unicode(1)
                    if aliquot_data_dict['Platform'] == 'HumanMethylation450':
                        sample_barcode_dict['has_450k'] = unicode(1)
    
    return samples_metadata

def process_tumortype(config, log_dir, tumor_type, platform2archive_types2archives, platform2archive2metadata, cghub_records, barcode2metadata, barcode2annotations):
    '''
    process the study/tumor_type for uploading the files from the dcc to GCS and to obtain meatadata to save to mysql.  loops through the
    platforms in parallel
    
    parameters:
        config: the configuration map
        log_dir: the base directory for the logs
        tumor_type: the TCGA study being processed
        platform2archive_types2archives: map platforms to archive types ('maf', 'mage-tab', and 'data') to archives
        cghub_records: cghub metadata
        platform2archive2metadata: map of platforms to archive name to the archive metadata
        barcode2metadata: metadata from metadata.current.txt
        barcode2annotations: map of barcodes to TCGA annotations
    
    returns:
        clinical_metadata:  metadata from the clinical bio files
        biospecimen_metadata: metadata from the biospecimen bio files
        flattened_data_map: metadata from the file paths and SDRF files
    '''
    print '\t', datetime.now(), 'processing tumor type %s' % (tumor_type)
    log_name = create_log(log_dir + tumor_type + '/', tumor_type)
    log = logging.getLogger(log_name)
    log.info( '\tprocessing tumor type %s' % (tumor_type))
    
    if config['process_bio']:
        try:
            clinical_metadata, biospecimen_metadata, exclude_samples = parse_bio(config, platform2archive_types2archives['bio']['bio'], tumor_type, platform2archive2metadata['bio'], create_log(log_dir + tumor_type + '/', tumor_type + '_bio'))
        except Exception as e:
            log.exception('problem parsing bio and sample files')
            raise e
    else:
        clinical_metadata = {}
        biospecimen_metadata = {}
        exclude_samples = set()
        
    all_platforms = True
    platforms = []
    if 'platforms' in config:
        platforms = config['platforms']
        if not (0 == len(platforms) or (1 == len(platforms) and 'all' == platforms[0])):
            all_platforms = False
    aliquot2filename2metadata = {}
    future2platform = {}
    for platform, archive_types2archives in platform2archive_types2archives.iteritems():
        if 'bio' == platform:
            continue
        if all_platforms or platform in (platforms):
            log_name = tumor_type + '_' + platform
            future2platform[executor.submit(process_platform, config, log_dir, log_name, tumor_type, platform, platform2archive2metadata[platform], archive_types2archives, barcode2annotations, exclude_samples)] = platform
 
    platform2retry = {}
    future_keys = future2platform.keys()
    while future_keys:
        future_done, _ = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
        try:
            for future in future_done:
                platform = future2platform.pop(future)
                if future.exception() is not None:
                    # TODO only retry on connection refused, not other exceptions
                    retry_ct = platform2retry.setdefault(platform, 0)
                    if retry_ct > 3:
                        raise ValueError('%s failed multiple times: %s' % (platform, future.exception()))
                    platform2retry[platform] = retry_ct + 1
                    log.warning('\tWARNING: resubmitting %s: %s.  try %s' % (platform, future.exception(), retry_ct))
                    new_future = executor.submit(process_platform, config, log_dir, tumor_type + '_' + platform + '_' + str(retry_ct + 1), tumor_type, 
                            platform, platform2archive2metadata[platform], platform2archive_types2archives[platform], barcode2annotations, exclude_samples)
                    future2platform[new_future] = platform
                else:
                    merge_metadata(aliquot2filename2metadata, future.result(), platform, log)
                    future_keys = future2platform.keys()
        except:
            future_keys = future2platform.keys()
            log.exception('%s failed' % (platform))
 
    try:
        merge_metadata_current_metadata(aliquot2filename2metadata, barcode2metadata, log)
        merge_cghup(config, aliquot2filename2metadata, cghub_records, log)
        # data map has a different structure than the clinical and biospecimen maps, remove the top map of aliquot to file_list metadata and combine all the files
        # for compatibility in calls to the data store and etl
        flattened_data_map = {}
        for aliquot, file_name2field2value in aliquot2filename2metadata.iteritems():
            for file_name, field2value in file_name2field2value.iteritems():
                flattened_data_map[aliquot + ':' + file_name] = field2value

        # do this per platform to parallelize
        store_metadata(config, log, 'metadata_clinical', clinical_metadata)
        store_metadata(config, log, 'metadata_biospecimen', biospecimen_metadata)
        store_metadata(config, log, 'metadata_data', flattened_data_map)

        samples_metadata = process_metadata_samples(clinical_metadata, biospecimen_metadata, aliquot2filename2metadata, log)
        store_metadata(config, log, 'metadata_samples', samples_metadata)
    except Exception as e:
        log.exception('problem storing metadata for %s' % (tumor_type))
        raise e
    print '\t', datetime.now(), 'finished tumor type %s' % (tumor_type)
    log.info('\tfinished tumor type %s' % (tumor_type))
    return clinical_metadata, biospecimen_metadata, flattened_data_map

def process_tumortypes(config, log_dir, tumor_type2platform2archive_types2archives, platform2archive2metadata, tumor_type2cghub_records, barcode2metadata, barcode2annotations, log):
    '''
    loop through in parallel and process the studies/tumor_types.  at the end, saves the metadata to mysql and GCS
    
    parameters:
        config: the configuration map
        log_dir: the base directory for the logs
        tumor_type2platform2archive_types2archives: map tumor types to platforms to archive types ('maf', 'mage-tab', and 'data') to archives
        platform2archive2metadata: map of platforms to archive name to the archive metadata
        tumor_type2cghub_records: map tumor type to cghub metadata
        barcode2metadata: metadata from metadata.current.txt
        barcode2annotations: map of barcodes to TCGA annotations
        log: logger to log any messages
    '''
    log.info('begin process_tumortypes()')
    future2tumortype = {}
    for tumor_type, platform2archive_types2archives in tumor_type2platform2archive_types2archives.iteritems():
        if tumor_type in config['skip_tumor_types']:
            log.info('\tskipping tumor type %s' % (tumor_type))
            continue
        log.info('\tprocessing tumor type %s' % (tumor_type))
        future2tumortype[executor.submit(process_tumortype, config, log_dir, tumor_type, platform2archive_types2archives, 
            platform2archive2metadata, tumor_type2cghub_records.get(tumor_type.upper(), []), barcode2metadata, barcode2annotations)] = tumor_type
 
    total_clinical_metadata = {}
    total_biospecimen_metadata = {}
    total_data_metadata = {}
    future_keys = future2tumortype.keys()
    success = True
    while future_keys:
        future_done, future_keys = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
        for future in future_done:
            tumor_type = future2tumortype.pop(future)
            if future.exception() is not None:
                success = False
                log.exception('\t%s generated an exception--%s' % (tumor_type, future.exception()))
            else:
                result = future.result()
                total_clinical_metadata.update(result[0])
                total_biospecimen_metadata.update(result[1])
                total_data_metadata.update(result[2])
                log.info('\tfinished tumor type %s' % (tumor_type))

    if success:
        upload_etl_file(config, config['bio_etl_keys']['clinical'], total_clinical_metadata, log, 'Clinical')
        upload_etl_file(config, config['bio_etl_keys']['biospecimen'], total_biospecimen_metadata, log, 'Biospecimen', remove_keys=[['is_ffpe', ['YES']]])
        upload_etl_file(config, config['bio_etl_keys']['data'], total_data_metadata, log, 'Data')
    log.info('finished process_tumortypes()')


def info_status(config, log):
    '''
    outputs a summery of options choosen for the current run 
    
    parameters:
        config: the configuration map
        log: logger to log any messages
    '''
    if not config['upload_files']:
        log.warning('\n\t====================\n\tno files will be uploaded this run!\n\t====================')
    if not config['upload_etl_files']:
        log.warning('\n\t====================\n\tno etl files will be uploaded this run!\n\t====================')
    if not config['download_archives']:
        log.warning('\n\t====================\n\tno data archives will be downloaded this run!\n\t====================')
    if config['upload_open']:
        log.warning('\n\t====================\n\topen-access files will be processed this run!\n\t====================')
    else:
        log.warning('\n\t====================\n\tno open-access files will be processed this run!\n\t====================')
    if config['upload_controlled']:
        log.warning('\n\t====================\n\tcontrolled-access files will be processed this run!\n\t====================')
    else:
        log.warning('\n\t====================\n\tno controlled-access files will be processed this run!\n\t====================')
    # make sure not processing bio makes sense
    if not config['process_bio'] and config['upload_etl_files']:
        raise ValueError('\tbad configuration: must process bio to upload etl files')
    if not config['process_bio'] and 'mock' not in config['database_module']:
        raise ValueError('\tbad configuration: must process bio to save to cloud sql')
    
    if config['process_bio']:
        log.warning('\n\t====================\n\tnot processing bio this run!\n\t====================')
    else:
        log.warning('\n\t====================\n\tnot processing bio this run!\n\t====================')


def write_element_stats(study2element2values2count, element2count, prefix):
    '''
    helper method to output per study metadata count information from the bio XML files
    
    parameters:
        study2element2values2count: map of study to element to values to count per value
        element2count: map of element to total count over all studies
        prefix: prefix for the output filename
    '''
    with open('%s_element_counts.tsv' % prefix, 'w') as output_file:
        output_file.write('field\tcount\t')
        studies = study2element2values2count.keys()
        studies.sort()
        output_file.write('\t'.join(studies) + '\n')
        for element, count in element2count.iteritems():
            output_file.write('%s\t%s\t' % (element, count))
            for study in studies:
                if element in study2element2values2count[study]:
                    values2count = study2element2values2count[study][element]
                    output_file.write('%s\t' % (', '.join('"%s": %s' % (value, count) for (value, count) in values2count.iteritems()) if len(values2count) < 11 else ('#distinct: %s' % (len(values2count)))))
                else:
                    output_file.write('<empty>\t')
            output_file.write('\n')

def uploadTCGA(configFileName):
    '''
    based on the configuration map loaded from the configFileName, loads the DCC data into GCS.  also
    obtains metadata based on file paths, SDRF values, and CGHub manifest values
    
    parameters:
        configFileName: the file name of the configuration map
    '''
    print datetime.now(), 'begin uploadTCGA()'
    global executor
    gcs_wrapper = None
    try:
        with open(configFileName) as configFile:
            config = json.load(configFile)
        
        run_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(run_dir, 'top_processing')
        log = logging.getLogger(log_name)
        log.info('begin uploadTCGA()')
        executor = futures.ThreadPoolExecutor(max_workers=config['threads'])
        
        module = import_module(config['database_module'])
        module.ISBCGC_database_helper.initialize(config, log)
     
        if config['upload_files'] or config['upload_etl_files']:
            # open the GCS wrapper here so it can be used by all the tumor types/platforms to save files
            gcs_wrapper = import_module(config['gcs_wrapper'])
            gcs_wrapper.open_connection(config, log)
        info_status(config, log)
        tumor_type2platform2archive_types2archives, platform2archive2metadata = process_latestarchive(config, run_dir, log_name)
        prepare_upload(tumor_type2platform2archive_types2archives, log)
        if 'process_cghub' not in config or config['process_cghub']:
            tumor_type2cghub_records = process_cghub(config, run_dir, log=log, removedups=True, limit=-1)
        else:
            log.warning('\n\t====================\n\tnot processing CGHub records this run!\n\t====================')
            tumor_type2cghub_records = {}
        barcode2metadata = process_metadata_current(config, run_dir, log_name)
        if 'process_annotations' not in config or config['process_annotations']:
            barcode2annotations = process_annotations(config, run_dir, log_name)
        else:
            log.warning('\n\t====================\n\tnot processing annotations this run!\n\t====================')
            barcode2annotations = {}
        process_tumortypes(config, run_dir, tumor_type2platform2archive_types2archives, platform2archive2metadata, tumor_type2cghub_records, barcode2metadata, barcode2annotations, log)
        
        
        # print out the stats
        metadata_modules = config['metadata_modules']
        for metadata_module in metadata_modules:
            module = import_module(metadata_module)
            module.print_combined_stats(log)
    finally:
        if executor:
            executor.shutdown(wait=False)
        if gcs_wrapper:
            gcs_wrapper.close_connection()
        write_element_stats(ssf_study2element2values2count, ssf_element2count, 'ssf')
        write_element_stats(omf_study2element2values2count, omf_element2count, 'omf')
    log.info('finish uploadTCGA()')
    
    try:
        # upload the logs and TCGA files used for upload to GCS
        upload_run_files(config, run_dir, log)
    except Exception as e:
        log.exception('problem moving the logs and run files to GCS')

    print datetime.now(), 'finish uploadTCGA()'

if __name__ == '__main__':
    uploadTCGA(sys.argv[1])