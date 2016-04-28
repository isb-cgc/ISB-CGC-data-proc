'''
Created on Oct 21, 2014

import CGHub information from LATEST_MANIFEST.tsv

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
from datetime import datetime
import urllib

from cghub_record_info import CGHubRecordInfo, CGHubFileInfo
import util

platform = 'CCLE'
# platform = 'TCGA'
manifest_uri = 'https://cghub.ucsc.edu/reports/SUMMARY_STATS/LATEST_MANIFEST.tsv'

header2record_index = {
    'analysis_id': CGHubRecordInfo.analysis_id_index, 
    'state': CGHubRecordInfo.state_index, 
    'reason': CGHubRecordInfo.reason_index, 
    'modified': CGHubRecordInfo.last_modified_index, 
    'uploaded': CGHubRecordInfo.upload_date_index, 
    'published': CGHubRecordInfo.published_date_index, 
    'center': CGHubRecordInfo.center_name_index, 
    'study': CGHubRecordInfo.study_index, 
    'aliquot_id': CGHubRecordInfo.aliquot_id_index,
    'filename': CGHubRecordInfo.bamFilename_index,
    'files_size': CGHubRecordInfo.bamFilesize_index,
    'checksum': CGHubRecordInfo.bamMD5_index, 
    'barcode': CGHubRecordInfo.legacy_sample_id_index, 
    'disease': CGHubRecordInfo.disease_abbr_index, 
    'analyte_type_code': CGHubRecordInfo.analyte_code_index, 
    'sample_type_code': CGHubRecordInfo.sample_type_index, 
    'library_type': CGHubRecordInfo.library_strategy_index, 
    'platform': CGHubRecordInfo.platform_index, 
    'assembly': CGHubRecordInfo.refassem_short_name_index, 
    'tss_id': CGHubRecordInfo.tss_id_index,
    'participant_id': CGHubRecordInfo.participant_id_index,
    'sample_id': CGHubRecordInfo.sample_id_index,
    'platform_full_name': CGHubRecordInfo.INSTRUMENT_MODEL_index,
}
index2none = {
    CGHubRecordInfo.baiFilename_index: None, 
    CGHubRecordInfo.baiFilesize_index: None, 
    CGHubRecordInfo.baiMD5_index: None,
    CGHubRecordInfo.analysis_data_uri_index: None,
    CGHubRecordInfo.analysis_full_uri_index: None,
    CGHubRecordInfo.analysis_submission_uri_index: None
}

def createDateTime(datestr):
    if '/' in datestr:
        days = datestr.split('/')
        if 4 != len(days[2]):
            raise ValueError('unexpected format: %s' % (datestr))
        return datetime(int(days[2]), int(days[0]), int(days[1]))
    elif '-' in datestr:
        days = datestr.split('-')
        if 4 != len(days[0]):
            raise ValueError('unexpected format: %s' % (datestr))
        return datetime(int(days[0]), int(days[1]), int(days[2]))
    else:
        raise ValueError('unrecognized date format: %s' % (datestr))

def statistics(log, filename2cghubRecords, minmaxsize, verbose):
    states = {}
    centers = {}
    studies = {}
    sampleIDs = [{}, {}, {}, {}, {}, {}, {}, {}]
    diseases = {}
    analyte_codes = {}
    sample_types = {}
    strategies = {}
    platforms = {}
    refassems = {}
    models = {}
    for record in filename2cghubRecords.itervalues():
        states[record.state] = states.setdefault(record.state, 0) + 1
        centers[record.center_name] = centers.setdefault(record.center_name, 0) + 1
        studies[record.study] = studies.setdefault(record.study, 0) + 1
        diseases[record.disease_abbr] = diseases.setdefault(record.disease_abbr, 0) + 1
        analyte_codes[record.analyte_code] = analyte_codes.setdefault(record.analyte_code, 0) + 1
        sample_types[record.sample_type] = sample_types.setdefault(record.sample_type, 0) + 1
        strategies[record.library_strategy] = strategies.setdefault(record.library_strategy, 0) + 1
        platforms[record.platform] = platforms.setdefault(record.platform, 0) + 1
        refassems[record.refassem_short_name] = refassems.setdefault(record.refassem_short_name, 0) + 1
        models[record.platform_full_name] = models.setdefault(record.platform_full_name, 0) + 1
        
        try:
            fields = record.legacy_sample_id.split('-')
            for index, field in enumerate(fields[:-3]):
                sampleIDs[index][field] = sampleIDs[index].setdefault(field, 0) + 1
            for index, field in enumerate(fields[-3:]):
                sampleIDs[index + len(fields[:-3])][field] = sampleIDs[index + len(fields[:-3])].setdefault(field, 0) + 1
        except:
            util.log_exception(log, 'problem splitting %s(%s:%s)' % (record.legacy_sample_id, index, field))
            
    util.log_info(log, '\nStates')
    count = 0
    for state, value in states.iteritems():
        if count < 15:
            count += 1
            util.log_info(log, '%s: %s' % (state, value))
        else:
            util.log_info(log, '\t(of %s)' % (len(states.keys())))
            break
    util.log_info(log, '')
    
    util.log_info(log, 'Centers')
    count = 0
    for center, value in centers.iteritems():
        if count < 15:
            count += 1
            util.log_info(log, '%s: %s' % (center, value))
        else:
            util.log_info(log, '	(of %s)' % (len(centers.keys())))
            break
    util.log_info(log, '')
    
    util.log_info(log, 'Studies')
    count = 0
    for studie, value in studies.iteritems():
        if count < 15:
            count += 1
            util.log_info(log, '%s: %s' % (studie, value))
        else:
            util.log_info(log, '(of %s)' % (len(studies.keys())))
            break
    util.log_info(log, '')
    
    if verbose:
        util.log_info(log, 'Sample ids:')
        count = 0
        for sampleMap in sampleIDs:
            util.log_info(log, 'next part:')
            for sampleID, value in sampleMap.iteritems():
                if count < 15:
                    count += 1
                    util.log_info(log, '%s: %s' % (sampleID, value))
                else:
                    util.log_info(log, '(of %s)' % (len(sampleMap.keys())))
                    break
            util.log_info(log, '')
            count = 0
    
    util.log_info(log, 'Diseases:')
    count = 0
    for disease, value in diseases.iteritems():
        count += 1
        util.log_info(log, '%s: %s' % (disease, value))
    util.log_info(log, '')
    
    util.log_info(log, 'Analyte codes:')
    count = 0
    for analyte_code, value in analyte_codes.iteritems():
        if count < 15:
            count += 1
            util.log_info(log, '%s: %s' % (analyte_code, value))
        else:
            util.log_info(log, '(of %s)' % (len(analyte_codes.keys())))
            break
    util.log_info(log, '')
    
    util.log_info(log, 'Sample types')
    count = 0
    for sample_type, value in sample_types.iteritems():
        if count < 15:
            count += 1
            util.log_info(log, '%s: %s' % (sample_type, value))
        else:
            util.log_info(log, '(of %s)' % (len(sample_types.keys())))
            break
    util.log_info(log, '')
    
    util.log_info(log, 'Strategies:')
    count = 0
    for strategie, value in strategies.iteritems():
        if count < 15:
            count += 1
            util.log_info(log, '%s: %s' % (strategie, value))
        else:
            util.log_info(log, '(of %s)' % (len(strategies.keys())))
            break
    util.log_info(log, '')
    
    util.log_info(log, 'Platforms:')
    count = 0
    for platform, value in platforms.iteritems():
        if count < 15:
            count += 1
            util.log_info(log, '%s: %s' % (platform, value))
        else:
            util.log_info(log, '(of %s)' % (len(platforms.keys())))
            break
    util.log_info(log, '')
    
    util.log_info(log, 'Reference Assembles:')
    count = 0
    for refassem, value in refassems.iteritems():
        if count < 15:
            count += 1
            util.log_info(log, '%s: %s' % (refassem, value))
        else:
            util.log_info(log, '(of %s)' % (len(refassems.keys())))
            break
    util.log_info(log, '')
    
    util.log_info(log, 'Models:')
    count = 0
    for model, value in models.iteritems():
        if count < 15:
            count += 1
            util.log_info(log, '%s: %s' % (model, value))
        else:
            util.log_info(log, '(of %s)' % (len(models.keys())))
            break
    
    util.log_info(log, '')
    util.log_info(log, '\n\t\tmax: %s\n\t\tmin: %s' % (minmaxsize['max'].write(), minmaxsize['min'].write()))

def main(platform, type_uri = 'detail', log = None, removedups = False, limit = -1, verbose = False, print_response = False):
    util.log_info(log, 'begin reading cghub archive')
    filename2cghubRecords = {}
    minmaxsize = {'min': CGHubFileInfo('', 500000000000, ''), 'max': CGHubFileInfo('', 1, '')}
    try:
#         archives = util.getURLData(manifest_uri, 'latest_manifest', log)
        response = urllib.urlopen(manifest_uri)
        archives = response.read()

        lines = archives.split('\n')
        util.log_info(log, '\tarchive size is %s with %s lines' % (len(archives), len(lines)))
        util.log_info(log, '\n\t' + '\n\t'.join(lines[:10]))
    except Exception as e:
        util.log_exception(log, 'problem fetching latest_manifest: %s')
        raise e
    
    headers = lines[0].split('\t')
    column_index2header = {}
    for index, header in enumerate(headers):
        if header in header2record_index.keys():
            column_index2header[index] = header
        
    count = 0
    dupcount = 0
    for line in lines[1:]:
        if not line:
            continue
        if 0 == count % 4096:
            util.log_info(log, 'processed %s records' % (count))
        count += 1
        fields = line.split('\t')
        header2record = {}
        try:
            for index in column_index2header.keys():
                header2record[header2record_index[column_index2header[index]]] = fields[index]
        except Exception as e:
            util.log_info(log, 'problem with parsing line(%s): %s' % (count, line))
        if platform not in header2record[CGHubRecordInfo.study_index]:
            continue
        header2record.update(index2none)
        record = CGHubRecordInfo(header2record)
        filename = header2record[CGHubRecordInfo.bamFilename_index]
        if removedups and filename in filename2cghubRecords:
            if 'Live' == header2record[CGHubRecordInfo.state_index]:
                dupcount += 1
                # check the dates and keep the latest
                currentdate = createDateTime(filename2cghubRecords[filename].upload_date)
                newdate = createDateTime(record.upload_date)
                if currentdate < newdate:
                    filename2cghubRecords[filename] = record
        else:
            filename2cghubRecords[filename] = record
        if 'Live' == record.state:
            if minmaxsize['min'].filesize > record.files['bam'].filesize and record.files['bam'].filesize:
                minmaxsize['min'] = record.files['bam']
            if minmaxsize['max'].filesize < record.files['bam'].filesize:
                minmaxsize['max'] = record.files['bam']
            if not record.files['bam'].filesize:
                util.log_info(log, 'no file size: %s--%s' % (record.write(), record.files['bam'].write()))

    statistics(log, filename2cghubRecords, minmaxsize, verbose)
    util.log_info(log, 'finished reading cghub archive.  %s total records, %s duplicates' % (count, dupcount))
    return filename2cghubRecords.values(), minmaxsize, archives
    
if __name__ == '__main__':
    main(platform, removedups = True)
