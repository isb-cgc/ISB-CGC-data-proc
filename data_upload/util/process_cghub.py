'''
Created on May 7, 2015

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
from util import import_module, log_exception, log_info, post_run_file

import pprint

def addPlatformPipelineFields(config, metadata, seen_bad_codes, log):
    fullPlatform2platform = config['uploadfullplatform2platform']
    analyte2strategy2moltype = config['analyte2strategy2moltype']
    shortname2centername = config['shortname2centername']
    try:
        platformName = fullPlatform2platform[metadata['platform_full_name']]
    except KeyError, e:
        print "metadata['platform_full_name']"
        pprint.pprint(metadata['platform_full_name'])
        log_exception(log, 'KeyError in fullPlatform2platform[metadata["platform_full_name"]]: %s' % str(e))
    centerName = shortname2centername[metadata['DataCenterCode']]
    assembly = metadata['GenomeReference']
    try:
        moltype = analyte2strategy2moltype[metadata['analyte_code']][metadata['library_strategy']]
    except Exception as e:
        log_exception(log, 'problem setting molecular type: \'%s\' \'%s\'' % (metadata['analyte_code'], metadata['library_strategy']))
        raise e
    metadata['Platform'] = platformName + '_' + moltype
    metadata['Pipeline'] = centerName + '__' + moltype
    
    analyte2center_type = config['analyte2center_type']
    if metadata['analyte_code'] != '':
        metadata['DataCenterType'] = analyte2center_type[metadata['analyte_code']]

    shortname2centercodes = config['shortname2centercodes']
    shortname = metadata['DataCenterCode']
    if metadata['analyte_code'] in ('H', 'R', 'T'):
        metadata['DataCenterCode'] = shortname2centercodes[metadata['DataCenterCode']][1]
    elif metadata['analyte_code'] in ('D', 'W', 'X'):
        metadata['DataCenterCode'] = shortname2centercodes[metadata['DataCenterCode']][0]
    else:
        metadata['DataCenterCode'] = None
    if "00" == metadata['DataCenterCode'] and shortname not in seen_bad_codes:
        log.warning('did not find a proper center code for %s with analyte %s' % (shortname, metadata['analyte_code']))
        seen_bad_codes.add(shortname)
    
    upload_platforms = config['upload_platforms']
    deprecated_centers = config['deprecated_centers']
    deprecated_assemblies = config['deprecated_assemblies']
    deprecated = False
    for deprecated_assembly in deprecated_assemblies:
        if deprecated_assembly in assembly:
            deprecated = True
# will let CGHub upload update records (#718)
#     if platformName in upload_platforms and centerName not in deprecated_centers and moltype != 'None' and not deprecated:
#         metadata['DatafileUploaded'] = 'true' if 'Live' == metadata['state'] else 'false'
#     else:
#         metadata['DatafileUploaded'] = 'false'

def create_cghub_metadata(mappings, cghub_record, seen_bad_codes, log):
    metadata = cghub_record.flattened_record()

    # keys in metadata:
    # AliquotBarcode
    # SampleUUID
    # DatafileUploaded
    # platform_full_name
    # SecurityProtocol
    # reason_for_state
    # SampleBarcode
    # analysis_id
    # Repository
    # ParticipantUUID
    # project_accession
    # state
    # DataCenterCode
    # TSSCode
    # library_strategy
    # IncludeForAnalysis
    # DataLevel
    # DatafileMD5
    # Study
    # Project
    # last_modified
    # ParticipantBarcode
    # analyte_code
    # GenomeReference
    # DatafileName
    # SampleTypeCode
    # Species
    # AliquotUUID

    add = mappings['add']
    for part in add:
        for field in add[part]:
            fields = field.split(':')
            if 2 == len(fields) and 'literal' == fields[0]:
                metadata[part] = fields[1]
            elif 3 == len(fields) and 'map' == fields[0]:
                metadata[part] = mappings[fields[1]][metadata[fields[2]]]
            elif 4 == len(fields) and 'substring' == fields[0]:
                metadata[part] = metadata[fields[1]][int(fields[2]):int(fields[3])]
            elif 5 == len(fields) and 'match' == fields[0]:
                metadata[part] = fields[3] if fields[2] == metadata[fields[1]] else fields[4]
    maps = mappings['map']
    for field in maps:
        value = metadata.pop(field)
        newkeys = maps[field]
        for newkey in newkeys:
            metadata[newkey] = value
    minus = mappings['minus']
    for remove in minus:
        if remove in metadata:
            metadata.pop(remove)
    addPlatformPipelineFields(mappings, metadata, seen_bad_codes, log)
    return metadata

def process_cghub(config, run_dir, type_uri = 'detail', log = None, removedups = False, limit = -1, verbose = False, print_response = False):
    """
    return type:
        tumor_type2cghub_records: organizes the cghub record classes per tumor type
    """
    log_info(log, 'begin process cghub')
    module = import_module(config['cghub_module'])
    mappings = config['metadata_locations']['cghub']
    cghub_records, _, cghub_manifest = module.main(mappings['study'], log = log, removedups = removedups, limit = limit)
    tumor_type2cghub_records = {}
    count = 0
    seen_bad_codes = set()
    for cghub_record in cghub_records:
        if 0 == count % 8192:
            log_info(log, '\tprocess %s cghub records' % (count))
        count += 1
        tumor_type2cghub_records.setdefault(cghub_record.disease_abbr, []).append(create_cghub_metadata(mappings, cghub_record, seen_bad_codes, log))
    post_run_file(run_dir, 'CGHub_LATEST_MANIFEST.tsv', cghub_manifest)
    log_info(log, 'finished process cghub: %s total records' % (count))
    return tumor_type2cghub_records
