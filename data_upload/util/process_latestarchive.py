'''
Created on Mar 26, 2015

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
from datetime import date
import logging
import os
import re

import util

def upload_latestarchive_file(config, archive_file_path, log):
    bucket_name = config['buckets']['open']
    key_name = '/%s/%s' % (config['latestarchive_folder'], str(date.today()).replace('-', '_') + '_' + 'latestarchive.txt')
    if config['upload_files'] and config['upload_open']:
        log.info('\tnot uploading %s to %s' % (archive_file_path, key_name))
        util.upload_file(config, archive_file_path, bucket_name, key_name, log)
    else:
        log.info('\tnot uploading %s to %s' % (archive_file_path, key_name))
 
# <project>/<study(tumor type)>/<platform>/<pipeline>/<data level>/<file name>
# <center name>_<tumor type>.<platform>.<archive type>.<archive version>.tar.gz
def add_stats(stats, config, archive_name, archive_path, platform2pipeline_tag):
    # exclude the archive # and revision #
    center = archive_name[:archive_name.index('_')]
    fields = archive_name[archive_name.index('_') + 1:].split('.')
    tumor_type = fields[0].lower()
    platform = fields[1]
    pipeline = center + '__' + platform2pipeline_tag.get(platform, 'none')
    archive_type = fields[2]
    
    tumortype2platform2pipeline2levels = stats.setdefault('by_tumortype', {})
    platform2pipeline2levels = tumortype2platform2pipeline2levels.setdefault(tumor_type, {})
    pipeline2levels = platform2pipeline2levels.setdefault(platform, {})
    levels = pipeline2levels.setdefault(pipeline, [])
    levels += [archive_type]
    
    ignore_centers = config['ignored_centers']
    if center in ignore_centers:
        access2center2platform2level2count = stats.setdefault('by_ignore_center', {})
    else:
        access2center2platform2level2count = stats.setdefault('by_center', {})
    center2platform2level2count = access2center2platform2level2count.setdefault('public' if 'anonymous' in archive_path else 'protected', {})
    platform2level2count = center2platform2level2count.setdefault(center, {})
    level2count = platform2level2count.setdefault(platform, {})
    level2count[archive_type] = level2count.setdefault(archive_type, 0) + 1

def write_stats(stats):
    with open(str(date.today()).replace('-', '_') + '_archive_stats.txt', 'w') as stats_file:
        stats_file.write('archive statistics by centers:\n')
        access2center2platform2level2count = stats['by_center']
        center2platform2level2count = access2center2platform2level2count['public']
        centers = center2platform2level2count.keys()
        centers.sort()
        for center in centers:
            platform2level2count = center2platform2level2count[center]
            for platform, level2count in platform2level2count.iteritems():
                for level, level_count in level2count.iteritems():
                    stats_file.write('\t%s\t%s\t%s\t%s\n' % (level_count, center, platform, level))


def write_archive(archives):
    tmp_dir_parent = os.environ.get('ISB_TMP', '/tmp/')
    if not os.path.isdir(tmp_dir_parent):
        os.makedirs(tmp_dir_parent)
    archive_file_path = tmp_dir_parent + 'latestarchive.txt'
    chunk_size = 512 * 1024
    curpos = 0
    with open(archive_file_path, 'wb') as out:
        while True:
            chunk = archives[curpos:curpos + chunk_size]
            curpos = curpos + chunk_size
            if not chunk:
                break
            out.write(chunk)
            out.flush()
    
    return archive_file_path

def process_latestarchive(config, log_name):
    """
    return types:
        tumor_type2platform2archive_types2archives: this map organizes the archives per
          tumor_type per platform per archive_type ('mage-tab', 'bio', 'maf', or 'data')
        platform2archive2metadata: this map organizes the metadata across all tumor types
          per platform per archive.  the metadata fields are gotten from the archive url:
          DataArchiveURL; DataCenterType; DataCenterName; and Platform
    """
    log = logging.getLogger(log_name)
    log.info('start process latestarchive')
    processAll, process = util.getTumorTypes(config, log)
    metadata_spec = config['metadata_locations']['latestarchive']
    for key, value in metadata_spec.iteritems():
        metadata_spec[key] = value.split('#')
    
    local = False
    platform2pipeline_tag = config['platform2pipeline_tag']
    latestarchivesURL = config['downloads']['latestarchive']
    try:
        archives = util.getURLData(latestarchivesURL, 'latestarchive', log)
        lines = archives.split('\n')
        log.info('\tarchive size is %s with %s lines' % (len(archives), len(lines)))
        if 20 > len(lines) and 'test' == config['mode']:
            local = True
    except Exception as e:
        log.exception('problem fetching latestarchive')
        if 'test' == config['mode']:
            local = True
        else:
            raise e

    if local:
        archives = open('../latestarchive.txt')
        log.warning('using local copy for testing purposes')
        archives = archives.read()
        lines = archives.split('\n')
    archive_file_path = write_archive(archives)

    desired_platforms = config['platform2datatype'].keys()
    maf_level = config["maflevel"]
    maf_platforms = config["mafplatforms"]
    count = 0
    keep = 0
    tumor_type2platform2archive_types2archives = {}
    platform2archive2metadata = {}
    stats = {}
    for archive_info in lines[1:]:
        if not archive_info:
            continue
        
        if 0 == count % 1024:
            log.info('\t\tprocessed %s archives' % (count))
        count += 1
        archive_fields = archive_info.split('\t')
        archive_name = archive_fields[0]
        archive_url = archive_fields[2]
        fields = archive_name[archive_name.index('_') + 1:].split('.')
        tumor_type = fields[0].lower()
        platform = fields[1]
        add_stats(stats, config, archive_name, archive_url, platform2pipeline_tag)
        open_access = config['access_tags']['open']
        controlled_access = config['access_tags']['controlled']
        if (processAll or tumor_type in process) and platform in desired_platforms:
            keep += 1
            platform2archive_types2archives = tumor_type2platform2archive_types2archives.setdefault(tumor_type, {})
            archive_types2archives = platform2archive_types2archives.setdefault(platform, {})
            archive2metadata = platform2archive2metadata.setdefault(platform, {})
            if 'mage-tab' in archive_name: # bcgsc.ca genome.wustl.edu
                if 'mage-tab' in archive_types2archives:
                    log.warning('\t\tWARNING: found two mage-tab archives for %s[%s]: \n\t\t\t%s\n\t\t%s\n\t\t\t\tand\n\t\t\t%s\n\t\t%s' % 
                        (platform, tumor_type, archive_types2archives['mage-tab'][0][0], archive_types2archives['mage-tab'][0][2], archive_name, archive_url))
                magetab_archives = archive_types2archives.setdefault('mage-tab', [])
                magetab_archives += [archive_fields]
            elif 'bio' == platform:
                clinical_archives = archive_types2archives.setdefault('bio', [])
                clinical_archives += [archive_fields]
            elif 'Level_1' in archive_name or 'Level_2' in archive_name or 'Level_3' in archive_name:
                data_archives = archive_types2archives.setdefault('data', [])
                data_archives += [archive_fields]
                if maf_level in archive_name and platform in maf_platforms:
                    maf_archives = archive_types2archives.setdefault('maf', [])
                    maf_archives += [archive_fields]
            metadata = archive2metadata.setdefault(archive_name, {})
            metadata['Project'] = 'TCGA'
            for key, values in metadata_spec.iteritems():
                if 'ARCHIVE_URL' == key:
                    field = archive_url
                else:
                    field = archive_name
                for value in values:
                    fields = value.split(':')
                    if 1 == len(fields):
                        metadata[fields[0]] = field
                    elif 'split' == fields[0]:
                        metadata[fields[3]] = field.split(fields[1])[int(fields[2])]
                    elif 'pattern' == fields[0]:
                        match = re.match(fields[2], field)
                        metadata[fields[1]] = match.group(1)
            metadata['Pipeline'] = metadata['DataCenterName'] + '__' + platform2pipeline_tag[metadata['Platform']]
            metadata['SecurityProtocol'] = controlled_access if 'tcga4yeo' in metadata['DataArchiveURL'] else open_access
    log.info('\tprocessed %s total archives' % (count))

    if 0 < count:
        write_stats(stats)
    else:
        log.error('no archives found!!!')
    if config['upload_open']:
        upload_latestarchive_file(config, archive_file_path, log)
    log.info('finished process latestarchive: %s total archives, kept %s' % (count, keep))
    return tumor_type2platform2archive_types2archives, platform2archive2metadata
