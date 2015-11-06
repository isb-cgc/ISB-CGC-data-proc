'''
Created on Mar 27, 2015

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
import re
import shutil

import gcs_wrapper
import parseSDRF
import util

def getSDRFKeyName(file_name, metadata, log):
    try:
        return '/%s/%s/%s/%s/%s' % ('tcga', metadata['Study'].lower(), metadata['Platform'], metadata['Pipeline'], file_name)
    except Exception as e:
        log.exception('problem with sdrf keypath: %s' % (metadata))
        raise e

def upload_sdrf_file(config, archive_path, file_name, metadata, log):
    center2platform = config['upload_archives']['mage-tab']
    try:
        if metadata['DataCenterName'] not in center2platform or metadata['Platform'] not in center2platform[metadata['DataCenterName']]:
            log.info('\t\tskipping uploading %s from sdrf archive' % (file_name))
            return
    except Exception as e:
        log.exception('problem checking uploading SDRF file')
        raise e
    bucket_name = config['buckets']['open']
    key_name = getSDRFKeyName(file_name, metadata, log)
    if config['upload_files'] and config['upload_open']:
        gcs_wrapper.upload_file(archive_path + file_name, bucket_name, key_name, log)
    else:
        log.info('\t\tnot uploading %s from sdrf archive to %s' % (file_name, key_name))
 
def findPlatformPipelineFromDescendents(config, node, nextNodes, term2value, start_index, end_index, index2node2term2value, log):
    # TODO: verify this is appropriate to back fill these values, usually into bam file metadata from maf file metadata
    for index in range(start_index, end_index):
        node2term2value = index2node2term2value.setdefault(index, {})
        for node in nextNodes:
            if node.name in node2term2value:
                descterm2value = node2term2value[node.name]
                if 'Platform' in descterm2value:
                    term2value['Platform'] = descterm2value['Platform']
                    term2value['Pipeline'] = descterm2value['Pipeline']
                    term2value['DataCenterName'] = descterm2value['DataCenterName']
                    term2value['DataCenterType'] = descterm2value['DataCenterType']
                    term2value['Project'] = descterm2value['Project']
                    term2value['Study'] = descterm2value['Study']
                    return
        
def checkNextNodes(prev_node_name, nextNodes, index, barcode, term2value, index2node2term2value, barcode2files2term2values, log):
    if not nextNodes or 0 == len(nextNodes):
        # stop condition
        return
    
    for node in nextNodes:
        node2term2value = index2node2term2value.setdefault(nextNodes[node], {})
        if node.name in node2term2value:
            dataterm2value = node2term2value[node.name]
            totalterm2value = dict(dataterm2value)
            totalterm2value.update(term2value)
            derivedFromList = totalterm2value.setdefault('wasDerivedFrom', [])
            if prev_node_name and '->' != prev_node_name:
                derivedFromList += [prev_node_name]
            datamap = barcode2files2term2values.setdefault(barcode, {})
            if totalterm2value['DatafileName'] in datamap and datamap[totalterm2value['DatafileName']] != totalterm2value:
                # see if the only difference is the wasDerivedFrom
                datamap_totalterm2value = dict(datamap[totalterm2value['DatafileName']])
                current_derived = datamap_totalterm2value.pop('wasDerivedFrom', None)
                temp_totalterm2value = dict(totalterm2value)
                temp_totalterm2value.pop('wasDerivedFrom', None)
                if datamap_totalterm2value != temp_totalterm2value:
                    log.warning('\t=======================different values for %s:\n\t%s\n\t%s\n\t=======================\n\t' % \
                        (temp_totalterm2value['DatafileName'], datamap_totalterm2value, temp_totalterm2value))
                if current_derived:
                    if isinstance(current_derived, basestring):
                        derivedFromList += [current_derived]
                    else:
                        derivedFromList += current_derived
                datamap[totalterm2value['DatafileName']] = totalterm2value
            else:
                datamap[totalterm2value['DatafileName']] = totalterm2value
            datamap[totalterm2value['DatafileName']] = totalterm2value
        
        checkNextNodes(node.name, node.nextNodes, index + 1, barcode, term2value, index2node2term2value, barcode2files2term2values, log)

def parse_sdrf(config, log, file_name, archive2metadata, barcode2files2term2values, archive2barcodes, archive_fields, sdrf_file_name, barcode2annotations):
    header2term = config['metadata_locations']['sdrf']
    data2term = header2term['Data']
    ename2term = header2term['Extract Name']
    
    centerfields2values = {}
    index2nodeinfo = parseSDRF.main([file_name], log)
    index2nodes = index2nodeinfo[file_name]
    index2node2term2value = {}
    try:
        for index in range(len(index2nodes) - 1, -1, -1):
            if 'Data' in index2nodes[index][1].columnName:
                node2term2value = {}
                for node, nodeInstance in index2nodes[index][1].nodes.iteritems():
                    try:
                        if not node or '->' == node:
                            continue
                        term2value = {}
                        term2value['DatafileName'] = nodeInstance.name
                        for key, term in data2term.iteritems():
                            key = key.lower().replace(' ', '')
                            if key in nodeInstance.comment:
                                keyTerms = term.split('!')
                                for keyTerm in keyTerms:
                                    if keyTerm.startswith('pattern#'):
                                        fields = keyTerm.split('#')
                                        match = re.match(fields[2], nodeInstance.comment[key].name)
                                        term2value[fields[1]] = match.group(1)
                                    else:
                                        term2value[keyTerm] = nodeInstance.comment[key].name
                        if 'add' in header2term:
                            for term in header2term['add']:
                                fields = header2term['add'][term].split(':')
                                if 2 == len(fields) and 'literal' == fields[0]:
                                    term2value[term] = fields[1]
    
                        term2value['MAGETabArchiveName'] = archive_fields[0]
                        term2value['MAGETabArchiveURL'] = archive_fields[2]
                        term2value['SDRFFileName'] = sdrf_file_name
                        
                        
                        if 'DataArchiveName' in term2value:
                            term2value.update(archive2metadata[term2value['DataArchiveName']])
                        else:
                            term2value.update(centerfields2values)
                        if 'bam' in term2value['DatafileName'] or 'tar.gz' in term2value['DatafileName']:
                            if 'DataLevel' not in term2value:
                                term2value['DataLevel'] = 'Level 1'
                            term2value['SecurityProtocol'] = config['access_tags']['controlled']
                        node2term2value[nodeInstance.name] = term2value
                        term2value['SDRFFileNameKey'] = getSDRFKeyName(sdrf_file_name, term2value, log)
                        if 0 == len(centerfields2values) and 'Study' in term2value:
                            centerfields2values['Platform'] = term2value['Platform']
                            centerfields2values['Pipeline'] = term2value['Pipeline']
                            centerfields2values['DataCenterName'] = term2value['DataCenterName']
                            centerfields2values['DataCenterType'] = term2value['DataCenterType']
                            centerfields2values['Project'] = term2value['Project']
                            centerfields2values['Study'] = term2value['Study']
                    except Exception as e:
                        log.exception('problem processing %s' % (nodeInstance.name))
                        raise e
                if len(node2term2value):
                    index2node2term2value[index] = node2term2value
                
    except Exception as e:
        log.exception('problem in parse_sdrf() processing data nodes')
        raise e
    
    try:
        for index, index2node in enumerate(index2nodes):
            # look for the AliquotUUID column
            if 'Extract Name' == index2node[1].columnName:
                for node, nodeInstance in index2node[1].nodes.iteritems():
                    term2value = {}
                    if nodeInstance.name.startswith('TCGA'):
                        # hardcoding this for now for RPPA, which uses the barcode instead of UUID
                        # for the Extract column
                        term2value['AliquotBarcode'] = nodeInstance.name[:-2]
                        barcode = term2value['AliquotBarcode']
                    elif nodeInstance.name.startswith('Control'):
                        continue
                    else:
                        term2value['AliquotUUID'] = nodeInstance.name
                        for key, term in ename2term.iteritems():
                            key = key.lower().replace(' ', '')
                            if key in nodeInstance.comment:
                                term2value[term] = nodeInstance.comment[key].name
                            # and find the barcode to use as the key
                            if 'barcode' in term.lower():
                                try:
                                    barcode = term2value[term]
                                except Exception as e:
                                    log.exception('problem looking up barcode')
                                    raise e
                    term2value['ParticipantBarcode'] = term2value['AliquotBarcode'][:12]
                    term2value['SampleBarcode'] = term2value['AliquotBarcode'][:16]
                    term2value['SampleTypeCode'] = term2value['AliquotBarcode'][13:15]
                    term2value['SampleType'] = config['sample_code2type'][term2value['SampleTypeCode']]
                    
                    if term2value['AliquotBarcode'] in barcode2annotations:
                        try:
                            term2value.update(barcode2annotations[term2value['AliquotBarcode']])
                        except Exception as e:
                            log.error('problem with annotations for %s(%s): %s' % (term2value['AliquotBarcode'], e, barcode2annotations[term2value['AliquotBarcode']]))
                    if term2value['AliquotBarcode'][:12] in barcode2annotations:
                        try:
                            term2value.update(barcode2annotations[term2value['AliquotBarcode'][:12]])
                        except Exception as e:
                            log.error('problem with annotations for %s(%s): %s' % (term2value['AliquotBarcode'][:12], e, barcode2annotations[term2value['AliquotBarcode'][:12]]))
                    # iterate down nextNodes to find any nodes in index2node2term2value
                    checkNextNodes(node, nodeInstance.nextNodes, index + 1, barcode, term2value, index2node2term2value, barcode2files2term2values, log)
                break
    except Exception as e:
        log.exception('problem in parse_sdrf() checking next nodes')
        raise e

def process_sdrf(config, log, magetab_archives, archive2metadata, barcode2annotations):
    """
    return types:
        barcode2files2term2values: maps aliquot barcode to a map with filenames for that barcode as keys to another map of terms
          based on the ['metadata_locations']['sdrf'] section of the config file 
    """
    log.info('start processing sdrf')
    sdrf_pat = re.compile("^.*sdrf.txt$")
    anti_pat = re.compile("^.*antibody_annotation.txt$")
    barcode2files2term2values = {}
    archive2barcodes = {}
    for archive_fields in magetab_archives:
        try:
            log.info('\tprocessing %s' % (archive_fields[0]))
            archive_path = util.setup_archive(archive_fields, log)
            files = os.listdir(archive_path)
            antibody_files = []
            cur_barcode2files2term2values = {}
            for file_name in files:
                if sdrf_pat.match(file_name):
                    parse_sdrf(config, log, archive_path + file_name, archive2metadata, cur_barcode2files2term2values, 
                        archive2barcodes, archive_fields, file_name, barcode2annotations)
                    util.merge_metadata(barcode2files2term2values, cur_barcode2files2term2values, archive_fields[0] + ': ' + ','.join([archive_fields[0] for archive_fields in magetab_archives]), log)
                    upload_sdrf_file(config, archive_path, file_name, barcode2files2term2values.values()[0].values()[0], log)
                elif anti_pat.match(file_name):
                    antibody_files += [file_name]
            for file_name in antibody_files:
                upload_sdrf_file(config, archive_path, file_name, barcode2files2term2values.values()[0].values()[0], log)
        finally:
            shutil.rmtree(archive_path)
    log.info('finished processing sdrf')
    return barcode2files2term2values
