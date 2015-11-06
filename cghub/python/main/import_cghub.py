'''
Created on Oct 21, 2014

import CGHub information from the REST API.  deprecated for import_cghub_manifest.py

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
import httplib
import traceback
import urllib
import argparse

from xml import sax
from xml.sax.handler import ContentHandler
from xml.sax.handler import ErrorHandler

from cghub_record_info import CGHubRecordInfo, CGHubFileInfo

# platform = '*Other_Sequencing_Multiisolate', 
# platform = 'phs000178',
platform = 'TCGA_deprecated' 
detail_uri = 'http://cghub.ucsc.edu/cghub/metadata/analysisDetail'
full_uri = 'https://cghub.ucsc.edu/cghub/metadata/analysisFull/'

element2index = {
    'analysis_id': CGHubRecordInfo.analysis_id_index, 
    'state': CGHubRecordInfo.state_index, 
    'reason': CGHubRecordInfo.reason_index, 
    'last_modified': CGHubRecordInfo.last_modified_index, 
    'upload_date': CGHubRecordInfo.upload_date_index, 
    'published_date': CGHubRecordInfo.published_date_index, 
    'center_name': CGHubRecordInfo.center_name_index, 
    'study': CGHubRecordInfo.study_index, 
    'aliquot_id': CGHubRecordInfo.aliquot_id_index,
    'filename': [CGHubRecordInfo.bamFilename_index, CGHubRecordInfo.baiFilename_index],
    'filesize': [CGHubRecordInfo.bamFilesize_index, CGHubRecordInfo.baiFilesize_index],
    'checksum': [CGHubRecordInfo.bamMD5_index, CGHubRecordInfo.baiMD5_index], 
    'legacy_sample_id': CGHubRecordInfo.legacy_sample_id_index, 
    'disease_abbr': CGHubRecordInfo.disease_abbr_index, 
    'analyte_code': CGHubRecordInfo.analyte_code_index, 
    'sample_type': CGHubRecordInfo.sample_type_index, 
    'library_strategy': CGHubRecordInfo.library_strategy_index, 
    'platform': CGHubRecordInfo.platform_index, 
    'refassem_short_name': CGHubRecordInfo.refassem_short_name_index, 
    'analysis_submission_uri': CGHubRecordInfo.analysis_submission_uri_index, 
    'analysis_full_uri': CGHubRecordInfo.analysis_full_uri_index, 
    'analysis_data_uri': CGHubRecordInfo.analysis_data_uri_index,
    'tss_id': CGHubRecordInfo.tss_id_index,
    'participant_id': CGHubRecordInfo.participant_id_index,
    'sample_id': CGHubRecordInfo.sample_id_index,
    'INSTRUMENT_MODEL': CGHubRecordInfo.INSTRUMENT_MODEL_index,
    'files': -1,
    'file': -1,
    'sample_accession': -1
}
fileElements = set(['filename', 'filesize', 'checksum'])

class LimitReachedException(Exception):
    """
    """

class Content(ContentHandler):
    count = 0
    inElement = -1
    part2platforms = {}
    sample2platforms = {}

    include = set(['live'])
    barcode2info = {}
    platforms2samples = {}
    platforms2seensamples = {}
    totalFileSize = 0
    
    def __init__(self, log = None, limit = -1, removedups = False, verbose = False):
        self.logger = log
        self.limit = False
        self.removedups = removedups
        self.dupcount = 0
        if 0 < limit:
            self.limit_count = limit
            self.limit = True
        self.verbose = verbose
    
    def log(self, msg):
        if self.logger:
            self.logger.info('\t' + msg)
        else:
            print msg

    def startDocument(self):
        self.log('parsing CGHub')
        self.filename2cghubRecords = {}
        self.info = ['' for _ in range(CGHubRecordInfo.infoCount)]
        self.inElement = -1
        self.inResult = False
        self.hasChars = False
        self.unexpectedChars = ''
        self.fileIndices = 0
        self.minmaxsize = {'min': CGHubFileInfo('', 500000000000, ''), 'max': CGHubFileInfo('', 1, '')}
        
    def endDocument(self):
        if self.verbose:
            self.log('records: \n\t%s\n' % ('\n\t'.join([str(cghubrecord.write()) for cghubrecord in self.filename2cghubRecords.iterValues()])))
        self.statistics()
        self.log('processed total %s file references\n.  total BAM file size was %s' % (self.count, self.totalFileSize))
    
    def startElement(self, name, attrs):
        if 'Result' == name:
            self.inResult = True
        elif self.inResult:
            if name in fileElements:
                indices = element2index[name]
                if not self.info[indices[0]]:
                    self.inElement = indices[0]
                else:
                    self.inElement = indices[1]
                if 'checksum' == name:
                    if 'md5' != attrs['type'].lower():
                        raise ValueError('unexpected checksum type: %s' % (attrs['type']))
            elif name in element2index:
                self.inElement = element2index[name]
            else:
                self.inElement = -1
#2012-10-05T15:37:44Z
    def createDateTime(self, datestr):
        days, hours = datestr[:-1].split('T')
        dayfields = days.split('-')
        hourfields = hours.split(':')
        return datetime(int(dayfields[0]), int(dayfields[1]), int(dayfields[2]), 
                        int(hourfields[0]), int(hourfields[1]), int(hourfields[2]))

    def endElement(self, name):
        if 'Result' == name:
            self.count += 1
            if 0 == self.count % 256:
                self.log('processed %s file references' % (self.count))
            self.totalFileSize += int(self.info[CGHubRecordInfo.bamFilesize_index])
            record = CGHubRecordInfo(self.info)
            filename = record.files['bam'].filename
            if self.removedups and filename in self.filename2cghubRecords:
                self.dupcount += 1
                # check the dates and keep the latest
                currentdate = self.createDateTime(self.filename2cghubRecords[filename].upload_date)
                newdate = self.createDateTime(record.upload_date)
                if currentdate < newdate:
                    self.filename2cghubRecords[filename] = record
            else:
                self.filename2cghubRecords[filename] = record
            self.inResult = False
            self.info = ['' for _ in range(CGHubRecordInfo.infoCount)]
            self.fileIndices = 0
            if 'live' == record.state:
                if self.minmaxsize['min'].filesize > record.files['bam'].filesize and record.files['bam'].filesize:
                    self.minmaxsize['min'] = record.files['bam']
                if self.minmaxsize['max'].filesize < record.files['bam'].filesize:
                    self.minmaxsize['max'] = record.files['bam']
                if not record.files['bam'].filesize:
                    self.log('no file size: %s--%s' % (record.write(), record.files['bam'].write()))
            if self.limit and self.count >= self.limit_count:
                self.endDocument()
                raise LimitReachedException()
                
        elif name in element2index and -1 == element2index[name] and self.hasChars:
#             self.log('WARNING: found value for %s:%s = %s' % (name, self.count, self.unexpectedChars))
            pass
        self.inElement = -1
        self.hasChars = False

    def characters(self, content):
        if -1 < self.inElement:
            self.info[self.inElement] += content
        if content.strip():
            self.hasChars = True
            self.unexpectedChars = content

    def statistics(self):
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
        for record in self.filename2cghubRecords.itervalues():
            states[record.state] = states.setdefault(record.state, 0) + 1
            centers[record.center_name] = centers.setdefault(record.center_name, 0) + 1
            studies[record.study] = studies.setdefault(record.study, 0) + 1
            diseases[record.disease_abbr] = diseases.setdefault(record.disease_abbr, 0) + 1
            analyte_codes[record.analyte_code] = analyte_codes.setdefault(record.analyte_code, 0) + 1
            sample_types[record.sample_type] = sample_types.setdefault(record.sample_type, 0) + 1
            strategies[record.library_strategy] = strategies.setdefault(record.library_strategy, 0) + 1
            platforms[record.platform] = platforms.setdefault(record.platform, 0) + 1
            refassems[record.refassem_short_name] = refassems.setdefault(record.refassem_short_name, 0) + 1
            models[record.platform_full_name] = refassems.setdefault(record.platform_full_name, 0) + 1
            
            try:
                fields = record.legacy_sample_id.split('-')
                for index, field in enumerate(fields[:-3]):
                    sampleIDs[index][field] = sampleIDs[index].setdefault(field, 0) + 1
                for index, field in enumerate(fields[-3:]):
                    sampleIDs[index + len(fields[:-3])][field] = sampleIDs[index + len(fields[:-3])].setdefault(field, 0) + 1
            except Exception as e:
                self.log('problem splitting %s(%s:%s): %s' % (record.legacy_sample_id, index, field, e))
                
        self.log('States')
        count = 0
        for state, value in states.iteritems():
            if count < 15:
                count += 1
                self.log('%s: %s' % (state, value))
            else:
                self.log('\t(of %s)' % (len(states.keys())))
                break
        self.log('')
        
        self.log('Centers')
        count = 0
        for center, value in centers.iteritems():
            if count < 15:
                count += 1
                self.log('%s: %s' % (center, value))
            else:
                self.log('	(of %s)' % (len(centers.keys())))
                break
        self.log('')
        
        self.log('Studies')
        count = 0
        for studie, value in studies.iteritems():
            if count < 15:
                count += 1
                self.log('%s: %s' % (studie, value))
            else:
                self.log('(of %s)' % (len(studies.keys())))
                break
        self.log('')
        
        if self.verbose:
            self.log('Sample ids:')
            count = 0
            for sampleMap in sampleIDs:
                self.log('next part:')
                for sampleID, value in sampleMap.iteritems():
                    if count < 15:
                        count += 1
                        self.log('%s: %s' % (sampleID, value))
                    else:
                        self.log('(of %s)' % (len(sampleMap.keys())))
                        break
                self.log('')
                count = 0
        
        self.log('Diseases:')
        count = 0
        for disease, value in diseases.iteritems():
            count += 1
            self.log('%s: %s' % (disease, value))
        self.log('')
        
        self.log('Analyte codes:')
        count = 0
        for analyte_code, value in analyte_codes.iteritems():
            if count < 15:
                count += 1
                self.log('%s: %s' % (analyte_code, value))
            else:
                self.log('(of %s)' % (len(analyte_codes.keys())))
                break
        self.log('')
        
        self.log('Sample types')
        count = 0
        for sample_type, value in sample_types.iteritems():
            if count < 15:
                count += 1
                self.log('%s: %s' % (sample_type, value))
            else:
                self.log('(of %s)' % (len(sample_types.keys())))
                break
        self.log('')
        
        self.log('Strategies:')
        count = 0
        for strategie, value in strategies.iteritems():
            if count < 15:
                count += 1
                self.log('%s: %s' % (strategie, value))
            else:
                self.log('(of %s)' % (len(strategies.keys())))
                break
        self.log('')
        
        self.log('Platforms:')
        count = 0
        for platform, value in platforms.iteritems():
            if count < 15:
                count += 1
                self.log('%s: %s' % (platform, value))
            else:
                self.log('(of %s)' % (len(platforms.keys())))
                break
        self.log('')
        
        self.log('Reference Assembles:')
        count = 0
        for refassem, value in refassems.iteritems():
            if count < 15:
                count += 1
                self.log('%s: %s' % (refassem, value))
            else:
                self.log('(of %s)' % (len(refassems.keys())))
                break
        self.log('')
        
        self.log('Models:')
        count = 0
        for model, value in models.iteritems():
            if count < 15:
                count += 1
                self.log('%s: %s' % (model, value))
            else:
                self.log('(of %s)' % (len(models.keys())))
                break
        
        if 0 < self.dupcount:
            self.log('processed %s duplicate records' % (self.dupcount))
        self.log('')
        self.log('max: %s\nmin: %s' % (self.minmaxsize['max'].write(), self.minmaxsize['min'].write()))

class Error(ErrorHandler):
    def error(self, exception):
        traceback.print_exc()
        self.log('error occurred: %s' % (exception.getMessage()))
    
    def fatalError(self, exception):
        traceback.print_exc()
        self.log('error occurred: %s' % (exception.getMessage()))
     
    def warning(self, exception):
        traceback.print_exc()
        self.log('warning occurred: %s' % (exception.getMessage()))
        
def main(platform, type_uri = 'detail', log = None, removedups = False, limit = -1, verbose = False, \
         print_response = False, outputFileName = None, outputTumorTypeCode = 'BRCA', outputSequenceType = 'R'):
    try:
        if log:
            log.info('begin processing archive')
        else:
            print datetime.now(), 'begin processing archive'
        if 'detail' == type_uri:
            uri = detail_uri
        elif 'full' == type_uri:
            uri = full_uri
        else:
            raise ValueError("didn't recognize %s for type_uri" % (type_uri))
        url = uri + '?study=%s' % (platform)
        if verbose:
            httplib.HTTPConnection.debuglevel = 1
        if print_response:
            print datetime.now(), 'print response'
            response = urllib.urlopen(url)
            response.read()
            print datetime.now(), 'print done'
        handler = Content(log, limit, removedups, verbose)
        error_handler = Error()
        sax.parse(url, handler, error_handler)
        if log:
            log.info('finished processing archive')
        else:
            print datetime.now(), 'finished processing archive'


        if (outputFileName != None):
            if log:
                log.info("outputting file to %s" % outputFileName)
            else:
                print datetime.now(), "outputting file to %s" % outputFileName

            outputFile = open(outputFileName,'w')

            outputCount = 0
            totalsize = 0

            if log:
                log.info("filtering for tumor type %s" % outputTumorTypeCode)
                log.info("filtering for sequence type %s" % outputSequenceType)
            else:
                print datetime.now(), "filtering for tumor type %s" % outputTumorTypeCode
                print datetime.now(), "filtering for sequence type %s" % outputSequenceType

            for record in handler.filename2cghubRecords.itervalues():
                if (record.refassem_short_name == 'HG19_Broad_variant') and (record.state == 'live') \
                  and (record.disease_abbr == outputTumorTypeCode) \
                  and (record.analyte_code == outputSequenceType):
                    bamfile = record.files['bam']
                    # found record info:
                    outputFile.write("%s\t%s,%s,%s\n" % (bamfile.analysis_id, bamfile.filename, bamfile.filesize, bamfile.md5))
                    #print "  analyte code: %s, library strategy:%s" % (record.analyte_code, record.library_strategy)
                    outputCount += 1;
                    totalsize += bamfile.filesize

            if log:
                log.info("found %s BAM entries matching filters" % outputCount)
                log.info("total selected BAM file size: %s" % totalsize)
            else:
                print datetime.now(), "found %s BAM entries matching filters" % outputCount
                print datetime.now(), "total selected BAM file size: %s" % totalsize

            outputFile.close()


    except LimitReachedException:
        if log:
            log.info('finished processing archive with limit of %s' % limit)
        else:
            print datetime.now(), 'finished processing archive with limit of %s' % limit
    return handler.filename2cghubRecords.values(), handler.minmaxsize
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='import CGHub information from the REST API')
    parser.add_argument('--outputFileName', help='a filename for the output file')
    parser.add_argument('--outputTumorTypeCode', help='a filter for the output file generation, i.e. BRCA')
    parser.add_argument('--outputSequenceType', help='a filter for the output file generation, i.e. D or R')
    args = parser.parse_args()
    main(platform, removedups = True, outputFileName = args.outputFileName, \
         outputTumorTypeCode = args.outputTumorTypeCode, outputSequenceType = args.outputSequenceType)
