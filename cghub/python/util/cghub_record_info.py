'''
# <Result id="1292">
#     <analysis_id>0ed34acf-c8cb-4b5f-85ff-d669a823cb73</analysis_id>
#     <state>suppressed</state>
#     <reason>These were generated for benchmarking purposes by a different TCGA project and at different site (they are not from the Broad). Plus they were aligned by a different aligner to a slighter different set of contigs than the CCLE set from the Broad</reason>
#     <last_modified>2014-11-04T18:55:35Z</last_modified>
#     <upload_date>2014-08-22T07:24:48Z</upload_date>
#     <published_date>2014-08-25T14:50:56Z</published_date>
#     <center_name>BCM</center_name>
#     <study>Homo sapiens Other_Sequencing_Multiisolate</study>
#     <aliquot_id>c0e1ad41-68f4-4874-82ff-5d8c4c7132cd</aliquot_id>
#     <files>
#         <file>
#             <filename>CCLE-HCC1143BL-RNA-10_Illumina.bam</filename>
#             <filesize>13216915509</filesize>
#             <checksum type="MD5">29b4d4c6df84d574ae06abe9dc163679</checksum>
#         </file>
#         <file>
#             <filename>CCLE-HCC1143BL-RNA-10_Illumina.bam.bai</filename>
#             <filesize>5872920</filesize>
#             <checksum type="MD5">93d39e5dfde4ef495922b4ebf5370063</checksum>
#         </file>
#     </files>
#     <sample_accession></sample_accession>
#     <legacy_sample_id>CCLE-HCC1143BL-RNA-10</legacy_sample_id>
#     <disease_abbr>BRCA</disease_abbr>
#     <tss_id></tss_id>
#     <participant_id></participant_id>
#     <sample_id></sample_id>
#     <analyte_code>R</analyte_code>
#     <sample_type>13</sample_type>
#     <library_strategy>RNA-Seq</library_strategy>
#     <platform>ILLUMINA</platform>
#     <refassem_short_name>GRCh37</refassem_short_name>
#     <analysis_submission_uri>https://cghub.ucsc.edu/cghub/metadata/analysisSubmission/0ed34acf-c8cb-4b5f-85ff-d669a823cb73</analysis_submission_uri>
#     <analysis_full_uri>https://cghub.ucsc.edu/cghub/metadata/analysisFull/0ed34acf-c8cb-4b5f-85ff-d669a823cb73</analysis_full_uri>
#     <analysis_data_uri>https://cghub.ucsc.edu/cghub/data/analysis/download/0ed34acf-c8cb-4b5f-85ff-d669a823cb73</analysis_data_uri>
# </Result>

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

class CGHubFileInfo():
    def __init__(self, filename, filesize, md5, analysis_id = None):
        self.filename = filename
        self.filesize = int(filesize) if filesize else 0
        self.md5 = md5
        self.analysis_id = analysis_id
        
    def write(self):
        return 'filename: %s size: %s md5: %s UUID: %s' % (self.filename, self.filesize, self.md5, self.analysis_id)

class CGHubRecordInfo():
    ''' captures the record information from the CGHub REST API '''
    analysis_id_index = 0
    state_index = 1
    reason_index = 2
    last_modified_index = 3
    upload_date_index = 4
    published_date_index = 5
    center_name_index = 6
    study_index = 7
    aliquot_id_index = 8
    bamFilename_index = 9
    bamFilesize_index = 10
    bamMD5_index = 11
    baiFilename_index = 12
    baiFilesize_index = 13
    baiMD5_index = 14
    legacy_sample_id_index = 15
    disease_abbr_index = 16
    analyte_code_index = 17
    sample_type_index = 18
    library_strategy_index = 19
    platform_index = 20
    refassem_short_name_index = 21
    analysis_submission_uri_index = 22
    analysis_full_uri_index = 23
    analysis_data_uri_index = 24
    tss_id_index = 25
    participant_id_index = 26
    sample_id_index = 27
    INSTRUMENT_MODEL_index = 28
    infoCount = 29

    def __init__(self, info):
        self.analysis_id = info[self.analysis_id_index]
        self.state = info[self.state_index]
        self.reason = info[self.reason_index]
        self.last_modified = info[self.last_modified_index]
        self.upload_date = info[self.upload_date_index]
        self.published_date = info[self.published_date_index]
        self.center_name = info[self.center_name_index]
        self.study = info[self.study_index]
        self.aliquot_id = info[self.aliquot_id_index]
        self.files = {
                      'bam': CGHubFileInfo(info[self.bamFilename_index], info[self.bamFilesize_index], info[self.bamMD5_index], info[self.analysis_id_index]), 
                      'bai': CGHubFileInfo(info[self.baiFilename_index], info[self.baiFilesize_index], info[self.baiMD5_index])
                    }
        self.legacy_sample_id = info[self.legacy_sample_id_index]
        self.disease_abbr = info[self.disease_abbr_index]
        self.analyte_code = info[self.analyte_code_index]
        self.sample_type = info[self.sample_type_index]
        self.library_strategy = info[self.library_strategy_index]
        self.platform = info[self.platform_index]
        self.refassem_short_name = info[self.refassem_short_name_index]
        self.analysis_submission_uri = info[self.analysis_submission_uri_index]
        self.analysis_full_uri = info[self.analysis_full_uri_index]
        self.analysis_data_uri = info[self.analysis_data_uri_index]
        self.tss_id = info[self.tss_id_index]
        self.participant_id = info[self.participant_id_index]
        self.sample_id = info[self.sample_id_index]
        self.platform_full_name = info[self.INSTRUMENT_MODEL_index]

    def flattened_record(self):
        return {
                'analysis_id': self.analysis_id,
                'state': self.state,
                'reason': self.reason,
                'last_modified': self.last_modified,
                'upload_date': self.upload_date,
                'published_date': self.published_date,
                'center_name': self.center_name,
                'study': self.study,
                'aliquot_id': self.aliquot_id,
                'filename': self.files['bam'].filename,
                'filesize': self.files['bam'].filesize, 
                'md5': self.files['bam'].md5, 
                'legacy_sample_id': self.legacy_sample_id,
                'disease_abbr': self.disease_abbr,
                'analyte_code': self.analyte_code,
                'sample_type': self.sample_type,
                'library_strategy': self.library_strategy,
                'platform': self.platform,
                'refassem_short_name': self.refassem_short_name,
                'analysis_submission_uri': self.analysis_submission_uri,
                'analysis_full_uri': self.analysis_full_uri,
                'analysis_data_uri': self.analysis_data_uri,
                'tss_id': self.tss_id,
                'participant_id': self.participant_id,
                'sample_id': self.sample_id,
                'platform_full_name': self.platform_full_name
            }
        
    def write(self):
        try:
            return 'filename: %s:%s\taliquot: %s\tstate: %s\ttype: %s' % (self.files['bam'].filename, self.legacy_sample_id, self.aliquot_id, self.state, self.sample_type)
        except Exception as e:
            return "problem printing %s: %s" % (self.aliquot_id, e)
