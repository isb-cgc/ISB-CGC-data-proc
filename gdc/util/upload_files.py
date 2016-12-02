'''
Created on Jul 27, 2016

Copyright 2016, Institute for Systems Biology.

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
from collections import OrderedDict
from datetime import date
import json
import logging
import os
import requests
import tarfile
import time
import zipfile

from util import create_log, delete_dir_contents, delete_objects, import_module, upload_file

def write_response(config, response, start, end, outputdir, log):
    try:
        log.info('\t\tstarting write of gdc files')
        accum = 0
        count = 0
        file_name = outputdir + config['download_output_file_template'] % (start, end - 1)
        with open(file_name, 'wb') as f:
            chunk_size=2048
            for chunk in response.iter_content(chunk_size):
                if chunk: # filter out keep-alive new chunks
                    if 0 == count % 8000:
                        log.info('\t\t\twritten %skb' % (accum / 1024))
                    count += 1
                    accum += len(chunk)
                    f.write(chunk)
        log.info('\t\tfinished write of gdc files to %s.  wrote %skb' % (file_name, accum / 1024))
    except:
        log.exception('problem saving file to %s' % (file_name))
        raise

def request_try(config, url, file_ids, start, end, outputdir, log):
    headers = {
        'Content-Type':'application/json'
    }
    params = {'ids':[key.split('/')[0] for key in file_ids[start:end]]}
    retries = 0
    while True:
        try:
            response = requests.post(url, data=json.dumps(params), headers=headers, stream=True)
            response.raise_for_status()
            break
        except Exception as e:
            response = None
            if retries < 3:
                retries += 1
                time.sleep(1 * retries)
                log.warn('\t\trequest try %d after error %s' % (retries, e))
                continue
#             if 100 < start - end:
#                 log.error('range too small to continue--%s:%s' % (end, start))
            # divide the interval into 2 segments
            else:
                log.exception('request failed too many times')
                raise
    
    if response:
        write_response(config, response, start, end, outputdir, log)
    return 


def process_files(config, file2info, outputdir, start, end, project, data_type, etl_class, log):
    try:
        filepath = outputdir + config['download_output_file_template'] % (start, end - 1)
        with tarfile.open(filepath) as tf:
            log.info('\t\textract tar files from %s' % (filepath))
            tf.extractall(outputdir)
            log.info('\t\tdone extract tar files from %s' % (filepath))
     
        with open(outputdir + 'MANIFEST.txt') as manifest:
            lines = manifest.read().split('\n')
            paths = []
            filenames = set()
            for line in lines[1:]:
                filepath = line.split('\t')[1]
                paths += [filepath]
                filenames.add(filepath.split('/')[1])
        paths.sort(key = lambda path:path.split('/')[1])
         
        if config['upload_files']:
#             use_dir_in_name = False if len(paths) == len(filenames) else True
            use_dir_in_name = True
            for path in paths:
                basefolder = config['buckets']['folders']['base_file_folder']
                key_name = basefolder + '%s/%s/' % (project, data_type) + (path.replace('/', '_') if use_dir_in_name else path.split('/')[1])
                log.info('\t\tuploading %s' % (key_name))
                upload_file(config, outputdir + path, config['buckets']['open'], key_name, log)
            
        if config['upload_etl_files'] and data_type in config['process_files']['datatype2bqscript'] and etl_class is not None:
            etl_class.upload_batch_etl(config, outputdir, paths, file2info, project, data_type, log)
        else:
            log.warning('\t\tnot processing files for ETL for project %s and datatype %s%s' % (project, data_type, ' because there is no script specified' if config['upload_etl_files'] else ''))
    except:
        log.exception('problem process file %s for project %s and data_type %s' % (filepath, project, data_type))
        raise
    finally:
        if 'delete_dir_contents' not in config or config['delete_dir_contents']:
            delete_dir_contents(outputdir)

def request(config, url, file2info, outputdir, project, data_type, log):
    log.info('\tstarting requests fetch of gdc files')
    log.info('first set of sorted files:\n\t' + '\n\t'.join(sorted([(info['file_id'] + '/' + info['file_name']) for info in file2info.values()], key = lambda t:t.split('/')[1])[:20]))
    ordered2info = OrderedDict(sorted([(info['file_id'] + '/' + info['file_name'], info) for info in file2info.values()], key = lambda t:t[1]['file_name']))
    download_files_per = min(config['download_files_per'], len(file2info))
    start = 0
    end = download_files_per
    etl_class = None
    if data_type in config['process_files']['datatype2bqscript']:
        etl_module_name = config['process_files']['datatype2bqscript'][data_type]['python_module']
        module = import_module(etl_module_name)
        etl_class_name = config['process_files']['datatype2bqscript'][data_type]['class']
        Etl_class = getattr(module, etl_class_name)
        etl_class = Etl_class()
    while start < len(file2info):
        log.info('\t\tfetching range %d:%d' % (start, end))
        request_try(config, url, ordered2info.keys(), start, end, outputdir, log)
        process_files(config, ordered2info, outputdir, start, end, project, data_type, etl_class, log)
        start = end
        end += download_files_per
        
    if config['upload_etl_files'] and data_type in config['process_files']['datatype2bqscript'] and etl_class is not None:
        etl_class.finish_etl(config, project, data_type, log)
    else:
        log.warning('\t\tnot finishing for ETL for project %s and datatype %s%s' % (project, data_type, ' because there is no script specified' if config['upload_etl_files'] else ''))

    log.info('\tfinished fetch of gdc files')

def upload_files(config, endpt_type, file2info, project, data_type, log):
    try:
        log.info('starting upload of gdc files')
        outputdir = config['download_base_output_dir'] + '%s/%s/' % (project, data_type)
        if not os.path.isdir(outputdir):
            os.makedirs(outputdir)
        
        url = config['data_endpt']['%s endpt' % (endpt_type)]
        start = time.clock()
        request(config, url, file2info, outputdir, project, data_type, log)
        log.info('finished upload of gdc files in %s minutes' % ((time.clock() - start) / 60))
    except:
        # clean-up
        log.exception('failed to upload files for project %s and datatype %s' % (project, data_type))
        log.warning('cleaning up GCS for failed project %s and datatype %s' % (project, data_type))
        delete_objects(config, config['buckets']['open'], config['buckets']['folders']['base_file_folder'], log)
        log.warning('finished cleaning up GCS for failed project %s and datatype %s' % (project, data_type))
        raise

def setup_file_ids(config):
    file_ids = {}
    with open(config['input_id_file']) as file_id_file:
        for line in file_id_file:
            info = {
                'data_type': '', 
                'experimental_strategy': '',
                'analysis': {
                },
                'cases': [
                    {
                        'submitter_id': '', 
                        'case_id': '',
                        'project': {
                            'project_id': '',
                            'program': {
                                    'name': ''
                            }
                        },
                        'samples': [
                            {
                                'sample_id': '', 
                                'submitter_id': 'TCGA-00-0000-01A',
                                'portions': [
                                    {
                                        'analytes': [{
                                            'aliquots': [{
                                                'submitter_id': '', 
                                                'aliquot_id': '' 
                                            }]
                                        }]
                                    }
                                ]
                             }
                        ]
                    }
                ]
            }
            fields = line.strip().split('\t')
            info['file_id'] = fields[1]
            info['file_name'] = fields[2]
            file_fields = fields[2].split('.')
            if 'htseq' == file_fields[1]:
                info['analysis']['workflow_type'] = 'HTSeq - Counts'
            else:
                info['analysis']['workflow_type'] = 'HTSeq - ' + file_fields[1]
            
            file_ids[fields[1]] = info
    
    return file_ids

if __name__ == '__main__':
    config = {
        "upload_files": True,
        'upload_etl_files': True,
        'download_base_output_dir': '/tmp/project/datatype/',
        'download_output_file_template': 'gdc_download_%s_%s.tar.gz',
        'input_id_file': 'gdc/doc/gdc_manifest_geq.2016-12-02_test_60.tsv',
        'download_files_per': 0,
        'gcs_wrapper': 'gcs_wrapper_gcloud',
        'cloud_projects': {
            'open': 'isb-cgc'
        },
        "buckets": {
            "open": "isb-cgc-scratch",
            "controlled": "62f2c827-test-a",
            "folders": {
                "base_file_folder": "gdc/test_local_gdc_upload/",
                "base_run_folder": "gdc/test_local_gdc_upload_run/"
            }
        },
        "sample_code_position" : {
            "TCGA": {
              "start": 13,
              "end": 15
            },
            "TARGET": {
              "start": 17,
              "end": 19
            }
        },
        "sample_code2letter": {
            "01": "TP",
            "02": "TR",
            "03": "TB",
            "04": "TRBM",
            "05": "TAP",
            "06": "TM",
            "07": "TAM",
            "08": "THOC",
            "09": "TBM",
            "10": "NB",
            "11": "NT",
            "12": "NBC",
            "13": "NEBV",
            "14": "NBM",
            "20": "CELLC",
            "40": "TRB",
            "50": "CELL",
            "60": "XP",
            "61": "XCL"
        },
        "data_endpt": {
            "current endpt": "https://gdc-api.nci.nih.gov/data?compress=true",
            "legacy endpt": "https://gdc-api.nci.nih.gov/legacy/data?compress=true"
        },
        'process_files': {
            'upload_run_folder': 'gdc/test_gdc_upload_run/',
            "data_table_mapping": {
                "value": {
                    "file_id": "FileID",
                    "data_type": "DataType", 
                    "file_name": "FileName", 
                    "experimental_strategy": "ExperimentalStrategy"
                },
                "map_list": {
                    "cases": {
                        "cases": "cases",
                        "value": {
                            "submitter_id": "CasesSubmitterID", 
                            "case_id": "CaseID"
                        },
                        "map": {
                            "project": {
                                "project": "project",
                                "value": {
                                    "project_id": "ProjectID",
                                },
                                "map": {
                                    "program": {
                                        "program": "program",
                                        "value": {
                                            "name": "ProgramName",
                                        }
                                    }
                                }
                            }
                        },
                        "map_list": {
                            "samples": {
                                "samples": "samples",
                                "value": {
                                    "sample_id": "SampleID", 
                                    "submitter_id": "SamplesSubmitterID"
                                },
                                "map_list": {
                                    "portions": {
                                        "portions": "portions",
                                        "map_list": {
                                            "analytes": {
                                                "analytes": "analytes",
                                                "map_list": {
                                                    "aliquots": {
                                                        "aliquots": "aliquots",
                                                        "value": {
                                                            "submitter_id": "AliquotsSubmitterID", 
                                                            "aliquot_id": "AliquotsAliquotID" 
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "datatype2bqscript": {
                "Gene Expression Quantification": {
                    "python_module":"gdc.etl.gene_expression_quantification",
                    "class":"gene_expression_quantification",
                    "bq_dataset": "GDC_data_open",
                    "bq_table": "TCGA_GeneExpressionQuantification_local_test",
                    "schema_file": "gdc/schemas/geq.json",
                    "write_disposition": "WRITE_APPEND",
                    "analysis_types": [
                        "HTSeq - FPKM-UQ",
                        "HTSeq - FPKM",
                        "HTSeq - Counts"
                    ]
                }
            }
        }
    }
    
    log_dir = str(date.today()).replace('-', '_') + '_gdc_upload_run/'
    log_name = create_log(log_dir, 'gdc_upload')
    log = logging.getLogger(log_name)
    module = import_module(config['gcs_wrapper'])
    module.open_connection(config, log)

    try:
        file_ids = setup_file_ids(config)
        project = 'TCGA-UCS'
        data_type = 'Gene Expression Quantification'
        for download_files_per in [6]:
            config['download_files_per'] = download_files_per
            try:
                upload_files(config, 'current', file_ids, project, data_type, log)
            except:
                log.exception('failed with lines per @ %d' % (download_files_per))
    finally:
        module.close_connection()
