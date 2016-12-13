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
from multiprocessing import Semaphore
import os
import requests
import tarfile
import time

from util import create_log, delete_dir_contents, delete_objects, flatten_map, import_module, upload_file

semaphore = Semaphore(4)

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
                log.warn('\t\trequest try %d after error %s for %s' % (retries, e, params))
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


def process_files(config, endpt_type, file2info, outputdir, start, end, project, data_type, etl_class, log):
    try:
        filepath = outputdir + config['download_output_file_template'] % (start, end - 1)
        with tarfile.open(filepath) as tf:
            log.info('\t\tacquire lock to extract tar files from %s' % (filepath))
            with semaphore:
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
            for path in paths:
                basefolder = config['buckets']['folders']['base_file_folder']
                
                metadata = flatten_map(file2info[path], config['process_files']['data_table_mapping'])
                keypath_template = config['process_files']['bucket_path_template']
                key_path_components = []
                for part in config['process_files']['bucket_path']:
                    fields = part.split(':')
                    if 1 == len(fields):
                        if 'endpoint_type' == part:
                            key_path_components += [endpt_type]
                        else:
                            key_path_components += [metadata[0][part]]
                    elif 'alt' == fields[0]:
                        if fields[1] in metadata[0] and metadata[0][fields[1]]:
                            key_path_components += [metadata[0][fields[1]]]
                        else:
                            key_path_components += [metadata[0][fields[2]]]
                
                key_name = basefolder + (keypath_template % tuple(key_path_components))
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

def request(config, endpt_type, url, file2info, outputdir, project, data_type, log):
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
        process_files(config, endpt_type, ordered2info, outputdir, start, end, project, data_type, etl_class, log)
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
        request(config, endpt_type, url, file2info, outputdir, project, data_type, log)
        log.info('finished upload of gdc files in %s minutes' % ((time.clock() - start) / 60))
    except:
        # clean-up
        log.exception('failed to upload files for project %s and datatype %s' % (project, data_type))
        log.warning('cleaning up GCS for failed project %s and datatype %s' % (project, data_type))
        delete_objects(config, config['buckets']['open'], config['buckets']['folders']['base_file_folder'], log)
        log.warning('finished cleaning up GCS for failed project %s and datatype %s' % (project, data_type))
        raise

def setup_file_ids(config, data_type):
    file_ids = {}
    with open(config['input_id_file']) as file_id_file:
        for line in file_id_file:
            info = {
                'data_type': 'Methylation Beta Value', 
                'experimental_strategy': 'Methylation Array',
                'analysis': {
                },
                'cases': [
                    {
                        'submitter_id': 'TCGA-00-0000', 
                        'case_id': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
                        'project': {
                            'project_id': 'TCGA-UCS',
                            'program': {
                                    'name': 'TCGA'
                            }
                        },
                        'samples': [
                            {
                                'sample_id': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx', 
                                'submitter_id': 'TCGA-00-0000-01A',
                                'portions': [
                                    {
                                        'analytes': [{
                                            'aliquots': [{
                                                'submitter_id': 'TCGA-00-0000-01A-01D-A385-10', 
                                                'aliquot_id': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' 
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
            if 'Methylation27' in fields[2]:
                info['platform'] = 'Illumina Human Methylation 27'
                info['cases'][0]['samples'][0]['portions'][0]['analytes'][0]['aliquots'][0]['submitter_id'] = fields[2].split('.')[5]
            elif 'Methylation450' in fields[2]:
                info['platform'] = 'Illumina Human Methylation 450'
                info['cases'][0]['samples'][0]['portions'][0]['analytes'][0]['aliquots'][0]['submitter_id'] = fields[2].split('.')[5]
            
            if data_type == 'Gene Expression Quantification':
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
        'input_id_file': 'gdc/doc/gdc_manifest_mirnaeq.2016-12-09_test_100.tsv',
        'download_files_per': 0,
        'gcs_wrapper': 'gcs_wrapper_gcloud',
        'cloud_projects': {
            'open': 'isb-cgc'
        },
        "buckets": {
            "open": "isb-cgc-scratch",
            "controlled": "62f2c827-test-a",
            "folders": {
                "base_file_folder": "gdc/NCI-GDC_local/",
                "base_run_folder": "gdc/NCI-GDC_local_run/"
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
        "process_files": {
            "fetch_count": 10,
            "bucket_path_template": "%s/%s/%s/%s/%s/%s/%s",
            "bucket_path": [
                "endpoint_type",
                "program_name",
                "project_short_name",
                "alt:experimental_strategy:data_category",
                "data_type",
                "file_gdc_id",
                "file_name"
            ],
            "data_table_mapping": {
                "value": {
                    "acl": "list2str,acl",
                    "access": "access",
                    "file_id": "file_gdc_id",
                    "submitter_id": "file_submitter_id",
                    "data_type": "data_type", 
                    "file_name": "file_name", 
                    "md5sum": "md5sum", 
                    "data_format": "data_format", 
                    "platform": "platform",
                    "state": "state", 
                    "data_category": "data_category", 
                    "file_size": "file_size", 
                    "type": "type", 
                    "file_state": "file_state", 
                    "experimental_strategy": "experimental_strategy",
                    "created_datetime": "substr,0,10,created_datetime",
                    "updated_datetime": "substr,0,10,updated_datetime"
                },
                "map": {
                    "analysis": {
                        "analysis": "analysis",
                        "value": {
                            "analysis_id": "analysis_analysis_id",
                            "workflow_link": "analysis_workflow_link",
                            "workflow_type": "analysis_workflow_type"
                        },
                    }
                },
                "map_list": {
                    "cases": {
                        "cases": "cases",
                        "value": {
                            "submitter_id": "case_barcode", 
                            "case_id": "case_gdc_id"
                        },
                        "map": {
                            "project": {
                                "project": "project",
                                "value": {
                                    "project_id": "project_short_name"
                                },
                                "map": {
                                    "program": {
                                        "program": "program",
                                        "value": {
                                            "name": "program_name"
                                        }
                                    }
                                }
                            }
                        },
                        "map_list": {
                            "samples": {
                                "samples": "samples",
                                "value": {
                                    "sample_id": "sample_gdc_id", 
                                    "submitter_id": "sample_barcode"
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
                                                            "submitter_id": "aliquot_barcode", 
                                                            "aliquot_id": "aliquot_gdc_id" 
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
                }
            },
            "datatype2bqscript": {
                "Gene Expression Quantification": {
                    "python_module":"gdc.etl.gene_expression_quantification",
                    "class":"Gene_expression_quantification",
                    "file_compressed": True,
                    "bq_dataset": "test",
                    "bq_table": "TCGA_GeneExpressionQuantification_local_test",
                    "schema_file": "gdc/schemas/geq.json",
                    "write_disposition": "WRITE_APPEND",
                    "analysis_types": [
                        "HTSeq - FPKM-UQ",
                        "HTSeq - FPKM",
                        "HTSeq - Counts"
                    ]
                },
                "Methylation Beta Value": {
                    "python_module":"gdc.etl.methylation",
                    "class":"Methylation",
                    "file_compressed": False,
                    "bq_dataset": "test",
                    "bq_table": "TCGA_Methylation_local_test",
                    "schema_file": "gdc/schemas/meth.json",
                    "write_disposition": "WRITE_APPEND",
                    "use_columns": {
                        "Composite Element REF": "probe_id",
                        "Beta_value": "beta_value"
                    },
                    "add_metadata_columns": [
                        "sample_barcode",
                        "project_short_name",
                        "program_name",
                        "sample_type_code",
                        "file_name",
                        "file_gdc_id",
                        "aliquot_barcode",
                        "case_barcode",
                        "case_gdc_id",
                        "sample_gdc_id",
                        "aliquot_gdc_id"
                    ],
                    "order_columns": [
                        "sample_barcode",
                        "probe_id",
                        "beta_value",
                        "project_short_name",
                        "program_name",
                        "sample_type_code",
                        "file_name",
                        "file_gdc_id",
                        "aliquot_barcode",
                        "case_barcode",
                        "case_gdc_id",
                        "sample_gdc_id",
                        "aliquot_gdc_id"
                    ]
                },
                "miRNA Expression Quantification": {
                    "python_module":"gdc.etl.mirna_expression_quantification",
                    "class":"Mirna_expression_quantification",
                    "file_compressed": False,
                    "bq_dataset": "test",
                    "bq_table": "TCGA_miRNAExpressionQuantification_local_test",
                    "schema_file": "gdc/schemas/mirnaeq.json",
                    "write_disposition": "WRITE_APPEND",
                    "use_columns": {
                        "miRNA_ID": "miRNA_ID",
                        "read_count": "read_count",
                        "reads_per_million_miRNA_mapped": "reads_per_million_miRNA_mapped",
                        "cross-mapped": "cross-mapped"
                    },
                    "add_metadata_columns": [
                        "sample_barcode",
                        "project_short_name",
                        "program_name",
                        "sample_type_code",
                        "file_name",
                        "file_gdc_id",
                        "aliquot_barcode",
                        "case_barcode",
                        "case_gdc_id",
                        "sample_gdc_id",
                        "aliquot_gdc_id"
                    ],
                    "order_columns": [
                        "sample_barcode",
                        "miRNA_ID",
                        "read_count",
                        "reads_per_million_miRNA_mapped",
                        "cross-mapped",
                        "project_short_name",
                        "program_name",
                        "sample_type_code",
                        "file_name",
                        "file_gdc_id",
                        "aliquot_barcode",
                        "case_barcode",
                        "case_gdc_id",
                        "sample_gdc_id",
                        "aliquot_gdc_id"
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
        project = 'TCGA-UCS'
        data_type = 'miRNA Expression Quantification'
        file_ids = setup_file_ids(config, data_type)
        for download_files_per in [20]:
            config['download_files_per'] = download_files_per
            try:
                upload_files(config, 'current', file_ids, project, data_type, log)
            except:
                log.exception('failed with lines per @ %d' % (download_files_per))
    finally:
        module.close_connection()
