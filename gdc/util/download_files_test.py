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
from datetime import date
import json
import logging
import os
import pycurl
from pycurl import Curl
import requests
import time

from util import create_log, delete_dir_contents

def write_response(config, response, start, end, log):
    log.info('\t\tstarting write of gdc files')
    accum = 0
    count = 0
    file_name = config['output_dir'] + config['output_file'] % (start, end - 1)
    with open(file_name, 'wb') as f:
        chunk_size=2048
        for chunk in response.iter_content(chunk_size):
            if chunk: # filter out keep-alive new chunks
                if 0 == count % 8000:
                    log.info('\t\t\twritten %skb' % (accum / 1024))
                count += 1
                accum += chunk_size
                f.write(chunk)
    log.info('\t\tfinished write of gdc files to %s.  wrote <= %skb' % (file_name, accum / 1024))

def request_try(config, url, file_ids, start, end, log):
    headers = {
        'Content-Type':'application/json'
    }
    params = {'ids':file_ids[start:end]}
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
                log.warn('\t\trequest try %d on error %s for fetch of %s' % (retries, e, end - start))
                continue
#             if 100 < start - end:
#                 log.error('range too small to continue--%s:%s' % (end, start))
            # divide the interval into 2 segments
            else:
                raise RuntimeError('request failed too many times')
    
    if response:
        write_response(config, response, start, end, log)
    return 

def request(config, url, file_ids, log):
    log.info('\tstarting requests fetch of gdc files')
    lines_per = min(config['lines_per'], len(file_ids))
    start = 0
    end = lines_per
    while start < len(file_ids):
        log.info('\t\tfetching range %d:%d' % (start, end))
        request_try(config, url, file_ids, start, end, log)
        start = end
        end += lines_per
        
#     delete_dir_contents(config['output_dir'])
    log.info('\tfinished fetch of gdc files')

def curl(url, file_ids, log):
    log.info('\tstarting curl fetch of gdc files')
    params = {'ids':file_ids}
    c = None
    with open('gdc_curl_download.tar.gz', 'wb') as f:
        try:
            c = Curl()
            c.setopt(c.URL, url)
            c.setopt(c.WRITEDATA, f)
            c.setopt(c.HTTPHEADER, ["Content-Type: application/json"])
            c.setopt(pycurl.CUSTOMREQUEST, "POST")
            c.setopt(pycurl.POSTFIELDS, json.dumps(params))
            # TODO: set up using a local certificate
            c.setopt(pycurl.SSL_VERIFYPEER, 0)
            c.setopt(pycurl.SSL_VERIFYHOST, 0)
            c.perform()
        except:
            log.exception('problem with curl')
            raise
        finally:
            if None != c:
                if int(c.getinfo(pycurl.RESPONSE_CODE)) != 200:
                    f.close()
                    with open('gdc_curl_download.tar.gz') as e:
                        err = e.read()
                    log.error('\tbad status on curl call(%s):\n%s' % (c.getinfo(pycurl.RESPONSE_CODE), err))
                c.close()
            

def get_file_ids(config):
    return [
        'ed935a40-3e5e-49ca-ac8c-93e9faf0e79b',
        '46cb1dd1-f498-48fb-8f33-7161a0cdbf87',
        'c1971c16-ef58-4417-9d47-a60b51a75527',
        '769c02ba-a3c8-40c7-a9c6-3c2c9bfe24b8',
        '8f625078-9301-4b5e-b234-b1c8870b36b4',
        '05ddb6d8-771a-44c9-8005-d3d96fb1f274',
        '09757d3b-0fff-4413-8e07-f1250f139368',
        '550451f7-29fb-437d-bd89-4d0a43273d57',
        '06275b6b-258a-4a9f-b755-5c8bef2b8e75',
        'd3c016cf-8051-4bea-9f8a-2a076496f449',
        '2d7133e5-8c61-445c-840c-4fbaf33d554f'
    ]
    file_ids = []
    with open(config['input_id_file']) as file_id_file:
        for line in file_id_file:
            file_ids += [line.strip()]
    
    return file_ids

def get_status(log):
    log.info('\tgetting gdc status')
    status_endpt = 'https://gdc-api.nci.nih.gov/status'
    response = requests.get(status_endpt)
    log.info('\n' + json.dumps(response.json(), indent=2))

def main(config, log):
    try:
        get_status(log)
        file_ids = get_file_ids(config)
        if not os.path.isdir(config['output_dir']):
            os.makedirs(config['output_dir'])
        
        url = 'https://gdc-api.nci.nih.gov/data'
#         curl(url, file_ids, log)
        log.info('starting upload of gdc files')
        begin = time.clock()
        request(config, url, file_ids, log)
        end = time.clock()
        log.info('finished upload of gdc files in %s minutes for %s lines per' % (int(float(end - begin) / float(60)), config['lines_per']))
    except:
        raise

if __name__ == '__main__':
    config = {
        'output_dir': './gdc_download/',
        'output_file': 'gdc_download_eq_%s_%s.tar.gz',
        'input_id_file': 'gdc/doc/2016_10_05_expression_quantification_file_ids_5000.txt',
#         'input_id_file': 'Z:\\tcga\\cgc\\dataproc\\gdc\\doc\\gdc_manifest.2016-09-09_head_1000.tsv',
        'lines_per': 0
    }
    log_dir = str(date.today()).replace('-', '_') + '_ge_gdc_upload_run/'
    log_name = create_log(log_dir, 'gdc_upload')
    log = logging.getLogger(log_name)
#     for lines_per in [500, 100, 50]:
    for lines_per in [50]:
        config['lines_per'] = lines_per
        try:
            main(config, log)
        except:
            log.exception('failed with lines per @ %d' % (lines_per))

