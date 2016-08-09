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
import pycurl
from pycurl import Curl
import requests
import sys

from util import create_log

def header_function(info):
    print info


def request(url, params, log):
    log.info('\tstarting requests fetch of gdc files')
    headers = {
        'Content-Type':'application/json', 
        'Accept':'*/*', 
        'User-Agent':'python-requests/1.2.0', 
        'Accept-Encoding':'gzip'}
    retries = 0
    while True:
        try:
            response = requests.post(url, params=params, headers=headers, stream=True)
            break
        except Exception as e:
            if retries < 2:
                retries += 1
                log.info('\t\trequest try %d on error %s' % (retries, e))
                continue
            raise
    
    log.info('\tfinished fetch of gdc files')
    response.raise_for_status()
    lines = response.text.split('\n')
    local_filename = "gdc_download.tar.gz"
    log.info('\tstarting write of gdc files')
    with open(local_filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=2048):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
    
    log.info('\tfinished write of gdc files')
    # expected the compressed file but getting text
    mirna2count = {}
    for line in lines:
        fields = line.split('\t')
        count = mirna2count.setdefault(fields[0], 0)
        mirna2count[fields[0]] = count + 1
    
    log.info('\n'.join('%s: %d' % (key, value) for (key, value) in mirna2count.iteritems()))


def curl(url, params, log):
    log.info('\tstarting curl fetch of gdc files')
    with open('gdc_curl_download.tar.gz', 'wb') as f:
        c = Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, f)
        c.setopt(c.HTTPHEADER, ["Content-Type: application/json"])
        c.setopt(c.HEADERFUNCTION, header_function)
        c.setopt(pycurl.CUSTOMREQUEST, "POST")
        c.setopt(pycurl.POSTFIELDS, json.dumps(params))
        # TODO: set up using a local certificate
        c.setopt(pycurl.SSL_VERIFYPEER, 0)
        c.setopt(pycurl.SSL_VERIFYHOST, 0)
        c.perform()
        c.close()


def create_json():
    file_ids = []
    params = {'ids':file_ids}
    with open('gdc/main/2016_07_28_run/TCGA/TCGA-COAD/TCGA-COAD_miRNAExpressionQuantification_file_ids.txt') as file_id_file:
        for line in file_id_file:
            file_ids += [line.strip()]
    
    return params


def get_status(log):
    log.info('\tgetting gdc status')
    status_endpt = 'https://gdc-api.nci.nih.gov/status'
    response = requests.get(status_endpt)
    log.info(json.dumps(response.json(), indent=2))

def main(config, log):
    try:
        log.info('starting upload of gdc files')
        get_status(log)
        params = create_json()
        url = 'https://gdc-api.nci.nih.gov/data'
        curl(url, params, log)
        request(url, params, log)
        log.info('finished upload of gdc files')
    except:
        raise

if __name__ == '__main__':
#     with open(sys.argv[1]) as configFile:
#         config = json.load(configFile)
    config = None
    log_dir = str(date.today()).replace('-', '_') + '_run' + '/'
    log_name = create_log(log_dir, 'gdc_upload')
    log = logging.getLogger(log_name)
    main(config, log)

